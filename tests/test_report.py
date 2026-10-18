import pandas as pd
import pytest

from pipeline import brain, report, storage
from tests.test_brain import verdict
from tests.test_runner import AFTERNOON, execute, settings  # noqa: F401  (settings is a fixture)


def result(*verdicts):
    return brain.LLMResult(ok=True, data={"verdicts": list(verdicts)}, usage={"output_tokens": 5})


@pytest.fixture
def two_runs(settings):  # noqa: F811
    execute(settings, result(verdict("AAPL", "bearish", "low"), verdict("MSFT", "neutral", "low")))
    hostile = verdict("AAPL", "watch", "med")
    hostile["key_reasons"][0] = "<script>alert(1)</script> earnings in 13d"
    execute(settings, result(hostile, verdict("MSFT", "neutral", "low")), now=AFTERNOON, slot="preopen")
    conn = storage.connect(settings.paths.db)
    yield settings, conn
    conn.close()


def test_report_sections(two_runs):
    settings, conn = two_runs
    data = report.load(conn, 2)
    assert [v["ticker"] for v in data["flagged"]] == ["AAPL"]
    assert [v["ticker"] for v in data["neutral"]] == ["MSFT"]
    [changed] = data["changed"]  # MSFT stayed neutral/low, so only AAPL counts as changed
    assert changed["ticker"] == "AAPL" and changed["previous"]["verdict"] == "bearish"
    assert changed["previous"]["px_chg_pct"] == pytest.approx(0.0)
    assert {r["ticker"]: (r["verdict"], r["verdict_is_new"]) for r in data["table"]} == {
        "AAPL": ("watch", True), "MSFT": ("neutral", True)
    }  # fmt: skip


def test_first_run_lists_first_reviews_separately(two_runs):
    _, conn = two_runs
    data = report.load(conn, 1)
    assert data["changed"] == [] and data["first_reviews"] == ["AAPL", "MSFT"]
    assert report.load(conn, 2)["first_reviews"] == []


def test_runs_write_report_files(two_runs):
    settings, _ = two_runs
    reports = settings.paths.reports
    assert (reports / "2026-10-16-morning.html").exists()
    html = (reports / "2026-10-16-preopen.html").read_text()
    assert html == (reports / "latest.html").read_text()
    assert "Flagged (1)" in html and "Changed since last review (1)" in html and "All tickers (2)" in html


def test_report_escapes_model_text(two_runs):
    settings, _ = two_runs
    html = (settings.paths.reports / "latest.html").read_text()
    assert "<script>alert(1)</script>" not in html
    assert "&lt;script&gt;alert(1)&lt;/script&gt;" in html


def test_summary_text(two_runs):
    _, conn = two_runs
    text = report.summary_text(report.load(conn, 2))
    assert text.startswith("Market research 2026-10-16 preopen (as of 2026-10-15): 2 reviewed, 1 flagged, 0 at extremes")
    assert "WATCH (med) AAPL" in text and "changed AAPL: bearish -> watch" in text


def test_manual_run_report_name(settings):  # noqa: F811
    execute(settings, result(verdict("AAPL")), tickers=["AAPL"], slot=None)
    assert (settings.paths.reports / "2026-10-16-manual-1.html").exists()


def test_formatting():
    assert (report.fmt_pct(1.234), report.fmt_pct(-0.5), report.fmt_pct(0), report.fmt_pct(None)) == (
        "+1.2%", "−0.5%", "0.0%", "–"
    )  # fmt: skip
    assert report.fmt_pct(1.9, signed=False) == "1.9%"
    assert report.fmt_num(1234.5) == "1,234.50"


class FakeTelegram:
    def __init__(self):
        self.messages, self.documents = [], []

    def send(self, text, **kw):
        self.messages.append(text)
        return [100 + len(self.messages)]

    def send_document(self, path, caption=None, **kw):
        self.documents.append((path.name, caption))
        return 200


def test_deliverers(settings):  # noqa: F811
    assert report.make_deliverer("none", settings) is None
    with pytest.raises(NotImplementedError):
        report.make_deliverer("email", settings).send({}, None)


def test_telegram_delivery_logs_alert(two_runs):
    from pipeline import runner

    settings, conn = two_runs
    path, data = report.build(conn, 2, settings.paths.reports, settings)
    fake = FakeTelegram()
    assert runner.deliver_report(conn, settings, data, path, report.TelegramDeliverer(settings, client=fake))
    [msg] = fake.messages
    assert msg.startswith("<b>Pre-open digest</b> · 2026-10-16") and "◆ <b>AAPL</b> watch (med)" in msg
    assert "&lt;script&gt;" in msg and "<script>" not in msg  # model text is escaped for Telegram HTML
    assert fake.documents == [("2026-10-16-preopen.html", "Full report · 2026-10-16 preopen")]
    row = conn.execute("SELECT kind, status, telegram_message_id FROM alerts").fetchone()
    assert tuple(row) == ("digest", "sent", 101)


def test_failed_delivery_is_logged_not_raised(two_runs):
    from pipeline import runner

    class Broken:
        def send(self, data, path):
            raise RuntimeError("network down")

    settings, conn = two_runs
    path, data = report.build(conn, 2, settings.paths.reports, settings)
    assert not runner.deliver_report(conn, settings, data, path, Broken())
    row = conn.execute("SELECT status, reason FROM alerts").fetchone()
    assert row["status"] == "failed" and "network down" in row["reason"]


def test_extremes_section_lists_unsent_tickers(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    df = storage.load_prices(conn, "MSFT")
    df.iloc[-15:, df.columns.get_loc("close")] = df["close"].iloc[-16] * (1 + 0.02 * pd.Series(range(1, 16))).values
    df["high"] = df[["high", "close"]].max(axis=1)
    storage.replace_prices(conn, "MSFT", df)  # 15 straight up days: RSI near 100, far above its 20-day mean
    conn.close()
    execute(settings, result(verdict("MSFT")), full=False)
    data = report.load(storage.connect(settings.paths.db), 1)
    [e] = data["extremes"]
    assert e["ticker"] == "MSFT" and any(c.startswith("RSI") and "overbought" in c for c in e["conditions"])
    html = (settings.paths.reports / "latest.html").read_text()
    assert "At extremes (1)" in html and "EXTREME MSFT" in report.summary_text(data)
