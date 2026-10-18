import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from pipeline import commands, storage, telegram, watcher
from tests.test_live import FakeFinnhub, FakePrices, FakeStream
from tests.test_runner import settings  # noqa: F401  (fixture)

NOW = datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)  # Monday 16:00 Prague


class FakeTelegram:
    chat_id = "42"

    def __init__(self):
        self.sent, self.docs = [], []

    def send(self, text, reply_to=None, silent=False):
        self.sent.append(text)
        return [len(self.sent)]

    def send_document(self, path, caption=None, silent=True):
        self.docs.append((path.name, caption))
        return 99


def make(settings, **kw):  # noqa: F811
    settings.watch.etfs = ["SPY"]
    settings.news_watch.live = False
    tg = FakeTelegram()
    w = watcher.Watcher(settings, finnhub=FakeFinnhub(), stream_factory=FakeStream, price_source=FakePrices(),
                        macro_fetcher=lambda s: {}, telegram_client=tg, **kw)
    return w, tg


def ask(w, text, now=NOW):
    return asyncio.run(w.commands.handle(text, now))


# --- parsing ---


def test_parse_command_and_duration():
    assert commands.parse_command("/why@boobogo_bot nvda") == ("why", ["NVDA"])
    assert commands.parse_command("hello") is None
    assert commands.parse_duration("30m") == timedelta(minutes=30)
    assert commands.parse_duration("2H") == timedelta(hours=2)
    assert commands.parse_duration("1.5d") == timedelta(days=1.5)
    assert commands.parse_duration("soon") is None


# --- commands ---


def test_help_unknown_and_free_text(settings):  # noqa: F811
    w, _ = make(settings)
    assert ask(w, "/help").startswith("<b>Commands</b>")
    assert "Unknown command /dance" in ask(w, "/dance")
    assert ask(w, "hi there") == "Send /help for the list of commands."


def test_mute_and_unmute(settings):  # noqa: F811
    w, _ = make(settings)
    reply = ask(w, "/mute 2h")
    assert "muted until 18:00" in reply and "Crashes" in reply  # 16:00 Prague + 2h
    assert datetime.fromisoformat(storage.get_state(w.conn, "mute_until")) == NOW + timedelta(hours=2)
    assert "Usage" in ask(w, "/mute forever") and "Usage" in ask(w, "/mute 30d")
    assert ask(w, "/unmute") == "🔔 Alerts are back on."
    assert datetime.fromisoformat(storage.get_state(w.conn, "mute_until")) == NOW


def test_status_and_watchlist(settings):  # noqa: F811
    w, _ = make(settings)
    asyncio.run(w.refresh_reference_data())
    w.book.on_trades([{"s": "AAPL", "p": 300.0, "t": time.time() * 1000, "v": 1}])
    status = ask(w, "/status")
    assert "alerts shadow (not sent)" in status and "Next digests:" in status and "stream up" in status
    assert "AAPL $300.00" in ask(w, "/watchlist")


def test_news_and_alerts(settings):  # noqa: F811
    w, _ = make(settings)
    from pipeline.news import NewsItem

    storage.upsert_news(w.conn, [
        NewsItem(title="Apple <beats> estimates", source="Reuters", published=NOW - timedelta(hours=1),
                 tickers=["AAPL"], category="company", provider="finnhub", provider_id="1"),
        NewsItem(title="Fed holds rates", source="Reuters", published=NOW - timedelta(hours=2),
                 category="general", provider="finnhub", provider_id="2"),
    ])  # fmt: skip
    news = ask(w, "/news aapl")
    assert news.startswith("<b>Latest AAPL headlines</b>") and "Apple &lt;beats&gt; estimates" in news
    assert "Fed holds rates" in ask(w, "/news") and "Fed holds" not in news
    assert ask(w, "/alerts") == "No alerts today."
    storage.log_alert(w.conn, created_at=NOW.isoformat(timespec="seconds"), kind="price", severity="alert",
                      text="⚡ <b>NVDA</b> -4% in 15 min\nmore", status="held", reason="quiet hours")
    assert "[held (quiet hours)] ⚡ NVDA -4% in 15 min" in ask(w, "/alerts")


def test_report_sends_the_latest_file(settings):  # noqa: F811
    w, tg = make(settings)
    assert "No report yet" in ask(w, "/report")
    settings.paths.reports.mkdir(parents=True, exist_ok=True)
    (settings.paths.reports / "latest.html").write_text("<html></html>")
    assert ask(w, "/report") is None and tg.docs == [("latest.html", "Latest full report")]


def test_why_runs_a_review_and_replies_with_a_card(settings):  # noqa: F811
    def fake_run(settings_, opts):  # stands in for runner.execute: logs one verdict
        conn = storage.connect(settings_.paths.db)
        run_id = storage.start_run(conn, run_date="2026-10-19", slot="manual", mode="full", args={})
        storage.save_verdict(conn, run_id, None, {
            "ticker": opts.tickers[0], "verdict": "bearish", "confidence": "med", "key_reasons": ["a", "b", "<c>"],
            "risks": ["r"], "what_would_change_my_mind": "a close above SMA50", "data_gaps": []},
            as_of="2026-10-16", price=300.0)  # fmt: skip
        return 0

    w, tg = make(settings, why_runner=fake_run)
    assert "not on the watchlist" in ask(w, "/why TSLA") and "Usage" in ask(w, "/why")

    async def go():
        reply = await w.commands.handle("/why aapl", NOW)
        assert reply.startswith("🔍 Looking at AAPL")
        assert "Already looking" in await w.commands.handle("/why AAPL", NOW)
        while w.why_running:
            await asyncio.sleep(0.01)

    asyncio.run(go())
    [card] = tg.sent
    assert card.startswith("🔍 <b>AAPL</b> ▼ bearish (med)") and "• &lt;c&gt;" in card
    assert "<b>Would change my mind:</b> a close above SMA50" in card


def test_digest_command_starts_one_build(settings):  # noqa: F811
    w, tg = make(settings, digest_command=["sh", "-c", "sleep 0.3", "digest"])  # ignores the slot arguments

    async def go():
        first = await w.commands.handle("/digest", NOW)
        second = await w.commands.handle("/digest", NOW)
        await w.digest_proc.wait()
        await asyncio.sleep(0.05)
        return first, second

    first, second = asyncio.run(go())
    assert first.startswith("🛠 Building a digest") and second.startswith("A digest is already")
    assert tg.sent == []  # sleep exits 0: no failure notice


def test_failed_digest_is_reported(settings):  # noqa: F811
    w, tg = make(settings, digest_command=["false"])

    async def go():
        await w.launch_digest("morning", [], reason="test")
        await w.digest_proc.wait()
        await asyncio.sleep(0.05)

    asyncio.run(go())
    assert tg.sent and tg.sent[0].startswith("⚠️ The morning digest failed (exit 1)")


# --- updates: who may talk to the bot ---


def update(chat="42", text="/help", age_s=0):
    return {"update_id": 1, "message": {"chat": {"id": int(chat)}, "date": int(time.time() - age_s), "text": text}}


def test_only_the_configured_chat_and_only_fresh_commands(settings):  # noqa: F811
    w, tg = make(settings)
    asyncio.run(w.handle_update(update(chat="666")))
    asyncio.run(w.handle_update(update(age_s=3600)))
    assert tg.sent == []
    asyncio.run(w.handle_update(update()))
    assert tg.sent[0].startswith("<b>Commands</b>")


# --- the digest schedule ---


def due(w, now):
    return asyncio.run(w.run_due_digests(now))


def test_schedule_runs_due_digests_once(settings):  # noqa: F811
    w, _ = make(settings, digest_command=["true"])
    morning = datetime(2026, 10, 19, 5, 31, tzinfo=timezone.utc)  # 07:31 Prague (CEST)
    assert due(w, morning) == ["morning"]
    assert due(w, morning + timedelta(minutes=1)) == []  # still running / just tried
    run = storage.start_run(w.conn, run_date="2026-10-19", slot="morning", mode="screened", args={})
    storage.finish_run(w.conn, run, status="ok")
    assert due(w, morning + timedelta(minutes=20)) == []  # done
    assert due(w, datetime(2026, 10, 19, 9, 0, tzinfo=timezone.utc)) == []  # nothing else due at 11:00


def test_schedule_grace_holiday_and_retry_limit(settings):  # noqa: F811
    w, _ = make(settings, digest_command=["true"])
    preopen = datetime(2026, 10, 19, 13, 2, tzinfo=timezone.utc)  # 09:02 New York
    w.finnhub_session = (time.time(), None, True)  # Finnhub says: holiday
    assert due(w, preopen) == []
    w.finnhub_session = None
    late = datetime(2026, 10, 19, 16, 30, tzinfo=timezone.utc)  # 3.5 h after 09:00 New York: too late
    assert due(w, late) == []

    async def failing_runs():  # the run never records success: at most 2 tries, 10 minutes apart
        started = []
        for minutes in (0, 5, 11, 30):
            started += await w.run_due_digests(preopen + timedelta(minutes=minutes))
            await w.digest_proc.wait()
        return started

    assert asyncio.run(failing_runs()) == ["preopen", "preopen"]


# --- watchdog ---


def test_watchdog_warns_once_and_says_when_back(settings, monkeypatch):  # noqa: F811
    import main

    tg = FakeTelegram()
    monkeypatch.setattr(telegram.Telegram, "from_env", classmethod(lambda cls, cfg: tg))
    conn = storage.connect(settings.paths.db)
    args = SimpleNamespace(max_age=10)
    storage.set_state(conn, "heartbeat", datetime.now(timezone.utc).isoformat())
    assert main.cmd_watchdog(args, settings) == 0 and tg.sent == []
    storage.set_state(conn, "heartbeat", (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat())
    assert main.cmd_watchdog(args, settings) == 1 and tg.sent[0].startswith("⚠️ The market watcher has been silent")
    assert main.cmd_watchdog(args, settings) == 1 and len(tg.sent) == 1  # once per outage
    storage.set_state(conn, "heartbeat", datetime.now(timezone.utc).isoformat())
    assert main.cmd_watchdog(args, settings) == 0 and tg.sent[1] == "✅ The market watcher is running again."
