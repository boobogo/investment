import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import alerts, brain, config, detect, newsdesk, newswatch, storage, triage
from pipeline.config import AlertsCfg, TriageCfg
from pipeline.news import NewsItem
from tests.test_brain import FakeBackend
from tests.test_runner import settings  # noqa: F401  (fixture)

NOW = datetime(2026, 10, 19, 14, 0, tzinfo=timezone.utc)  # 16:00 Prague, US regular session
SETTINGS = config.load_settings()
WATCH = [config.Ticker(symbol="AAPL", name="Apple"), config.Ticker(symbol="MSFT", name="Microsoft")]


def item(title, source="Reuters", minutes_ago=5, tickers=(), category="general", snippet=None):
    return NewsItem(title=title, source=source, published=NOW - timedelta(minutes=minutes_ago), snippet=snippet,
                    tickers=list(tickers), category=category, provider="finnhub", provider_id=title)


# --- the strict live filter ---


@pytest.mark.parametrize("it,expected", [
    (item("Apple to acquire Peloton for $8 billion", "Yahoo", tickers=["AAPL"], category="company"), True),
    (item("Apple launches new iPad", "Yahoo", tickers=["AAPL"], category="company"), False),  # no impact word
    (item("Chip stocks slide after guidance cut", "Yahoo", tickers=["AAPL"], category="company",
          snippet="Apple among them"), False),  # company only named in the summary
    (item("Should You Buy Apple After Its Merger Talks?", "Yahoo", tickers=["AAPL"], category="company"), False),
    (item("Apple merger talks reported", "Benzinga", tickers=["AAPL"], category="company"), False),  # low quality
    (item("Fed signals emergency rate cut"), True),  # top outlet, market topic
    (item("Fed signals emergency rate cut", source="smallblog.com"), False),
    (item("Fed signals emergency rate cut", minutes_ago=90), False),  # too old for an instant alert
    (item("Celebrity wedding draws crowds"), False),
])  # fmt: skip
def test_breaking_candidate_rule(it, expected):
    pf = newswatch.Prefilter(SETTINGS.news, SETTINGS.news_watch, WATCH)
    assert pf.is_breaking_candidate(it, NOW, 45) is expected


# --- triage pieces ---


def judgement(news_id, impact=5, scope="ticker", tickers=("AAPL",), new=True, headline="Apple to buy Peloton"):
    return {"id": news_id, "impact": impact, "scope": scope, "tickers": list(tickers), "new_information": new,
            "headline": headline, "why": "A big deal for Apple."}


def test_parse_and_alert_rules():
    data = {"items": [judgement(1), judgement(2, impact=9), judgement(99)], "event": {"explained": False, "explanation": ""}}
    got, event = triage.parse(data, {1, 2})
    assert list(got) == [1] and event.explained is False
    j = got[1]
    cfg = TriageCfg()
    assert triage.is_alert(j, cfg) and triage.severity(j) == "alert"
    assert not triage.is_alert(j.model_copy(update={"new_information": False}), cfg)
    assert not triage.is_alert(j.model_copy(update={"impact": 3}), cfg)
    assert triage.severity(j.model_copy(update={"scope": "market"})) == "critical"


def test_format_news_alert_escapes():
    j = triage.Judgement(**judgement(1, headline="<b>Deal</b> & more"))
    c = triage.Candidate(1, NOW, "Reuters", "x", None, ["AAPL"], 'https://x/"a"')
    text = triage.format_news_alert(j, c, "Europe/Prague")
    assert text.startswith("📰 <b>&lt;b&gt;Deal&lt;/b&gt; &amp; more</b> (AAPL)")
    assert 'href="https://x/&quot;a&quot;"' in text and "16:00 · impact 5/5" in text


# --- the desk, end to end with fakes ---


class FakeFH:
    key = "K"

    def __init__(self, general=(), company=None):
        self.general, self.company = list(general), company or {}
        self.min_ids = []

    def market_news(self, category="general", min_id=0):
        self.min_ids.append(min_id)
        return [g for g in self.general if g["id"] > min_id]

    def company_news(self, symbol, start, end):
        return self.company.get(symbol, [])


def fh_row(i, headline, source="Reuters", minutes_ago=5, related=""):
    return {"id": i, "datetime": int((NOW - timedelta(minutes=minutes_ago)).timestamp()), "headline": headline,
            "source": source, "summary": "", "url": f"https://x/{i}", "related": related}


class FakeTelegram:
    def __init__(self):
        self.sent = []

    def send(self, text, reply_to=None, silent=False):
        self.sent.append((text, reply_to, silent))
        return [700 + len(self.sent)]


def desk_for(settings, fh, *results, shadow=True, tg=None, triage_cfg=None):  # noqa: F811
    settings.triage = triage_cfg or TriageCfg()
    settings.news, settings.news_watch = SETTINGS.news, SETTINGS.news_watch  # the real filter patterns
    conn = storage.connect(settings.paths.db)
    backend = FakeBackend(*results)
    d = newsdesk.NewsDesk(settings, conn, fh, watchlist=WATCH, policy=alerts.AlertPolicy(AlertsCfg(), conn),
                          telegram_client=tg, shadow=shadow, session_fn=lambda: "regular",
                          context_fn=lambda t: ["SPY -0.4% today"], backend_factory=lambda cfg: backend)
    return d, backend


def triage_result(*items, explained=False, explanation=""):
    return brain.LLMResult(ok=True, data={"items": list(items), "event": {"explained": explained,
                                                                          "explanation": explanation}},
                           usage={"output_tokens": 30})


def run(coro):
    return asyncio.run(coro)


def test_poll_queue_judge_and_alert(settings):  # noqa: F811
    fh = FakeFH(general=[fh_row(1, "Fed announces emergency rate cut - Reuters"),
                         fh_row(2, "Celebrity wedding draws crowds")],
                company={"AAPL": [fh_row(10, "Apple agrees to acquire Peloton", "Yahoo", related="AAPL")]})
    d, backend = desk_for(settings, fh, triage_result(judgement(0, 5, "market", (), headline="Fed cuts rates"),
                                                      judgement(0, 3)))
    run(d.poll_round(NOW))
    assert [c.title for c in d.queue] == ["Fed announces emergency rate cut", "Apple agrees to acquire Peloton"]
    assert fh.min_ids == [0]
    ids = [c.news_id for c in d.queue]
    backend.results[0].data["items"][0]["id"], backend.results[0].data["items"][1]["id"] = ids
    run(d.run_candidates(NOW))
    assert d.queue == [] and len(backend.calls) == 1
    user = backend.calls[0]["user"]
    assert "Market now: SPY -0.4% today" in user and "Watchlist: AAPL (Apple), MSFT (Microsoft)" in user
    [a] = d.conn.execute("SELECT kind, severity, status, text FROM alerts").fetchall()
    assert (a["kind"], a["severity"], a["status"]) == ("news", "critical", "shadow")  # market-wide impact 5
    assert a["text"].startswith("🚨 <b>Fed cuts rates</b>")
    assert d.conn.execute("SELECT count(*) FROM news_items WHERE triage_json IS NOT NULL").fetchone()[0] == 2
    assert d.conn.execute("SELECT purpose FROM llm_calls").fetchone()[0] == "triage"
    # the next poll only asks for newer market news, and the same story is not queued again
    fh.general.append(fh_row(3, "Fed announces emergency cut in interest rates - Reuters"))
    run(d.poll_round(NOW + timedelta(minutes=2)))
    assert fh.min_ids[-1] == 2 and d.queue == []


def test_interval_and_caps(settings):  # noqa: F811
    d, backend = desk_for(settings, FakeFH(), triage_result(), triage_result(),
                          triage_cfg=TriageCfg(max_calls_per_hour=1))
    d.ingest([item("Fed announces emergency rate cut")], NOW)
    run(d.run_candidates(NOW))
    d.ingest([item("Treasury yields jump after jobs report", minutes_ago=1)], NOW + timedelta(minutes=1))
    run(d.run_candidates(NOW + timedelta(minutes=1)))  # within the 3-minute interval
    run(d.run_candidates(NOW + timedelta(minutes=10)))  # past the interval, but the hourly cap is used up
    assert len(backend.calls) == 1 and len(d.queue) == 1


def price_event(kind="window", symbol="AAPL"):
    return detect.Event(symbol, kind, "alert", -4.2, 6.0, 210.0, "-4.2% in 15 min")


def test_follow_up_explains_the_move_in_its_thread(settings):  # noqa: F811
    tg = FakeTelegram()
    fh = FakeFH(company={"AAPL": [fh_row(20, "Apple shares fall as DOJ files antitrust suit", "Yahoo", minutes_ago=3,
                                         related="AAPL")]})
    d, backend = desk_for(settings, fh, triage_result(judgement(0, 4), explained=True,
                                                      explanation="The DOJ filed an antitrust suit (Yahoo)."),
                          shadow=False, tg=tg)
    alert_id = storage.log_alert(d.conn, kind="price", severity="alert", symbol="AAPL", text="⚡ AAPL", status="sent",
                                 telegram_message_id=555)
    d.register(alert_id, price_event(), 555, NOW)
    run(d.run_followups(NOW + timedelta(minutes=1)))  # nothing due yet
    assert backend.calls == []
    run(d.run_followups(NOW + timedelta(minutes=5)))
    assert "Price move to explain: AAPL -4.2% in 15 min" in backend.calls[0]["user"]
    assert tg.sent == [("🔎 <b>Why AAPL moved:</b> The DOJ filed an antitrust suit (Yahoo).", 555, False)]
    assert d.followups == []
    row = d.conn.execute("SELECT kind, status, reply_to, telegram_message_id FROM alerts WHERE kind = 'follow_up'").fetchone()
    assert tuple(row) == ("follow_up", "sent", alert_id, 701)


def test_follow_up_gives_up_after_the_last_try(settings):  # noqa: F811
    d, backend = desk_for(settings, FakeFH(), triage_cfg=TriageCfg(follow_up_minutes=[5, 15]))
    alert_id = storage.log_alert(d.conn, kind="price", severity="alert", symbol="SPY", text="📉 SPY", status="shadow")
    d.register(alert_id, price_event("index_day", "SPY"), None, NOW)
    run(d.run_followups(NOW + timedelta(minutes=5)))
    run(d.run_followups(NOW + timedelta(minutes=15)))
    assert backend.calls == []  # no market news turned up, so no model call
    [row] = d.conn.execute("SELECT status, text FROM alerts WHERE kind = 'follow_up'").fetchall()
    assert row["status"] == "shadow" and row["text"].startswith("🔎 No news explaining the SPY move")


def test_breaking_news_about_a_moving_stock_goes_into_its_thread(settings):  # noqa: F811
    tg = FakeTelegram()
    d, backend = desk_for(settings, FakeFH(), triage_result(judgement(0, 5)), shadow=False, tg=tg)
    alert_id = storage.log_alert(d.conn, kind="price", severity="alert", symbol="AAPL", text="⚡ AAPL", status="sent",
                                 telegram_message_id=555)
    d.register(alert_id, price_event(), 555, NOW)
    d.ingest([item("Apple agrees to acquire Peloton", "Yahoo", tickers=["AAPL"], category="company")], NOW)
    backend.results[0].data["items"][0]["id"] = d.queue[0].news_id
    run(d.run_candidates(NOW))
    assert len(tg.sent) == 1 and tg.sent[0][1] == 555  # a reply to the price alert, not a new message
    assert d.conn.execute("SELECT count(*) FROM alerts WHERE kind = 'news'").fetchone()[0] == 0


def test_calls_left_counts_the_day(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    run_id = storage.watch_run_id(conn, "2026-10-19")
    assert storage.watch_run_id(conn, "2026-10-19") == run_id  # one watch run per day
    cfg = TriageCfg(max_calls_per_hour=6, max_calls_per_day=2)
    assert triage.calls_left(conn, cfg, datetime.now(timezone.utc), "Europe/Prague") == 2
    for _ in range(2):
        storage.save_call(conn, run_id, {**brain.result_row(brain.LLMResult(ok=True), "fake"), "batch_no": 0,
                                         "attempt": 1, "purpose": "triage"})
    assert triage.calls_left(conn, cfg, datetime.now(timezone.utc), "Europe/Prague") == 0
