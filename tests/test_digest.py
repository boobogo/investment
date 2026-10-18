import json
from datetime import date, datetime, timedelta, timezone

import pytest

from pipeline import brain, config, digest, newswatch, storage
from pipeline.config import NewsCfg, NewsWatchCfg, Ticker
from pipeline.news import NewsItem
from tests.test_brain import FakeBackend
from tests.test_runner import execute, rows, settings  # noqa: F401  (settings is a fixture)

NOW = datetime(2026, 10, 19, 5, 30, tzinfo=timezone.utc)
WATCH = [Ticker(symbol="KO", name="Coca-Cola"), Ticker(symbol="NVDA", name="NVIDIA")]
SETTINGS = config.load_settings()  # the real patterns from settings.yaml


def fh_item(i, headline, source="Reuters", hours_ago=1, summary="", related=""):
    return {"id": i, "datetime": int((NOW - timedelta(hours=hours_ago)).timestamp()), "headline": headline,
            "source": source, "summary": summary, "url": f"https://x/{i}", "related": related, "category": "top news"}


def item(title, source="Reuters", hours_ago=1, tickers=(), snippet=None):
    return NewsItem(title=title, source=source, published=NOW - timedelta(hours=hours_ago), snippet=snippet,
                    tickers=list(tickers), provider="finnhub", provider_id=title)


# --- newswatch ---


def test_from_finnhub_strips_source_suffix_and_matches_watchlist():
    it = newswatch.from_finnhub(fh_item(7, "Nvidia wins export license - Reuters"), "general", WATCH)
    assert it.title == "Nvidia wins export license" and it.tickers == ["NVDA"]
    assert it.provider_id == "7" and it.published == NOW - timedelta(hours=1)
    # general news: Finnhub's "related" field counts; company news must actually name the company
    assert newswatch.from_finnhub(fh_item(8, "Soda tax vote", related="KO"), "general", WATCH).tickers == ["KO"]
    assert newswatch.from_finnhub(fh_item(9, "Treasury yields hit a high", related="KO"), "company", WATCH).tickers == []
    assert newswatch.from_finnhub(fh_item(10, "Coca-Cola raises prices", related="KO"), "company", WATCH).tickers == ["KO"]


def test_prefilter_scores():
    pf = newswatch.Prefilter(SETTINGS.news, SETTINGS.news_watch)
    assert pf.score(item("3 Dividend Stocks to Buy Now"), NOW) is None  # listicle
    assert pf.score(item("Should Tesla merge with SpaceX?", source="SeekingAlpha"), NOW) is None  # skipped source
    plain = pf.score(item("Company opens a new office", source="smallblog.com", hours_ago=30), NOW)
    big = pf.score(item("Coca-Cola agrees takeover of rival", tickers=["KO"]), NOW)
    macro = pf.score(item("Fed signals rate cut in December"), NOW)
    assert plain == 0 and big > macro > plain


def test_rank_stories_rewards_wide_coverage():
    pf = newswatch.Prefilter(SETTINGS.news, SETTINGS.news_watch)
    items = [
        item("Fed holds rates steady, signals patience"),
        item("Fed holds rates steady and signals patience on cuts", source="CNBC"),
        item("Fed keeps rates steady, signals patience", source="Bloomberg"),
        item("Oil prices climb on supply worries"),
    ]
    ranked = newswatch.rank_stories(items, pf, NewsCfg(), NOW, limit=5)
    assert len(ranked) == 2 and len(ranked[0][1]) == 3 and ranked[0][1][0].title.startswith("Fed holds")


# --- digest pieces ---


def test_parse_highlights():
    good = {"id": 1, "title": "Fed holds", "why": "Rates path.", "scope": "market", "tickers": [], "importance": 5}
    data = {"highlights": [good, {**good, "id": 99}, {**good, "scope": "galaxy"}, good, {**good, "id": 2}]}
    picks = digest.parse_highlights(data, {1, 2}, n=8)
    assert [p["news_id"] for p in picks] == [1, 2] and "id" not in picks[0]
    assert digest.parse_highlights(data, {1, 2}, n=1) == picks[:1]
    assert digest.parse_highlights("garbage", {1}, 3) == []


def test_candidate_lines():
    a, b = item("Fed holds", snippet="Long summary " * 40, tickers=[]), item("Fed holds rates", source="CNBC")
    [line] = digest.candidate_lines([(5.0, [a, b])], {id(a): 11, id(b): 12})
    entry = json.loads(line)
    assert entry["id"] == 11 and entry["outlets"] == 2 and len(entry["summary"]) <= 221 and "tickers" not in entry


def test_news_window(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    cfg = NewsWatchCfg(min_window_hours=12, max_window_hours=72)
    run = storage.start_run(conn, run_date="2026-10-19", slot="morning", mode="full", args={})
    assert digest.news_window(conn, run, cfg, NOW) == NOW - timedelta(hours=72)  # no earlier digest
    conn.execute("UPDATE runs SET status = 'ok', started_at = ? WHERE id = ?", ((NOW - timedelta(hours=30)).isoformat(), run))
    later = storage.start_run(conn, run_date="2026-10-19", slot="preopen", mode="full", args={})
    assert digest.news_window(conn, later, cfg, NOW) == NOW - timedelta(hours=30)
    conn.execute("UPDATE runs SET started_at = ? WHERE id = ?", ((NOW - timedelta(hours=2)).isoformat(), run))
    assert digest.news_window(conn, later, cfg, NOW) == NOW - timedelta(hours=12)  # at least 12 hours


class FakeFinnhub:
    def __init__(self):
        self.calls = []

    def market_news(self, category="general", min_id=0):
        self.calls.append(("news", category))
        return [fh_item(1, "Fed signals December rate cut - Reuters", summary="Officials see easing."),
                fh_item(2, "Stocks slide as Treasury yields jump", source="CNBC"),
                fh_item(3, "Old story", hours_ago=200)]

    def company_news(self, symbol, start, end):
        self.calls.append(("company", symbol))
        return [fh_item(10 + len(self.calls), f"{symbol} beats estimates and raises guidance", source="Yahoo", related=symbol)]

    def earnings_calendar(self, start, end, symbol=None):
        return [{"date": "2026-10-19", "symbol": "KO", "hour": "bmo"}, {"date": "2026-10-19", "symbol": "AAPL", "hour": "amc"},
                {"date": "2026-10-20", "symbol": "XYZ", "hour": "amc"}]


def test_build_picks_highlights_and_calendar(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    storage.save_fundamentals(conn, "AAPL", {"next_earnings": "2026-10-28"})
    watch = config.load_tickers(settings.paths.tickers)  # AAPL, MSFT
    run = storage.start_run(conn, run_date="2026-10-19", slot="morning", mode="full", args={})
    picks = {"highlights": [{"id": 1, "title": "Fed hints at December cut", "why": "Lower rates support stocks.",
                             "scope": "market", "tickers": [], "importance": 5}]}
    backend = FakeBackend(brain.LLMResult(ok=True, data=picks, usage={"output_tokens": 40}))
    ex = digest.build(conn, run, settings, "morning", watch, {}, backend_factory=lambda cfg: backend,
                      use_model=True, online=True, now=NOW, finnhub_client=FakeFinnhub())
    assert len(ex.ranked) == 4  # the 200-hour-old story is outside the window
    assert ex.highlights[0]["news_id"] == 1 and "Officials see easing." in backend.calls[0]["user"]
    assert "Watchlist: AAPL (Apple), MSFT (Microsoft)" in backend.calls[0]["user"]
    [h] = storage.load_highlights(conn, run)
    assert h["title"] == "Fed hints at December cut" and h["url"] == "https://x/1"
    assert ex.earnings == [{"date": "2026-10-19", "symbol": "AAPL", "when": "after the close", "yahoo_date": "2026-10-28"}]


def test_build_without_model_or_news(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    run = storage.start_run(conn, run_date="2026-10-19", slot="morning", mode="dry", args={})
    ex = digest.build(conn, run, settings, "morning", [], {}, backend_factory=None, use_model=False, online=False, now=NOW)
    assert ex.ranked == [] and ex.highlights == [] and ex.calls == []


def test_weekly_movers(settings, aapl, spy):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    [m, _] = digest.weekly_movers(conn, config.load_tickers(settings.paths.tickers), "SPY")
    r = (aapl["close"].iloc[-1] / aapl["close"].iloc[-6] - 1) * 100
    b = (spy["close"].iloc[-1] / spy["close"].iloc[-6] - 1) * 100
    assert m["ret_pct"] == pytest.approx(r, abs=0.01) and m["vs_bench_pct"] == pytest.approx(r - b, abs=0.01)


def test_weekly_slot_skips_the_stock_review(settings):  # noqa: F811
    from datetime import datetime as dt
    from zoneinfo import ZoneInfo

    sunday = dt(2026, 10, 18, 19, 0, tzinfo=ZoneInfo("Europe/Prague"))
    code, backend = execute(settings, now=sunday, slot="weekly")
    assert code == 0 and backend.calls == []  # offline: no news, so no highlights call either
    run = rows(settings, "SELECT * FROM runs")[0]
    extras = json.loads(run["extras_json"])
    assert run["n_sent"] == 0 and [m["symbol"] for m in extras["movers"]] and "window" in extras


def test_different_companies_are_never_one_story():
    pf = newswatch.Prefilter(SETTINGS.news, SETTINGS.news_watch)
    items = [item("AAPL beats estimates and raises guidance", tickers=["AAPL"]),
             item("MSFT beats estimates and raises guidance", tickers=["MSFT"])]
    assert len(newswatch.rank_stories(items, pf, NewsCfg(), NOW, limit=5)) == 2
