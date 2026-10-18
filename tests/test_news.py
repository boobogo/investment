from datetime import datetime, timedelta, timezone

import pytest

from pipeline import news
from pipeline.config import NewsCfg, Ticker

NOW = datetime(2026, 10, 16, 6, 0, tzinfo=timezone.utc)
CFG = NewsCfg(
    max_items=3,
    snippet_chars=60,
    preferred_sources=["Reuters"],
    blocked_sources=["Audacy"],
    skip_title_patterns=[r"(?i)\b\d+\s+(?:\w+\s+){0,2}stocks?\b", r"^Watch\b"],
)
JNJ = Ticker(symbol="JNJ", name="Johnson & Johnson", aliases=["J&J"])
AMZN = Ticker(symbol="AMZN", name="Amazon", news_exclude=["rainforest"])


def item(title, hours_ago, source="Some Blog", snippet=None):
    return news.NewsItem(title=title, source=source, published=NOW - timedelta(hours=hours_ago), snippet=snippet)


def titles(items):
    return [i.title for i in items]


def test_normalize_title():
    assert news.normalize_title("J&J's Q3: Beats!") == "j and j s q3 beats"


def test_mentions_names_aliases_and_symbol():
    m = news.mention_matcher(JNJ)
    assert m("Johnson & Johnson raises guidance")
    assert m("J&J talc ruling")
    assert m("Why $JNJ fell") and m("JNJ (NYSE) update")
    assert not m("jnj lowercase is not the ticker")
    assert not m("Johnsonville sausages")
    assert not m(None)


def test_parse_google_strips_source_suffix():
    entries = [{
        "title": "Nvidia beats estimates - Reuters",
        "source": {"title": "Reuters"},
        "published_parsed": NOW.timetuple(),
        "link": "https://news.google.com/x",
        "summary": '<a href="x">Nvidia beats estimates</a>',
    }]
    [it] = news.parse_google(entries)
    assert (it.title, it.source, it.snippet) == ("Nvidia beats estimates", "Reuters", None)


def test_parse_yahoo_uses_domain_and_drops_echo_snippet():
    entries = [
        {"title": "Apple &amp; AI", "link": "https://www.fool.com/a", "summary": "Apple & AI",
         "published_parsed": NOW.timetuple()},
        {"title": "Apple up", "link": "https://finance.yahoo.com/b", "summary": "<p>Shares rose.</p>",
         "published_parsed": NOW.timetuple()},
        {"title": "no date", "link": "x"},
    ]
    a, b = news.parse_yahoo(entries)
    assert (a.title, a.source, a.snippet) == ("Apple & AI", "fool.com", None)
    assert (b.source, b.snippet) == ("Yahoo Finance", "Shares rose.")


def test_select_filters():
    items = [
        item("Johnson & Johnson wins appeal", 2),
        item("Johnson & Johnson trial update", 100),  # older than lookback
        item("Pharma sector roundup", 3),  # does not mention the company...
        item("Sector roundup", 3, snippet="J&J led the gains"),  # ...but this snippet does
        item("3 Dividend Stocks like Johnson & Johnson", 1),  # listicle
        item("Watch J&J CEO interview", 1),  # video
        item("Rundown with J&J", 1, source="Audacy"),  # blocked source
    ]
    got = news.select(items, JNJ, CFG, NOW, seen=set())
    assert titles(got) == ["Johnson & Johnson wins appeal", "Sector roundup"]


def test_select_ticker_exclusions():
    items = [item("Amazon rainforest fires spread", 1), item("Amazon launches devices", 1)]
    assert titles(news.select(items, AMZN, CFG, NOW, seen=set())) == ["Amazon launches devices"]


def test_select_ranking_dedup_and_cap():
    items = [
        item("Amazon old but preferred", 30, source="Reuters"),
        item("Amazon fresh blog post", 5),
        item("Amazon fresh wire story", 6, source="Reuters"),
        item("Amazon fresh wire story!", 4, snippet="The snippet from the duplicate."),
        item("Amazon another fresh one", 7),
        item("Shopping news", 1, snippet="AMZN mentioned only in the snippet"),
    ]
    got = news.select(items, AMZN, CFG, NOW, seen=set())
    # fresh + named in title + preferred first; the snippet-only mention ranks below title mentions
    assert titles(got) == ["Amazon fresh blog post", "Amazon fresh wire story", "Amazon another fresh one"]
    wire = got[1]
    assert wire.source == "Reuters" and wire.snippet == "The snippet from the duplicate."
    assert all(i.fresh for i in got)


def test_select_marks_new_and_does_not_mutate_input():
    a, b = item("Amazon raises Prime price", 1), item("Amazon opens new warehouse in Ohio", 2)
    got = news.select([a, b], AMZN, CFG, NOW, seen={a.key})
    assert {i.title: i.new for i in got} == {a.title: False, b.title: True}
    assert a.new is False and a.fresh is False  # originals untouched


def test_snippet_trim():
    assert news._trim("First sentence here. Second sentence is long.", 30) == "First sentence here."
    out = news._trim("one two three four five six seven eight nine ten", 20)
    assert out.endswith("…") and len(out) <= 21


@pytest.mark.parametrize("hours,expected", [(72, "when:3d"), (24, "when:1d"), (30, "when:2d")])
def test_google_query(hours, expected):
    q = news.google_query(JNJ, hours)
    assert expected.replace(":", "%3A") in q and "JNJ" in q and "J%26J" in q


def test_select_groups_same_story_with_different_wording():
    nvda = Ticker(symbol="NVDA", name="NVIDIA")
    items = [
        item("Volvo Cars expands automated driving partnership with Nvidia", 1, source="Reuters"),
        item("Volvo makes a bigger bet on Nvidia for hands-free automated driving", 2,
             snippet="Volvo deepens its Nvidia tie-up."),
        item("Nvidia and other chip stocks face renewed pressure as AI fears swell", 3),
        item("Nvidia Stock Falls as the Chip Leader Can't Escape the AI Selloff", 4),  # shares only 'chip'
    ]
    got = news.select(items, nvda, CFG, NOW, seen=set())
    assert titles(got) == [
        "Volvo Cars expands automated driving partnership with Nvidia",
        "Nvidia and other chip stocks face renewed pressure as AI fears swell",
        "Nvidia Stock Falls as the Chip Leader Can't Escape the AI Selloff",
    ]
    assert got[0].snippet == "Volvo deepens its Nvidia tie-up."  # borrowed from the merged duplicate


def test_content_words_ignore_company_and_filler():
    assert news.content_words("Why NVIDIA stock falls after the earnings", {"nvidia", "nvda"}) == {"falls", "earnings"}
