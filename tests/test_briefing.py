import json

from pipeline import briefing
from pipeline.config import ROOT
from tests.conftest import quote, utc

NOW = utc("2026-10-23T05:30")  # 07:30 Prague, 01:30 New York
QUOTES = [
    quote("ES=F", "S&P 500 fut", "US stocks", price=7706, day=0.04),
    quote("^GDAXI", "DAX", "Europe", price=25371.75, day=-0.4, live=False),
    quote("^TNX", "US 10y yield", "Rates", price=4.95, day=-2.5),
]

DIGEST = {
    "headline": "AI sell-off <spreads> to Asia",
    "markets": ["US futures flat after Thursday's slide", "Oil steady"],
    "stories": [{"title": "Chip stocks slide", "why": "Data-center spending doubts & weak guidance.",
                 "source": "Reuters", "time": "Thu 22:10"}],
    "stretched": [{"symbol": "NVDA", "state": "oversold", "evidence": "RSI 28 after a 6-day drop", "source": "Barron's"}],
    "calendar": [{"when": "Fri 14:30", "event": "US consumer sentiment"}],
    "watch": "Whether the chip slide stops at the open.",
}  # fmt: skip


def test_prompts_exist_and_match_the_schemas():
    digest = briefing.load_prompt(ROOT / "prompts", "digest")
    for field in briefing.DIGEST_SCHEMA["required"]:
        assert field in digest, field
    explain = briefing.load_prompt(ROOT / "prompts", "explain")
    for field in briefing.EXPLAIN_SCHEMA["required"]:
        assert field in explain, field
    json.dumps(briefing.DIGEST_SCHEMA)  # serialisable for --json-schema


def test_digest_request_has_time_window_snapshot_and_moves():
    since = utc("2026-10-22T13:00")
    text = briefing.digest_request("morning", NOW, since, QUOTES, ["- 18:30 (sent): Nasdaq -3%"], ["NVDA", "AAPL"],
                                   "Europe/Prague")  # fmt: skip
    assert "Digest kind: morning" in text
    assert "Now: Friday 23 October 2026, 07:30 Prague (01:30 New York)." in text
    assert "Window: news since Thursday 22 October 2026, 15:00 Prague (09:00 New York)" in text
    assert "US stocks: S&P 500 fut 7,706 (+0.0%)" in text
    assert "Europe: DAX 25,372 (closed, -0.4%)" in text
    assert "Rates: US 10y yield 4.95% (-13 bp)" in text
    assert "- 18:30 (sent): Nasdaq -3%" in text and "Followed stocks: NVDA, AAPL" in text


def test_digest_request_without_moves():
    text = briefing.digest_request("weekly", NOW, None, QUOTES, [], [], "Europe/Prague")
    assert "flagged in the window:\nnone" in text and "Window" not in text and "Followed" not in text


def test_render_digest_escapes_and_includes_our_snapshot():
    html = briefing.render_digest("morning", DIGEST, QUOTES, NOW, "Europe/Prague")
    assert html.startswith("📰 <b>Morning digest</b> · Fri 23 Oct 07:30")
    assert "<i>AI sell-off &lt;spreads&gt; to Asia</i>" in html
    assert "1. <b>Chip stocks slide</b> Data-center spending doubts &amp; weak guidance. <i>(Reuters, Thu 22:10)</i>" in html
    assert "🔻 <b>NVDA</b> oversold: RSI 28 after a 6-day drop <i>(Barron's)</i>" in html
    assert "• Fri 14:30: US consumer sentiment" in html
    assert "<b>Snapshot</b>" in html and "DAX 25,372 (closed, -0.4%)" in html


def test_render_digest_tolerates_missing_parts():
    html = briefing.render_digest("preopen", {"headline": "Quiet", "stories": []}, [], NOW, "Europe/Prague")
    assert "Pre-open digest" in html and "Top stories" not in html


def test_failure_message_still_has_the_snapshot():
    html = briefing.render_digest_failure("morning", "usage limit <reached>", QUOTES, NOW, "Europe/Prague")
    assert "usage limit &lt;reached&gt;" in html and "S&amp;P 500 fut" in html


def test_explain_request_and_render():
    text = briefing.explain_request(["📉 Nasdaq 100 fut -2.6% in 1h"], QUOTES, NOW, "Europe/Prague")
    assert "📉 Nasdaq 100 fut -2.6% in 1h" in text and "US stocks: S&P 500 fut" in text
    html = briefing.render_explain({
        "cause": "Tariff threat", "confidence": "high", "details": "The White House said <50%>.",
        "sources": [{"outlet": "Reuters", "time": "15:22", "headline": "US threatens tariffs"}],
        "watch": "EU response.",
    })  # fmt: skip
    assert html.splitlines()[0] == "🔎 <b>Why:</b> Tariff threat <i>(confidence: high)</i>"
    assert "&lt;50%&gt;" in html and "• Reuters 15:22: US threatens tariffs" in html and "👀 EU response." in html
