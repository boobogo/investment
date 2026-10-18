import json
from datetime import date, datetime, timezone

import pytest

from pipeline import indicators, packets
from pipeline.config import Ticker
from pipeline.news import NewsItem

TODAY = date(2026, 10, 16)
RAW_FUND = {
    "name": "Apple Inc.",
    "industry": "Consumer Electronics",
    "market_cap": 4_991e9,
    "pe": 39.04,
    "fwd_pe": 35.71,
    "ev_ebitda": 29.87,
    "rev_growth": 0.164,
    "eps_growth": 0.287,
    "gross_margin": 0.4862,
    "op_margin": 0.3262,
    "net_margin": 0.2762,
    "debt_to_equity_pct": 78.4,
    "fcf": 107.7e9,
    "next_earnings": "2026-11-02",
}
NEWS = [
    NewsItem(title="Apple ships new Macs", source="Reuters",
             published=datetime(2026, 10, 15, 21, 5, tzinfo=timezone.utc), snippet="Short snippet.", new=True),
    NewsItem(title="Apple supplier update", source="CNBC",
             published=datetime(2026, 10, 14, 9, 0, tzinfo=timezone.utc), new=False),
]


@pytest.fixture
def tech(aapl, spy):
    return indicators.technicals(aapl, spy)


def build(tech, ticker=None, **kw):
    ticker = ticker or Ticker(symbol="AAPL", name="Apple", sector="Technology")
    return packets.build_packet(ticker, tech, indicators.fundamentals(RAW_FUND), NEWS, today=TODAY, **kw)


def test_packet_shape(tech):
    p = build(tech)
    assert p["t"] == "AAPL" and p["name"] == "Apple" and p["industry"] == "Consumer Electronics"
    assert p["as_of"] == tech["as_of"]
    assert p["px"] == round(tech["close"], 2)
    assert p["tech"]["ret_1d"] == round(tech["chg_1d_pct"], 1)
    assert p["tech"]["sma50"] == round(tech["sma50"], 2) and p["tech"]["hi_52w"] == round(tech["high_52w"], 2)
    assert p["tech"]["rsi14"] == round(tech["rsi14"]) and isinstance(p["tech"]["rsi14"], int)
    assert p["tech"]["sma50_gt_sma200"] == (tech["sma50"] > tech["sma200"])
    assert p["tech"]["macd_hist_pct"] == round(tech["macd_hist"] / tech["close"] * 100, 2)
    assert p["fund"] == {
        "mcap_bn": 4991, "pe": 39.0, "fwd_pe": 35.7, "ev_ebitda": 29.9, "rev_g_q_yoy": 16.4,
        "eps_g_q_yoy": 28.7, "gross_m": 48.6, "op_m": 32.6, "net_m": 27.6, "debt_eq": 0.78,
        "fcf_yield": 2.2,
    }
    assert p["earnings_in_days"] == 17
    assert "missing" not in p and "held" not in p and "signals" not in p


def test_packet_is_json_without_nulls(tech):
    p = build(tech)
    text = packets.dumps(p)
    assert "null" not in text and json.loads(text) == p


def test_missing_values_are_listed_not_null(tech):
    tech = {**tech, "rsi14": None, "macd_hist": None, "from_52w_high_pct": None}
    fund = indicators.fundamentals({**RAW_FUND, "ev_ebitda": None, "next_earnings": "2026-10-01"})
    p = packets.build_packet(Ticker(symbol="AAPL"), tech, fund, [], today=TODAY)
    assert {"rsi14", "macd_hist_pct", "from_52w_high", "ev_ebitda", "earnings_date"} <= set(p["missing"])
    assert "rsi14" not in p["tech"] and "ev_ebitda" not in p["fund"]
    assert p["name"] == "Apple Inc."  # falls back to the source's name
    assert p["news"] == []
    assert "null" not in packets.dumps(p)


def test_held_position(tech):
    t = Ticker(symbol="AAPL", held=True, cost_basis=200.0, notes="core holding")
    p = build(tech, ticker=t)
    assert p["held"] is True and p["cost_basis"] == 200.0
    assert p["pnl_pct"] == round((tech["close"] / 200 - 1) * 100, 1)
    assert p["notes"] == "core holding"


def test_news_entries_and_new_flag(tech):
    first_review = build(tech)
    assert first_review["news"][0] == {
        "utc": "10-15 21:05", "src": "Reuters", "title": "Apple ships new Macs", "snippet": "Short snippet."
    }
    assert all("new" not in n for n in first_review["news"])  # nothing to compare against yet

    since = {"verdict": "bullish", "date": "2026-10-14"}
    later = build(tech, since_last=since)
    assert later["since_last"] == since
    assert [n.get("new", False) for n in later["news"]] == [True, False]


def test_signals_and_issues(tech):
    p = build(tech, signals=["rsi_high"], data_issues=["Yahoo news feed failed"])
    assert p["signals"] == ["rsi_high"] and p["data_issues"] == ["Yahoo news feed failed"]


def test_packet_size_budget(tech):
    # full packet with two headlines; five headlines take it to ~600-750 real tokens
    assert packets.estimate_tokens(build(tech)) < 500
