"""Compact per-company packets: what the analyst sees for one ticker.

Values are rounded and missing ones are left out and listed under "missing"
(never sent as null). Units are explained once in prompts/analyst.md rather
than per packet: every ret_*, px_vs_*, from_52w_*, *_pct, *_m, *_g_q_yoy and
fcf_yield value is a percentage; sma50/sma200/hi_52w/lo_52w are price levels,
so the analyst can quote levels instead of deriving them.
"""

from __future__ import annotations

import json
import math
from datetime import date

from pipeline.config import Ticker
from pipeline.news import NewsItem

# (packet key, indicator key, decimals)
TECH_FIELDS = [
    ("ret_1d", "chg_1d_pct", 1),
    ("ret_5d", "chg_5d_pct", 1),
    ("ret_20d", "chg_20d_pct", 1),
    ("ret_20d_vs_bench", "rel_20d_vs_bench_pct", 1),
    ("sma50", "sma50", 2),
    ("sma200", "sma200", 2),
    ("px_vs_sma50", "px_vs_sma50_pct", 1),
    ("px_vs_sma200", "px_vs_sma200_pct", 1),
    ("rsi14", "rsi14", 0),
    ("z20", "z20", 1),
    ("atr_pct", "atr_pct", 1),
    ("vol_vs_avg20", "vol_vs_avg20", 2),
    ("hi_52w", "high_52w", 2),
    ("lo_52w", "low_52w", 2),
    ("from_52w_high", "from_52w_high_pct", 1),
    ("from_52w_low", "from_52w_low_pct", 1),
]
FUND_FIELDS = [
    ("mcap_bn", "market_cap_bn", 0),
    ("pe", "pe", 1),
    ("fwd_pe", "fwd_pe", 1),
    ("ev_ebitda", "ev_ebitda", 1),
    ("rev_g_q_yoy", "rev_growth_q_yoy_pct", 1),
    ("eps_g_q_yoy", "eps_growth_q_yoy_pct", 1),
    ("gross_m", "gross_margin_pct", 1),
    ("op_m", "op_margin_pct", 1),
    ("net_m", "net_margin_pct", 1),
    ("debt_eq", "debt_to_equity", 2),
    ("fcf_yield", "fcf_yield_pct", 1),
]


def _round(x, decimals: int):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    return int(round(x)) if decimals == 0 else round(float(x), decimals)


def _section(fields, values: dict, missing: list[str]) -> dict:
    out = {}
    for key, src, decimals in fields:
        v = _round(values.get(src), decimals)
        if v is None:
            missing.append(key)
        else:
            out[key] = v
    return out


def news_entry(item: NewsItem, mark_new: bool) -> dict:
    entry = {"utc": item.published.strftime("%m-%d %H:%M"), "src": item.source, "title": item.title}
    if item.snippet:
        entry["snippet"] = item.snippet
    if mark_new and item.new:
        entry["new"] = True
    return entry


def build_packet(
    ticker: Ticker,
    tech: dict,
    fund: dict,
    news: list[NewsItem],
    *,
    today: date,
    signals: list[str] = (),
    since_last: dict | None = None,
    data_issues: list[str] = (),
    extended: dict | None = None,
    live: dict | None = None,
) -> dict:
    """Assemble one packet. `tech`/`fund` come from pipeline.indicators."""
    missing: list[str] = []
    close = tech["close"]

    p: dict = {"t": ticker.symbol, "name": ticker.name or fund.get("name") or ticker.symbol}
    for key, value in (("sector", ticker.sector), ("industry", fund.get("industry"))):
        if value:
            p[key] = value
    if ticker.held:
        p["held"] = True
        if ticker.cost_basis:
            p["cost_basis"] = ticker.cost_basis
            p["pnl_pct"] = _round((close / ticker.cost_basis - 1) * 100, 1)
    if ticker.notes:
        p["notes"] = ticker.notes

    p["as_of"] = tech["as_of"]
    p["px"] = _round(close, 2)
    if extended:
        p["ext"] = {k: extended[k] for k in ("px", "chg_pct", "session", "utc")}
    if live:
        p["live"] = live

    p["tech"] = _section(TECH_FIELDS, tech, missing)
    if tech.get("sma50") is not None and tech.get("sma200") is not None:
        p["tech"]["sma50_gt_sma200"] = tech["sma50"] > tech["sma200"]
    if tech.get("macd_hist") is not None:
        p["tech"]["macd_hist_pct"] = _round(tech["macd_hist"] / close * 100, 2)  # % of price
    else:
        missing.append("macd_hist_pct")

    if ticker.type == "stock":
        p["fund"] = _section(FUND_FIELDS, fund, missing)

    next_earn = fund.get("next_earnings")
    days = (date.fromisoformat(next_earn) - today).days if next_earn else None
    if days is not None and days >= 0:
        p["earnings_in_days"] = days
    else:
        missing.append("earnings_date")

    if signals:
        p["signals"] = list(signals)
    if since_last:
        p["since_last"] = since_last
    # with no earlier review every headline is new, so the flag only carries information after one
    p["news"] = [news_entry(i, mark_new=since_last is not None) for i in news]
    if missing:
        p["missing"] = missing
    if data_issues:
        p["data_issues"] = list(data_issues)
    return p


def dumps(packet: dict) -> str:
    return json.dumps(packet, separators=(",", ":"), ensure_ascii=False)


CHARS_PER_TOKEN = 1.9  # measured on the first real call: number-heavy JSON tokenizes densely


def estimate_tokens(packet: dict) -> int:
    """Rough token count for one compact packet."""
    return math.ceil(len(dumps(packet)) / CHARS_PER_TOKEN)
