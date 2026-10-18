"""Price detectors: pure functions from live numbers to events. No I/O, no time of day logic
beyond the session name, so every rule can be tested exactly.

Thresholds scale with each symbol's own volatility, measured from its daily history:
a "normal 15-minute move" is the daily standard deviation scaled by sqrt(15/390)
(390 minutes in a US session), and the average daily range is the 14-day ATR.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import pandas as pd

from pipeline import indicators
from pipeline.config import AlertsCfg

SESSION_MINUTES = 390


@dataclass
class Norms:
    """A symbol's typical moves, in percent."""

    daily_sigma_pct: float  # standard deviation of daily returns, last 60 sessions
    atr_pct: float  # 14-day average true range as % of price

    def window_sigma_pct(self, minutes: float) -> float:
        return self.daily_sigma_pct * math.sqrt(minutes / SESSION_MINUTES)


def norms_from_daily(df: pd.DataFrame, lookback: int = 60) -> Norms | None:
    if len(df) < max(lookback, 20) + 1:
        return None
    sigma = df["close"].pct_change().iloc[-lookback:].std() * 100
    atr = indicators.atr(df).iloc[-1] / df["close"].iloc[-1] * 100
    if not (math.isfinite(sigma) and math.isfinite(atr)) or sigma <= 0:
        return None
    return Norms(float(sigma), float(atr))


@dataclass
class Event:
    symbol: str
    kind: str  # window | day | ext | index_window | index_day | vix | futures
    severity: str  # alert | critical
    move_pct: float
    ratio: float | None  # move relative to the symbol's normal move (sigma or ATR multiples)
    price: float | None
    label: str  # human description, e.g. "-4.3% in 15 min (4.1x its normal 15-minute move)"

    @property
    def direction(self) -> str:
        return "up" if self.move_pct > 0 else "down"

    @property
    def key(self) -> str:
        """Same symbol, kind and direction = the same situation (for cooldowns)."""
        return f"{self.symbol}:{self.kind}:{self.direction}"

    @property
    def level(self) -> bool:
        """Level events (change since the previous close) stay true all day; window events are bursts."""
        return self.kind in ("day", "ext", "index_day", "vix", "futures")


def stock_events(
    symbol: str, session: str, price: float | None, window_pct: float | None, day_pct: float | None,
    norms: Norms | None, cfg: AlertsCfg,
) -> list[Event]:  # fmt: skip
    """A watchlist stock or sector ETF."""
    out = []
    if norms is None:
        return out
    if session == "regular":
        if window_pct is not None:
            sigma = norms.window_sigma_pct(cfg.stock_window_minutes)
            if abs(window_pct) >= max(cfg.stock_window_pct, cfg.stock_window_sigma * sigma):
                ratio = abs(window_pct) / sigma
                out.append(Event(symbol, "window", "alert", window_pct, ratio, price,
                                 f"{window_pct:+.1f}% in {cfg.stock_window_minutes:g} min "
                                 f"({ratio:.1f}x its normal {cfg.stock_window_minutes:g}-minute move)"))  # fmt: skip
        if day_pct is not None and abs(day_pct) >= max(cfg.stock_day_pct, cfg.stock_day_atr * norms.atr_pct):
            ratio = abs(day_pct) / norms.atr_pct
            out.append(Event(symbol, "day", "alert", day_pct, ratio, price,
                             f"{day_pct:+.1f}% today ({ratio:.1f}x its average daily range)"))  # fmt: skip
    elif session in ("pre", "post") and day_pct is not None:
        if abs(day_pct) >= max(cfg.ext_pct, cfg.ext_atr * norms.atr_pct):
            ratio = abs(day_pct) / norms.atr_pct
            when = "pre-market" if session == "pre" else "after hours"
            out.append(Event(symbol, "ext", "alert", day_pct, ratio, price,
                             f"{day_pct:+.1f}% {when} vs the last close ({ratio:.1f}x its average daily range)"))  # fmt: skip
    return out


def index_events(
    symbol: str, session: str, price: float | None, window_pct: float | None, day_pct: float | None, cfg: AlertsCfg
) -> list[Event]:
    """SPY / QQQ: market-wide moves. A fall of crash_pct or more since the previous close is critical."""
    out = []
    if session == "regular" and window_pct is not None and abs(window_pct) >= cfg.index_window_pct:
        out.append(Event(symbol, "index_window", "alert", window_pct, None, price,
                         f"{window_pct:+.1f}% in {cfg.stock_window_minutes:g} min"))  # fmt: skip
    if session in ("regular", "pre", "post") and day_pct is not None and abs(day_pct) >= cfg.index_day_pct:
        severity = "critical" if day_pct <= -cfg.crash_pct else "alert"
        when = {"regular": "today", "pre": "pre-market", "post": "after hours"}[session]
        out.append(Event(symbol, "index_day", severity, day_pct, None, price, f"{day_pct:+.1f}% {when}"))
    return out


def macro_events(
    quotes: dict[str, dict], names: dict[str, str], session: str, cfg: AlertsCfg, futures_open: bool = True
) -> list[Event]:
    """VIX (only meaningful while the US market is open) and index futures (outside regular hours, and
    only while futures trade: over a weekend the quote still shows Friday's move)."""
    out = []
    vix = quotes.get("^VIX")
    if session == "regular" and vix and vix.get("change_pct") is not None and vix["change_pct"] >= cfg.vix_jump_pct:
        out.append(Event("^VIX", "vix", "alert", vix["change_pct"], None, vix["price"],
                         f"VIX {vix['change_pct']:+.0f}% today to {vix['price']:.1f}: market stress"))  # fmt: skip
    if session != "regular" and futures_open:
        for sym, q in quotes.items():
            if sym.endswith("=F") and q.get("change_pct") is not None and abs(q["change_pct"]) >= cfg.futures_pct:
                severity = "critical" if q["change_pct"] <= -cfg.crash_pct else "alert"
                out.append(Event(sym, "futures", severity, q["change_pct"], None, q["price"],
                                 f"{names.get(sym, sym)} {q['change_pct']:+.1f}% since the last settle"))  # fmt: skip
    return out
