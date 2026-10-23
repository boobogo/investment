"""Market snapshot: the watched instruments' latest prices and how far they moved.

Prices come from Yahoo Finance 5-minute bars, one batched download per check
(futures are ~10 minutes delayed, some indexes ~15). Nothing is stored: each
check downloads the last few days again, and each instrument's typical daily
move (sigma, from a year of daily closes) is recomputed once a day.

Moves are measured over rolling windows, so no exchange calendar is needed:
  - 24h: the last price vs the last price at least 24 hours before it; across a
    weekend or a closed market that is simply the previous close
  - 1h: the last price vs the price an hour before it, only if the market was
    trading then (otherwise the first bar after an open would count the
    overnight gap as a one-hour move)
A market whose last bar is older than max_quote_age is closed right now: it is
shown but never alerted on (yesterday's move must not alert again today).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta

import pandas as pd

log = logging.getLogger(__name__)

DAY, HOUR = timedelta(hours=24), timedelta(hours=1)
HOUR_SLACK = timedelta(minutes=15)  # the 1h reference bar may be this much older than exactly an hour
YIELDS = {"^TNX", "^FVX", "^TYX", "^IRX"}  # quoted in percent: moves read better in basis points


@dataclass
class Quote:
    symbol: str
    name: str
    group: str
    price: float
    at: datetime  # start of the last 5-minute bar, UTC
    day: float | None  # % change over 24 hours
    hour: float | None  # % change over the last hour
    ref_day: float | None  # the price 24 hours earlier
    sigma: float | None  # typical daily move, %
    live: bool  # traded recently (its market is open)

    def level(self) -> str:
        """The price as people quote it: '4.95%' for a yield, '29,050' for an index."""
        if self.symbol in YIELDS:
            return f"{self.price:.2f}%"
        return f"{self.price:,.{price_decimals(self.price)}f}"

    def change(self, pct: float | None = None, window: str = "") -> str:
        """'+1.2%' (or '+12 bp' for a yield) with an optional window label."""
        pct = self.day if pct is None else pct
        if pct is None:
            return "n/a"
        if self.symbol in YIELDS:
            bp = self.price * pct / (100 + pct) * 100  # pct is relative to the earlier level
            text = f"{bp:+.0f} bp"
        else:
            text = f"{pct:+.1f}%"
        return f"{text} {window}".strip()


def price_decimals(price: float) -> int:
    return 0 if price >= 1000 else 2 if price >= 10 else 4


def measure(closes: pd.Series) -> dict | None:
    """Last price, 24h and 1h changes from 5-minute closes with a UTC index."""
    closes = closes.dropna()
    if closes.empty:
        return None
    last_t, price = closes.index[-1], float(closes.iloc[-1])
    before_day = closes[closes.index <= last_t - DAY]
    ref_day = float(before_day.iloc[-1]) if not before_day.empty else None
    target = last_t - HOUR
    before_hour = closes[(closes.index <= target) & (closes.index >= target - HOUR_SLACK)]
    ref_hour = float(before_hour.iloc[-1]) if not before_hour.empty else None
    pct = lambda ref: (price / ref - 1) * 100 if ref else None  # noqa: E731
    return {"price": price, "at": last_t.to_pydatetime(), "day": pct(ref_day), "hour": pct(ref_hour), "ref_day": ref_day}


def daily_sigma(closes: pd.Series, days: int) -> float | None:
    """Standard deviation of daily % changes over the last `days` sessions."""
    changes = closes.dropna().pct_change().dropna().tail(days)
    return float(changes.std() * 100) if len(changes) >= 20 else None


def snapshot(
    closes: dict[str, pd.Series], sigmas: dict[str, float], names: dict[str, str], groups: dict[str, str],
    now: datetime, max_age: timedelta,
) -> list[Quote]:  # fmt: skip
    """Quotes in the configured order; instruments without data are left out (and logged)."""
    out, missing = [], []
    for symbol, name in names.items():
        m = measure(closes[symbol]) if symbol in closes else None
        if m is None:
            missing.append(symbol)
            continue
        out.append(Quote(symbol, name, groups[symbol], m["price"], m["at"], m["day"], m["hour"], m["ref_day"],
                         sigmas.get(symbol), now - m["at"] <= max_age))  # fmt: skip
    if missing:
        log.warning("no prices for %s", ", ".join(missing))
    return out


# --- Yahoo downloads ----------------------------------------------------------------------


def _split(raw: pd.DataFrame, symbols: list[str]) -> dict[str, pd.Series]:
    out = {}
    for s in symbols:
        if isinstance(raw.columns, pd.MultiIndex):
            if s not in raw.columns.get_level_values(0):
                continue
            col = raw[s]["Close"]
        elif len(symbols) == 1 and "Close" in raw:
            col = raw["Close"]
        else:
            continue
        col = col.dropna()
        if not col.empty:
            col.index = col.index.tz_localize("UTC") if col.index.tz is None else col.index.tz_convert("UTC")
            out[s] = col
    return out


def _download(symbols: list[str], **kw) -> pd.DataFrame:
    import yfinance as yf

    return yf.download(symbols, auto_adjust=False, group_by="ticker", progress=False, threads=True, **kw)


def fetch_closes(symbols: list[str]) -> dict[str, pd.Series]:
    """5-minute closes for the last 5 days (enough for a 24h window across a weekend)."""
    return _split(_download(symbols, period="5d", interval="5m", prepost=True), symbols)


def fetch_sigmas(symbols: list[str], days: int) -> dict[str, float]:
    period = "1y" if days <= 240 else "2y"
    daily = _split(_download(symbols, period=period, interval="1d"), symbols)
    return {s: v for s, c in daily.items() if (v := daily_sigma(c, days)) is not None}
