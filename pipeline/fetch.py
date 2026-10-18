"""Market data fetching behind a small interface, so yfinance can be swapped out.

A source only has to implement `PriceSource`; `refresh()` handles caching.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import date
from typing import Protocol

import pandas as pd

from pipeline import storage

log = logging.getLogger(__name__)


class PriceSource(Protocol):
    def history(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """Daily OHLCV per symbol, split/dividend adjusted, columns = storage.PRICE_COLS."""
        ...

    def fundamentals(self, symbol: str) -> dict:
        """Raw fundamentals (see YFinanceSource.FIELDS for the expected keys)."""
        ...

    def intraday(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        """Recent 5-minute OHLCV bars including pre-market and after-hours, indexed by UTC time."""
        ...


class YFinanceSource:
    # our key -> Yahoo `info` key
    FIELDS = {
        "name": "shortName",
        "sector": "sector",
        "industry": "industry",
        "currency": "currency",
        "market_cap": "marketCap",
        "pe": "trailingPE",
        "fwd_pe": "forwardPE",
        "ev_ebitda": "enterpriseToEbitda",
        "rev_growth": "revenueGrowth",
        "eps_growth": "earningsGrowth",
        "gross_margin": "grossMargins",
        "op_margin": "operatingMargins",
        "net_margin": "profitMargins",
        "debt_to_equity_pct": "debtToEquity",  # Yahoo reports D/E in percent
        "fcf": "freeCashflow",
    }

    def __init__(self, period: str = "2y"):
        self.period = period

    def history(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        import yfinance as yf

        raw = yf.download(
            symbols,
            period=self.period,
            auto_adjust=True,
            group_by="ticker",
            progress=False,
            threads=True,
        )
        out = {}
        for sym in symbols:
            if isinstance(raw.columns, pd.MultiIndex):
                if sym not in raw.columns.get_level_values(0):
                    continue
                df = raw[sym]
            else:
                df = raw
            df = df.rename(columns=str.lower)[storage.PRICE_COLS].dropna(subset=["close"])
            df.index = pd.to_datetime(df.index).tz_localize(None).normalize()
            if not df.empty:
                out[sym] = df
        return out

    def intraday(self, symbols: list[str]) -> dict[str, pd.DataFrame]:
        import yfinance as yf

        raw = yf.download(
            symbols, period="2d", interval="5m", prepost=True, auto_adjust=True,
            group_by="ticker", progress=False, threads=True,
        )  # fmt: skip
        out = {}
        for sym in symbols:
            if isinstance(raw.columns, pd.MultiIndex):
                if sym not in raw.columns.get_level_values(0):
                    continue
                df = raw[sym]
            else:
                df = raw
            df = df.rename(columns=str.lower)[storage.PRICE_COLS].dropna(subset=["close"])
            if not df.empty:
                df.index = df.index.tz_convert("UTC")
                out[sym] = df
        return out

    def fundamentals(self, symbol: str) -> dict:
        import yfinance as yf

        t = yf.Ticker(symbol)
        info = t.info or {}
        data = {k: info.get(src) for k, src in self.FIELDS.items()}
        data["next_earnings"] = None
        try:
            cal = t.calendar or {}
            today = date.today()
            upcoming = sorted(d for d in cal.get("Earnings Date", []) if d >= today)
            if upcoming:
                data["next_earnings"] = upcoming[0].isoformat()
        except Exception as e:  # calendar is best-effort
            log.warning("%s: earnings calendar unavailable: %s", symbol, e)
        return data


US_CLOSE = (16, 0)  # regular session close, New York time


def keep_newer_cached(fresh: pd.DataFrame, cached: pd.DataFrame) -> pd.DataFrame:
    """Never let a download that ends earlier than the cache drop bars we already have."""
    if cached.empty or fresh.empty or fresh.index[-1] >= cached.index[-1]:
        return fresh
    return pd.concat([fresh, cached[cached.index > fresh.index[-1]]])


def drop_unfinished_bar(df: pd.DataFrame, now_utc: pd.Timestamp) -> pd.DataFrame:
    """Drop today's daily bar while the US session is still running (it is a partial candle)."""
    if df.empty:
        return df
    now_ny = now_utc.tz_convert("America/New_York")
    if df.index[-1].date() == now_ny.date() and (now_ny.hour, now_ny.minute) < US_CLOSE:
        return df.iloc[:-1]
    return df


def complete_last_session(daily: pd.DataFrame, intraday: pd.DataFrame, now_utc: pd.Timestamp) -> pd.DataFrame:
    """Append the latest finished regular session built from 5-minute bars, if the daily data lacks it.

    Yahoo's daily series sometimes shows the newest session with an empty close for a
    few hours after the after-hours session ends; the 5-minute bars still have it.
    The next download replaces this bar with Yahoo's official one.
    """
    if intraday is None or intraday.empty:
        return daily
    ny = intraday.index.tz_convert("America/New_York")
    minutes = ny.hour * 60 + ny.minute
    regular = intraday[(minutes >= 9 * 60 + 30) & (minutes < US_CLOSE[0] * 60 + US_CLOSE[1])]
    if regular.empty:
        return daily
    day = regular.index.tz_convert("America/New_York")[-1].date()
    session_end = pd.Timestamp(f"{day} {US_CLOSE[0]:02d}:{US_CLOSE[1]:02d}", tz="America/New_York")
    if now_utc < session_end or (not daily.empty and daily.index[-1].date() >= day):
        return daily
    bars = regular[regular.index.tz_convert("America/New_York").date == day]
    bar = pd.DataFrame(
        {"open": bars["open"].iloc[0], "high": bars["high"].max(), "low": bars["low"].min(),
         "close": bars["close"].iloc[-1], "volume": bars["volume"].sum()},
        index=pd.DatetimeIndex([pd.Timestamp(day)]),
    )  # fmt: skip
    return pd.concat([daily, bar])


def extended_quote(series: pd.Series, last_close: float, as_of: str) -> dict | None:
    """Latest pre-market/after-hours price traded after the `as_of` regular close, if any.

    Returns {px, chg_pct, session ("post" or "pre"), utc ("MM-DD HH:MM"), key} where
    `key` identifies the session, so a move is only reported once per session.
    """
    close_time = pd.Timestamp(f"{as_of} {US_CLOSE[0]:02d}:{US_CLOSE[1]:02d}", tz="America/New_York")
    after = series[series.index > close_time.tz_convert("UTC")]
    if after.empty or not last_close:
        return None
    t = after.index[-1]
    ny = t.tz_convert("America/New_York")
    session = "post" if ny.date() == close_time.date() else "pre"
    if session == "pre" and (ny.hour, ny.minute) >= (9, 30):
        return None  # the regular session has started; that's not an extended-hours quote
    px = float(after.iloc[-1])
    return {
        "px": round(px, 2),
        "chg_pct": round((px / last_close - 1) * 100, 2),
        "session": session,
        "utc": t.strftime("%m-%d %H:%M"),
        "key": f"{ny.date()}:{session}",
    }


def make_source(name: str, period: str) -> PriceSource:
    if name == "yfinance":
        return YFinanceSource(period)
    raise ValueError(f"unknown data source: {name}")


@dataclass
class MarketData:
    prices: dict[str, pd.DataFrame]
    fundamentals: dict[str, dict]
    errors: dict[str, list[str]] = field(default_factory=dict)
    extended: dict[str, dict] = field(default_factory=dict)  # see extended_quote()

    def add_error(self, symbol: str, msg: str) -> None:
        self.errors.setdefault(symbol, []).append(msg)


def refresh(
    conn: sqlite3.Connection,
    source: PriceSource,
    symbols: list[str],
    benchmark: str,
    fundamentals_max_age_hours: float,
    offline: bool = False,
    extended_hours: bool = True,
    with_fundamentals: bool = True,
) -> MarketData:
    """Download prices (+ benchmark) and stale fundamentals, cache them, return from cache.

    A failed download falls back to whatever is cached, and is recorded in
    `errors` so it can surface as a data gap.
    """
    all_syms = list(dict.fromkeys([*symbols, benchmark]))
    data = MarketData(prices={}, fundamentals={})

    intraday: dict[str, pd.DataFrame] = {}
    if not offline:
        try:
            fresh = source.history(all_syms)
        except Exception as e:
            log.error("price download failed, using cache: %s", e)
            fresh = {}
        try:
            intraday = source.intraday(all_syms)
        except Exception as e:
            log.warning("5-minute prices unavailable: %s", e)
        now = pd.Timestamp.now(tz="UTC")
        for sym in all_syms:
            if sym in fresh:
                df = drop_unfinished_bar(fresh[sym], now)
                patched = complete_last_session(df, intraday.get(sym), now)
                if len(patched) > len(df):
                    log.warning("%s: daily data lacks %s; rebuilt it from 5-minute bars", sym, patched.index[-1].date())
                    data.add_error(sym, f"close on {patched.index[-1].date()} rebuilt from 5-minute bars "
                                        "(official daily close not yet available)")  # fmt: skip
                    df = patched
                cached = storage.load_prices(conn, sym)
                merged = keep_newer_cached(df, cached)
                if len(merged) > len(df):
                    log.warning("%s: download ends %s but the cache has %s; keeping the cached newer bars",
                                sym, df.index[-1].date(), cached.index[-1].date())  # fmt: skip
                storage.replace_prices(conn, sym, merged)
            else:
                data.add_error(sym, "price download failed; using cached prices")
                log.warning("%s: no fresh prices", sym)

    for sym in all_syms:
        df = storage.load_prices(conn, sym)
        if df.empty:
            data.add_error(sym, "no price history")
        else:
            data.prices[sym] = df

    for sym in symbols if with_fundamentals else []:
        cached = storage.load_fundamentals(conn, sym, fundamentals_max_age_hours)
        if cached is None and not offline:
            try:
                cached = source.fundamentals(sym)
                storage.save_fundamentals(conn, sym, cached)
            except Exception as e:
                log.warning("%s: fundamentals download failed: %s", sym, e)
                data.add_error(sym, "fundamentals download failed")
                cached = storage.load_fundamentals(conn, sym)  # stale beats nothing
        data.fundamentals[sym] = cached or {}

    if extended_hours:
        for sym, bars in intraday.items():
            if sym in data.prices and sym in symbols:
                df = data.prices[sym]
                quote = extended_quote(bars["close"], float(df["close"].iloc[-1]), df.index[-1].strftime("%Y-%m-%d"))
                if quote:
                    data.extended[sym] = quote

    log.info(
        "market data: %d/%d tickers with prices (latest bar %s), %d extended-hours quotes, %d with errors",
        len(data.prices),
        len(all_syms),
        max((df.index[-1].date() for df in data.prices.values()), default=None),
        len(data.extended),
        len(data.errors),
    )
    return data
