import pandas as pd
import pytest

from pipeline import fetch
from tests.conftest import make_ohlcv


def utc(s):
    return pd.Timestamp(s, tz="UTC")


def intraday(*points):
    return pd.Series([p for _, p in points], index=pd.DatetimeIndex([utc(t) for t, _ in points]))


def test_drop_unfinished_bar():
    df = make_ohlcv([1.0, 2.0, 3.0])  # bars dated 2024-01-01..03 (Mon-Wed)
    assert len(fetch.drop_unfinished_bar(df, utc("2024-01-03 18:00"))) == 2  # 13:00 New York: session open
    assert len(fetch.drop_unfinished_bar(df, utc("2024-01-03 21:30"))) == 3  # 16:30 New York: closed
    assert len(fetch.drop_unfinished_bar(df, utc("2024-01-04 14:00"))) == 3  # bar is from yesterday


def test_extended_quote_after_hours():
    s = intraday(("2026-10-15 19:55", 100.0), ("2026-10-15 23:50", 103.0))  # 15:55 and 19:50 New York
    q = fetch.extended_quote(s, 100.0, "2026-10-15")
    assert q == {"px": 103.0, "chg_pct": 3.0, "session": "post", "utc": "10-15 23:50", "key": "2026-10-15:post"}


def test_extended_quote_pre_market_and_open():
    pre = intraday(("2026-10-16 12:55", 95.0))  # 08:55 New York next day
    q = fetch.extended_quote(pre, 100.0, "2026-10-15")
    assert q["session"] == "pre" and q["chg_pct"] == -5.0 and q["key"] == "2026-10-16:pre"
    opened = intraday(("2026-10-16 13:45", 95.0))  # 09:45 New York: regular session, not extended
    assert fetch.extended_quote(opened, 100.0, "2026-10-15") is None
    assert fetch.extended_quote(intraday(("2026-10-15 19:00", 99.0)), 100.0, "2026-10-15") is None  # in session


def five_minute_bars(day: str, closes: list[float], start="13:30", volume=1000.0) -> pd.DataFrame:
    idx = pd.date_range(f"{day} {start}", periods=len(closes), freq="5min", tz="UTC")
    c = pd.Series(closes, index=idx)
    return pd.DataFrame({"open": c, "high": c + 1, "low": c - 1, "close": c, "volume": volume})


def test_complete_last_session_from_intraday():
    daily = make_ohlcv([100.0, 101.0])  # 2024-01-01, 2024-01-02
    # 2024-01-03 regular session 09:30-15:55 New York (14:30-20:55 UTC) plus an after-hours bar
    session = five_minute_bars("2024-01-03", [102.0 + i * 0.1 for i in range(78)], start="14:30")
    after = five_minute_bars("2024-01-03", [120.0], start="22:00")
    bars = pd.concat([session, after])
    out = fetch.complete_last_session(daily, bars, utc("2024-01-04 01:00"))
    last = out.iloc[-1]
    assert out.index[-1] == pd.Timestamp("2024-01-03") and len(out) == 3
    assert last["open"] == 102.0 and last["close"] == pytest.approx(102.0 + 77 * 0.1)  # not the after-hours 120
    assert last["high"] == pytest.approx(103.0 + 77 * 0.1) and last["volume"] == 78 * 1000.0
    # session still running, or the daily data already has the day: unchanged
    assert len(fetch.complete_last_session(daily, bars, utc("2024-01-03 19:00"))) == 2
    assert fetch.complete_last_session(out, bars, utc("2024-01-04 01:00")) is out


def test_keep_newer_cached():
    cached = make_ohlcv([1.0, 2.0, 3.0, 4.0])
    behind = make_ohlcv([1.5, 2.5, 3.5])  # same dates, one bar short (and newly adjusted values)
    merged = fetch.keep_newer_cached(behind, cached)
    assert list(merged["close"]) == [1.5, 2.5, 3.5, 4.0]
    assert fetch.keep_newer_cached(cached, behind) is cached  # fresh is ahead: use it as is
