from datetime import timedelta

import pandas as pd
import pytest

from pipeline import markets
from tests.conftest import bars, quote, utc


def test_day_and_hour_moves():
    # 25 hours of 5-minute bars at 100, then the last hour climbs to 103
    closes = bars("2026-10-22T12:00", [100] * 289 + [101, 102, 103])
    m = markets.measure(closes)
    assert m["price"] == 103 and m["at"] == closes.index[-1]
    assert m["day"] == pytest.approx(3.0) and m["ref_day"] == 100
    assert m["hour"] == pytest.approx(3.0)


def test_day_reference_across_a_closed_market():
    # Friday session, then nothing until Monday: Monday's 24h reference is Friday's last price
    friday = bars("2026-10-23T07:00", [100] * 101 + [110])  # closes at 110
    monday = bars("2026-10-26T08:00", [99, 98, 99])
    m = markets.measure(pd.concat([friday, monday]))
    assert m["ref_day"] == 110 and m["day"] == pytest.approx(-10.0)


def test_no_hour_move_right_after_an_open():
    # the market opened 30 minutes ago: an hour back there is only last night's close, not a 1h move
    closes = pd.concat([bars("2026-10-22T07:00", [100] * 12), bars("2026-10-23T07:00", [105] * 7)])
    m = markets.measure(closes)
    assert m["hour"] is None and m["day"] == pytest.approx(5.0)


def test_empty_and_short_history():
    assert markets.measure(pd.Series(dtype=float)) is None
    m = markets.measure(bars("2026-10-23T10:00", [100, 101]))
    assert m["day"] is None and m["hour"] is None


def test_daily_sigma():
    closes = pd.Series([100, 101, 100, 101, 100] * 10, dtype=float)
    assert markets.daily_sigma(closes, 250) == pytest.approx(closes.pct_change().std() * 100)
    assert markets.daily_sigma(closes.head(10), 250) is None  # too little history


def test_snapshot_marks_closed_markets():
    now = utc("2026-10-23T14:00")
    closes = {
        "ES=F": bars("2026-10-22T12:00", [100] * 312),  # last bar 13:55: trading
        "^GDAXI": bars("2026-10-22T07:00", [100] * 100),  # last bar yesterday: closed
    }
    names = {"ES=F": "S&P 500 fut", "^GDAXI": "DAX", "^N225": "Nikkei"}
    groups = {"ES=F": "US", "^GDAXI": "Europe", "^N225": "Asia"}
    quotes = markets.snapshot(closes, {"ES=F": 0.8}, names, groups, now, timedelta(minutes=45))
    assert [(q.symbol, q.live, q.sigma) for q in quotes] == [("ES=F", True, 0.8), ("^GDAXI", False, None)]


def test_split_handles_multi_and_single_symbol_frames():
    idx = pd.date_range("2026-10-23 10:00", periods=3, freq="5min", tz="America/New_York")
    multi = pd.concat({"ES=F": pd.DataFrame({"Close": [1.0, 2.0, 3.0]}, index=idx),
                       "GC=F": pd.DataFrame({"Close": [None, None, None]}, index=idx)}, axis=1)  # fmt: skip
    out = markets._split(multi, ["ES=F", "GC=F", "CL=F"])
    assert list(out) == ["ES=F"] and str(out["ES=F"].index.tz) == "UTC"
    single = pd.DataFrame({"Close": [1.0, 2.0, 3.0]}, index=idx)
    assert list(markets._split(single, ["ES=F"])) == ["ES=F"]


def test_levels_and_changes_read_naturally():
    assert quote(price=29050.25).level() == "29,050"
    assert quote(price=99.164).level() == "99.16"
    assert quote(symbol="EURUSD=X", price=1.12345).level() == "1.1235"
    tnx = quote(symbol="^TNX", price=4.95, day=-2.5)
    assert tnx.level() == "4.95%" and tnx.change() == "-13 bp"  # 5.077 -> 4.95
    assert quote(day=-2.04).change(window="in 24h") == "-2.0% in 24h"
    assert quote(day=None).change() == "n/a"
