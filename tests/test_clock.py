"""DST-sensitive schedule tests. In 2026 Europe falls back on Oct 25 and the US on Nov 1,
so Oct 26-30 is a mismatch week: New York is 5 hours behind Prague instead of 6."""

from datetime import datetime, timezone

import pytest

from pipeline import clock
from pipeline.config import DigestsCfg, TimeWindow, WatchCfg

CFG = DigestsCfg()


def utc(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


@pytest.mark.parametrize(
    "after,expected",
    [
        ("2026-10-22T14:00", "2026-10-23T13:00"),  # both on summer time: 09:00 NY = 15:00 Prague = 13:00 UTC
        ("2026-10-23T14:00", "2026-10-26T13:00"),  # skips the weekend; mismatch week: still 13:00 UTC = 14:00 Prague
        ("2026-10-30T14:00", "2026-11-02T14:00"),  # both on winter time: 14:00 UTC = 15:00 Prague
    ],
)
def test_preopen_follows_new_york(after, expected):
    assert clock.next_occurrence(CFG.preopen, utc(after)) == utc(expected)


@pytest.mark.parametrize(
    "after,expected",
    [
        ("2026-10-23T06:00", "2026-10-26T06:30"),  # Friday 07:30 CEST = 05:30 UTC already passed; Monday 07:30 CET = 06:30 UTC
        ("2026-10-22T05:00", "2026-10-22T05:30"),  # summer time: 07:30 Prague = 05:30 UTC
    ],
)
def test_morning_follows_prague(after, expected):
    assert clock.next_occurrence(CFG.morning, utc(after)) == utc(expected)


def test_weekly_on_sunday_and_last_occurrence():
    assert clock.next_occurrence(CFG.weekly, utc("2026-10-19T00:00")) == utc("2026-10-25T18:00")  # 19:00 CET
    assert clock.last_occurrence(CFG.weekly, utc("2026-10-20T00:00")) == utc("2026-10-18T17:00")  # 19:00 CEST


def test_occurrences_in_range():
    got = clock.occurrences(CFG.morning, utc("2026-10-19T00:00"), utc("2026-10-25T23:59"))
    assert [t.day for t in got] == [19, 20, 21, 22, 23]


@pytest.mark.parametrize(
    "when,open_",
    [
        ("2026-10-23T06:59", False),  # 08:59 Berlin (summer)
        ("2026-10-23T07:00", True),  # 09:00 Berlin: Xetra opens
        ("2026-10-23T15:29", True),  # 17:29 Berlin
        ("2026-10-23T16:00", True),  # 12:00 New York
        ("2026-10-23T19:59", True),  # 15:59 New York
        ("2026-10-23T20:00", False),  # 16:00 New York: closed
        ("2026-10-26T13:30", True),  # mismatch week: 14:30 Berlin (winter) and 09:30 New York (summer)
        ("2026-10-26T15:30", True),  # 16:30 Berlin
        ("2026-10-26T20:00", False),  # 16:00 New York
        ("2026-10-24T12:00", False),  # Saturday
    ],
)
def test_market_hours(when, open_):
    hours = WatchCfg().market_hours
    assert any(clock.in_window(utc(when), w) for w in hours) == open_


def test_quiet_hours_cross_midnight_and_dst():
    window = TimeWindow(start="23:00", end="07:00", timezone="Europe/Prague")
    quiet = lambda s: clock.in_window(utc(s), window)  # noqa: E731
    assert quiet("2026-10-24T21:00")  # 23:00 CEST
    assert not quiet("2026-10-25T21:30")  # 22:30 CET (after the change, 21:30 UTC is no longer 23:30)
    assert quiet("2026-10-25T22:00")  # 23:00 CET
    assert quiet("2026-10-26T05:59")  # 06:59 CET
    assert not quiet("2026-10-26T06:00")  # 07:00 CET


def test_window_days_follow_the_start_day():
    fri_night = TimeWindow(start="22:00", end="02:00", timezone="Europe/Prague", days=["fri"])
    assert clock.in_window(utc("2026-10-23T21:00"), fri_night)  # Friday 23:00
    assert clock.in_window(utc("2026-10-23T23:30"), fri_night)  # Saturday 01:30, still Friday's window
    assert not clock.in_window(utc("2026-10-24T21:00"), fri_night)  # Saturday 23:00


def test_local_day_start():
    assert clock.local_day_start(utc("2026-10-23T12:00"), "Europe/Prague") == utc("2026-10-22T22:00")
    assert clock.local_day_start(utc("2026-10-26T12:00"), "Europe/Prague") == utc("2026-10-25T23:00")
