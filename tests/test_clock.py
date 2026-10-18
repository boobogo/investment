"""DST-sensitive schedule tests. In 2026 Europe falls back on Oct 25 and the US on Nov 1,
so Oct 26-30 is a mismatch week: New York is 5 hours behind Prague instead of 6."""

from datetime import datetime, timezone

import pytest

from pipeline import clock
from pipeline.config import DigestsCfg

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
    "when,session",
    [
        ("2026-10-23T12:00", "pre"),  # 08:00 New York
        ("2026-10-23T13:30", "regular"),  # 09:30 New York (summer)
        ("2026-11-02T14:29", "pre"),  # 09:29 New York (winter)
        ("2026-11-02T14:30", "regular"),
        ("2026-10-23T20:30", "post"),  # 16:30 New York
        ("2026-10-24T15:00", "closed"),  # Saturday
    ],
)
def test_us_session(when, session):
    assert clock.us_session(utc(when)) == session


def test_quiet_hours_cross_midnight_and_dst():
    quiet = lambda s: clock.in_window(utc(s), "23:00", "07:00", "Europe/Prague")  # noqa: E731
    assert quiet("2026-10-24T21:00")  # 23:00 CEST
    assert not quiet("2026-10-25T21:30")  # 22:30 CET (after the change, 21:30 UTC is no longer 23:30)
    assert quiet("2026-10-25T22:00")  # 23:00 CET
    assert quiet("2026-10-26T05:59")  # 06:59 CET
    assert not quiet("2026-10-26T06:00")  # 07:00 CET
