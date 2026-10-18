"""Time logic in real time zones, never fixed offsets.

Prague and New York switch daylight-saving time on different dates (about three
weeks a year are mismatched), so every schedule is computed in its own zone
with zoneinfo and compared in UTC.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline.config import DigestTime

NY = ZoneInfo("America/New_York")
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]

# US equity sessions, New York time
PRE_OPEN, REGULAR_OPEN, REGULAR_CLOSE, POST_CLOSE = time(4, 0), time(9, 30), time(16, 0), time(20, 0)


def _hhmm(s: str) -> time:
    h, m = s.split(":")
    return time(int(h), int(m))


def occurrences(entry: DigestTime, start_utc: datetime, end_utc: datetime) -> list[datetime]:
    """Scheduled times (UTC) of a digest in [start, end]."""
    tz = ZoneInfo(entry.timezone)
    at = _hhmm(entry.time)
    out = []
    d, last = start_utc.astimezone(tz).date(), end_utc.astimezone(tz).date()
    while d <= last:
        if DAYS[d.weekday()] in entry.days:
            t = datetime.combine(d, at, tzinfo=tz).astimezone(timezone.utc)
            if start_utc <= t <= end_utc:
                out.append(t)
        d += timedelta(days=1)
    return out


def next_occurrence(entry: DigestTime, after_utc: datetime) -> datetime:
    found = occurrences(entry, after_utc + timedelta(microseconds=1), after_utc + timedelta(days=8))
    return found[0]


def last_occurrence(entry: DigestTime, now_utc: datetime) -> datetime | None:
    found = occurrences(entry, now_utc - timedelta(days=8), now_utc)
    return found[-1] if found else None


def us_session(now_utc: datetime) -> str:
    """'pre' | 'regular' | 'post' | 'closed' from the New York clock (holidays are checked separately)."""
    ny = now_utc.astimezone(NY)
    if ny.weekday() >= 5:
        return "closed"
    t = ny.time()
    if PRE_OPEN <= t < REGULAR_OPEN:
        return "pre"
    if REGULAR_OPEN <= t < REGULAR_CLOSE:
        return "regular"
    if REGULAR_CLOSE <= t < POST_CLOSE:
        return "post"
    return "closed"


def futures_open(now_utc: datetime) -> bool:
    """CME equity-index futures: Sunday 18:00 to Friday 17:00 New York time, with a daily 17:00-18:00 break.
    (Exchange holidays are not modelled; a stale quote on a holiday just doesn't move.)"""
    ny = now_utc.astimezone(NY)
    t, wd = ny.time(), ny.weekday()  # Monday = 0
    if wd == 5 or (wd == 4 and t >= time(17, 0)) or (wd == 6 and t < time(18, 0)):
        return False
    return not (time(17, 0) <= t < time(18, 0))


def ny_date(now_utc: datetime) -> date:
    return now_utc.astimezone(NY).date()


def in_window(now_utc: datetime, start: str, end: str, tz: str) -> bool:
    """True if the local time in `tz` is within [start, end); the window may cross midnight."""
    local = now_utc.astimezone(ZoneInfo(tz)).time()
    a, b = _hhmm(start), _hhmm(end)
    return a <= local < b if a <= b else (local >= a or local < b)
