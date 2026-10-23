"""Time logic in real time zones, never fixed offsets.

Prague and New York switch daylight-saving time on different dates (about three
weeks a year are mismatched), so every schedule is computed in its own zone
with zoneinfo and compared in UTC.
"""

from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline.config import DigestTime, TimeWindow

NY = ZoneInfo("America/New_York")
DAYS = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


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


def next_occurrence(entry: DigestTime, after_utc: datetime) -> datetime | None:
    found = occurrences(entry, after_utc + timedelta(microseconds=1), after_utc + timedelta(days=8))
    return found[0] if found else None


def last_occurrence(entry: DigestTime, now_utc: datetime) -> datetime | None:
    found = occurrences(entry, now_utc - timedelta(days=8), now_utc)
    return found[-1] if found else None


def in_window(now_utc: datetime, w: TimeWindow) -> bool:
    """True if the local time in the window's zone is within [start, end) on one of its days.

    A window may cross midnight; its day is the day it started on."""
    local = now_utc.astimezone(ZoneInfo(w.timezone))
    a, b, t = _hhmm(w.start), _hhmm(w.end), local.time()
    if a <= b:
        return a <= t < b and DAYS[local.weekday()] in w.days
    if t >= a:
        return DAYS[local.weekday()] in w.days
    return t < b and DAYS[(local.weekday() - 1) % 7] in w.days


def local_day_start(now_utc: datetime, tz: str) -> datetime:
    """Local midnight of `now` in `tz`, in UTC (budgets and counts follow the user's local day)."""
    zone = ZoneInfo(tz)
    return datetime.combine(now_utc.astimezone(zone).date(), time.min, zone).astimezone(timezone.utc)
