"""Alert policy: which detector events (and triaged news) reach Telegram, when, and how they read.

In order: repeats are suppressed (cooldown for sudden moves, once a day for day-level
moves, unless the move grows by escalation_factor), quiet hours and /mute hold
non-critical alerts for the morning digest, a daily budget caps the noise, and
shadow mode logs instead of sending. Every non-repeat event is recorded in
price_events, and every decision except "repeat" in the alerts table.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline import clock, storage, telegram
from pipeline.config import AlertsCfg
from pipeline.detect import Event

log = logging.getLogger(__name__)

STATE_KEY = "alert_memory"  # watch_state: last alert per event key, survives restarts
ICONS = {"window": "⚡", "day": "⚡", "ext": "🌙", "index_window": "📉", "index_day": "📉", "vix": "⚠️", "futures": "🌙"}


def day_start(now: datetime, tz: str) -> datetime:
    """Local midnight of `now` in `tz`, in UTC (a "day" for budgets and counts is the user's local day)."""
    zone = ZoneInfo(tz)
    return datetime.combine(now.astimezone(zone).date(), datetime.min.time(), zone).astimezone(timezone.utc)


def counts_today(conn: sqlite3.Connection, now: datetime, tz: str) -> dict[str, int]:
    """Instant alerts (price and news) recorded today, by status."""
    rows = conn.execute(
        "SELECT status, count(*) FROM alerts WHERE kind IN ('price', 'news') AND created_at >= ? GROUP BY status",
        (day_start(now, tz).isoformat(timespec="seconds"),),
    )
    return {status: n for status, n in rows}


@dataclass
class Decision:
    status: str  # repeat | held | shadow | send
    reason: str = ""


class AlertPolicy:
    def __init__(self, cfg: AlertsCfg, conn: sqlite3.Connection):
        self.cfg, self.conn = cfg, conn
        self.memory: dict[str, dict] = json.loads(storage.get_state(conn, STATE_KEY, "{}"))

    def _local_day(self, now: datetime) -> str:
        return now.astimezone(ZoneInfo(self.cfg.quiet_hours.timezone)).date().isoformat()

    def is_repeat(self, e: Event, now: datetime) -> bool:
        last = self.memory.get(e.key)
        if e.kind == "day" and not last:
            # the same move already reported after hours or pre-market (typically earnings): don't report it again
            ext = self.memory.get(f"{e.symbol}:ext:{e.direction}")
            if ext and now - datetime.fromisoformat(ext["at"]) < timedelta(hours=24):
                return abs(e.move_pct) < self.cfg.escalation_factor * ext["move"]
        if not last:
            return False
        grew = abs(e.move_pct) >= self.cfg.escalation_factor * last["move"]
        if e.level:  # change since the previous close: once per US trading day, unless it grows
            return last["day"] == clock.ny_date(now).isoformat() and not grew
        recent = now - datetime.fromisoformat(last["at"]) < timedelta(minutes=self.cfg.cooldown_minutes)
        return recent and not grew

    def sent_today(self, now: datetime) -> int:
        """Alerts sent since local midnight (shadow alerts count too, so shadow numbers are realistic)."""
        counts = counts_today(self.conn, now, self.cfg.quiet_hours.timezone)
        return counts.get("sent", 0) + counts.get("shadow", 0)

    def decide(self, e: Event, now: datetime, *, shadow: bool) -> Decision:
        if self.is_repeat(e, now):
            return Decision("repeat")
        return self.gate(e.severity, now, shadow=shadow)

    def gate(self, severity: str, now: datetime, *, shadow: bool) -> Decision:
        """Quiet hours, mute and the daily budget (shared by price and news alerts)."""
        critical = severity == "critical"
        q = self.cfg.quiet_hours
        if not critical and clock.in_window(now, q.start, q.end, q.timezone):
            return Decision("held", "quiet hours")
        mute = storage.get_state(self.conn, "mute_until")
        if not critical and mute and now < datetime.fromisoformat(mute):
            return Decision("held", "muted")
        if not critical and self.sent_today(now) >= self.cfg.daily_budget:
            return Decision("held", f"daily budget of {self.cfg.daily_budget} reached")
        return Decision("shadow" if shadow else "send")

    def remember(self, e: Event, now: datetime) -> None:
        self.memory[e.key] = {"at": now.isoformat(), "move": abs(e.move_pct), "day": clock.ny_date(now).isoformat()}
        storage.set_state(self.conn, STATE_KEY, json.dumps(self.memory))


def display_name(symbol: str) -> str:
    return {"^VIX": "VIX", "ES=F": "S&P 500 futures", "NQ=F": "Nasdaq-100 futures"}.get(symbol, symbol)


def format_alert(e: Event, now: datetime, tz: str, day_pct: float | None = None) -> str:
    """Telegram HTML for one event; any outside text is escaped."""
    esc = telegram.esc
    local = now.astimezone(ZoneInfo(tz)).strftime("%H:%M")
    icon = "🚨" if e.severity == "critical" else ICONS.get(e.kind, "⚡")
    head = f"{icon} <b>{esc(display_name(e.symbol))}</b> {esc(e.label)}"
    facts = []
    if e.price is not None and e.kind not in ("vix",):
        facts.append(f"${e.price:,.2f}" if not e.symbol.endswith("=F") else f"{e.price:,.2f}")
    if day_pct is not None and e.kind in ("window", "index_window"):
        facts.append(f"day {day_pct:+.1f}%")
    facts.append(local)
    tail = "\n<i>Market-wide drop: this one ignores quiet hours.</i>" if e.severity == "critical" else ""
    return f"{head}\n{' · '.join(facts)}{tail}"


def record(
    conn: sqlite3.Connection, e: Event, decision: Decision, text: str, now: datetime, message_id: int | None = None
) -> int:
    """Store the event and the alert decision at the detection time; returns the alerts row id."""
    at = now.astimezone(timezone.utc).isoformat(timespec="seconds")
    with conn:
        cur = conn.execute(
            "INSERT INTO price_events (detected_at, symbol, kind, severity, move_pct, ratio, price, details_json)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (at, e.symbol, e.kind, e.severity, round(e.move_pct, 3),
             None if e.ratio is None else round(e.ratio, 2), e.price, json.dumps({"label": e.label, "key": e.key})),
        )  # fmt: skip
    return storage.log_alert(
        conn, created_at=at, kind="price", severity=e.severity, symbol=e.symbol, text=text,
        status={"send": "sent"}.get(decision.status, decision.status), reason=decision.reason or None,
        telegram_message_id=message_id, event_id=cur.lastrowid,
    )  # fmt: skip
