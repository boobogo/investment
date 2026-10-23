"""Telegram commands for the watcher (/status, /markets, /mute, /digest, ...).

Only messages from TELEGRAM_CHAT_ID reach `Commands.handle` (the watcher checks the
chat before calling it). Arguments are validated; everything shown back is escaped.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipeline import telegram

esc = telegram.esc

HELP = """<b>Commands</b>
/status - is the watcher healthy, alerts and Claude calls today, next digest
/markets - the watched instruments now (24h change)
/alerts - today's alerts, including held ones
/mute 2h - pause alerts (e.g. 30m, 2h, 1d); very large moves still come through
/unmute - alerts back on
/digest - a news digest now (one of today's Claude calls)
/help - this list"""

DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([mhd])$")
MAX_MUTE = timedelta(days=7)


def parse_command(text: str) -> tuple[str, list[str]] | None:
    """'/mute@boobogo_bot 2h' -> ('mute', ['2h'])."""
    text = (text or "").strip()
    if not text.startswith("/"):
        return None
    parts = text.split()
    return parts[0][1:].split("@")[0].lower(), parts[1:]


def parse_duration(arg: str) -> timedelta | None:
    m = DURATION_RE.match(arg.strip().lower())
    if not m:
        return None
    value, unit = float(m.group(1)), m.group(2)
    return timedelta(minutes=value) if unit == "m" else timedelta(hours=value) if unit == "h" else timedelta(days=value)


def local_time(when: datetime, tz: str, now: datetime) -> str:
    local, today = when.astimezone(ZoneInfo(tz)), now.astimezone(ZoneInfo(tz)).date()
    return local.strftime("%H:%M") if local.date() == today else local.strftime("%a %d %b %H:%M")


class Commands:
    """`w` is the running Watcher (status, prices, state, digest launcher)."""

    def __init__(self, watcher):
        self.w = watcher

    async def handle(self, text: str, now: datetime) -> str | None:
        parsed = parse_command(text)
        if parsed is None:
            return "Send /help for the list of commands."
        cmd, args = parsed
        handler = getattr(self, f"cmd_{cmd}", None)
        if handler is None:
            return f"Unknown command /{esc(cmd)}. Send /help for the list."
        return await handler(args, now)

    async def cmd_help(self, args, now) -> str:
        return HELP

    async def cmd_start(self, args, now) -> str:  # Telegram sends /start when a chat opens
        return HELP

    async def cmd_status(self, args, now) -> str:
        return self.w.status_report(now)

    async def cmd_markets(self, args, now) -> str:
        return self.w.markets_report()

    async def cmd_alerts(self, args, now) -> str:
        return self.w.alerts_report(now)

    async def cmd_mute(self, args, now) -> str:
        span = parse_duration(args[0] if args else "2h")
        if span is None or span <= timedelta(0) or span > MAX_MUTE:
            return "Usage: /mute 30m, /mute 2h or /mute 1d (at most 7 days)."
        until = now + span
        self.w.state.set("mute_until", until.isoformat())
        return (f"🔕 Alerts muted until {local_time(until, self.w.tz, now)}. "
                "Very large moves still come through. /unmute to undo.")  # fmt: skip

    async def cmd_unmute(self, args, now) -> str:
        self.w.state.set("mute_until", now.isoformat())
        return "🔔 Alerts are back on."

    async def cmd_digest(self, args, now) -> str:
        if self.w.extra_left(now) <= 0:
            return "No Claude calls left today (the scheduled digests are reserved). The next digest comes on schedule."
        if not self.w.start_manual_digest(now):
            return "Claude is busy with another request; try again in a few minutes."
        return "🛠 Researching the news now; the digest arrives in a few minutes."
