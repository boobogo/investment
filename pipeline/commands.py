"""Telegram commands for the watcher (/status, /mute, /why, ...).

Only messages from TELEGRAM_CHAT_ID reach `Commands.handle` (the watcher checks the
chat before calling it). Arguments are validated; everything shown back is escaped.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipeline import alerts, storage, telegram

esc = telegram.esc

HELP = """<b>Commands</b>
/status - is the watcher healthy, market session, alerts and model use today
/mute 2h - pause non-critical alerts (e.g. 30m, 2h, 1d); crashes still come through
/unmute - alerts back on
/digest - build and send a digest now (uses Claude)
/why NVDA - what's going on with a watchlist stock right now (one Claude call)
/news NVDA - latest headlines for a stock; /news alone: market news (no Claude)
/alerts - today's alerts, including held and shadow ones
/report - the latest full report as a file
/watchlist - live prices for everything watched
/help - this list"""

DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([mhd])$")
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
MAX_MUTE = timedelta(days=7)


def parse_command(text: str) -> tuple[str, list[str]] | None:
    """'/why@boobogo_bot nvda' -> ('why', ['NVDA'])."""
    text = (text or "").strip()
    if not text.startswith("/"):
        return None
    parts = text.split()
    return parts[0][1:].split("@")[0].lower(), [a.upper() for a in parts[1:]]


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
    """`w` is the running Watcher (status, live prices, digest launcher, /why runner)."""

    def __init__(self, watcher):
        self.w = watcher
        self.tz = watcher.acfg.quiet_hours.timezone

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

    async def cmd_mute(self, args, now) -> str:
        span = parse_duration(args[0] if args else "2h")
        if span is None or span <= timedelta(0) or span > MAX_MUTE:
            return "Usage: /mute 30m, /mute 2h or /mute 1d (at most 7 days)."
        until = now + span
        storage.set_state(self.w.conn, "mute_until", until.isoformat())
        return (f"🔕 Non-critical alerts muted until {local_time(until, self.tz, now)}. "
                "Crashes and market-wide emergencies still come through. /unmute to undo.")  # fmt: skip

    async def cmd_unmute(self, args, now) -> str:
        storage.set_state(self.w.conn, "mute_until", now.isoformat())
        return "🔔 Alerts are back on."

    async def cmd_digest(self, args, now) -> str:
        if not await self.w.launch_digest("manual", ["--highlights"], reason="/digest"):
            return "A digest is already being built; it will arrive shortly."
        return "🛠 Building a digest now; it arrives in a minute or two."

    async def cmd_why(self, args, now) -> str:
        if not args or not TICKER_RE.match(args[0]):
            return "Usage: /why NVDA (a ticker from your watchlist)."
        symbol = args[0]
        if symbol not in {t.symbol for t in self.w.watchlist}:
            return f"{esc(symbol)} is not on the watchlist (add it to config/tickers.yaml on the Pi)."
        if not self.w.start_why(symbol):
            return f"Already looking into {esc(symbol)}."
        return f"🔍 Looking at {esc(symbol)}: one Claude call, about half a minute."

    async def cmd_news(self, args, now) -> str:
        since = (now - timedelta(hours=48)).isoformat(timespec="seconds")
        if args:
            if not TICKER_RE.match(args[0]):
                return "Usage: /news NVDA, or /news alone for market news."
            symbol = args[0]
            rows = [r for r in storage.news_for_event(self.w.conn, since, symbol)][-8:]
            title = f"Latest {esc(symbol)} headlines"
        else:
            rows = [r for r in storage.news_for_event(self.w.conn, since, None) if r["category"] == "general"][-8:]
            title = "Latest market headlines"
        if not rows:
            return f"{title}: nothing in the last 48 hours."
        lines = [f"<b>{title}</b>"]
        for r in reversed(rows):
            when = local_time(datetime.fromisoformat(r["published_at"]), self.tz, now)
            judged = json.loads(r["triage_json"]) if r.get("triage_json") else None
            impact = f" · impact {judged['impact']}/5" if judged else ""
            lines.append(f"• {when} {esc(r['source'] or '')}: {esc(r['headline'])}{impact}")
        return "\n".join(lines)

    async def cmd_alerts(self, args, now) -> str:
        rows = self.w.conn.execute(
            "SELECT created_at, kind, status, reason, text FROM alerts WHERE kind IN ('price', 'news', 'follow_up')"
            " AND created_at >= ? ORDER BY created_at",
            (alerts.day_start(now, self.tz).isoformat(timespec="seconds"),),
        ).fetchall()
        if not rows:
            return "No alerts today."
        lines = [f"<b>Alerts today ({len(rows)})</b>"]
        for r in rows[-15:]:
            first = re.sub(r"<[^>]+>", "", r["text"].splitlines()[0])
            when = local_time(datetime.fromisoformat(r["created_at"]), self.tz, now)
            note = f" ({esc(r['reason'])})" if r["reason"] else ""
            lines.append(f"• {when} [{r['status']}{note}] {esc(first)}")
        return "\n".join(lines)

    async def cmd_report(self, args, now) -> str | None:
        latest = self.w.settings.paths.reports / "latest.html"
        if not latest.exists():
            return "No report yet. /digest builds one."
        await self.w.send_document(latest, "Latest full report")
        return None

    async def cmd_watchlist(self, args, now) -> str:
        return self.w.watchlist_report()
