"""Alert policy: which unusual moves reach Telegram, when, and how they read.

A move is unusual when it is several times the instrument's typical daily move
(sigma), over the last 24 hours or within the last hour. In order: repeats are
suppressed (the same instrument, window and direction alerts again only after
repeat_hours, or sooner if the move grows by `escalation`), quiet hours and
/mute hold everything except critical moves on critical_symbols, a daily budget
caps the noise, and shadow mode records instead of sending. All hits of one
check go out as one message.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from pipeline import clock, telegram
from pipeline.config import AlertsCfg
from pipeline.markets import Quote
from pipeline.state import State

WINDOW_LABEL = {"hour": "in 1h", "day": "in 24h"}


@dataclass
class Hit:
    quote: Quote
    window: str  # hour | day
    move: float  # %
    ratio: float  # |move| / typical daily move
    critical: bool

    @property
    def key(self) -> str:
        return f"{self.quote.symbol}:{self.window}:{'up' if self.move > 0 else 'down'}"


def find_hits(quotes: list[Quote], cfg: AlertsCfg) -> list[Hit]:
    hits = []
    for q in quotes:
        if not q.live or not q.sigma:
            continue
        for window, move, need, critical in (
            ("hour", q.hour, cfg.hour_sigma, cfg.critical_hour_sigma),
            ("day", q.day, cfg.day_sigma, cfg.critical_day_sigma),
        ):
            if move is None:
                continue
            ratio = abs(move) / q.sigma
            if ratio >= need:
                hits.append(Hit(q, window, move, ratio, ratio >= critical and q.symbol in cfg.critical_symbols))
    return hits


@dataclass
class Decision:
    status: str  # send | held | shadow
    reason: str = ""


class Policy:
    def __init__(self, cfg: AlertsCfg, state: State):
        self.cfg, self.state = cfg, state

    @property
    def memory(self) -> dict:
        return self.state.data.setdefault("alert_memory", {})

    def is_repeat(self, h: Hit, now: datetime) -> bool:
        last = self.memory.get(h.key)
        if not last:
            return False
        hours = self.cfg.repeat_hours_hour if h.window == "hour" else self.cfg.repeat_hours_day
        recent = now - datetime.fromisoformat(last["at"]) < timedelta(hours=hours)
        return recent and abs(h.move) < self.cfg.escalation * last["move"]

    def new_hits(self, hits: list[Hit], now: datetime) -> list[Hit]:
        return [h for h in hits if not self.is_repeat(h, now)]

    def sent_today(self, now: datetime) -> int:
        since = clock.local_day_start(now, self.cfg.quiet_hours.timezone)
        return sum(1 for a in self.state.items_since("alerts", since) if a["status"] in ("sent", "shadow"))

    def gate(self, hits: list[Hit], now: datetime, *, shadow: bool) -> Decision:
        critical = any(h.critical for h in hits)
        if not critical and clock.in_window(now, self.cfg.quiet_hours):
            return Decision("held", "quiet hours")
        mute = self.state.get("mute_until")
        if not critical and mute and now < datetime.fromisoformat(mute):
            return Decision("held", "muted")
        if not critical and self.sent_today(now) >= self.cfg.daily_budget:
            return Decision("held", f"daily budget of {self.cfg.daily_budget} reached")
        return Decision("shadow" if shadow else "send")

    def remember(self, hits: list[Hit], now: datetime) -> None:
        for h in hits:
            self.memory[h.key] = {"at": now.isoformat(), "move": round(abs(h.move), 3)}
        self.state.save()


def hit_lines(hits: list[Hit]) -> list[str]:
    """One plain-text line per instrument, strongest first: 'Nasdaq 100 fut -2.1% in 1h, -3.4% in 24h ...'."""
    by_symbol: dict[str, list[Hit]] = {}
    for h in hits:
        by_symbol.setdefault(h.quote.symbol, []).append(h)
    groups = sorted(by_symbol.values(), key=lambda hs: -max(h.ratio for h in hs))
    lines = []
    for hs in groups:
        q = hs[0].quote
        hs = sorted(hs, key=lambda h: h.window != "hour")  # 1h first, then 24h
        moves = ", ".join(q.change(h.move, WINDOW_LABEL[h.window]) for h in hs)
        ratio = max(h.ratio for h in hs)
        arrow = "📈" if hs[0].move > 0 else "📉"
        lines.append(f"{arrow} {q.name} {moves} · at {q.level()} · {ratio:.1f}× a normal day")
    return lines


def context_line(quotes: list[Quote], symbols: list[str], skip: set[str]) -> str:
    """The rest of the market at a glance, from the configured context symbols."""
    by = {q.symbol: q for q in quotes}
    parts = []
    for s in symbols:
        q = by.get(s)
        if q is None or s in skip:
            continue
        parts.append(f"{q.name} {q.level()} ({q.change()})" if s == "^VIX" else f"{q.name} {q.change()}")
    return " · ".join(parts)


def format_alert(hits: list[Hit], quotes: list[Quote], now: datetime, cfg: AlertsCfg) -> str:
    """Telegram HTML; every outside text is escaped."""
    esc = telegram.esc
    local = now.astimezone(ZoneInfo(cfg.quiet_hours.timezone)).strftime("%H:%M")
    critical = any(h.critical for h in hits)
    head = "🚨 <b>Market shock</b>" if critical else "⚡ <b>Unusual market move</b>"
    lines = [f"{head} · {local}", *(esc(line) for line in hit_lines(hits))]
    context = context_line(quotes, cfg.context, {h.quote.symbol for h in hits})
    if context:
        lines.append(f"<i>24h: {esc(context)}</i>")
    if critical:
        lines.append("<i>Very large move: this one comes through quiet hours.</i>")
    return "\n".join(lines)
