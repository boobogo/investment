"""What Claude is asked (digests and move explanations) and how the answers read in Telegram.

Claude researches with web search and answers in JSON (schemas below); we turn
that into Telegram HTML, escaping every piece of text. The market snapshot under
each digest comes from our own price data, not from Claude.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from pipeline import clock, telegram
from pipeline.markets import Quote

esc = telegram.esc

TITLES = {"morning": "Morning digest", "preopen": "Pre-open digest", "weekly": "Week ahead", "manual": "Market digest"}

_TEXT = {"type": "string"}
DIGEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["headline", "markets", "stories", "stretched", "calendar", "watch"],
    "properties": {
        "headline": _TEXT,
        "markets": {"type": "array", "items": _TEXT},
        "stories": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["title", "why", "source", "time"],
                "properties": {"title": _TEXT, "why": _TEXT, "source": _TEXT, "time": _TEXT},
            },
        },
        "stretched": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["symbol", "state", "evidence", "source"],
                "properties": {
                    "symbol": _TEXT,
                    "state": {"type": "string", "enum": ["overbought", "oversold"]},
                    "evidence": _TEXT,
                    "source": _TEXT,
                },
            },
        },
        "calendar": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["when", "event"],
                "properties": {"when": _TEXT, "event": _TEXT},
            },
        },
        "watch": _TEXT,
    },
}

EXPLAIN_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["cause", "confidence", "details", "sources", "watch"],
    "properties": {
        "cause": _TEXT,
        "confidence": {"type": "string", "enum": ["high", "medium", "low"]},
        "details": _TEXT,
        "sources": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["outlet", "time", "headline"],
                "properties": {"outlet": _TEXT, "time": _TEXT, "headline": _TEXT},
            },
        },
        "watch": _TEXT,
    },
}


def load_prompt(prompts_dir: Path, name: str) -> str:
    return (prompts_dir / f"{name}.md").read_text()


def when_text(t: datetime, tz: str) -> str:
    """'Friday 23 October 2026, 07:30 Prague (01:30 New York)'."""
    local, ny = t.astimezone(ZoneInfo(tz)), t.astimezone(clock.NY)
    city = tz.split("/")[-1].replace("_", " ")
    return f"{local:%A %d %B %Y, %H:%M} {city} ({ny:%H:%M} New York)"


def snapshot_lines(quotes: list[Quote]) -> list[str]:
    """Plain text, one line per group: 'Europe: DAX 25,372 (-0.4%) · FTSE 100 10,725 (closed, +0.2%)'."""
    groups: dict[str, list[str]] = {}
    for q in quotes:
        closed = "" if q.live else "closed, "
        groups.setdefault(q.group, []).append(f"{q.name} {q.level()} ({closed}{q.change()})")
    return [f"{g}: {' · '.join(items)}" for g, items in groups.items()]


def digest_request(
    kind: str, now: datetime, since: datetime | None, quotes: list[Quote], moves: list[str], follow: list[str], tz: str
) -> str:
    lines = [f"Digest kind: {kind}", f"Now: {when_text(now, tz)}."]
    if since:
        lines.append(f"Window: news since {when_text(since, tz)}, the previous digest.")
    lines += ["", "Market snapshot (24h changes; Yahoo Finance 5-minute data, up to ~15 minutes delayed; "
              "'closed' = no trades in the last 45 minutes):", *snapshot_lines(quotes)]  # fmt: skip
    lines += ["", "Unusual moves our watcher flagged in the window:", *(moves or ["none"])]
    if follow:
        lines += ["", f"Followed stocks: {', '.join(follow)}"]
    return "\n".join(lines)


def explain_request(alert_lines: list[str], quotes: list[Quote], now: datetime, tz: str) -> str:
    return "\n".join([
        f"Now: {when_text(now, tz)}.",
        "Our watcher just flagged these unusual moves (Yahoo Finance 5-minute data, up to ~15 minutes delayed; "
        "'a normal day' = the instrument's typical daily move over the past year):",
        *alert_lines,
        "",
        "The rest of the market (24h changes):",
        *snapshot_lines(quotes),
    ])  # fmt: skip


def _snapshot_block(quotes: list[Quote]) -> list[str]:
    return ["", "<b>Snapshot</b> <i>(24h change; our data)</i>", *(esc(line) for line in snapshot_lines(quotes))]


def render_digest(kind: str, data: dict, quotes: list[Quote], now: datetime, tz: str) -> str:
    local = now.astimezone(ZoneInfo(tz))
    out = [f"📰 <b>{TITLES.get(kind, 'Digest')}</b> · {local:%a %d %b %H:%M}"]
    if data.get("headline"):
        out.append(f"<i>{esc(data['headline'])}</i>")
    if data.get("markets"):
        out += ["", "<b>Markets</b>", *(f"• {esc(m)}" for m in data["markets"])]
    if data.get("stories"):
        out += ["", "<b>Top stories</b>"]
        for i, s in enumerate(data["stories"], 1):
            src = ", ".join(x for x in (s.get("source"), s.get("time")) if x)
            out.append(f"{i}. <b>{esc(s.get('title', ''))}</b> {esc(s.get('why', ''))}"
                       + (f" <i>({esc(src)})</i>" if src else ""))  # fmt: skip
    if data.get("stretched"):
        out += ["", "<b>Stretched</b> <i>(technical, not advice)</i>"]
        for s in data["stretched"]:
            mark = "🔺" if s.get("state") == "overbought" else "🔻"
            out.append(f"{mark} <b>{esc(s.get('symbol', ''))}</b> {esc(s.get('state', ''))}: {esc(s.get('evidence', ''))}"
                       + (f" <i>({esc(s['source'])})</i>" if s.get("source") else ""))  # fmt: skip
    if data.get("calendar"):
        out += ["", "<b>Coming up</b> <i>(Prague time)</i>",
                *(f"• {esc(c.get('when', ''))}: {esc(c.get('event', ''))}" for c in data["calendar"])]  # fmt: skip
    if data.get("watch"):
        out += ["", f"👀 {esc(data['watch'])}"]
    return "\n".join(out + _snapshot_block(quotes))


def render_digest_failure(kind: str, error: str, quotes: list[Quote], now: datetime, tz: str) -> str:
    local = now.astimezone(ZoneInfo(tz))
    return "\n".join([
        f"📰 <b>{TITLES.get(kind, 'Digest')}</b> · {local:%a %d %b %H:%M}",
        f"⚠️ Claude couldn't write the news part: {esc(error[:300])}",
        *_snapshot_block(quotes),
    ])  # fmt: skip


def render_explain(data: dict) -> str:
    out = [f"🔎 <b>Why:</b> {esc(data.get('cause', ''))} <i>(confidence: {esc(data.get('confidence', '?'))})</i>"]
    if data.get("details"):
        out.append(esc(data["details"]))
    sources = [f"{esc(s.get('outlet', ''))} {esc(s.get('time', ''))}: {esc(s.get('headline', ''))}"
               for s in data.get("sources") or []]  # fmt: skip
    if sources:
        out += ["<i>Sources:</i>", *(f"• {s}" for s in sources)]
    if data.get("watch"):
        out.append(f"👀 {esc(data['watch'])}")
    return "\n".join(out)
