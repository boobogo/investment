"""Triage: the model judges a few strong headline candidates (and, for a price alert,
whether any of them explains the move). Calls are batched and hard-capped.
"""

from __future__ import annotations

import html
import json
import logging
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pipeline import alerts, brain, config, storage, telegram

log = logging.getLogger(__name__)


class Judgement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    impact: int = Field(ge=1, le=5)
    scope: Literal["market", "sector", "ticker"]
    tickers: list[str]
    new_information: bool
    headline: str = Field(min_length=1, max_length=200)
    why: str


class EventVerdict(BaseModel):
    model_config = ConfigDict(extra="forbid")

    explained: bool
    explanation: str


SCHEMA = {
    "type": "object",
    "properties": {
        "items": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "impact": {"type": "integer", "description": "1-5"},
                    "scope": {"type": "string", "enum": ["market", "sector", "ticker"]},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "new_information": {"type": "boolean"},
                    "headline": {"type": "string"},
                    "why": {"type": "string"},
                },
                "required": ["id", "impact", "scope", "tickers", "new_information", "headline", "why"],
                "additionalProperties": False,
            },
        },
        "event": {
            "type": "object",
            "properties": {"explained": {"type": "boolean"}, "explanation": {"type": "string"}},
            "required": ["explained", "explanation"],
            "additionalProperties": False,
        },
    },
    "required": ["items", "event"],
    "additionalProperties": False,
}


@dataclass
class Candidate:
    news_id: int
    published: datetime
    source: str
    title: str
    summary: str | None
    tickers: list[str]
    url: str = ""


@dataclass
class PriceEvent:
    alert_id: int
    symbol: str
    label: str  # e.g. "-4.3% in 15 min"
    detected_at: datetime
    telegram_message_id: int | None = None


@dataclass
class Outcome:
    judgements: dict[int, Judgement] = field(default_factory=dict)
    event: EventVerdict | None = None
    results: list[brain.LLMResult] = field(default_factory=list)
    error: str | None = None


def calls_left(conn: sqlite3.Connection, cfg: config.TriageCfg, now: datetime, tz: str) -> int:
    """How many triage calls the hourly and daily caps still allow."""
    hour = storage.calls_since(conn, "triage", (now - timedelta(hours=1)).isoformat(timespec="seconds"))
    day = storage.calls_since(conn, "triage", alerts.day_start(now, tz).isoformat(timespec="seconds"))
    return max(0, min(cfg.max_calls_per_hour - hour, cfg.max_calls_per_day - day))


def build_user_message(
    candidates: list[Candidate], *, now: datetime, session: str, watchlist: list[config.Ticker],
    context: list[str], recent_alerts: list[str], event: PriceEvent | None = None,
) -> str:  # fmt: skip
    names = ", ".join(f"{t.symbol} ({t.name})" if t.name else t.symbol for t in watchlist)
    lines = [
        f"Now: {now:%Y-%m-%d %H:%M} UTC. US market session: {session}.",
        f"Watchlist: {names}",
        f"Market now: {'; '.join(context) if context else 'no live quotes'}",
        "Recent alerts already sent: " + ("; ".join(recent_alerts) if recent_alerts else "none"),
    ]
    if event:
        lines.append(f"Price move to explain: {event.symbol} {event.label}, detected {event.detected_at:%H:%M} UTC.")
    lines += ["", "<candidates>"]
    for c in candidates:
        entry = {"id": c.news_id, "utc": f"{c.published:%m-%d %H:%M}", "src": c.source, "title": c.title}
        if c.summary:
            entry["summary"] = c.summary[:300]
        if c.tickers:
            entry["tickers"] = c.tickers
        lines.append(json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
    lines.append("</candidates>")
    return "\n".join(lines)


def parse(data, valid_ids: set[int]) -> tuple[dict[int, Judgement], EventVerdict | None]:
    judgements = {}
    if not isinstance(data, dict):
        return judgements, None
    for entry in data.get("items") or []:
        try:
            j = Judgement.model_validate(entry)
        except ValidationError as e:
            log.warning("triage: dropped an invalid judgement: %s", e.errors()[0]["msg"])
            continue
        if j.id in valid_ids:
            judgements[j.id] = j
    try:
        event = EventVerdict.model_validate(data.get("event")) if data.get("event") else None
    except ValidationError:
        event = None
    return judgements, event


def judge(
    backend: brain.LLMBackend, settings: config.Settings, candidates: list[Candidate], *, now: datetime,
    session: str, watchlist: list[config.Ticker], context: list[str], recent_alerts: list[str],
    event: PriceEvent | None = None, sleep=None,
) -> Outcome:  # fmt: skip
    user = build_user_message(candidates, now=now, session=session, watchlist=watchlist, context=context,
                              recent_alerts=recent_alerts, event=event)  # fmt: skip
    kwargs = {"sleep": sleep} if sleep else {}
    results = brain.complete_with_retry(
        backend, (config.ROOT / "prompts" / "triage.md").read_text(), user, SCHEMA, settings.llm,
        label=f"triage ({len(candidates)} headlines{', explaining ' + event.symbol if event else ''})", **kwargs,
    )  # fmt: skip
    out = Outcome(results=results)
    last = results[-1]
    if not last.ok:
        out.error = last.error
        return out
    out.judgements, out.event = parse(last.data, {c.news_id for c in candidates})
    return out


def is_alert(j: Judgement, cfg: config.TriageCfg) -> bool:
    return j.new_information and j.impact >= cfg.alert_min_impact


def severity(j: Judgement) -> str:
    """Only market-wide impact-5 news is critical (it may come through quiet hours, like a crash)."""
    return "critical" if j.impact == 5 and j.scope == "market" else "alert"


def format_news_alert(j: Judgement, c: Candidate, tz: str) -> str:
    esc = telegram.esc
    icon = "🚨" if severity(j) == "critical" else "📰"
    tickers = f" ({esc(', '.join(j.tickers))})" if j.tickers else ""
    source = esc(c.source)
    if c.url:
        source = f'<a href="{html.escape(c.url, quote=True)}">{source}</a>'
    when = c.published.astimezone(ZoneInfo(tz)).strftime("%H:%M")
    return (f"{icon} <b>{esc(j.headline)}</b>{tickers}\n{esc(j.why)}\n"
            f"<i>{source} · {when} · impact {j.impact}/5</i>")  # fmt: skip


def format_explanation(event: PriceEvent, verdict: EventVerdict | None) -> str:
    if verdict and verdict.explained and verdict.explanation:
        return f"🔎 <b>Why {telegram.esc(event.symbol)} moved:</b> {telegram.esc(verdict.explanation)}"
    return f"🔎 No news explaining the {telegram.esc(event.symbol)} move turned up within the hour; it may be sector or market flow."

