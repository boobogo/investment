"""Digest extras: news highlights chosen by the model, earnings calendar, last week's movers, held alerts.

Runs after the stock review in scheduled digests (morning, preopen, weekly).
Nothing here can fail a run: problems are logged and the report shows what it has.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pipeline import brain, config, news, newswatch, storage
from pipeline.finnhub import Finnhub, FinnhubError

log = logging.getLogger(__name__)

DIGEST_SLOTS = ("morning", "preopen", "weekly")
HOUR_LABELS = {"bmo": "before the open", "amc": "after the close", "dmh": "during market hours"}


class Highlight(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    title: str = Field(min_length=1, max_length=160)
    why: str = Field(min_length=1)
    scope: Literal["market", "sector", "ticker"]
    tickers: list[str]
    importance: int = Field(ge=1, le=5)


HIGHLIGHTS_SCHEMA = {
    "type": "object",
    "properties": {
        "highlights": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "title": {"type": "string"},
                    "why": {"type": "string"},
                    "scope": {"type": "string", "enum": ["market", "sector", "ticker"]},
                    "tickers": {"type": "array", "items": {"type": "string"}},
                    "importance": {"type": "integer", "description": "3, 4 or 5"},
                },
                "required": ["id", "title", "why", "scope", "tickers", "importance"],
                "additionalProperties": False,
            },
        }
    },
    "required": ["highlights"],
    "additionalProperties": False,
}


@dataclass
class Extras:
    since: datetime
    until: datetime
    ranked: list = field(default_factory=list)  # (score, story) candidates, best first
    highlights: list[dict] = field(default_factory=list)
    calls: list[brain.LLMResult] = field(default_factory=list)
    earnings: list[dict] = field(default_factory=list)
    movers: list[dict] = field(default_factory=list)
    held: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def to_json(self) -> str:
        return json.dumps({
            "window": {"since": self.since.isoformat(), "until": self.until.isoformat()},
            "n_candidates": len(self.ranked), "earnings": self.earnings, "movers": self.movers,
            "held": self.held, "errors": self.errors,
        })  # fmt: skip


def news_window(conn: sqlite3.Connection, run_id: int, cfg: config.NewsWatchCfg, now: datetime) -> datetime:
    """News since the previous digest, but at least min_window_hours and at most max_window_hours back."""
    prev = storage.previous_digest_start(conn, run_id)
    since = datetime.fromisoformat(prev) if prev else now - timedelta(hours=cfg.max_window_hours)
    return max(now - timedelta(hours=cfg.max_window_hours), min(since, now - timedelta(hours=cfg.min_window_hours)))


def candidate_lines(ranked, news_ids: dict[int, int], snippet_chars: int = 220) -> list[str]:
    lines = []
    for _, story in ranked:
        lead = story[0]
        entry = {
            "id": news_ids[id(lead)],
            "utc": lead.published.strftime("%m-%d %H:%M"),
            "src": lead.source,
            "title": lead.title,
        }
        snippet = next((m.snippet for m in story if m.snippet), None)
        if snippet:
            entry["summary"] = news._trim(snippet, snippet_chars)
        tickers = sorted({t for m in story for t in m.tickers})
        if tickers:
            entry["tickers"] = tickers
        if len(story) > 1:
            entry["outlets"] = len({m.source for m in story})
        lines.append(json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
    return lines


def build_user_message(lines: list[str], watchlist: list[config.Ticker], slot: str, now: datetime, n: int) -> str:
    names = ", ".join(f"{t.symbol} ({t.name})" if t.name else t.symbol for t in watchlist)
    return "\n".join([
        f"Now: {now:%Y-%m-%d %H:%M} UTC. Digest: {slot}. Pick at most {n} highlights.",
        f"Watchlist: {names}",
        "",
        "<candidates>",
        *lines,
        "</candidates>",
    ])  # fmt: skip


def parse_highlights(data, valid_ids: set[int], n: int) -> list[dict]:
    picks = []
    for entry in (data or {}).get("highlights", []) if isinstance(data, dict) else []:
        try:
            h = Highlight.model_validate(entry)
        except ValidationError as e:
            log.warning("highlights: dropped an invalid pick: %s", e.errors()[0]["msg"])
            continue
        if h.id in valid_ids and h.id not in {p["news_id"] for p in picks}:
            picks.append({**h.model_dump(exclude={"id"}), "news_id": h.id})
    return picks[:n]


def earnings_calendar(
    fh: Finnhub | None, conn: sqlite3.Connection, watchlist: list[config.Ticker], start: date, end: date
) -> list[dict]:
    """Watchlist earnings in [start, end] from Finnhub; Yahoo's date is attached where the two disagree."""
    if fh is None:
        return []
    symbols = {t.symbol for t in watchlist}
    rows = [e for e in fh.earnings_calendar(start, end) if e.get("symbol") in symbols]
    out = []
    for e in sorted(rows, key=lambda e: (e["date"], e["symbol"])):
        yahoo = (storage.load_fundamentals(conn, e["symbol"]) or {}).get("next_earnings")
        out.append({
            "date": e["date"], "symbol": e["symbol"], "when": HOUR_LABELS.get(e.get("hour") or "", ""),
            "yahoo_date": yahoo if yahoo and yahoo != e["date"] else None,
        })  # fmt: skip
    return out


def weekly_movers(conn: sqlite3.Connection, watchlist: list[config.Ticker], benchmark: str, days: int = 5) -> list[dict]:
    """Each watchlist stock's last-week return and its return relative to the benchmark, biggest gaps first."""
    bench = storage.load_prices(conn, benchmark)["close"]
    if len(bench) <= days:
        return []
    b = (bench.iloc[-1] / bench.iloc[-1 - days] - 1) * 100
    out = []
    for t in watchlist:
        close = storage.load_prices(conn, t.symbol)["close"]
        if len(close) > days:
            r = (close.iloc[-1] / close.iloc[-1 - days] - 1) * 100
            out.append({"symbol": t.symbol, "ret_pct": round(r, 2), "vs_bench_pct": round(r - b, 2)})
    return sorted(out, key=lambda m: -abs(m["vs_bench_pct"]))


def build(
    conn: sqlite3.Connection,
    run_id: int,
    settings: config.Settings,
    slot: str,
    watchlist: list[config.Ticker],
    rss: dict[str, news.NewsResult],
    *,
    backend_factory,
    use_model: bool,
    online: bool,
    now: datetime | None = None,
    finnhub_client: Finnhub | None = None,
) -> Extras:
    now = now or datetime.now(timezone.utc)
    ex = Extras(since=news_window(conn, run_id, settings.news_watch, now), until=now)

    fh = finnhub_client
    if fh is None and online:
        try:
            fh = Finnhub.from_env()
        except config.MissingSecret as e:
            ex.errors.append(str(e))

    # RSS items arrive per ticker: record which one (copies, so the run's packets are untouched)
    items = [replace(it, category=it.category or "company", tickers=it.tickers or [sym])
             for sym, res in rss.items() for it in res.items]  # fmt: skip
    if fh is not None:
        fetched, errors = newswatch.fetch_finnhub(fh, watchlist, settings.news_watch, ex.since, now)
        items += fetched
        ex.errors += errors
    items = [i for i in items if i.published >= ex.since]
    news_ids = storage.upsert_news(conn, items)
    prefilter = newswatch.Prefilter(settings.news, settings.news_watch)
    ex.ranked = newswatch.rank_stories(items, prefilter, settings.news, now, settings.news_watch.candidates_max)
    log.info("digest: %d news items since %s -> %d candidate stories", len(items), f"{ex.since:%m-%d %H:%M}", len(ex.ranked))

    n = settings.digests.highlights
    if use_model and ex.ranked and n:
        lines = candidate_lines(ex.ranked, news_ids)
        backend = backend_factory(settings.llm)
        ex.calls = brain.complete_with_retry(
            backend,
            (config.ROOT / "prompts" / "highlights.md").read_text(),
            build_user_message(lines, watchlist, slot, now, n),
            HIGHLIGHTS_SCHEMA,
            settings.llm,
            label="highlights",
        )
        last = ex.calls[-1]
        if last.ok:
            ex.highlights = parse_highlights(last.data, {news_ids[id(st[0])] for _, st in ex.ranked}, n)
            storage.save_highlights(conn, run_id, ex.highlights)
        else:
            ex.errors.append(f"highlights: {last.error}")

    today = now.astimezone(ZoneInfo(settings.schedule.timezone)).date()
    if slot == "weekly":
        start = today + timedelta(days=(7 - today.weekday()) % 7 or 7)  # next Monday
        span = (start, start + timedelta(days=4))
        ex.movers = weekly_movers(conn, watchlist, settings.data.benchmark)
    else:
        span = (today, today + timedelta(days=1))
    try:
        ex.earnings = earnings_calendar(fh, conn, watchlist, *span)
    except FinnhubError as e:
        ex.errors.append(f"earnings calendar: {e}")
    ex.held = storage.held_alerts(conn, ex.since.isoformat())
    return ex

