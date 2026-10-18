"""The watcher's news desk: polls news feeds, sends the few strong candidates to the model,
alerts on serious news, and explains price alerts with follow-ups in the alert's Telegram thread.

Model use is the scarce resource: only headlines passing the strict pre-filter are
judged, calls are batched (at most one per `triage.min_interval_minutes`) and
hard-capped per hour and per day (counted from llm_calls, so the caps survive
restarts). Over the caps, news simply waits for the next digest.
"""

from __future__ import annotations

import asyncio
import logging
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline import alerts, brain, clock, config, detect, news, newswatch, storage, telegram, triage
from pipeline.finnhub import Finnhub, FinnhubError

log = logging.getLogger(__name__)

MARKET_KINDS = ("index_window", "index_day", "vix", "futures")


@dataclass
class FollowUp:
    event: triage.PriceEvent
    due: list[datetime]
    market: bool  # a market-wide move: look at general news instead of one company's
    used: set[int] = field(default_factory=set)


class NewsDesk:
    def __init__(
        self,
        settings: config.Settings,
        conn: sqlite3.Connection,
        fh: Finnhub,
        *,
        watchlist: list[config.Ticker],
        policy: alerts.AlertPolicy,
        telegram_client: telegram.Telegram | None,
        shadow: bool,
        session_fn,
        context_fn,
        backend_factory=brain.make_backend,
    ):
        self.settings, self.conn, self.fh = settings, conn, fh
        self.ncfg, self.tcfg = settings.news_watch, settings.triage
        self.watchlist = watchlist
        self.prefilter = newswatch.Prefilter(settings.news, settings.news_watch, watchlist)
        self.policy, self.telegram, self.shadow = policy, telegram_client, shadow
        self.session_fn, self.context_fn = session_fn, context_fn
        self.backend_factory, self.backend = backend_factory, None
        self.tz = settings.alerts.quiet_hours.timezone
        self.queue: list[triage.Candidate] = []
        self.followups: list[FollowUp] = []
        self.last_general_id = 0
        self.rotation = 0
        self.last_triage: datetime | None = None
        self.cap_logged: str | None = None
        self.stats = {"new": 0, "candidates": 0, "triaged": 0}

    # --- polling ----------------------------------------------------------------------

    def fetch_general(self, now: datetime) -> list[news.NewsItem]:
        """Blocking: market news newer than the last one seen."""
        rows = self.fh.market_news("general", min_id=self.last_general_id)
        if rows:
            self.last_general_id = max(self.last_general_id, max(r.get("id") or 0 for r in rows))
        return [newswatch.from_finnhub(r, "general", self.watchlist) for r in rows]

    def fetch_company(self, symbols: list[str], now: datetime) -> list[news.NewsItem]:
        """Blocking: today's (and yesterday's) company news for these tickers, kept only if it names the company."""
        items = []
        start, end = now.date() - timedelta(days=1), now.date()
        for s in symbols:
            try:
                rows = self.fh.company_news(s, start, end)
            except FinnhubError as e:
                log.warning("%s company news: %s", s, e)
                continue
            items += [it for it in (newswatch.from_finnhub(r, "company", self.watchlist) for r in rows)
                      if s in it.tickers]  # fmt: skip
        return items

    def next_company_batch(self) -> list[str]:
        """Symbols whose company news is due this round, rotating through the watchlist."""
        symbols = [t.symbol for t in self.watchlist]
        if not symbols:
            return []
        per_round = max(1, round(len(symbols) * self.ncfg.general_minutes / self.ncfg.company_minutes))
        batch = [symbols[(self.rotation + i) % len(symbols)] for i in range(min(per_round, len(symbols)))]
        self.rotation = (self.rotation + len(batch)) % len(symbols)
        return batch

    def ingest(self, items: list[news.NewsItem], now: datetime) -> int:
        """Store items; queue new ones that pass the strict filter and aren't a story already judged."""
        new: set[int] = set()
        ids = storage.upsert_news(self.conn, items, new)
        added = 0
        for it in items:
            if id(it) not in new:
                continue
            self.stats["new"] += 1
            if not self.prefilter.is_breaking_candidate(it, now, self.ncfg.candidate_max_age_minutes):
                continue
            if self.seen_story(it, now):
                continue
            self.queue.append(triage.Candidate(ids[id(it)], it.published, it.source, it.title, it.snippet,
                                               it.tickers, it.url))  # fmt: skip
            added += 1
        self.stats["candidates"] += added
        return added

    def seen_story(self, it: news.NewsItem, now: datetime) -> bool:
        """True if the same story is already queued or was judged in the last 24 hours."""
        earlier = [news.NewsItem(title=c.title, source=c.source, published=c.published, tickers=c.tickers)
                   for c in self.queue]  # fmt: skip
        since = (now - timedelta(hours=24)).isoformat(timespec="seconds")
        earlier += [news.NewsItem(title=r["headline"], source="", published=now, tickers=r["tickers"])
                    for r in storage.triaged_since(self.conn, since)]  # fmt: skip
        if not earlier:
            return False
        stories = news.cluster_stories(earlier + [it], set(), self.settings.news)
        return any(it in st and len(st) > 1 for st in stories)

    async def poll_round(self, now: datetime) -> int:
        items: list[news.NewsItem] = []
        try:
            items += await asyncio.to_thread(self.fetch_general, now)
        except FinnhubError as e:
            log.warning("market news: %s", e)
        items += await asyncio.to_thread(self.fetch_company, self.next_company_batch(), now)
        added = self.ingest(items, now)
        if added:
            log.info("news: %d new breaking-news candidate(s) queued for triage", added)
        return added

    async def poll_loop(self) -> None:
        while True:
            try:
                await self.poll_round(datetime.now(timezone.utc))
            except Exception:
                log.exception("news desk: polling round failed")
            await asyncio.sleep(self.ncfg.general_minutes * 60)

    # --- triage -----------------------------------------------------------------------

    def calls_left(self, now: datetime) -> int:
        left = triage.calls_left(self.conn, self.tcfg, now, self.tz)
        if left <= 0 and self.cap_logged != now.strftime("%Y-%m-%d %H"):
            self.cap_logged = now.strftime("%Y-%m-%d %H")
            log.warning("triage: call cap reached; %d candidate(s) wait for the next digest", len(self.queue))
        return left

    def recent_alerts(self, now: datetime, hours: float = 6) -> list[str]:
        rows = self.conn.execute(
            "SELECT text FROM alerts WHERE kind IN ('price', 'news') AND status IN ('sent', 'shadow')"
            " AND created_at >= ? ORDER BY created_at DESC LIMIT 10",
            ((now - timedelta(hours=hours)).isoformat(timespec="seconds"),),
        )
        return [re.sub(r"<[^>]+>", "", r["text"].splitlines()[0]) for r in rows]

    async def judge(self, candidates: list[triage.Candidate], now: datetime, event=None) -> triage.Outcome:
        if self.backend is None:
            self.backend = self.backend_factory(self.settings.llm)
        tickers = sorted({t for c in candidates for t in c.tickers} | ({event.symbol} if event else set()))
        outcome = await asyncio.to_thread(
            triage.judge, self.backend, self.settings, candidates, now=now, session=self.session_fn(),
            watchlist=self.watchlist, context=self.context_fn(tickers), recent_alerts=self.recent_alerts(now),
            event=event,
        )  # fmt: skip
        run_id = storage.watch_run_id(self.conn, now.astimezone(ZoneInfo(self.tz)).date().isoformat())
        for i, res in enumerate(outcome.results, start=1):
            storage.save_call(self.conn, run_id, {**brain.result_row(res, self.backend.name), "batch_no": 0,
                                                  "attempt": i, "purpose": "triage",
                                                  "created_at": now.isoformat(timespec="seconds")})  # fmt: skip
        for news_id, j in outcome.judgements.items():
            storage.set_triage(self.conn, news_id, j.model_dump())
        self.stats["triaged"] += len(candidates)
        self.last_triage = now
        if outcome.error:
            log.warning("triage failed: %s", outcome.error)
        return outcome

    async def run_candidates(self, now: datetime) -> None:
        if not self.queue or not self.tcfg.enabled:
            return
        if self.last_triage and now - self.last_triage < timedelta(minutes=self.tcfg.min_interval_minutes):
            return
        if self.calls_left(now) <= 0:
            return
        fresh_after = now - timedelta(minutes=self.ncfg.candidate_max_age_minutes * 2)
        self.queue = [c for c in self.queue if c.published >= fresh_after]  # stale candidates go to the digest
        batch, self.queue = self.queue[: self.tcfg.max_items_per_call], self.queue[self.tcfg.max_items_per_call :]
        if not batch:
            return
        outcome = await self.judge(batch, now)
        for c in batch:
            j = outcome.judgements.get(c.news_id)
            if j and triage.is_alert(j, self.tcfg):
                await self.news_alert(j, c, now)

    async def news_alert(self, j: triage.Judgement, c: triage.Candidate, now: datetime) -> None:
        text = triage.format_news_alert(j, c, self.tz)
        f = next((f for f in self.followups if f.event.symbol in j.tickers), None)
        if f is not None:  # the stock already has an open price alert: answer in its thread
            await self.reply(f, text, now)
            return
        decision = self.policy.gate(triage.severity(j), now, shadow=self.shadow)
        message_id = await self.send(text, decision)
        status = "failed" if decision.status == "send" and message_id is None else {"send": "sent"}.get(
            decision.status, decision.status)  # fmt: skip
        storage.log_alert(self.conn, created_at=now.isoformat(timespec="seconds"), kind="news",
                          severity=triage.severity(j), symbol=(j.tickers or [None])[0], text=text, status=status,
                          reason=decision.reason or None, telegram_message_id=message_id)  # fmt: skip
        log.info("news alert %s%s: %s", status, f" ({decision.reason})" if decision.reason else "", j.headline)

    async def send(self, text: str, decision: alerts.Decision, reply_to: int | None = None, silent=False):
        if decision.status != "send" or self.telegram is None:
            return None
        try:
            return (await asyncio.to_thread(self.telegram.send, text, reply_to=reply_to, silent=silent))[0]
        except telegram.TelegramError as e:
            log.warning("telegram: %s", e)
            return None

    # --- follow-ups -------------------------------------------------------------------

    def register(self, alert_id: int, e: detect.Event, message_id: int | None, now: datetime) -> None:
        """A price alert went out (or was shadow-recorded): look for news explaining it."""
        if not self.tcfg.enabled:
            return
        event = triage.PriceEvent(alert_id, e.symbol, e.label, now, message_id)
        due = [now + timedelta(minutes=m) for m in self.tcfg.follow_up_minutes]
        self.followups.append(FollowUp(event, due, market=e.kind in MARKET_KINDS))

    async def run_followups(self, now: datetime) -> None:
        for f in list(self.followups):
            if not f.due or f.due[0] > now:
                continue
            f.due.pop(0)
            last_try = not f.due
            if not f.market:
                fresh = await asyncio.to_thread(self.fetch_company, [f.event.symbol], now)
                self.ingest(fresh, now)
            since = (f.event.detected_at - timedelta(minutes=60)).isoformat(timespec="seconds")
            rows = storage.news_for_event(self.conn, since, None if f.market else f.event.symbol)
            if f.market:
                rows = [r for r in rows if r["category"] == "general"]
            rows = [r for r in rows if r["id"] not in f.used][: self.tcfg.max_items_per_call]
            if rows and self.calls_left(now) > 0:
                f.used |= {r["id"] for r in rows}
                self.queue = [c for c in self.queue if c.news_id not in f.used]  # judged here instead
                candidates = [triage.Candidate(r["id"], datetime.fromisoformat(r["published_at"]), r["source"] or "",
                                               r["headline"], r["summary"], r["tickers"], r["url"] or "")
                              for r in rows]  # fmt: skip
                outcome = await self.judge(candidates, now, event=f.event)
                if outcome.event and outcome.event.explained:
                    await self.reply(f, triage.format_explanation(f.event, outcome.event), now)
                    self.followups.remove(f)
                    continue
            if last_try:
                await self.reply(f, triage.format_explanation(f.event, None), now)
                self.followups.remove(f)

    async def reply(self, f: FollowUp, text: str, now: datetime) -> None:
        """Reply under the price alert; silently during quiet hours (it answers a message already sent)."""
        q = self.settings.alerts.quiet_hours
        quiet = clock.in_window(now, q.start, q.end, q.timezone)
        decision = alerts.Decision("shadow" if self.shadow or f.event.telegram_message_id is None else "send")
        message_id = await self.send(text, decision, reply_to=f.event.telegram_message_id, silent=quiet)
        status = "sent" if message_id else ("failed" if decision.status == "send" else "shadow")
        storage.log_alert(self.conn, created_at=now.isoformat(timespec="seconds"), kind="follow_up",
                          severity="info", symbol=f.event.symbol, text=text, status=status,
                          telegram_message_id=message_id, reply_to=f.event.alert_id)  # fmt: skip
        log.info("follow-up %s for %s: %s", status, f.event.symbol, re.sub(r"<[^>]+>", "", text)[:120])

    async def triage_loop(self, every_s: float = 20) -> None:
        while True:
            await asyncio.sleep(every_s)
            now = datetime.now(timezone.utc)
            try:
                await self.run_followups(now)
                await self.run_candidates(now)
            except Exception:
                log.exception("news desk: triage round failed")

    def status(self) -> str:
        return (f"news {self.stats['new']} new, {self.stats['candidates']} candidates, "
                f"{self.stats['triaged']} judged, queue {len(self.queue)}, follow-ups {len(self.followups)}")  # fmt: skip

