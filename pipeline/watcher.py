"""The always-on watcher (`uv run main.py watch`), run on the Raspberry Pi by systemd.

One asyncio program with three loops:
  - checks: every 15 minutes while European or US stock markets are open, every
    30 minutes otherwise, download the instruments' recent prices and alert on
    unusual moves (one Telegram message per check); a sent alert gets a Claude
    explanation as a reply, within the daily Claude cap
  - digests: at the scheduled times, Claude researches the news with web search
    and the digest goes to Telegram
  - commands: Telegram commands from the configured chat
Blocking work (downloads, Claude, Telegram) runs in worker threads; the state
file is only touched from the event-loop thread.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from pipeline import alerts, brain, briefing, clock, commands, config, markets, telegram
from pipeline.markets import Quote
from pipeline.state import State

log = logging.getLogger(__name__)

DIGEST_GRACE = timedelta(hours=3)  # a digest missed while the watcher was down still runs this long after its time
DIGEST_RETRY = timedelta(minutes=10)
CHECK_OFFSET_S = 30  # checks run just after the quarter-hour, when the bar that just closed is in


def make_telegram(cfg: config.TelegramCfg) -> telegram.Telegram | None:
    try:
        return telegram.Telegram.from_env(cfg)
    except config.MissingSecret as e:
        log.warning("telegram not configured (%s): messages are only logged", e)
        return None


class Watcher:
    def __init__(
        self, settings: config.Settings, *, shadow: bool = False, backend: brain.LLMBackend | None = None,
        telegram_client: telegram.Telegram | None = None, state: State | None = None,
        fetch_closes=markets.fetch_closes, fetch_sigmas=markets.fetch_sigmas,
    ):  # fmt: skip
        self.settings = settings
        self.cfg, self.acfg, self.ccfg = settings.watch, settings.alerts, settings.claude
        self.tz = settings.digests.timezone
        self.shadow = shadow or self.acfg.mode == "shadow"
        self.state = state or State(settings.paths.state)
        self.policy = alerts.Policy(self.acfg, self.state)
        self.backend = backend or brain.make_backend(self.ccfg)
        self.telegram = telegram_client
        self.fetch_closes, self.fetch_sigmas = fetch_closes, fetch_sigmas
        self.names, self.groups = self.cfg.names(), self.cfg.groups()
        self.quotes: list[Quote] = []
        self.sigmas: dict[str, float] = {}
        self.sigma_date: date | None = None
        self.last_check: datetime | None = None
        self.last_digest_error = ""
        self.claude_lock = asyncio.Lock()
        self.digest_tries: dict[str, list[datetime]] = {}
        self.background: set[asyncio.Task] = set()  # keeps fire-and-forget tasks alive until done
        self.started = time.time()
        self.commands = commands.Commands(self)

    # --- prices and alerts ---------------------------------------------------------------

    def markets_open(self, now: datetime) -> bool:
        return any(clock.in_window(now, w) for w in self.cfg.market_hours)

    def next_check(self, now: datetime) -> datetime:
        """The next quarter- or half-hour mark (plus a few seconds), depending on whether markets are open."""
        step = 60 * (self.cfg.check_minutes_open if self.markets_open(now) else self.cfg.check_minutes_closed)
        ts = (now.timestamp() // step + 1) * step + CHECK_OFFSET_S
        return datetime.fromtimestamp(ts, timezone.utc)

    async def refresh_quotes(self, now: datetime) -> list[Quote]:
        symbols = list(self.names)
        today = now.astimezone(ZoneInfo(self.tz)).date()
        if self.sigma_date != today:
            try:
                sigmas = await asyncio.to_thread(self.fetch_sigmas, symbols, self.cfg.sigma_days)
                if sigmas:
                    self.sigmas, self.sigma_date = sigmas, today
                    log.info("typical daily moves: %s", ", ".join(f"{s} {v:.2f}%" for s, v in sigmas.items()))
            except Exception:
                log.exception("daily history download failed; keeping the previous typical moves")
        closes = await asyncio.to_thread(self.fetch_closes, symbols)
        self.quotes = markets.snapshot(closes, self.sigmas, self.names, self.groups, now,
                                       timedelta(minutes=self.cfg.max_quote_age_minutes))  # fmt: skip
        return self.quotes

    async def check(self, now: datetime) -> dict | None:
        """One price check; returns the alert record if one was made."""
        try:
            quotes = await self.refresh_quotes(now)
        finally:
            self.last_check = now
            self.state.set("heartbeat", now.isoformat())
        hits = self.policy.new_hits(alerts.find_hits(quotes, self.acfg), now)
        if not hits:
            log.info("check: %d instruments, %d trading, nothing unusual", len(quotes), sum(q.live for q in quotes))
            return None
        decision = self.policy.gate(hits, now, shadow=self.shadow)
        record = {"at": now.isoformat(), "status": "sent" if decision.status == "send" else decision.status,
                  "reason": decision.reason, "lines": alerts.hit_lines(hits),
                  "critical": any(h.critical for h in hits)}  # fmt: skip
        log.info("alert %s%s: %s", record["status"], f" ({decision.reason})" if decision.reason else "",
                 " | ".join(record["lines"]))  # fmt: skip
        blocker = self.explain_blocker(now)
        if decision.status == "send":
            text = alerts.format_alert(hits, self.quotes, now, self.acfg)
            text += "\n<i>🔎 Asking Claude what caused it…</i>" if blocker is None else f"\n<i>No Claude check: {blocker}.</i>"
            record["message_id"] = await self.notify(text)
            if record["message_id"] is None:
                record["status"] = "failed"
        self.policy.remember(hits, now)
        self.state.append("alerts", record)
        if record["status"] == "sent" and blocker is None:
            self.spawn(self.explain(record, now))
        return record

    async def check_loop(self) -> None:
        now = datetime.now(timezone.utc)
        while True:
            try:
                await self.check(now)
            except Exception:
                log.exception("price check failed")
            wake = self.next_check(datetime.now(timezone.utc))
            await asyncio.sleep(max(1.0, (wake - datetime.now(timezone.utc)).total_seconds()))
            now = datetime.now(timezone.utc)

    # --- Claude: budget, calls, explanations ---------------------------------------------------

    def day_start(self, now: datetime) -> datetime:
        return clock.local_day_start(now, self.tz)

    def calls_today(self, now: datetime) -> list[dict]:
        return self.state.items_since("claude_calls", self.day_start(now))

    def scheduled_today(self, now: datetime) -> int:
        start = self.day_start(now)
        end = start + timedelta(hours=24) - timedelta(seconds=1)  # a DST day is 23 or 25 hours; close enough here
        return sum(len(clock.occurrences(e, start, end)) for e in self.settings.digests.scheduled().values())

    def extra_left(self, now: datetime) -> int:
        """Claude calls left today for explanations and /digest: the scheduled digests are reserved first."""
        used = sum(1 for c in self.calls_today(now) if c["kind"] in ("explain", "manual"))
        return max(0, self.ccfg.daily_calls - self.scheduled_today(now) - used)

    def next_digest(self, now: datetime) -> tuple[str, datetime] | None:
        upcoming = [(t, k) for k, e in self.settings.digests.scheduled().items() if (t := clock.next_occurrence(e, now))]
        if not upcoming:
            return None
        t, k = min(upcoming)
        return k, t

    def explain_blocker(self, now: datetime) -> str | None:
        """Why an alert should not get a Claude explanation now (None: go ahead)."""
        nxt = self.next_digest(now)
        if nxt and nxt[1] - now <= timedelta(minutes=self.ccfg.explain_skip_before_digest_minutes):
            return f"the {nxt[0]} digest at {commands.local_time(nxt[1], self.tz, now)} covers it"
        last = [c for c in self.calls_today(now) if c["kind"] == "explain"]
        if last and now - datetime.fromisoformat(last[-1]["at"]) < timedelta(minutes=self.ccfg.explain_min_gap_minutes):
            return f"one ran at {commands.local_time(datetime.fromisoformat(last[-1]['at']), self.tz, now)} (see above)"
        if self.extra_left(now) <= 0:
            return "today's Claude calls are used up; the next digest covers it"
        return None

    async def call_claude(self, kind: str, prompt: str, user: str, schema: dict, now: datetime) -> brain.LLMResult:
        """One logical Claude call with web search (retries included), recorded for the daily cap."""
        # counted before waiting for the lock: an alert 15 minutes later must already see this call
        entry = {"at": now.isoformat(), "kind": kind, "ok": None}
        self.state.append("claude_calls", entry)
        async with self.claude_lock:
            system = briefing.load_prompt(self.settings.paths.prompts, prompt)
            attempts = await asyncio.to_thread(brain.complete_with_retry, self.backend, system, user, schema,
                                               self.ccfg, label=kind, web_search=True)  # fmt: skip
        res = attempts[-1]
        entry.update(ok=res.ok, error=res.error, seconds=round(res.duration_ms / 1000),
                     turns=res.usage.get("turns"), output_tokens=res.usage.get("output_tokens"))  # fmt: skip
        self.state.save()
        log.info("claude %s: %s in %ds, %s turns", kind, "ok" if res.ok else f"FAILED ({res.error})",
                 res.duration_ms // 1000, res.usage.get("turns"))  # fmt: skip
        return res

    async def explain(self, record: dict, now: datetime) -> None:
        user = briefing.explain_request(record["lines"], self.quotes, now, self.tz)
        try:
            res = await self.call_claude("explain", "explain", user, briefing.EXPLAIN_SCHEMA, now)
            if res.ok:
                record["explained"] = res.data.get("cause")
                self.state.save()
                text = briefing.render_explain(res.data)
            else:
                text = f"⚠️ Claude couldn't explain this move: {telegram.esc((res.error or '')[:200])}"
            await self.notify(text, reply_to=record.get("message_id"))
        except Exception:
            log.exception("explanation failed")

    # --- digests ---------------------------------------------------------------------------

    def previous_digest(self, before: datetime) -> datetime | None:
        times = [clock.last_occurrence(e, before - timedelta(seconds=1)) for e in self.settings.digests.scheduled().values()]
        times = [t for t in times if t]
        return max(times) if times else None

    def window_moves(self, since: datetime | None, now: datetime) -> list[str]:
        """The alerts since the previous digest, as lines for the digest request."""
        out = []
        for a in self.state.items_since("alerts", since or datetime.min.replace(tzinfo=timezone.utc)):
            status = "held in quiet hours, not seen yet" if a["status"] == "held" else a["status"]
            line = f"- {commands.local_time(datetime.fromisoformat(a['at']), self.tz, now)} ({status}): {'; '.join(a['lines'])}"
            if a.get("explained"):
                line += f" | Claude's explanation then: {a['explained']}"
            out.append(line)
        return out

    async def digest(self, kind: str, now: datetime, since: datetime | None) -> bool:
        """Research and send one digest; False if Claude failed (nothing is sent then)."""
        quotes = self.quotes
        try:
            quotes = await self.refresh_quotes(now)
        except Exception:
            log.exception("prices for the digest failed; using the last check's")
        user = briefing.digest_request(kind, now, since, quotes, self.window_moves(since, now),
                                       self.settings.digests.follow, self.tz)  # fmt: skip
        res = await self.call_claude(kind, "digest", user, briefing.DIGEST_SCHEMA, now)
        if not res.ok:
            self.last_digest_error = res.error or "unknown error"
            return False
        await self.notify(briefing.render_digest(kind, res.data, quotes, now, self.tz))
        return True

    async def run_due_digests(self, now: datetime) -> list[str]:
        """Run every scheduled digest whose time has come (or passed within DIGEST_GRACE) and isn't done.

        Two tries, 10 minutes apart; if both fail, the snapshot goes out with the error."""
        ran = []
        done = self.state.data.setdefault("digests_done", {})
        for kind, entry in self.settings.digests.scheduled().items():
            when = clock.last_occurrence(entry, now)
            if when is None or now - when > DIGEST_GRACE or done.get(kind) == when.isoformat():
                continue
            tries = self.digest_tries.setdefault(f"{kind}:{when.isoformat()}", [])
            if len(tries) >= 2 or (tries and now - tries[-1] < DIGEST_RETRY):
                continue
            tries.append(now)
            ok = await self.digest(kind, now, self.previous_digest(when))
            if ok or len(tries) >= 2:
                if not ok:
                    await self.notify(briefing.render_digest_failure(kind, self.last_digest_error, self.quotes, now, self.tz))
                done[kind] = when.isoformat()
                self.state.save()
            ran.append(kind)
        return ran

    async def schedule_loop(self) -> None:
        while True:
            try:
                await self.run_due_digests(datetime.now(timezone.utc))
            except Exception:
                log.exception("digest schedule check failed")
            await asyncio.sleep(30)

    def start_manual_digest(self, now: datetime) -> bool:
        if self.claude_lock.locked():
            return False
        self.spawn(self._manual_digest(now))
        return True

    def spawn(self, coro) -> None:
        task = asyncio.create_task(coro)
        self.background.add(task)
        task.add_done_callback(self.background.discard)

    async def _manual_digest(self, now: datetime) -> None:
        try:
            if not await self.digest("manual", now, self.previous_digest(now)):
                await self.notify(briefing.render_digest_failure("manual", self.last_digest_error, self.quotes, now, self.tz))
        except Exception:
            log.exception("manual digest failed")
            await self.notify("⚠️ The digest failed; see the watcher log.")

    # --- Telegram ----------------------------------------------------------------------------

    async def notify(self, text: str, *, reply_to: int | None = None, silent: bool = False) -> int | None:
        if self.telegram is None:
            log.info("telegram not configured; message: %s", text[:300])
            return None
        try:
            return (await asyncio.to_thread(self.telegram.send, text, reply_to=reply_to, silent=silent))[0]
        except telegram.TelegramError as e:
            log.warning("telegram: %s", e)
            return None

    async def handle_update(self, update: dict) -> None:
        msg = update.get("message") or {}
        chat = str((msg.get("chat") or {}).get("id"))
        if self.telegram is None or chat != str(self.telegram.chat_id):
            log.warning("ignored a Telegram message from an unknown chat (%s)", chat)
            return
        if time.time() - msg.get("date", 0) > 600:  # queued while the watcher was down
            log.info("ignored an old command: %s", (msg.get("text") or "")[:40])
            return
        reply = await self.commands.handle(msg.get("text") or "", datetime.now(timezone.utc))
        if reply:
            await self.notify(reply)

    async def commands_loop(self) -> None:
        if self.telegram is None:
            return
        offset = self.state.get("tg_offset")
        while True:
            try:
                updates = await asyncio.to_thread(self.telegram.get_updates, offset, 25)
            except telegram.TelegramError as e:  # e.g. 409: another process is polling the same bot
                log.warning("telegram updates: %s", e)
                await asyncio.sleep(15)
                continue
            for u in updates:
                offset = u["update_id"] + 1
                self.state.set("tg_offset", offset)
                try:
                    await self.handle_update(u)
                except Exception:
                    log.exception("command failed")
                    await self.notify("⚠️ That command failed; see the watcher log.")

    # --- reports for commands ------------------------------------------------------------------

    def alerts_today(self, now: datetime) -> list[dict]:
        return self.state.items_since("alerts", self.day_start(now))

    def status_report(self, now: datetime) -> str:
        up = timedelta(seconds=int(time.time() - self.started))
        lt = lambda t: commands.local_time(t, self.tz, now)  # noqa: E731
        check = (f"last check {lt(self.last_check)} ({len(self.quotes)} instruments, "
                 f"{sum(q.live for q in self.quotes)} trading)" if self.last_check else "no check yet")  # fmt: skip
        counts: dict[str, int] = {}
        for a in self.alerts_today(now):
            counts[a["status"]] = counts.get(a["status"], 0) + 1
        mute = self.state.get("mute_until")
        muted = f" · muted until {lt(datetime.fromisoformat(mute))}" if mute and now < datetime.fromisoformat(mute) else ""
        nxt = self.next_digest(now)
        return "\n".join([
            f"<b>Watcher</b> · up {up} · alerts {'shadow (not sent)' if self.shadow else 'LIVE'}",
            f"Prices: {check} · next {lt(self.next_check(now))}"
            + (" · markets open" if self.markets_open(now) else " · markets closed"),
            "Alerts today: " + (", ".join(f"{n} {s}" for s, n in counts.items()) or "none") + muted,
            f"Claude today: {len(self.calls_today(now))} call(s); {self.extra_left(now)} left for moves or /digest "
            f"(cap {self.ccfg.daily_calls} a day, scheduled digests reserved)",
            f"Next digest: {nxt[0]} {lt(nxt[1])}" if nxt else "No digests scheduled",
        ])  # fmt: skip

    def markets_report(self) -> str:
        if not self.quotes:
            return "No prices yet; the first check runs at startup."
        lines = (telegram.esc(x) for x in briefing.snapshot_lines(self.quotes))
        return "<b>Markets</b> <i>(24h change)</i>\n" + "\n".join(lines)

    def alerts_report(self, now: datetime) -> str:
        rows = self.alerts_today(now)
        if not rows:
            return "No alerts today."
        lines = [f"<b>Alerts today ({len(rows)})</b>"]
        for a in rows[-15:]:
            note = f" ({a['reason']})" if a.get("reason") else ""
            when = commands.local_time(datetime.fromisoformat(a["at"]), self.tz, now)
            lines.append(telegram.esc(f"• {when} [{a['status']}{note}] {'; '.join(a['lines'])}"))
        return "\n".join(lines)

    # --- lifecycle -------------------------------------------------------------------------------

    async def announce_start(self) -> None:
        """Say the watcher (re)started, at most once an hour (a crash loop must not spam)."""
        last = self.state.get("start_notice")
        now = datetime.now(timezone.utc)
        if last and now - datetime.fromisoformat(last) < timedelta(hours=1):
            return
        self.state.set("start_notice", now.isoformat())
        await self.notify(f"✅ Market watcher started ({'alerts in shadow mode' if self.shadow else 'alerts LIVE'}). "
                          "Send /help for commands.", silent=True)  # fmt: skip

    async def run(self, duration_s: float | None = None) -> None:
        log.info("watcher starting (alerts %s): %d instruments", "in shadow mode" if self.shadow else "LIVE", len(self.names))
        tasks = [
            asyncio.create_task(self.check_loop(), name="checks"),
            asyncio.create_task(self.schedule_loop(), name="digests"),
            asyncio.create_task(self.commands_loop(), name="commands"),
        ]
        await self.announce_start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        try:
            waiters = [asyncio.create_task(stop.wait()), *tasks]
            if duration_s:
                waiters.append(asyncio.create_task(asyncio.sleep(duration_s)))
            done, _ = await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                if t in tasks and t.exception():
                    log.error("watcher task %s crashed: %r", t.get_name(), t.exception())
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info("watcher stopped")
