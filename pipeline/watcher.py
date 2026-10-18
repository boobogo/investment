"""The always-on watcher (`uv run main.py watch`).

One asyncio program. Blocking work (HTTP calls, yfinance) runs in worker
threads so the price stream never stalls; SQLite is only touched from the
event-loop thread (writes are small and quick).

It keeps live prices (the Finnhub trade stream as 1-minute bars, previous closes,
VIX and index futures from yfinance, a yfinance fallback when the stream is silent
outside regular hours), knows the US session, and every few seconds runs the price
detectors through the alert policy: alerts go to Telegram in live mode and are only
recorded in shadow mode. The news desk (news polling, triage, follow-ups) runs
alongside, the digests are scheduled from here, and Telegram commands are
answered from here (commands always reply, also in shadow mode).
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from pipeline import alerts, brain, clock, commands, config, detect, fetch, newsdesk, runctl, runner, storage, telegram
from pipeline.finnhub import Finnhub, FinnhubError, TradeStream
from pipeline.live import LiveBook

log = logging.getLogger(__name__)

CORE_ETFS = 2  # the first ETFs in settings (SPY, QQQ) always get a stream slot: market-wide moves depend on them
# how recent a symbol's last trade must be for its price to count in a detector
PRICE_MAX_AGE_S = {"regular": 15 * 60, "pre": 60 * 60, "post": 60 * 60}
MACRO_MAX_AGE = timedelta(minutes=30)  # VIX and futures quotes older than this are not acted on
DIGEST_GRACE = timedelta(hours=3)  # a digest missed while the watcher was down still runs this long after its time
DIGEST_SLOTS = ("morning", "preopen", "weekly")
FINNHUB_SESSIONS = {"pre-market": "pre", "regular": "regular", "post-market": "post"}


@dataclass
class Universe:
    streamed: list[str]
    polled: list[str] = field(default_factory=list)  # beyond the stream limit: polled with /quote


def build_universe(watchlist: list[str], etfs: list[str], limit: int) -> Universe:
    ordered = list(dict.fromkeys([*etfs[:CORE_ETFS], *watchlist, *etfs[CORE_ETFS:]]))
    return Universe(ordered[:limit], ordered[limit:])


def session_kind(session: str) -> str:
    return {"regular": "regular", "pre": "extended", "post": "extended"}.get(session, "closed")


def quote_key(row: dict) -> tuple:
    return row["price"], row["prev_close"], row["day_change_pct"], row["session"]


class Watcher:
    def __init__(
        self,
        settings: config.Settings,
        *,
        shadow: bool = False,
        finnhub: Finnhub | None = None,
        stream_factory=TradeStream,
        price_source=None,
        macro_fetcher=None,
        telegram_client=None,
        backend_factory=brain.make_backend,
        digest_command: list[str] | None = None,
        why_runner=runner.execute,
    ):
        self.settings = settings
        self.cfg, self.acfg = settings.watch, settings.alerts
        # alerts are only sent with `alerts.mode: live` and without --shadow
        self.shadow = shadow or self.acfg.mode == "shadow"
        self.conn = storage.connect(settings.paths.db)
        self.fh = finnhub or Finnhub.from_env()
        self.watchlist = config.load_tickers(settings.paths.tickers)
        watchlist = [t.symbol for t in self.watchlist]
        self.universe = build_universe(watchlist, self.cfg.etfs, self.cfg.stream_limit)
        self.book = LiveBook(self.universe.streamed)
        self.stream = stream_factory(self.fh.key, self.universe.streamed, self.book.on_trades)
        self.prices = price_source or fetch.YFinanceSource(settings.data.history_period)
        self.fetch_macro = macro_fetcher or fetch_macro_quotes
        self.macro: dict[str, dict] = {}
        self.polled: dict[str, dict] = {}
        self.saved_quotes: dict[str, tuple] = {}  # what live_quotes holds, to skip unchanged rows
        self.finnhub_session: tuple[float, str | None, bool] | None = None  # (checked at, session, holiday)
        self.reference_day = None  # New York date the daily reference data was loaded for
        self.daily: dict[str, object] = {}  # symbol -> daily OHLCV frame
        self.norms: dict[str, detect.Norms] = {}  # symbol -> typical moves, for volatility-scaled thresholds
        self.policy = alerts.AlertPolicy(self.acfg, self.conn)
        self.telegram = telegram_client
        if self.telegram is None:  # needed in shadow mode too: commands always reply
            try:
                self.telegram = telegram.Telegram.from_env(settings.telegram)
            except config.MissingSecret as e:
                log.error("%s: no Telegram commands, and alerts stay in shadow mode", e)
                self.shadow = True
        self.commands = commands.Commands(self)
        self.digest_command = digest_command or [str(config.ROOT / "scripts" / "run_pipeline.sh")]
        self.digest_proc: asyncio.subprocess.Process | None = None
        self.digest_tries: dict[str, list[datetime]] = {}
        self.why_runner, self.why_running = why_runner, set()
        self.desk = None
        if settings.news_watch.live:
            self.desk = newsdesk.NewsDesk(
                settings, self.conn, self.fh, watchlist=self.watchlist, policy=self.policy,
                telegram_client=self.telegram, shadow=self.shadow, session_fn=self.session,
                context_fn=self.market_context, backend_factory=backend_factory,
            )  # fmt: skip
        self.started = time.time()

    # --- session ---------------------------------------------------------------

    def session(self, now: datetime | None = None) -> str:
        """'pre' | 'regular' | 'post' | 'closed'. Finnhub's market status wins when fresh (it knows holidays
        and early closes); otherwise the New York clock decides."""
        now = now or datetime.now(timezone.utc)
        if self.finnhub_session and time.time() - self.finnhub_session[0] < 20 * 60:
            _, session, holiday = self.finnhub_session
            return "closed" if holiday else FINNHUB_SESSIONS.get(session or "", "closed")
        return clock.us_session(now)

    async def refresh_session(self) -> None:
        try:
            ms = await asyncio.to_thread(self.fh.market_status)
            self.finnhub_session = (time.time(), ms.get("session"), bool(ms.get("holiday")))
        except FinnhubError as e:
            log.warning("market status unavailable (%s); using the New York clock", e)

    # --- previous closes and polled quotes -------------------------------------------

    async def refresh_reference_data(self) -> None:
        """Once per New York day: daily history for every watched symbol, previous closes, and seed quotes.

        The previous close comes from our own daily history (the last completed session
        before today in New York). Finnhub's /quote "pc" was seen to lag by a day over a
        weekend, so it is only a fallback.
        """
        today = clock.ny_date(datetime.now(timezone.utc))
        if self.reference_day == today:
            return
        symbols = self.universe.streamed + self.universe.polled
        try:
            await asyncio.to_thread(self._download_daily, symbols)
        except Exception as e:
            log.warning("daily history refresh failed (%s); using the cache", e)
        from_cache = 0
        for s in symbols:
            df = storage.load_prices(self.conn, s)
            self.daily[s] = df
            before = df[df.index.date < today] if not df.empty else df
            if not before.empty:
                self.book.prev_close[s] = float(before["close"].iloc[-1])
                from_cache += 1
                norms = detect.norms_from_daily(before)
                if norms:
                    self.norms[s] = norms
        for s in self.universe.streamed:
            try:
                q = await asyncio.to_thread(self.fh.quote, s)
            except FinnhubError as e:
                log.warning("%s: quote unavailable: %s", s, e)
                continue
            if q.get("c"):
                self.book.seed[s] = float(q["c"])  # shown until the first trade; never used in move windows
            if s not in self.book.prev_close and q.get("pc"):
                self.book.prev_close[s] = float(q["pc"])
        self.reference_day = today
        log.info("reference data for %s: previous closes from daily history for %d/%d symbols",
                 today, from_cache, len(symbols))  # fmt: skip

    def _download_daily(self, symbols: list[str]) -> None:
        """Runs in a worker thread, so it uses its own database connection."""
        conn = storage.connect(self.settings.paths.db)
        try:
            fetch.refresh(conn, self.prices, symbols, self.settings.data.benchmark, 0,
                          extended_hours=False, with_fundamentals=False)  # fmt: skip
        finally:
            conn.close()

    async def poll_overflow(self) -> None:
        for s in self.universe.polled:
            try:
                q = await asyncio.to_thread(self.fh.quote, s)
            except FinnhubError as e:
                log.warning("%s: quote failed: %s", s, e)
                continue
            if q.get("c"):
                self.polled[s] = {"price": float(q["c"]), "prev_close": q.get("pc"), "change_pct": q.get("dp")}

    async def poll_macro(self) -> None:
        try:
            self.macro = await asyncio.to_thread(self.fetch_macro, list(self.cfg.macro))
        except Exception as e:
            log.warning("macro quotes unavailable: %s", e)

    async def extended_fallback(self, now: float) -> None:
        """Outside regular hours the stream can go quiet; fill silent tapes from yfinance 5-minute bars."""
        silent = [s for s, tape in self.book.tapes.items()
                  if tape.last_ts is None or now - tape.last_ts > self.cfg.extended_fallback_minutes * 60]  # fmt: skip
        if not silent:
            return
        try:
            bars = await asyncio.to_thread(self.prices.intraday, silent)
        except Exception as e:
            log.warning("extended-hours fallback failed: %s", e)
            return
        fed = 0
        for s, df in bars.items():
            tape = self.book.tapes[s]
            for ts, row in df.iterrows():
                end = ts.timestamp() + 299  # treat each 5-minute bar's close as a trade at the end of the bar
                if tape.last_ts is None or end > tape.last_ts:
                    tape.add(float(row["close"]), min(end, now), float(row["volume"] or 0))
                    fed += 1
        if fed:
            log.info("extended-hours fallback: %d bars from yfinance for %d silent symbols", fed, len(bars))

    # --- loops ----------------------------------------------------------------------

    async def persist_loop(self) -> None:
        while True:
            await asyncio.sleep(self.cfg.persist_seconds)
            try:
                self.persist()
            except Exception:
                log.exception("saving live quotes failed")

    def persist(self) -> None:
        """Changed quotes, finished bars (if kept) and the heartbeat, in one transaction.

        The Pi runs from an SD card, so unchanged quotes are not rewritten: with the market
        closed a cycle writes only the heartbeat.
        """
        now_iso = storage.utcnow()
        session = self.session()
        rows = self.book.quote_rows(session, now_iso)
        rows += [{"symbol": s, "price": q["price"], "prev_close": q.get("prev_close"),
                  "day_change_pct": q.get("change_pct"), "session": session, "updated_at": now_iso}
                 for s, q in {**self.polled, **self.macro}.items()]  # fmt: skip
        changed = [r for r in rows if self.saved_quotes.get(r["symbol"]) != quote_key(r)]
        bars = self.book.take_finished_bars(time.time()) if self.cfg.save_bars else []
        storage.save_live(self.conn, changed, bars, now_iso)
        self.saved_quotes.update({r["symbol"]: quote_key(r) for r in changed})

    async def poll_loop(self) -> None:
        last_prune = 0.0
        while True:
            try:
                last_prune = await self.poll_round(last_prune)
            except Exception:
                log.exception("polling round failed")
            await asyncio.sleep(self.cfg.poll_minutes[session_kind(self.session())] * 60)

    async def poll_round(self, last_prune: float) -> float:
        """Session, reference data, macro and overflow quotes, extended-hours fallback, daily cleanup."""
        await self.refresh_session()
        session = self.session()
        await self.refresh_reference_data()
        await self.poll_macro()
        await self.poll_overflow()
        if session in ("pre", "post"):
            await self.extended_fallback(time.time())
        if time.time() - last_prune > 86400:
            self.housekeeping()
            last_prune = time.time()
        return last_prune

    def housekeeping(self) -> None:
        """Once a day: delete old bars, headlines, reports and log lines, so the SD card never fills up."""
        paths = self.settings.paths
        bars = storage.prune_bars(self.conn, self.cfg.bar_retention_days)
        news = storage.prune_news(self.conn, self.cfg.news_retention_days)
        reports = runctl.prune_files(paths.reports, self.cfg.report_retention_days, keep={"latest.html"})
        cap = int(self.cfg.log_max_mb * 1024 * 1024)
        logs = [name for name in ("cron.log", "watchdog.log") if runctl.trim_log(paths.logs / name, cap, 1024 * 1024)]
        if bars or news or reports or logs:
            log.info("cleanup: %d old bars, %d old headlines, %d old report files deleted; trimmed %s",
                     bars, news, reports, ", ".join(logs) or "no logs")  # fmt: skip

    # --- detection -------------------------------------------------------------------

    def fresh_price(self, symbol: str, now: float, session: str) -> float | None:
        """The symbol's last traded price if it is recent enough to act on (never the startup quote)."""
        tape = self.book.tapes[symbol]
        max_age = PRICE_MAX_AGE_S.get(session)
        if max_age is None or tape.last_ts is None or now - tape.last_ts > max_age:
            return None
        return tape.last_price

    def detect(self, now: datetime) -> list[detect.Event]:
        session, ts = self.session(now), now.timestamp()
        events = []
        for s in self.universe.streamed:
            price = self.fresh_price(s, ts, session)
            prev = self.book.prev_close.get(s)
            day = (price / prev - 1) * 100 if price and prev else None
            window = self.book.tapes[s].change_pct(self.acfg.stock_window_minutes, ts) if price else None
            if s in self.acfg.index_symbols:
                events += detect.index_events(s, session, price, window, day, self.acfg)
            else:
                events += detect.stock_events(s, session, price, window, day, self.norms.get(s), self.acfg)
        for s, q in self.polled.items():
            events += detect.stock_events(s, session, q["price"], None, q.get("change_pct"), self.norms.get(s), self.acfg)
        fresh_macro = {s: q for s, q in self.macro.items() if q.get("time") and now - q["time"] <= MACRO_MAX_AGE}
        events += detect.macro_events(fresh_macro, self.cfg.macro, session, self.acfg, clock.futures_open(now))
        return events

    async def check(self, now: datetime | None = None) -> list[tuple[detect.Event, alerts.Decision]]:
        now = now or datetime.now(timezone.utc)
        handled = []
        for e in self.detect(now):
            decision = self.policy.decide(e, now, shadow=self.shadow)
            if decision.status == "repeat":
                continue
            text = alerts.format_alert(e, now, self.acfg.quiet_hours.timezone, self.book.day_change_pct(e.symbol))
            message_id = None
            if decision.status == "send":
                try:
                    message_id = (await asyncio.to_thread(self.telegram.send, text))[0]
                except telegram.TelegramError as err:
                    decision = alerts.Decision("failed", str(err))
            alert_id = alerts.record(self.conn, e, decision, text, now, message_id)
            self.policy.remember(e, now)  # held and shadow too, so one situation is logged once
            if self.desk and (message_id or decision.status == "shadow"):
                self.desk.register(alert_id, e, message_id, now)  # look for news that explains it
            log.info("alert %s%s: %s %s", decision.status, f" ({decision.reason})" if decision.reason else "",
                     e.symbol, e.label)  # fmt: skip
            handled.append((e, decision))
        return handled

    def market_context(self, tickers: list[str]) -> list[str]:
        """Live numbers for the triage prompt: the indexes, VIX, and the tickers in question."""
        out = []
        for s in [*self.acfg.index_symbols, *[t for t in tickers if t not in self.acfg.index_symbols]]:
            change = self.book.day_change_pct(s) if s in self.book.tapes else (self.polled.get(s) or {}).get("change_pct")
            if change is not None:
                out.append(f"{s} {change:+.1f}% today")
        vix = self.macro.get("^VIX")
        if vix and vix.get("time") and datetime.now(timezone.utc) - vix["time"] <= MACRO_MAX_AGE:
            out.append(f"VIX {vix['price']:.1f} ({vix['change_pct']:+.0f}%)")
        return out

    async def detect_loop(self) -> None:
        while True:
            await asyncio.sleep(self.acfg.check_seconds)
            try:
                await self.check()
            except Exception:
                log.exception("detector check failed")

    # --- Telegram: notices, commands, documents --------------------------------------------

    async def notify(self, text: str, silent: bool = False) -> int | None:
        """A system message or a command reply (not an alert: shadow mode doesn't apply)."""
        if self.telegram is None:
            log.info("telegram not configured: %s", text[:200])
            return None
        try:
            return (await asyncio.to_thread(self.telegram.send, text, silent=silent))[0]
        except telegram.TelegramError as e:
            log.warning("telegram: %s", e)
            return None

    async def send_document(self, path, caption: str) -> None:
        if self.telegram is not None:
            try:
                await asyncio.to_thread(self.telegram.send_document, path, caption)
            except telegram.TelegramError as e:
                log.warning("telegram: %s", e)

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
        offset = int(storage.get_state(self.conn, "tg_offset", "0")) or None
        while True:
            try:
                updates = await asyncio.to_thread(self.telegram.get_updates, offset, 25)
            except telegram.TelegramError as e:  # e.g. 409: another process is polling the same bot
                log.warning("telegram updates: %s", e)
                await asyncio.sleep(15)
                continue
            for u in updates:
                offset = u["update_id"] + 1
                storage.set_state(self.conn, "tg_offset", str(offset))
                try:
                    await self.handle_update(u)
                except Exception:
                    log.exception("command failed")
                    await self.notify("⚠️ That command failed; see the watcher log.")

    # --- digests and /why ----------------------------------------------------------------

    async def launch_digest(self, slot: str, extra: list[str], reason: str) -> bool:
        """Run the pipeline in its own process (the cron wrapper); False if one is already running."""
        if self.digest_proc is not None and self.digest_proc.returncode is None:
            return False
        self.digest_proc = await asyncio.create_subprocess_exec(
            *self.digest_command, slot, *extra,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )  # fmt: skip
        log.info("digest %s started (%s), pid %d", slot, reason, self.digest_proc.pid)
        asyncio.create_task(self._digest_done(self.digest_proc, slot))
        return True

    async def _digest_done(self, proc, slot: str) -> None:
        code = await proc.wait()
        log.info("digest %s finished with exit code %d", slot, code)
        if code not in (runner.EXIT_OK, runner.EXIT_PARTIAL):
            await self.notify(f"⚠️ The {slot} digest failed (exit {code}); details in logs/cron.log on the Pi.")

    def holiday(self) -> bool:
        return bool(self.finnhub_session and time.time() - self.finnhub_session[0] < 20 * 60 and self.finnhub_session[2])

    async def run_due_digests(self, now: datetime) -> list[str]:
        """Start any scheduled digest whose time has come (or passed within DIGEST_GRACE) and isn't done."""
        started = []
        for slot in DIGEST_SLOTS:
            entry = getattr(self.settings.digests, slot)
            when = clock.last_occurrence(entry, now)
            if when is None or now - when > DIGEST_GRACE:
                continue
            if entry.trading_days_only and self.holiday():
                continue
            run_date = when.astimezone(ZoneInfo(self.settings.schedule.timezone)).date().isoformat()
            if storage.slot_done(self.conn, run_date, slot):
                continue
            tries = self.digest_tries.setdefault(f"{slot}:{when.isoformat()}", [])
            if len(tries) >= 2 or (tries and now - tries[-1] < timedelta(minutes=10)):
                continue
            if await self.launch_digest(slot, [], reason="schedule"):
                tries.append(now)
                started.append(slot)
        return started

    async def schedule_loop(self) -> None:
        while True:
            await asyncio.sleep(30)
            try:
                await self.run_due_digests(datetime.now(timezone.utc))
            except Exception:
                log.exception("digest schedule check failed")

    def start_why(self, symbol: str) -> bool:
        if symbol in self.why_running:
            return False
        self.why_running.add(symbol)
        asyncio.create_task(self._why(symbol))
        return True

    async def _why(self, symbol: str) -> None:
        try:
            opts = runner.RunOptions(tickers=[symbol], full=True, deliver=False)
            code = await asyncio.to_thread(self.why_runner, self.settings, opts)
            if code == runner.EXIT_LOCKED:
                await self.notify("A digest is being built right now; try /why again in a minute.")
            else:
                await self.notify(self.why_card(symbol))
        except Exception:
            log.exception("/why %s failed", symbol)
            await self.notify(f"⚠️ /why {telegram.esc(symbol)} failed; see the watcher log.")
        finally:
            self.why_running.discard(symbol)

    def why_card(self, symbol: str) -> str:
        esc = telegram.esc
        row = self.conn.execute(
            "SELECT * FROM verdicts WHERE ticker = ? AND created_at >= ? ORDER BY id DESC LIMIT 1",
            (symbol, (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat(timespec="seconds")),
        ).fetchone()
        if row is None:
            return f"⚠️ No verdict for {esc(symbol)} came back (usage limit or an error); see the watcher log."
        glyph = {"bullish": "▲", "bearish": "▼", "neutral": "●", "watch": "◆"}[row["verdict"]]
        price = self.book.price(symbol) if symbol in self.book.tapes else None
        change = self.book.day_change_pct(symbol) if symbol in self.book.tapes else None
        money = f" · ${price:,.2f}" + (f", {change:+.1f}% today" if change is not None else "") if price else ""
        reasons = "\n".join(f"• {esc(r)}" for r in json.loads(row["key_reasons_json"]))
        risks = "; ".join(esc(r) for r in json.loads(row["risks_json"]))
        gaps = json.loads(row["data_gaps_json"])
        return (f"🔍 <b>{esc(symbol)}</b> {glyph} {row['verdict']} ({row['confidence']}){money}\n{reasons}\n"
                f"<b>Risks:</b> {risks}\n<b>Would change my mind:</b> {esc(row['what_would_change_my_mind'])}"
                + (f"\n<i>Data gaps: {esc('; '.join(gaps))}</i>" if gaps else ""))  # fmt: skip

    # --- reports for commands ------------------------------------------------------------

    def next_digests(self, now: datetime) -> str:
        tz = self.acfg.quiet_hours.timezone
        upcoming = sorted((clock.next_occurrence(getattr(self.settings.digests, s), now), s) for s in DIGEST_SLOTS)
        return " · ".join(f"{s} {commands.local_time(t, tz, now)}" for t, s in upcoming[:2])

    def status_report(self, now: datetime) -> str:
        esc = telegram.esc
        up = timedelta(seconds=int(time.time() - self.started))
        quiet = self.book.seconds_since_trade(time.time())
        counts = alerts.counts_today(self.conn, now, self.acfg.quiet_hours.timezone)
        mute = storage.get_state(self.conn, "mute_until")
        muted = mute and now < datetime.fromisoformat(mute)
        calls = storage.calls_since(self.conn, "triage", alerts.day_start(now, self.acfg.quiet_hours.timezone)
                                    .isoformat(timespec="seconds"))  # fmt: skip
        ny = now.astimezone(clock.NY).strftime("%H:%M")
        lines = [
            f"<b>Watcher</b> · up {up} · alerts {'shadow (not sent)' if self.shadow else 'LIVE'}",
            f"Market: {self.session(now)} (New York {ny}) · stream {'up' if self.stream.connected else 'DOWN'}, "
            f"{len(self.universe.streamed)} symbols, last trade "
            + ("never" if quiet is None else f"{quiet:.0f}s ago"),
            " · ".join(self.market_context([])) or "no live index quotes yet",
            "Alerts today: " + (", ".join(f"{n} {s}" for s, n in counts.items()) or "none")
            + (f" · muted until {commands.local_time(datetime.fromisoformat(mute), self.acfg.quiet_hours.timezone, now)}"
               if muted else ""),
            f"Claude triage calls today: {calls}/{self.settings.triage.max_calls_per_day}"
            + (f" · {esc(self.desk.status())}" if self.desk else ""),
            f"Next digests: {self.next_digests(now)}",
        ]  # fmt: skip
        return "\n".join(lines)

    def watchlist_report(self) -> str:
        esc = telegram.esc
        lines = ["<b>Live prices</b>"]
        for s in self.universe.streamed:
            price, change = self.book.price(s), self.book.day_change_pct(s)
            if price is not None:
                lines.append(f"{esc(s)} ${price:,.2f}" + (f" ({change:+.1f}%)" if change is not None else ""))
        for s, q in self.macro.items():
            lines.append(f"{esc(alerts.display_name(s))} {q['price']:,.2f} ({q['change_pct']:+.1f}%)")
        return "\n".join(lines) if len(lines) > 1 else "No prices yet; the watcher just started."

    async def announce_start(self) -> None:
        """Say the watcher (re)started, at most once an hour (a crash loop must not spam)."""
        last = storage.get_state(self.conn, "start_notice")
        now = datetime.now(timezone.utc)
        if last and now - datetime.fromisoformat(last) < timedelta(hours=1):
            return
        storage.set_state(self.conn, "start_notice", now.isoformat())
        await self.notify(f"✅ Market watcher started ({'alerts in shadow mode' if self.shadow else 'alerts LIVE'}). "
                          "Send /help for commands.", silent=True)  # fmt: skip

    async def status_loop(self, every_s: float = 300) -> None:
        while True:
            await asyncio.sleep(every_s)
            log.info(self.status_line())

    def status_line(self) -> str:
        now = time.time()
        quiet = self.book.seconds_since_trade(now)
        trades = sum(t.trades for t in self.book.tapes.values())
        stamp = lambda q: "" if not q.get("time") else f" @{q['time'].astimezone(clock.NY):%a %H:%M} NY"  # noqa: E731
        macro = ", ".join(f"{s} {q['price']:.2f} ({q['change_pct']:+.1f}%{stamp(q)})" for s, q in self.macro.items()
                          if q.get("change_pct") is not None)  # fmt: skip
        return (
            f"status: session {self.session()}, stream {'up' if self.stream.connected else 'DOWN'} "
            f"({len(self.universe.streamed)} symbols, {trades} trades, last "
            f"{'never' if quiet is None else f'{quiet:.0f}s ago'})"
            + (f", polled {len(self.universe.polled)}" if self.universe.polled else "")
            + (f", {macro}" if macro else "")
            + f", alerts today: {self.alerts_today()}"
            + (f", {self.desk.status()}" if self.desk else "")
            + (" [shadow]" if self.shadow else "")
        )

    def alerts_today(self) -> str:
        counts = alerts.counts_today(self.conn, datetime.now(timezone.utc), self.acfg.quiet_hours.timezone)
        return ", ".join(f"{n} {status}" for status, n in counts.items()) or "none"

    async def run(self, duration_s: float | None = None) -> None:
        log.info("watcher starting%s: streaming %s%s", " (alerts in shadow mode)" if self.shadow else " (alerts LIVE)",
                 ", ".join(self.universe.streamed),
                 f"; polling {', '.join(self.universe.polled)}" if self.universe.polled else "")  # fmt: skip
        tasks = [
            asyncio.create_task(self.stream.run(), name="stream"),
            asyncio.create_task(self.poll_loop(), name="poll"),
            asyncio.create_task(self.persist_loop(), name="persist"),
            asyncio.create_task(self.status_loop(), name="status"),
            asyncio.create_task(self.detect_loop(), name="detect"),
        ]
        if self.desk:
            tasks += [asyncio.create_task(self.desk.poll_loop(), name="news"),
                      asyncio.create_task(self.desk.triage_loop(), name="triage")]  # fmt: skip
        tasks += [asyncio.create_task(self.commands_loop(), name="commands"),
                  asyncio.create_task(self.schedule_loop(), name="schedule")]  # fmt: skip
        await self.announce_start()
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop.set)
        try:
            waiters = [asyncio.create_task(stop.wait())] + tasks
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
            self.persist()
            log.info(self.status_line())
            log.info("watcher stopped")


def macro_quote(closes: pd.Series, futures: bool) -> dict | None:
    """Last price, reference close and change from 5-minute closes (UTC index).

    Futures: the reference is the last trade before the current session began
    (sessions start 18:00 New York). Others (VIX): the last value of the previous
    New York day. yfinance's quick quote was seen keeping Friday's previous close
    after the Sunday reopen, so we derive both numbers from timestamped bars.
    """
    closes = closes.dropna()
    if closes.empty:
        return None
    ny = closes.index.tz_convert(clock.NY)
    last_ny = ny[-1]
    if futures:
        start_day = last_ny.date() if last_ny.hour >= 18 else last_ny.date() - timedelta(days=1)
        start = datetime.combine(start_day, datetime.min.time().replace(hour=18), clock.NY)
        before = closes[ny < start]
    else:
        before = closes[ny.date < last_ny.date()]
    if before.empty:
        return None
    price, ref = float(closes.iloc[-1]), float(before.iloc[-1])
    return {"price": price, "prev_close": ref, "change_pct": (price / ref - 1) * 100,
            "time": closes.index[-1].to_pydatetime()}  # fmt: skip


def fetch_macro_quotes(symbols: list[str]) -> dict[str, dict]:
    """Index and futures quotes from yfinance 5-minute bars (not on Finnhub's free tier)."""
    import yfinance as yf

    raw = yf.download(symbols, period="5d", interval="5m", prepost=True, auto_adjust=False,
                      group_by="ticker", progress=False, threads=True)  # fmt: skip
    out = {}
    for s in symbols:
        if isinstance(raw.columns, pd.MultiIndex) and s in raw.columns.get_level_values(0):
            closes = raw[s]["Close"]
            closes.index = closes.index.tz_convert("UTC")
            q = macro_quote(closes, futures=s.endswith("=F"))
            if q:
                out[s] = q
    return out
