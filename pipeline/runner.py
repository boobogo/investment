"""One pipeline run: fetch -> indicators + news -> packets -> analyst -> log everything to SQLite.

Scheduled runs (the cron wrapper passes --slot morning|afternoon) are
idempotent per (local date, slot): a slot that already finished today is
skipped unless --force. Runs without --slot are "manual" and never block or get
blocked. A file lock keeps runs from overlapping.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass
from datetime import date

import pandas as pd

from pipeline import brain, config, digest, fetch, indicators, news, packets, report, runctl, screen, storage

log = logging.getLogger(__name__)

EXIT_OK, EXIT_FAILED, EXIT_PARTIAL, EXIT_LOCKED = 0, 1, 3, 75


@dataclass
class RunOptions:
    dry_run: bool = False
    tickers: list[str] | None = None
    full: bool = False
    offline: bool = False
    web_search: int = 0
    slot: str | None = None
    force: bool = False
    highlights: bool = False  # news highlights in a manual run (scheduled digests always have them)
    deliver: bool = True  # send the report via report.delivery (/why replies with its own short message)


@dataclass
class Prepared:
    packets: list[dict]  # every ticker with prices, most urgent first
    decisions: dict[str, screen.Decision]
    news: dict[str, news.NewsResult]
    prices: dict[str, tuple[float, str]]  # ticker -> (close, date) the packet was built on
    bench: pd.DataFrame | None
    as_of: str | None  # market date of the benchmark's latest bar

    @property
    def to_send(self) -> list[dict]:
        return [p for p in self.packets if self.decisions[p["t"]].send]

    def bench_close(self, on: str) -> float | None:
        if self.bench is None:
            return None
        v = self.bench["close"].get(pd.Timestamp(on))
        return None if v is None or pd.isna(v) else float(v)


def load_market(conn: sqlite3.Connection, settings: config.Settings, tickers: list[config.Ticker], offline: bool):
    source = fetch.make_source(settings.data.source, settings.data.history_period)
    return fetch.refresh(
        conn,
        source,
        [t.symbol for t in tickers],
        settings.data.benchmark,
        settings.data.fundamentals_max_age_hours,
        offline=offline,
        extended_hours=settings.data.extended_hours,
    )


def prepare(
    conn: sqlite3.Connection,
    settings: config.Settings,
    tickers: list[config.Ticker],
    opts: RunOptions,
    today: date,
    run_id: int,
    slot: str,
) -> Prepared:
    """Fetch everything, screen, and build one packet per ticker that has prices."""
    md = load_market(conn, settings, tickers, opts.offline)
    if opts.offline:
        news_results = {t.symbol: news.NewsResult([], ["news not fetched (offline run)"]) for t in tickers}
    else:
        news_results = news.gather(tickers, settings.news, conn)

    bench = md.prices.get(settings.data.benchmark)
    bench_as_of = bench.index[-1].strftime("%Y-%m-%d") if bench is not None else None
    previous = storage.last_verdicts(conn, [t.symbol for t in tickers], before_run_id=run_id)

    rows, conditions, prices = {}, {}, {}
    for t in tickers:
        if t.symbol not in md.prices:
            log.error("%s: no price data, skipped (%s)", t.symbol, "; ".join(md.errors.get(t.symbol, [])))
            continue
        tech = indicators.technicals(md.prices[t.symbol], bench)
        issues = md.errors.get(t.symbol, []) + news_results[t.symbol].errors
        if bench_as_of and tech["as_of"] < bench_as_of:
            issues.append(f"prices stale: last bar {tech['as_of']}, market {bench_as_of}")

        since_last = None
        if t.symbol in previous:
            prev = previous[t.symbol]
            since_last = {
                "verdict": prev["verdict"],
                "confidence": prev["confidence"],
                "as_of": prev["as_of"],
                "px_chg_pct": round((tech["close"] / prev["price"] - 1) * 100, 1),
            }
        fund = indicators.fundamentals(md.fundamentals.get(t.symbol, {}))
        conditions[t.symbol] = screen.conditions_for(
            tech, fund, news_results[t.symbol], md.extended.get(t.symbol), settings.screen, today,
            held=t.held,
            days_since_review=(today - date.fromisoformat(previous[t.symbol]["as_of"])).days
            if t.symbol in previous else None,
        )  # fmt: skip
        rows[t.symbol] = (t, tech, fund, issues, since_last)
        prices[t.symbol] = (tech["close"], tech["as_of"])

    if opts.full:
        review_all = "full review (--full)"
    elif slot == "morning" and settings.screen.full_review_weekday == today.weekday():
        review_all = "weekly full review"
    else:
        review_all = None
    decisions = screen.decide(
        conditions,
        storage.last_trigger_keys(conn),
        settings.screen,
        pending=storage.get_pending(conn),
        review_all=review_all,
    )

    built = []
    for d in decisions:
        t, tech, fund, issues, since_last = rows[d.ticker]
        built.append(
            packets.build_packet(
                t, tech, fund, news_results[t.symbol].items,
                today=today,
                signals=[c.label for c in d.conditions],
                since_last=since_last,
                data_issues=issues,
                extended=md.extended.get(t.symbol),
                live=_live_quote(conn, t.symbol),
            )
        )  # fmt: skip
    return Prepared(built, {d.ticker: d for d in decisions}, news_results, prices, bench, bench_as_of)


def execute(settings: config.Settings, opts: RunOptions, *, backend_factory=brain.make_backend, now=None) -> int:
    now_local = now or runctl.local_now(settings.schedule.timezone)
    slot = opts.slot or "manual"
    tickers = config.load_tickers(settings.paths.tickers, opts.tickers)
    try:
        with runctl.run_lock(settings.paths.db.parent / "run.lock"):
            conn = storage.connect(settings.paths.db)
            try:
                return _locked_run(conn, settings, opts, tickers, now_local, slot, backend_factory)
            finally:
                conn.close()
    except runctl.AlreadyRunning as e:
        log.error("%s; not starting another run", e)
        return EXIT_LOCKED


def _locked_run(conn, settings, opts, tickers, now_local, slot, backend_factory) -> int:
    run_date = now_local.date().isoformat()
    if not opts.dry_run and slot != "manual" and not opts.force and storage.slot_done(conn, run_date, slot):
        log.info("the %s run for %s already completed; nothing to do (--force runs it again)", slot, run_date)
        return EXIT_OK

    mode = "dry" if opts.dry_run else ("full" if opts.full else "screened")
    run_id = storage.start_run(conn, run_date=run_date, slot=slot, mode=mode, args=asdict(opts))
    log.info("run %d started: %s %s slot, mode %s, %d tickers", run_id, run_date, slot, mode, len(tickers))
    try:
        return _run(conn, run_id, settings, opts, tickers, now_local.date(), slot, backend_factory)
    except Exception as e:
        log.exception("run %d failed", run_id)
        storage.finish_run(conn, run_id, status="failed", error=f"{type(e).__name__}: {e}")
        return EXIT_FAILED


def _run(conn, run_id, settings, opts, tickers, today, slot, backend_factory) -> int:
    prep = prepare(conn, settings, tickers, opts, today, run_id, slot)
    # the Sunday digest is about news and the week ahead; Monday morning reviews every stock anyway
    to_send = [] if slot == "weekly" else prep.to_send
    held_back = [d.ticker for d in prep.decisions.values() if d.note]
    with_extras = slot in digest.DIGEST_SLOTS or opts.highlights

    if opts.dry_run:
        storage.save_packets(conn, run_id, _packet_rows(prep, sent=set(), batch_of={}))
        extras = digest.build(conn, run_id, settings, slot, tickers, prep.news, backend_factory=backend_factory,
                              use_model=False, online=not opts.offline) if with_extras else None  # fmt: skip
        storage.finish_run(conn, run_id, status="dry", as_of=prep.as_of, n_tickers=len(prep.packets), n_sent=0,
                           extras_json=extras.to_json() if extras else None)  # fmt: skip
        _print_dry_run(prep, settings, extras)
        return EXIT_OK
    if held_back:
        storage.set_pending(conn, run_id, held_back, f"over max_sends={settings.screen.max_sends}")

    outcome = brain.ReviewOutcome()
    backend_name = backend_model = None
    if to_send:
        backend = backend_factory(settings.llm)
        backend_name, backend_model = backend.name, backend.model
        outcome = brain.review(
            to_send,
            backend,
            settings.paths.prompt.read_text(),
            settings.llm,
            benchmark=settings.data.benchmark,
            web_tickers={p["t"] for p in to_send[: opts.web_search]},
        )
        _save_review(conn, run_id, prep, outcome, to_send, backend.name)
    else:
        storage.save_packets(conn, run_id, _packet_rows(prep, sent=set(), batch_of={}))
        log.info("screen: nothing to review%s", " (weekly digest)" if slot == "weekly" else "")

    extras = None
    if with_extras and not outcome.stop_reason:  # after a usage limit, don't spend more calls on highlights
        try:
            extras = digest.build(conn, run_id, settings, slot, tickers, prep.news, backend_factory=backend_factory,
                                  use_model=True, online=not opts.offline)  # fmt: skip
            for i, res in enumerate(extras.calls, start=1):
                storage.save_call(conn, run_id, {**brain.result_row(res, backend_name or settings.llm.backend),
                                                 "batch_no": 0, "attempt": i, "purpose": "highlights"})  # fmt: skip
        except Exception:
            log.exception("run %d: digest extras failed; the report goes out without them", run_id)

    totals = outcome.totals()
    for res in extras.calls if extras else []:
        for k in storage.USAGE_COLS:
            totals[k] += res.usage.get(k, 0) or 0
        totals["cost_usd"] = round(totals["cost_usd"] + (res.usage.get("cost_usd") or 0), 6)
    pending = len(outcome.pending) + len(held_back)
    storage.finish_run(
        conn, run_id,
        status="partial" if pending else "ok",
        as_of=prep.as_of, backend=backend_name, model=backend_model or settings.llm.model, effort=settings.llm.effort,
        n_tickers=len(prep.packets), n_sent=len(to_send), n_verdicts=len(outcome.verdicts),
        n_pending=pending, cost_usd=totals["cost_usd"], stop_reason=outcome.stop_reason,
        extras_json=extras.to_json() if extras else None,
        **{k: totals[k] for k in storage.USAGE_COLS},
    )  # fmt: skip
    if to_send:
        print_review(outcome, to_send)
    print(f"run {run_id} logged to {settings.paths.db}")
    publish_report(conn, run_id, settings, opts.deliver)
    return EXIT_PARTIAL if pending else EXIT_OK


def _save_review(conn, run_id, prep: Prepared, outcome: brain.ReviewOutcome, to_send: list[dict], backend_name: str):
    batch_of = {t: c.batch_no for c in outcome.calls for t in c.tickers}
    storage.save_packets(conn, run_id, _packet_rows(prep, sent={p["t"] for p in to_send}, batch_of=batch_of))
    call_ids = [storage.save_call(conn, run_id, _call_row(c, backend_name)) for c in outcome.calls]
    for t, v in outcome.verdicts.items():
        close, on = prep.prices[t]
        call_index = outcome.source[t]
        storage.save_verdict(
            conn, run_id, call_ids[call_index], v.model_dump(),
            as_of=on, price=close, bench_price=prep.bench_close(on),
            model=outcome.calls[call_index].result.model, horizon_days=brain.HORIZON_DAYS,
        )  # fmt: skip
    storage.log_triggers(conn, run_id, [
        (t, c.kind, c.key) for t in outcome.verdicts for c in prep.decisions[t].conditions
    ])  # fmt: skip
    storage.clear_pending(conn, list(outcome.verdicts))
    if outcome.pending:
        storage.set_pending(conn, run_id, outcome.pending, outcome.stop_reason or "model call failed")
    news.mark_seen(conn, {t: r for t, r in prep.news.items() if t in outcome.verdicts})


def _live_quote(conn: sqlite3.Connection, symbol: str) -> dict | None:
    """The watcher's live price for the packet, if the watcher is running and the session is open."""
    if not storage.watcher_alive(conn, max_age_s=180):
        return None
    q = storage.live_quote(conn, symbol)
    if not q or q.get("session") not in ("pre", "regular", "post") or q.get("day_change_pct") is None:
        return None
    return {"px": round(q["price"], 2), "chg_pct": round(q["day_change_pct"], 2), "session": q["session"],
            "utc": q["updated_at"][5:16].replace("T", " ")}  # fmt: skip


def publish_report(conn: sqlite3.Connection, run_id: int, settings: config.Settings, deliver: bool = True) -> None:
    """Write the HTML report and deliver it; a report problem never fails the run itself."""
    try:
        path, data = report.build(conn, run_id, settings.paths.reports, settings)
    except Exception:
        log.exception("run %d: report generation failed", run_id)
        return
    print(f"\n{report.summary_text(data)}\nreport: {path}")
    if deliver:
        deliver_report(conn, settings, data, path)


def deliver_report(conn: sqlite3.Connection, settings: config.Settings, data: dict, path, deliverer=None) -> bool:
    """Send a built report through the configured channel and log it in the alerts table."""
    deliverer = deliverer or report.make_deliverer(settings.report.delivery, settings)
    if deliverer is None:
        return False
    run = data["run"]
    try:
        text, message_id = deliverer.send(data, path)
    except Exception as e:
        log.exception("run %d: report delivery via %s failed", run["id"], settings.report.delivery)
        storage.log_alert(conn, kind="digest", severity="info", text=f"report for run {run['id']}",
                          status="failed", reason=f"{type(e).__name__}: {e}")  # fmt: skip
        return False
    storage.log_alert(conn, kind="digest", severity="info", text=text, status="sent", telegram_message_id=message_id)
    log.info("run %d: report delivered via %s", run["id"], settings.report.delivery)
    return True


def _packet_rows(prep: Prepared, sent: set[str], batch_of: dict[str, int]) -> list[dict]:
    return [
        {
            "ticker": p["t"], "sent": p["t"] in sent, "batch_no": batch_of.get(p["t"]),
            "signals": [asdict(c) for c in prep.decisions[p["t"]].conditions], "packet": p,
            "price": prep.prices[p["t"]][0], "price_date": prep.prices[p["t"]][1],
        }
        for p in prep.packets
    ]  # fmt: skip


def _call_row(c: brain.CallRecord, backend_name: str) -> dict:
    return {
        **brain.result_row(c.result, backend_name),
        "batch_no": c.batch_no, "attempt": c.attempt, "reask": c.reask, "web_search": c.web_search,
        "tickers": c.tickers,
    }  # fmt: skip


# --- console output ------------------------------------------------------------


def _print_dry_run(prep: Prepared, settings: config.Settings, extras: digest.Extras | None = None) -> None:
    out_dir = settings.paths.reports / "dry-run"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"packets-{pd.Timestamp.now():%Y%m%d-%H%M}.json"
    out.write_text(json.dumps(prep.packets, indent=1, ensure_ascii=False))

    rows = []
    for p in prep.packets:
        d = prep.decisions[p["t"]]
        rows.append({
            "ticker": p["t"],
            "send": "yes" if d.send else ("held back" if d.note else "-"),
            "≈tokens": packets.estimate_tokens(p),
            "news": f"{len(p['news'])}/{prep.news[p['t']].n_raw}",
            "conditions (* = fresh)": "; ".join(("*" if c.fresh else "") + c.label for c in d.conditions),
        })  # fmt: skip
    table = pd.DataFrame(rows).set_index("ticker")
    with pd.option_context("display.width", 250, "display.max_colwidth", 120):
        print(table)
    sent = [p for p in prep.packets if prep.decisions[p["t"]].send]
    est = sum(packets.estimate_tokens(p) for p in sent)
    print(f"\nwould send {len(sent)} of {len(prep.packets)} tickers, ≈{est} packet tokens")
    print(f"packets written to {out}  (dry run: no Claude call)")
    if extras:
        print(f"\nnews since {extras.since:%Y-%m-%d %H:%M} UTC: {len(extras.ranked)} candidate stories "
              f"for the highlights call (top 15 shown)")  # fmt: skip
        for score, story in extras.ranked[:15]:
            lead = story[0]
            tickers = ",".join(sorted({t for m in story for t in m.tickers}))
            print(f"  {score:4.1f}  {lead.published:%m-%d %H:%M}  [{lead.source[:12]}] {lead.title[:85]}"
                  f"{f'  ({tickers})' if tickers else ''}{f'  x{len(story)}' if len(story) > 1 else ''}")  # fmt: skip
        for e in extras.earnings:
            print(f"  earnings {e['date']} {e['symbol']} {e['when']}" + (f" (Yahoo: {e['yahoo_date']})" if e["yahoo_date"] else ""))
        for err in extras.errors:
            print(f"  ! {err}")


def print_review(outcome: brain.ReviewOutcome, sent: list[dict]) -> None:
    rows = []
    for p in sent:
        v = outcome.verdicts.get(p["t"])
        rows.append({
            "ticker": p["t"],
            "verdict": v.verdict if v else "-",
            "conf": v.confidence if v else "-",
            "first reason": (v.key_reasons[0] if v else "(pending)")[:110],
        })  # fmt: skip
    with pd.option_context("display.width", 250, "display.max_colwidth", 110):
        print(pd.DataFrame(rows).set_index("ticker"))
    print()
    for c in outcome.calls:
        u = c.result.usage
        print(
            f"call batch={c.batch_no} attempt={c.attempt}{' re-ask' if c.reask else ''}"
            f"{' web' if c.web_search else ''}: {'ok' if c.result.ok else 'FAILED: ' + str(c.result.error)} "
            f"model={c.result.model} {c.result.duration_ms / 1000:.1f}s "
            f"in={u.get('input_tokens', 0)} cache_w={u.get('cache_write_tokens', 0)} "
            f"cache_r={u.get('cache_read_tokens', 0)} out={u.get('output_tokens', 0)} "
            f"(thinking {u.get('thinking_tokens', 0)}) api_equiv=${u.get('cost_usd') or 0:.4f}"
        )
    t = outcome.totals()
    prompt_tokens = t["input_tokens"] + t["cache_write_tokens"] + t["cache_read_tokens"]
    print(f"\ntotal: {t['calls']} calls, prompt {prompt_tokens} tokens, output {t['output_tokens']} tokens, "
          f"API-equivalent ${t['cost_usd']:.4f}")  # fmt: skip
    if outcome.pending:
        print(f"pending (carried to next run): {', '.join(outcome.pending)}; reason: {outcome.stop_reason or 'failed'}")
