"""Daily market research pipeline. See README.md for setup and usage."""

from __future__ import annotations

import argparse
import logging
import sys

import pandas as pd

from pipeline import config, evaluate, indicators, report, runctl, runner, storage, telegram

log = logging.getLogger("main")


def _symbols(arg: str) -> list[str]:
    return [s for s in arg.replace(" ", ",").split(",") if s]


def cmd_indicators(args, settings: config.Settings) -> int:
    """Fetch data and print the indicator snapshot per ticker (inspection helper)."""
    tickers = config.load_tickers(settings.paths.tickers, args.tickers)
    md = runner.load_market(storage.connect(settings.paths.db), settings, tickers, args.offline)
    bench = md.prices.get(settings.data.benchmark)

    rows = {}
    for t in tickers:
        if t.symbol not in md.prices:
            log.error("%s: skipped, %s", t.symbol, "; ".join(md.errors.get(t.symbol, [])))
            continue
        rows[t.symbol] = {
            **indicators.technicals(md.prices[t.symbol], bench),
            **indicators.fundamentals(md.fundamentals.get(t.symbol, {})),
        }
    table = pd.DataFrame(rows)
    with pd.option_context(
        "display.max_rows", None,
        "display.max_columns", None,
        "display.width", 250,
        "display.precision", 2,
    ):  # fmt: skip
        print(table)
    return 0


def cmd_run(args, settings: config.Settings) -> int:
    opts = runner.RunOptions(
        dry_run=args.dry_run,
        tickers=args.tickers,
        full=args.full,
        offline=args.offline,
        web_search=args.web_search,
        slot=args.slot,
        force=args.force,
        highlights=args.highlights,
    )
    return runner.execute(settings, opts)


def cmd_report(args, settings: config.Settings) -> int:
    """(Re)build the HTML report for a run (default: the latest run with verdicts)."""
    conn = storage.connect(settings.paths.db)
    run_id = args.run or conn.execute(
        "SELECT max(id) FROM runs WHERE status IN ('ok', 'partial') AND n_verdicts > 0"
    ).fetchone()[0]
    if run_id is None:
        log.error("no completed run with verdicts yet")
        return 1
    path, data = report.build(conn, run_id, settings.paths.reports, settings)
    print(report.summary_text(data))
    print(f"report: {path}")
    if args.send and not runner.deliver_report(conn, settings, data, path):
        log.error("delivery failed or report.delivery is 'none' (see logs/pipeline.log)")
        return 1
    return 0


def cmd_telegram_test(args, settings: config.Settings) -> int:
    """Send one test message to the configured chat."""
    client = telegram.Telegram.from_env(settings.telegram)
    me = client.get_me()
    [message_id] = client.send(
        f"✅ <b>Test message</b> from the market research pipeline.\n"
        f"Bot @{telegram.esc(me.get('username'))} can reach this chat; alerts and digests will arrive here."
    )
    print(f"sent test message {message_id} via @{me.get('username')}")
    return 0


def _print_table(title: str, df: pd.DataFrame, min_sample: int) -> None:
    out = df.copy()
    if "hit_rate" in out:
        out["hit_rate"] = out["hit_rate"].map(lambda x: f"{x:.0%}")
    if "avg_excess" in out:
        out["avg_excess"] = out["avg_excess"].map(lambda x: f"{x:+.2f}%")
    out["n"] = [f"{n}" + ("  (too few to judge)" if n < min_sample else "") for n in out["n"]]
    print(f"\n{title}")
    print(out.to_string())


def cmd_evaluate(args, settings: config.Settings) -> int:
    """Score logged verdicts against later prices, and the trend-rule baseline over the cached history."""
    cfg, bench = settings.evaluate, settings.data.benchmark
    conn = storage.connect(settings.paths.db)
    horizons = [args.horizon] if args.horizon else cfg.horizons
    scored = evaluate.score_verdicts(conn, cfg, bench)
    n_logged = len(evaluate.latest_verdicts(conn))

    print(f"Claude's verdicts: {n_logged} logged (latest per ticker and date), excess returns vs {bench}")
    for h in horizons:
        tables = evaluate.summarize(scored, h) if not scored.empty else {}
        if not tables:
            when = evaluate.first_maturity(conn, h)
            print(f"\n{h} trading days: nothing to score yet" + (f" (first verdict matures around {when})" if when else ""))
            continue
        _print_table(f"{h} trading days, by verdict:", tables["by_verdict"], cfg.min_sample)
        _print_table(f"{h} trading days, by confidence:", tables["by_confidence"], cfg.min_sample)
        _print_table(f"{h} trading days, against baselines on the same dates:", tables["compare"], cfg.min_sample)

    if args.baseline or scored.empty:
        tickers = [t.symbol for t in config.load_tickers(settings.paths.tickers)]
        hist = evaluate.baseline_history(conn, tickers, cfg, bench)
        print(f"\nReference: the trend rule over the cached price history ({len(tickers)} tickers, every 5th day)")
        for h in horizons:
            d = hist[hist["horizon"] == h]
            if d.empty:
                continue
            _print_table(f"{h} trading days, trend rule by verdict:", evaluate._group(d, "verdict"), cfg.min_sample)
            print(f"  always bullish (stock beat {bench}): {(d['excess'] > 0).mean():.0%} of {len(d)} ticker-dates")
    return 0


def cmd_alerts(args, settings: config.Settings) -> int:
    """Recorded price alerts, or an estimate of what the current thresholds would have sent."""
    if args.estimate:
        from pipeline import tune

        found, n_days = tune.estimate(settings)
        print(f"Replayed {n_days} trading days of 5-minute bars with the current thresholds "
              f"(regular hours only; extended hours, futures and VIX not included).")  # fmt: skip
        if found.empty:
            print("No alerts would have fired.")
            return 0
        found["day"] = found["time"].dt.tz_convert(settings.alerts.quiet_hours.timezone).dt.date
        per_day = found.groupby("day").size().reindex(sorted(found["day"].unique()))
        print(f"{len(found)} alerts on {len(per_day)} of {n_days} days: {len(found) / n_days:.2f} per trading day, "
              f"busiest day {per_day.max()} ({per_day.idxmax()})")  # fmt: skip
        print("\nby kind:", found.groupby("kind").size().to_dict())
        print("by symbol:", found.groupby("symbol").size().sort_values(ascending=False).to_dict())
        print("held by the daily budget:", int((found["status"] == "held").sum()))
        print("\nmost recent 15:")
        for r in found.tail(15).itertuples():
            print(f"  {r.time.tz_convert(settings.alerts.quiet_hours.timezone):%m-%d %H:%M}  {r.symbol:5} {r.label}")
        return 0
    conn = storage.connect(settings.paths.db)
    rows = conn.execute(
        "SELECT created_at, status, severity, symbol, reason, text FROM alerts WHERE kind = 'price'"
        " AND created_at >= datetime('now', ?) ORDER BY created_at",
        (f"-{args.days} days",),
    ).fetchall()
    print(f"price alerts in the last {args.days} days: {len(rows)}")
    for r in rows:
        first = r["text"].splitlines()[0]
        print(f"  {r['created_at'][:16]}  {r['status']:6} {r['severity']:8} {first}"
              + (f"  ({r['reason']})" if r["reason"] else ""))  # fmt: skip
    return 0


def cmd_watchdog(args, settings: config.Settings) -> int:
    """For cron: warn on Telegram once when the watcher's heartbeat goes stale, and once when it is back."""
    from datetime import datetime, timedelta, timezone

    conn = storage.connect(settings.paths.db)
    beat = storage.get_state(conn, "heartbeat")
    now = datetime.now(timezone.utc)
    stale = beat is None or now - datetime.fromisoformat(beat) > timedelta(minutes=args.max_age)
    warned = storage.get_state(conn, "watchdog_warned") == "yes"
    if stale == warned:
        return 1 if stale else 0
    client = telegram.Telegram.from_env(settings.telegram)
    if stale:
        since = "never" if beat is None else datetime.fromisoformat(beat).astimezone().strftime("%a %H:%M")
        client.send(f"⚠️ The market watcher has been silent since {since}. "
                    "On the Pi: <code>systemctl status market-watcher</code>")  # fmt: skip
    else:
        client.send("✅ The market watcher is running again.", silent=True)
    storage.set_state(conn, "watchdog_warned", "yes" if stale else "no")
    return 1 if stale else 0


def cmd_watch(args, settings: config.Settings) -> int:
    """Run the always-on watcher (Ctrl+C or SIGTERM stops it)."""
    import asyncio

    from pipeline.watcher import Watcher

    asyncio.run(Watcher(settings, shadow=args.shadow).run(duration_s=args.minutes * 60 if args.minutes else None))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    ind = sub.add_parser("indicators", help="fetch data and print indicator snapshots")
    ind.add_argument("--tickers", type=_symbols, help="comma-separated subset, e.g. AAPL,MSFT")
    ind.add_argument("--offline", action="store_true", help="use cached data only")
    ind.set_defaults(func=cmd_indicators)

    run = sub.add_parser("run", help="run the pipeline")
    run.add_argument("--dry-run", action="store_true", help="build packets but make no Claude call")
    run.add_argument("--tickers", type=_symbols, help="comma-separated subset, e.g. AAPL,MSFT")
    run.add_argument("--full", action="store_true", help="skip the pre-filter: review every ticker")
    run.add_argument("--offline", action="store_true", help="use cached data only, no downloads")
    run.add_argument("--web-search", type=int, default=0, metavar="N",
                     help="let Claude use web search for the top N tickers (costs more)")
    run.add_argument("--slot", choices=runctl.SLOTS, default="manual",
                     help="schedule slot; the cron wrapper passes morning/afternoon (default: manual)")
    run.add_argument("--force", action="store_true", help="run even if this slot already completed today")
    run.add_argument("--highlights", action="store_true",
                     help="add news highlights to a manual run (scheduled digests always have them)")
    run.set_defaults(func=cmd_run)

    rep = sub.add_parser("report", help="rebuild the HTML report for a run")
    rep.add_argument("--run", type=int, help="run id (default: latest run with verdicts)")
    rep.add_argument("--send", action="store_true", help="also deliver it (report.delivery: telegram)")
    rep.set_defaults(func=cmd_report)

    ev = sub.add_parser("evaluate", help="score past verdicts against what prices did next")
    ev.add_argument("--horizon", type=int, help="only this horizon in trading days (default: all configured)")
    ev.add_argument("--baseline", action="store_true", help="also show the trend rule over the cached history")
    ev.set_defaults(func=cmd_evaluate)

    w = sub.add_parser("watch", help="run the always-on market watcher")
    w.add_argument("--shadow", action="store_true", help="log alerts instead of sending them")
    w.add_argument("--minutes", type=float, help="stop after this many minutes (for testing)")
    w.set_defaults(func=cmd_watch)

    al = sub.add_parser("alerts", help="recorded price alerts, or --estimate what the thresholds would send")
    al.add_argument("--days", type=int, default=7)
    al.add_argument("--estimate", action="store_true", help="replay ~60 days of 5-minute history")
    al.set_defaults(func=cmd_alerts)

    wd = sub.add_parser("watchdog", help="for cron: warn on Telegram if the watcher stopped")
    wd.add_argument("--max-age", type=float, default=10, help="minutes without a heartbeat before warning")
    wd.set_defaults(func=cmd_watchdog)

    tg = sub.add_parser("telegram-test", help="send one test message to the Telegram chat in .env")
    tg.set_defaults(func=cmd_telegram_test)

    args = p.parse_args(argv)
    config.load_env()
    settings = config.load_settings()
    runctl.setup_logging(settings.paths.logs, args.verbose)
    return args.func(args, settings)


if __name__ == "__main__":
    sys.exit(main())
