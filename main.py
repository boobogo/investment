"""Market watcher: alerts on unusual moves in the main markets, and Claude-written news digests.

See README.md for setup and usage.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import datetime, timedelta, timezone

from pipeline import alerts, briefing, config, markets, telegram
from pipeline.state import State

log = logging.getLogger("main")


def setup_logging(verbose: bool = False) -> None:
    """Everything goes to stderr: on the Pi that is the systemd journal (capped), so no extra log files."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        stream=sys.stderr,
    )
    for noisy in ("yfinance", "urllib3", "peewee"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def cmd_watch(args, settings: config.Settings) -> int:
    """Run the always-on watcher (Ctrl+C or SIGTERM stops it)."""
    from pipeline.watcher import Watcher, make_telegram

    w = Watcher(settings, shadow=args.shadow, telegram_client=make_telegram(settings.telegram))
    asyncio.run(w.run(duration_s=args.minutes * 60 if args.minutes else None))
    return 0


def cmd_check(args, settings: config.Settings) -> int:
    """One price check now: print the snapshot and what would alert (ignores repeats and quiet hours)."""
    w = settings.watch
    symbols = list(w.names())
    now = datetime.now(timezone.utc)
    quotes = markets.snapshot(markets.fetch_closes(symbols), markets.fetch_sigmas(symbols, w.sigma_days),
                              w.names(), w.groups(), now, timedelta(minutes=w.max_quote_age_minutes))  # fmt: skip
    for line in briefing.snapshot_lines(quotes):
        print(line)
    print("\ntypical daily move (sigma): " + ", ".join(f"{q.name} {q.sigma:.2f}%" for q in quotes if q.sigma))
    hits = alerts.find_hits(quotes, settings.alerts)
    print("\nwould alert:" if hits else "\nnothing unusual right now")
    for line in alerts.hit_lines(hits):
        print(f"  {line}")
    return 0


def cmd_digest(args, settings: config.Settings) -> int:
    """Build a digest now. --dry-run shows the request only; otherwise Claude is called (uses the Pro quota)."""
    from pipeline.watcher import Watcher, make_telegram

    # read-only state: on the Pi the service owns data/state.json (use /digest in Telegram there)
    state = State(settings.paths.state, readonly=True)
    client = make_telegram(settings.telegram) if args.send else None
    w = Watcher(settings, telegram_client=client, state=state)
    now = datetime.now(timezone.utc)
    since = w.previous_digest(now)

    async def go() -> int:
        quotes = await w.refresh_quotes(now)
        user = briefing.digest_request(args.kind, now, since, quotes, w.window_moves(since, now),
                                       settings.digests.follow, w.tz)  # fmt: skip
        if args.dry_run:
            print(user)
            return 0
        res = await w.call_claude(args.kind, "digest", user, briefing.DIGEST_SCHEMA, now)
        if not res.ok:
            log.error("Claude failed: %s", res.error)
            return 1
        text = briefing.render_digest(args.kind, res.data, quotes, now, w.tz)
        print(text)
        print(f"\n[{res.duration_ms // 1000}s, {res.usage.get('turns')} turns, {len(text)} characters, "
              f"{res.usage.get('output_tokens')} output tokens]")  # fmt: skip
        if args.send:
            await w.notify(text)
        return 0

    return asyncio.run(go())


def cmd_telegram_test(args, settings: config.Settings) -> int:
    """Send one test message to the configured chat."""
    client = telegram.Telegram.from_env(settings.telegram)
    me = client.get_me()
    [message_id] = client.send(
        f"✅ <b>Test message</b> from the market watcher.\n"
        f"Bot @{telegram.esc(me.get('username'))} can reach this chat; alerts and digests will arrive here."
    )
    print(f"sent test message {message_id} via @{me.get('username')}")
    return 0


def cmd_watchdog(args, settings: config.Settings) -> int:
    """For cron: warn on Telegram once when the watcher's heartbeat goes stale, and once when it is back."""
    beat = State(settings.paths.state, readonly=True).get("heartbeat")
    now = datetime.now(timezone.utc)
    stale = beat is None or now - datetime.fromisoformat(beat) > timedelta(minutes=args.max_age)
    flag = settings.paths.state.with_name("watchdog.json")  # its own file: the service owns state.json
    warned = flag.exists() and json.loads(flag.read_text()).get("warned", False)
    if stale == warned:
        return 1 if stale else 0
    client = telegram.Telegram.from_env(settings.telegram)
    if stale:
        since = "never" if beat is None else datetime.fromisoformat(beat).astimezone().strftime("%a %H:%M")
        client.send(f"⚠️ The market watcher has been silent since {since}. "
                    "On the Pi: <code>systemctl status market-watcher</code>")  # fmt: skip
    else:
        client.send("✅ The market watcher is running again.", silent=True)
    flag.write_text(json.dumps({"warned": stale}))
    return 1 if stale else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("-v", "--verbose", action="store_true")
    sub = p.add_subparsers(dest="command", required=True)

    w = sub.add_parser("watch", help="run the always-on watcher (the Pi's systemd service)")
    w.add_argument("--shadow", action="store_true", help="record alerts instead of sending them")
    w.add_argument("--minutes", type=float, help="stop after this many minutes (for testing)")
    w.set_defaults(func=cmd_watch)

    c = sub.add_parser("check", help="one price check now: snapshot and what would alert (no Claude, no Telegram)")
    c.set_defaults(func=cmd_check)

    d = sub.add_parser("digest", help="build a news digest now (calls Claude unless --dry-run)")
    d.add_argument("kind", nargs="?", default="manual", choices=["morning", "preopen", "weekly", "manual"])
    d.add_argument("--dry-run", action="store_true", help="print what would be sent to Claude; no call")
    d.add_argument("--send", action="store_true", help="also send it to Telegram")
    d.set_defaults(func=cmd_digest)

    tg = sub.add_parser("telegram-test", help="send one test message to the Telegram chat in .env")
    tg.set_defaults(func=cmd_telegram_test)

    wd = sub.add_parser("watchdog", help="for cron: warn on Telegram if the watcher stopped")
    wd.add_argument("--max-age", type=float, default=45, help="minutes without a heartbeat before warning")
    wd.set_defaults(func=cmd_watchdog)

    args = p.parse_args(argv)
    config.load_env()
    settings = config.load_settings()
    setup_logging(args.verbose)
    return args.func(args, settings)


if __name__ == "__main__":
    sys.exit(main())
