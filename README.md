# Market watcher

A small always-on assistant on a Raspberry Pi that keeps you from being left behind when something big happens in the markets. It does two things:

- **Watches the main markets** (US index futures, VIX, sector ETFs, the 10-year yield, the dollar, gold, oil, European and Asian indexes, bitcoin) and sends an **instant Telegram alert** when one moves unusually: a war headline, a presidential statement, a central bank surprise, an attack on oil facilities. **Claude** then looks up the cause with web search and replies under the alert.
- **Sends a news digest twice a day** (plus a week-ahead on Sunday), written by Claude with web search: what moved markets and why, the important stories with sources, notably overbought or oversold stocks, and what's scheduled next.

Claude is reached through the `claude` CLI on your Pro subscription, at most 4 calls a day. Nothing is traded, and nothing here is investment advice.

## Setup

Requirements: Linux (or WSL), [uv](https://docs.astral.sh/uv/), and [Claude Code](https://code.claude.com) logged in with your Pro account.

```bash
cd ~/coding/investment
uv sync                                # creates .venv from uv.lock
uv run pytest                          # all tests should pass (no network, no Claude)
uv run main.py check                   # one live price check: snapshot and what would alert
uv run main.py digest morning --dry-run   # what Claude would be asked; no call
```

Secrets go in `.env` (never committed): `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and on the Pi `CLAUDE_CODE_OAUTH_TOKEN`. Make sure `ANTHROPIC_API_KEY` is **not** set: with a key present the `claude` CLI bills the API instead of the subscription (the code removes it from Claude's environment anyway).

## Commands

```bash
uv run main.py watch [--minutes N] [--shadow]   # the watcher (on the Pi it runs as a service)
uv run main.py check                            # one price check now (no Claude, no Telegram)
uv run main.py digest [morning|preopen|weekly|manual] [--dry-run] [--send]
                                                # a digest now: calls Claude unless --dry-run (uses the Pro quota)
uv run main.py telegram-test                    # check the Telegram bot
uv run main.py watchdog                         # for cron: Telegram warning if the watcher stopped
```

## How it works

**Price checks.** Every **15 minutes** while the European (Xetra) or US stock market is open, every **30 minutes** otherwise, the watcher downloads the last few days of 5-minute prices for all instruments from Yahoo Finance in one request (futures arrive ~10 minutes delayed, some indexes ~15). Nothing is stored: prices live in memory only.

A move is **unusual** when it is several times the instrument's typical daily move (its standard deviation of daily changes over the past year), so quiet instruments alert on smaller % moves than jumpy ones:

| Window | Alert at | Example: S&P 500 futures | Example: oil |
|---|---|---|---|
| last 24 hours | 2.5 typical days | ~2% | ~9% |
| last hour | 2 typical days | ~1.6% | ~7% |
| critical (comes through quiet hours and /mute) | 4 days in 24 h, or 3 within an hour, on index futures, VIX, oil or gold | ~3.2% / ~2.4% | ~14% / ~10% |

Moves are measured over rolling windows (24 h back is the previous close across a weekend or a closed market; the 1-hour window is skipped right after an open so an overnight gap doesn't count as a sudden move). A market that hasn't traded for 45 minutes is closed and never alerts on old moves. Replaying Aug-Oct 2026 with these settings gave about **5 alerts a week**, including the Oct 22 AI sell-off, the Oct 15 Asia drop and the Oct 16 VIX spike.

**One message per check.** Everything that moved goes into one alert, with the S&P and Nasdaq futures, VIX, oil, gold and bitcoin underneath for context. The same instrument and direction alerts again only after 20 hours (3 hours for 1-hour moves), unless the move grows 1.5 times bigger. Quiet hours **23:00-07:00 Prague** hold everything except critical moves (held alerts go into the morning digest), and at most 6 alerts go out a day. `/mute 2h` pauses alerts.

**Claude, at most 4 calls a day.** The scheduled digests are reserved first (2 on weekdays, 1 on Sunday). The rest go to explaining alerts or to `/digest`. An alert gets an explanation only when a call is left, the last explanation was more than 2 hours ago (one per burst), and no digest is due within 45 minutes (the digest covers it). Otherwise the alert says why there is no Claude check. Each call uses web search and takes 1-4 minutes.

**Digests** (each time in its own time zone, so daylight-saving changes need no edits):

| Digest | When | Covers |
|---|---|---|
| morning | 07:30 Prague, Mon-Fri | since the previous digest: the US session and close, after-hours earnings, Asia, overnight news; today's calendar |
| preopen | 09:00 New York (15:00 Prague; 14:00 for a few weeks a year around the clock changes), Mon-Fri | Europe's session, US pre-market, 08:30 data releases; the rest of today |
| weekly | 19:00 Prague, Sunday | the week in brief, weekend news, the week ahead |

Each digest has a headline, the state of markets, 4-8 stories with outlet and time, up to 5 stretched (overbought/oversold) stocks with the evidence, the calendar, and our own price snapshot at the end. The prompt is `prompts/digest.md`; alert explanations use `prompts/explain.md`. Material news about the stocks in `digests.follow` is always included. A digest missed while the watcher was down still runs up to 3 hours late; if Claude fails twice, the snapshot goes out with the error.

**Telegram commands** (only your chat is answered): `/status`, `/markets`, `/alerts`, `/mute 2h`, `/unmute`, `/digest`, `/help`.

## Configuration

Everything is in `config/settings.yaml`: the instruments (any Yahoo Finance symbol, grouped as they appear in messages), check intervals, alert thresholds, quiet hours, the Claude model and daily cap, digest times and followed stocks. Check a change with `uv run main.py check` and `uv run pytest`.

## The Raspberry Pi

Deployed at `boobogo@192.168.0.55:~/investment` as the systemd service `market-watcher` (`deploy/market-watcher.service`): it starts at boot, restarts 30 s after any crash, and reads its secrets from `~/investment/.env`.

```bash
ssh boobogo@192.168.0.55
systemctl status market-watcher              # running?
journalctl -u market-watcher -f              # live log
sudo systemctl restart market-watcher        # after editing config/settings.yaml or .env on the Pi
```

**Updating the code** from the PC: `scripts/deploy_pi.sh` copies the code (never the Pi's `data/`, `logs/` or `.env`), syncs dependencies and restarts the service. Don't run `main.py watch` on the PC at the same time: two processes polling the same Telegram bot get in each other's way. On the Pi, ask for a digest with `/digest` rather than `main.py digest`: the service owns the state file and the daily Claude cap.

**Watchdog:** a cron job runs `main.py watchdog` every 15 minutes and sends one Telegram warning when the watcher's heartbeat (written at every check) is older than 45 minutes, and one note when it's back.

**Claude login on the Pi:** the service uses `CLAUDE_CODE_OAUTH_TOKEN` from `.env`, created once with `claude setup-token` (a long-lived token for your subscription). If Claude calls start failing with an authentication error, run it again and replace the line in `.env`.

**SD card:** the watcher keeps no database. Its only file is `data/state.json` (a few KB: heartbeat, Telegram offset, mute, which digests ran, recent alerts and Claude calls for the cap; entries older than 8 days are dropped), rewritten at each check and when something happens; writes go to a temporary file that replaces the old one, so a power cut can't corrupt it. Logs go only to the system journal, which is capped at 200 MB (`/etc/systemd/journald.conf.d/size.conf`).

## Development

```bash
uv run pytest            # offline tests: fake prices, a fake Claude and a fake Telegram
uv add <package>         # add a dependency
```

Layout: `main.py` (CLI) and `pipeline/`: `markets` (Yahoo downloads, moves), `alerts` (thresholds, repeats, quiet hours, message), `briefing` (Claude requests, schemas, digest rendering), `brain` (the `claude` CLI), `watcher` (the loops, Claude cap, digest schedule), `commands` (Telegram commands), `state` (the JSON state file), `clock` (time zones), `telegram`, `config`.
