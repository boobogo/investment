# CLAUDE.md

Always-on market watcher on a Raspberry Pi: checks ~24 macro instruments (index futures, VIX, sector ETFs, yields, FX, gold, oil, Europe, Asia, bitcoin) every 15/30 minutes, sends Telegram alerts on unusual moves, and has Claude (`claude -p` with web search, the user's Pro subscription) explain big moves and write two news digests a day. Research aid only, no trading. **README.md** has usage, config, the Pi and how alerts work; this file has what a developer needs on top.

## Commands

- Python 3.11+, managed with **uv only** (`uv add`, `uv run`; no pip, no manual venv).
- `uv run pytest`: offline tests (fake prices, `tests/test_brain.FakeBackend` for Claude, a fake Telegram).
- `uv run main.py check`: live prices and what would alert. Free; use it to check changes.
- `uv run main.py digest KIND --dry-run`: the request Claude would get. Free.
- `uv run main.py digest KIND` (without `--dry-run`) calls Claude and **uses the user's Pro quota: ask before running it.**
- Deploy: `scripts/deploy_pi.sh` (rsync + `uv sync --frozen` + restart). Don't run `main.py watch` on the PC while the Pi runs (Telegram getUpdates conflict).

## Layout

`main.py` (CLI) -> `pipeline/watcher.py` (asyncio: check loop, digest schedule loop, Telegram command loop). `markets` (yfinance downloads, rolling 24h/1h moves, sigma) -> `alerts` (thresholds in sigma units, repeats, quiet hours, budget, message) ; `briefing` (Claude requests, JSON schemas, Telegram rendering) + `brain` (`claude` CLI backend, retries) ; `state` (JSON file) ; `commands`, `clock`, `telegram`, `config`. Config: `config/settings.yaml`, validated by pydantic in `pipeline/config.py` (`extra="forbid"`: a new setting needs a model field too). Prompts: `prompts/digest.md`, `prompts/explain.md`.

## Decisions to keep (and why)

- **Redesign of 2026-10-23** (user's request): the earlier version (Finnhub price stream of watchlist stocks, keyword-filtered news feeds + Claude triage, daily stock screening with indicators, SQLite, HTML reports, verdict evaluation) was removed. The news feeds missed the cause of the 2026-10-22 AI sell-off (5 "no news found" follow-ups) while a fitting headline was in the database; the user prefers Claude with web search for news and Claude (not local indicators) for overbought/oversold names. The old code is in git history (commit 052f642 and earlier); the Pi's old database, reports and logs are backed up on the PC in `data/pi-backup-2026-10-23.tar.gz` (the PC's own old database, reports and logs were deleted).
- **Claude CLI flags** (`brain.ClaudeCLIBackend`): `--system-prompt` (replaces Claude Code's ~29k-token default prompt), `--tools WebSearch --allowedTools WebSearch`, `--strict-mcp-config`, `--safe-mode`, `--no-session-persistence`, `--json-schema`. **Never `--bare`**: it ignores the Pro login. `ANTHROPIC_API_KEY` is removed from the CLI's environment, or it bills the API.
- **Model is pinned** (`claude.model: claude-sonnet-5-5`), not an alias.
- **Claude cap:** `claude.daily_calls: 4` per Prague day; scheduled digests are reserved first (weekday 2, Sunday 1), the rest serve alert explanations and `/digest`. Explanations: one per 2 h burst, skipped when a digest is due within 45 min. Calls are serialised by a lock.
- **Thresholds in sigma units** (typical daily move from 250 daily closes), not fixed %, so one setting fits the S&P and oil. Day 2.5 / hour 2.0 were chosen by replaying 60 days of 5-minute bars (~5 alerts/week, catches the Oct 22 sell-off at 19:00 Prague; 3.0 missed it). Critical (passes quiet hours) only for `alerts.critical_symbols`.
- **Rolling windows instead of exchange calendars:** 24h = last price vs the last price at least 24 h before it (previous close across closures); 1h needs a reference bar within 75 minutes before the target, so an open's overnight gap is never a "1h move". Quotes older than 45 min are "closed" and never alert (otherwise yesterday's move re-alerts the next day).
- **Natural gas left out:** Yahoo's `=F` front-month series jump at contract rolls (big for NG), which would fake 24h moves. Oil rolls are small relative to its sigma.
- **SD card:** no database; `data/state.json` (atomic replace, lists pruned after 8 days) is written at each check (heartbeat) and on events; logs only to the journal (capped at 200 MB on the Pi). `main.py digest`/`watchdog` open the state read-only (the service owns it); the watchdog keeps its flag in `data/watchdog.json`.
- **Digest numbers:** the snapshot at the end of each digest comes from our data, and the prompt tells Claude to use it for current levels; Claude's stories must come from searches inside the window, with outlet and time.

## Conventions

- Flat modules, comments explain *why*, not what. Shared knobs belong in `settings.yaml`, not constants.
- Telegram text is HTML: everything from outside (Claude's text, instrument names) goes through `telegram.esc`.
- Times: every schedule in its own zone with zoneinfo (`clock.py`); tests cover the 2026-10-25 EU and 2026-11-01 US clock changes.

## Status (2026-10-23)

- Deployed on the Pi (`boobogo@192.168.0.55`, Debian 12, aarch64) at `~/investment`, service `market-watcher`, alerts live; watchdog cron every 15 min. Claude login via `CLAUDE_CODE_OAUTH_TOKEN` in the Pi's `.env` (works since 2026-10-19). `FINNHUB_API_KEY` in `.env` is no longer used (the user wants it kept).
- Real Claude call verified 2026-10-23 on the Pi (manual digest, web search + JSON schema): ok in 100 s, sent to Telegram, well sourced, but ~5,500 characters (2 messages), so the prompt now asks for under ~2,500. The CLI doesn't report WebSearch in `server_tool_use`, so calls log `num_turns` instead.
- Watch next: alert volume (~5/week expected), whether Claude's "stretched stocks" evidence is current and sourced (the test found one name, without an RSI figure), digest length after the prompt change. Thresholds are in `alerts:`, prompts in `prompts/`.
- Git: private repo `boobogo/investment` (branch `main`).
