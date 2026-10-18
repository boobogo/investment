# CLAUDE.md

Daily US-stock research pipeline: Python fetches data, computes indicators, screens, and sends compact packets to Claude via `claude -p` (the user's Pro subscription) for structured verdicts; everything is logged to SQLite and summarised in an HTML report. Research aid only, no trading. **README.md** has usage, config, scheduling, Raspberry Pi and evaluation docs; this file has what a developer needs on top.

## Commands

- Python 3.11+, managed with **uv only** (`uv add`, `uv run`; no pip, no manual venv).
- `uv run pytest`: unit and whole-run tests. No network, no Claude calls (a `FakeBackend` stands in).
- `uv run main.py run --dry-run`: real data and screening, **no Claude call**. Free; use it to check changes.
- `uv run main.py run` (without `--dry-run`) calls Claude and **uses the user's Pro quota: ask before running it.**
- `uv run main.py evaluate | report | indicators`: read-only, no Claude call.
- Report screenshots for visual checks: Windows Chrome headless at `/mnt/c/Program Files/Google/Chrome/Application/chrome.exe` (`--headless=new --screenshot=...`; use a 390px iframe for phone width, since the window can't go that narrow).

## Layout

`main.py` (CLI) -> `pipeline/runner.py` orchestrates one run: `fetch` (yfinance + 5-min bars) -> `indicators` -> `news` (RSS) -> `screen` (what to send) -> `packets` -> `brain` (LLM backends, batching, retries) -> `storage` (SQLite) -> `report` (Jinja2 HTML) ; `evaluate` scores verdicts later. Config: `config/settings.yaml` + `config/tickers.yaml`, validated by pydantic in `pipeline/config.py` (`extra="forbid"`: a new setting needs a model field too). Prompt: `prompts/analyst.md`.

## Decisions to keep (and why)

- **Claude CLI flags** (`brain.ClaudeCLIBackend`): `--system-prompt` (replaces Claude Code's ~29k-token default prompt; ~1k instead), `--tools ""`, `--strict-mcp-config`, `--safe-mode`, `--no-session-persistence`, `--json-schema`. **Never `--bare`**: it ignores the Pro login. `ANTHROPIC_API_KEY` must be unset, or the CLI bills the API.
- **Model is pinned** (`llm.model: claude-sonnet-5-5`), not an alias, so evaluation never mixes models silently. Opus is a one-line switch.
- **Output schema exists twice** (pydantic `Verdict` + JSON schema in `schema.py`); a test keeps them in sync. Item counts are checked in pydantic, not in the JSON schema.
- **Packets never contain nulls**: missing values go into `missing`. Units are explained once in `prompts/analyst.md`, so **a new packet field must be documented in the prompt**.
- **Slots:** runs default to `manual`; only the cron wrapper passes `--slot morning|afternoon`, and each scheduled slot runs once per day. (Deriving the slot from the clock was rejected: a 1am manual run would have consumed the morning slot.)
- **Price cache:** the full 2-year window is replaced each run (consistent split/dividend adjustment), but a download that ends earlier never overwrites newer cached bars; a session missing from Yahoo's daily data is rebuilt from 5-minute bars (Yahoo shows an empty latest close for hours after 20:00 New York time); unfinished sessions are dropped.
- **News:** yfinance's news endpoint returns 404, so Yahoo RSS + Google News RSS are used, with relevance filters, story-level dedup (shared content words) and per-ticker `aliases` / `news_exclude`.
- **Screen:** extremes (RSI 80/20, z20 2.5, new 52-week high/low) were requested by the user, who wants an early look before big moves. Extended-hours moves (after-hours in the morning run, pre-market in the afternoon) serve the same goal. Fresh news only triggers together with a move (otherwise every mega-cap is sent daily). `trigger_log` stops the same condition re-sending.
- **Evaluation:** excess return vs SPY, the latest verdict per (ticker, market date), and baselines on the same dates. The user wants to know whether Claude has an edge, so keep the comparisons honest (survivorship, overlap and selection caveats are in the README).
- **Report:** bullish = blue, bearish = red (colorblind-safe, not green/red), and every badge has a glyph + word. Model and headline text is HTML-escaped (autoescape).

## Conventions

- Flat modules, one per pipeline stage; comments explain *why*, not what.
- Tests build temp databases from `tests/fixtures/*.csv` (real AAPL/SPY history); `tests/test_runner.py` has the whole-run helpers.
- Shared knobs belong in `settings.yaml`, not constants.

## Redesign in progress (started 2026-10-18): always-on watcher on the Raspberry Pi

Plan: `~/.claude/plans/pasted-content-id-f057-project-proud-sedgewick.md`. Goal: Pi 4 (`boobogo@192.168.0.55`, aarch64, Debian 12; SSH key login works) runs one systemd service that streams prices (Finnhub websocket, max ~50 symbols on the free tier), sends instant Telegram alerts for unusual moves and pre-filtered breaking news (Claude Sonnet triage, capped 6/h and 40/day), and schedules digests (07:30 Prague, 09:00 New York, Sunday 19:00 Prague) with summary + HTML file. Quiet hours 23:00-07:00 Prague (crashes only); extremes stay in digests. Secrets in `.env`: FINNHUB_API_KEY, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID (loaded by `config.load_env`; never print them).

- Step 1 done: `.env` loader, `pipeline/telegram.py`, `pipeline/finnhub.py`, watcher tables in `storage.py`, digests delivered to Telegram (`report.TelegramDeliverer`, `report --send`, `telegram-test`).
- Step 2 done: digest extras in `pipeline/digest.py` (news highlights picked by one Sonnet call from pre-filtered candidates, `prompts/highlights.md`; Finnhub earnings calendar with Yahoo's date shown when they disagree; weekly movers; held alerts), `pipeline/newswatch.py` (Finnhub feeds + free pre-filter score), `pipeline/clock.py` (DST-safe schedule, US session, quiet-hours window; tests cover the 2026-10-25/11-01 changes). Slots are now morning | preopen | weekly | manual; the weekly slot skips the stock review.
- News pre-filter lessons: Finnhub company news includes general market pieces (keep only items naming the company); "any member" story matching chains hundreds of headlines into giant stories (digests compare with the lead only); items about different watchlist companies are never one story; law-firm lawsuit ads are filtered in `news.skip_title_patterns`.
- Step 3 done (code): `pipeline/live.py` (trades -> 1-minute bars; window changes refuse overnight/halt gaps), `finnhub.TradeStream` (websocket, reconnect with backoff, key never logged), `pipeline/watcher.py` (`main.py watch [--minutes N] [--shadow]`): stream universe = first 2 ETFs + watchlist + other ETFs (cap 50, rest polled via /quote), session from Finnhub market-status (holidays) else NY clock, VIX/ES=F/NQ=F via yfinance `fast_info`, yfinance 5-min fallback when the stream is silent in pre/post, quotes -> `live_quotes`, bars -> `bars_1m`, heartbeat in `watch_state`.
- Previous close = last daily bar before today's NY date from our own history (refreshed daily for all streamed symbols, no fundamentals). Finnhub /quote `pc` lagged a day over the weekend (Thursday instead of Friday), so it is only a fallback.
- Still to verify in a live US session: trade flow on the stream, and whether Finnhub streams pre-market/after-hours trades (if not, the yfinance fallback covers it).
- Step 4 done (shadow mode): `pipeline/detect.py` (pure detectors; thresholds = max(fixed %, multiple of the symbol's own sigma/ATR from daily history)), `pipeline/alerts.py` (policy: repeats suppressed by key symbol:kind:direction - cooldown for 15-min bursts, once per NY day for level moves, a "day" move after an "ext" alert within 24h is the same story; quiet hours 23-07 Prague hold all but critical; /mute; daily budget 6; shadow vs live via `alerts.mode`), events in `price_events`, decisions in `alerts` (recorded at event time). `main.py alerts [--days N]` lists them; `main.py alerts --estimate` replays ~60 days of 5-min bars: 0.20 alerts per trading day (12 in 59 days; all earnings moves, big QQQ days, sudden drops) - regular hours only.
- Lessons: yfinance `fast_info` kept Friday's previous close after the Sunday futures reopen, so VIX/futures quotes come from timestamped 5-min bars (`watcher.macro_quote`) and are ignored when older than 30 min; futures are only checked while CME trades (`clock.futures_open`).
- Still `alerts.mode: shadow`. Before going live: run the watcher through at least one US session (`uv run main.py watch`), check `main.py alerts`, then set `mode: live`.
- Step 5 done: `pipeline/newsdesk.py` inside the watcher polls Finnhub market news every 2 min (minId) and watchlist company news in rotation, stores everything in `news_items`, and queues only strict candidates (`Prefilter.is_breaking_candidate`: company named in the headline + impact word, or top-outlet market news + market topic; clickbait and low-quality outlets excluded; same story as one judged in 24h skipped). `pipeline/triage.py` + `prompts/triage.md`: one Sonnet call per batch (>= 3 min apart, capped 6/h and 40/day counted from llm_calls purpose 'triage' under a daily 'watch' run). Impact >= 4 with new information -> news alert through the same quiet/mute/budget gate (market-wide impact 5 = critical). Price alerts (sent or shadow) get follow-ups at +5/15/30/60 min; an explanation or "no news found" goes as a reply in the alert's thread; serious news about a stock with an open price alert also goes into that thread.
- Measured: ~7 strict candidates/day before triage (most market-wide Reuters/CNBC items that Claude rates 1-3); one real triage call = 2.6k in / 0.4k out tokens.
- Every watcher loop survives a failed round (logs and continues); a crashed task would stop the watcher, and systemd restarts it.
- Step 6 done: `pipeline/commands.py` (/status /mute /unmute /digest /why /news /alerts /report /watchlist /help; only TELEGRAM_CHAT_ID; commands older than 10 min ignored) answered by the watcher's long-poll loop (offset in watch_state); the watcher schedules digests itself (`run_due_digests`: 3 h grace, holiday skip for preopen, max 2 tries) by running `scripts/run_pipeline.sh <slot>` as a subprocess; /why = a manual full run of one ticker with `deliver=False`, replied as a card; packets get a `live` field from `live_quotes`; startup notice at most hourly; `main.py watchdog` for cron.
- Step 7 done (2026-10-19): deployed to the Pi at `~/investment` (uv 0.13.1, Claude Code 2.1.302 arm64, all tests pass there). systemd unit `market-watcher` (`deploy/market-watcher.service`, Restart=always, EnvironmentFile=.env; survived a SIGKILL test), watchdog in the Pi's crontab every 5 min. The Pi's `data/market.db` is now the live database (copied from the PC on 2026-10-19). Update code with `scripts/deploy_pi.sh` (never touches the Pi's data/logs/reports/.env). Don't run `main.py watch` on the PC while the Pi runs (Telegram getUpdates conflict).
- Pending: (1) the user runs `claude setup-token` and adds `CLAUDE_CODE_OAUTH_TOKEN=...` to the Pi's `.env`, then `sudo systemctl restart market-watcher`; until then Claude calls on the Pi fail (logged). (2) Shadow week: alerts.mode is shadow; review `main.py alerts` on the Pi, then set `alerts.mode: live` and deploy.

## Status (2026-10-16)

- All 7 planned build steps are done; 101 tests pass.
- **Not scheduled yet:** the user is testing manually on this PC (WSL) and may move to a Raspberry Pi (Pi 4 with 4 GB or Pi 5, 64-bit OS; see the README).
- **Git:** pushed to the private GitHub repo `boobogo/investment` (branch `main`, first commit 2026-10-19).
- First real verdicts were logged 2026-10-16 (as of the 2026-10-15 close). They mature for 5-day scoring around 2026-10-22 and for 20-day around 2026-11-12.

### Open ideas / next steps

- Run manually for 1-2 weeks; tune the `screen:` thresholds (e.g. RSI 75/25 if 80/20 is too quiet); add real holdings (`held`, `cost_basis`); grow to 50-100 tickers; then schedule (README: Task Scheduler snippet or crontab).
- Token use: a packet is ~700-1000 tokens and a verdict ~450 output tokens. If that matters, send a shared header per batch instead of repeating keys (~40% fewer input tokens), use 3 headlines instead of 5, or set effort `medium`.
- Most verdicts are `neutral` (the prompt prioritises calibration). Revisit only once evaluation data exists.
- Same-story headline dedup still misses rewordings that only share a brand name (e.g. "Bee" devices); improving it would need more than titles.
- Telegram and email delivery are stubs (`report.Deliverer`); the Anthropic API backend (`llm.backend: api`) is written but untested (no API key).
- `example.py`, `image.png` and `reports/last-review.json` are leftovers from before the build (deliberately not committed).
