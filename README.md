# Market watcher and daily research pipeline

A research assistant for a watchlist of US stocks, in two parts that share one database:

- **The watcher** (`main.py watch`, always on, on the Raspberry Pi) streams live prices for the watchlist and index ETFs, checks VIX and index futures, and sends **instant Telegram alerts** for unusual moves (volatility-scaled thresholds) and for **serious breaking news** (strictly pre-filtered, judged by Claude, hard-capped). It explains price alerts with follow-ups in the same Telegram thread, answers **Telegram commands**, and starts the digests on schedule.
- **The digests** (`main.py run`) twice a day plus Sunday evening: indicators, a screen for tickers where something meaningful happened, Claude verdicts on those, Claude-picked news highlights, the earnings calendar; logged to SQLite and sent to Telegram as a summary plus the full HTML report.

Claude is reached through the `claude` CLI on your Pro subscription. It flags things worth a closer look. It does not trade, and nothing here is a trade signal.

## Setup

Requirements: Linux (or WSL), [uv](https://docs.astral.sh/uv/), and [Claude Code](https://code.claude.com) logged in with your Pro account.

```bash
cd ~/coding/investment
uv sync                      # creates .venv from uv.lock
claude                       # once, interactively: log in with your Pro account, then exit
uv run pytest                # all tests should pass
uv run main.py run --dry-run # fetch data and build packets, no Claude call
```

Make sure `ANTHROPIC_API_KEY` is **not** set in the environment the pipeline runs in: with a key present the `claude` CLI bills the API instead of the subscription. The cron wrapper unsets it for you.

## Daily use

```bash
uv run main.py run                        # manual run: screen, review triggered tickers, report
uv run main.py run --dry-run              # show what would be sent and why; no Claude call
uv run main.py run --full                 # skip the screen, review every ticker
uv run main.py run --tickers NVDA,AAPL    # only these tickers (still screened unless --full)
uv run main.py run --web-search 3         # let Claude use web search for the 3 most urgent tickers
uv run main.py report                     # rebuild the latest report (--run ID for an older one)
uv run main.py evaluate [--baseline]      # score past verdicts against what prices did next
uv run main.py run --dry-run --slot morning  # digest preview: news candidates and earnings, no Claude call
uv run main.py run --highlights           # manual run that also picks news highlights (one extra Claude call)
uv run main.py report --send              # (re)send the latest report to Telegram
uv run main.py telegram-test              # check the Telegram bot
uv run main.py watch [--minutes N]        # the always-on watcher (on the Pi it runs as a service)
uv run main.py alerts [--days 7]          # alerts recorded by the watcher; --estimate replays history
uv run main.py watchdog                   # for cron: Telegram warning if the watcher stopped
uv run main.py indicators [--tickers X]   # print the raw indicator table
```

The report is written to `reports/latest.html` (plus one file per run). From Windows, open
`\\wsl.localhost\Ubuntu\home\boobogo\coding\investment\reports\latest.html`.

Exit codes: `0` ok, `1` failed, `3` partial (some tickers carried over to the next run), `75` another run was in progress.

## What gets sent to Claude (the screen)

Every ticker gets a list of conditions. A ticker is sent when at least one is *fresh*:

| Group | Condition (default threshold) | Fresh |
|---|---|---|
| Extremes | RSI ≥ 80 or ≤ 20 | on entering the zone, then at most every 3 days |
| | close ≥ 2.5 standard deviations from its 20-day mean | same |
| | new 52-week high or low | once per session |
| Moves | daily move ≥ 3% **and** ≥ 1.5× the stock's ATR | once per session |
| | after-hours (morning run) / pre-market (pre-open run) move, same rule | once per extended session |
| | volume ≥ 2.5× its 20-day average | once per session |
| Crossovers | SMA50 / SMA200 (golden / death cross), price / SMA200; MACD (off by default) | once per session |
| News | new headlines in the last 24h, **only together with a move or volume spike** | per new headline |
| Calendar | earnings within 5 days | on entering, then every 3 days |
| Portfolio | a `held` ticker not reviewed for 7 days | until reviewed |
| Always | tickers carried over from a run that hit the usage limit; every ticker on Monday morning | - |

All thresholds live under `screen:` in `config/settings.yaml`. A screened run sends at most `max_sends` (30) tickers, most urgent first; the rest wait for the next run. The report's **At extremes** section lists every ticker at an extreme reading, whether or not it was sent this run.

## Configuration

- `config/settings.yaml`: data source, news filters, the model (`llm.model`, default `claude-sonnet-5-5`; switch to `claude-opus-5-5` there), effort, batch size, screen thresholds, report delivery.
- `config/tickers.yaml`: the watchlist.
- `prompts/analyst.md`: the analyst's system prompt.

### Adding tickers

Add a line to `config/tickers.yaml`:

```yaml
  - {symbol: AVGO, name: Broadcom, sector: Technology}
  - {symbol: V, name: Visa, sector: Financials, aliases: [Visa Inc], held: true, cost_basis: 280}
```

- `symbol` is the Yahoo Finance symbol. `name` (and optional `aliases`) are how headlines are matched to the ticker, so use the name news outlets use.
- `news_exclude: [word, ...]` drops headlines for ambiguous names (AMZN excludes "rainforest").
- `held: true` (+ optional `cost_basis`) tells the analyst you own it and guarantees a review at least weekly.
- `notes:` is passed to the analyst verbatim; keep it short.

Check a new ticker with `uv run main.py run --dry-run --tickers AVGO` before the next scheduled run.

## The watcher on the Raspberry Pi

Deployed at `boobogo@192.168.0.55:~/investment` as the systemd service `market-watcher` (`deploy/market-watcher.service`): it starts at boot, restarts 30 s after any crash, and reads its secrets from `~/investment/.env` (`FINNHUB_API_KEY`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `CLAUDE_CODE_OAUTH_TOKEN`). The Pi's `data/market.db` is the live database.

**What it does, and when**

| Part | Rhythm |
|---|---|
| Live trades (Finnhub websocket, max ~50 symbols on the free tier): watchlist + SPY, QQQ, sector ETFs | continuous; detectors every 30 s |
| VIX and S&P / Nasdaq futures (yfinance 5-minute bars) | every 5 / 10 / 30 min (regular / extended / closed) |
| Market news and each watchlist company's news (Finnhub) | every 2 min / every 10 min per company |
| Claude triage of breaking-news candidates and price-alert follow-ups | at most 1 call per 3 min, 6 per hour, 40 per day |
| Digests: morning 07:30 Prague, pre-open 09:00 New York (14:00 or 15:00 Prague), week ahead Sunday 19:00 Prague | started by the watcher; a digest missed while it was down still runs up to 3 h late |

**Alerts** (thresholds under `alerts:` in `settings.yaml`; `uv run main.py alerts --estimate` replays ~60 days of history to show how many the current thresholds would send - about one a week for regular hours):

- a stock moving at least 3% in 15 minutes and 4x its normal 15-minute move; at least 5% on the day and 2.5x its average daily range; at least 5% in pre-market or after hours
- SPY/QQQ 1.2% in 15 minutes or 2.5% on the day; VIX up 20%; futures 2% outside US hours; an index or futures **down 3% = critical**
- breaking news Claude rates 4/5 or 5/5 and new; market-wide 5/5 is critical
- one alert per situation (60-minute cooldown, day-level moves once a day, unless the move grows 1.5x); quiet hours **23:00-07:00 Prague** hold everything except critical alerts (held alerts appear in the next digest); at most 6 alerts a day
- after a price alert the watcher looks for the cause at +5, +15, +30, +60 minutes and replies under the alert

**Alert mode:** `alerts.mode: live` (the current setting) sends alerts to Telegram. With `shadow` they are only recorded, not sent (digests and command replies still arrive); `uv run main.py alerts` lists both.

**SD card:** the watcher writes changed prices once a minute in one transaction, and once a day deletes headlines older than 90 days (digest highlights stay), reports older than 60 days, and trims `cron.log` / `watchdog.log` above 5 MB (`watch:` in `settings.yaml`). On the Pi the system journal is capped at 200 MB (`/etc/systemd/journald.conf.d/size.conf`).

**Telegram commands** (only your chat is answered): `/status`, `/mute 2h`, `/unmute`, `/digest`, `/why NVDA`, `/news NVDA`, `/news`, `/alerts`, `/report`, `/watchlist`, `/help`.

**Day-to-day on the Pi**

```bash
ssh boobogo@192.168.0.55
systemctl status market-watcher              # running?
journalctl -u market-watcher -f              # live log (also ~/investment/logs/pipeline.log)
sudo systemctl restart market-watcher        # after editing config/*.yaml or .env on the Pi
cd ~/investment && ~/.local/bin/uv run main.py alerts   # recorded alerts
```

**Updating the code** from the PC: `scripts/deploy_pi.sh` copies the code (never the Pi's `data/`, `logs/`, `reports/` or `.env`), syncs dependencies and restarts the service. Edit `config/*.yaml` on the PC and deploy, or on the Pi and restart. Don't run `main.py watch` on the PC at the same time: two processes polling the same Telegram bot get in each other's way.

**Watchdog:** a cron job on the Pi runs `main.py watchdog` every 5 minutes and sends one Telegram warning when the watcher's heartbeat is older than 10 minutes, and one note when it's back.

**Claude login on the Pi:** the service uses `CLAUDE_CODE_OAUTH_TOKEN` from `.env`, created once with `claude setup-token` (a long-lived token for your subscription). If Claude calls start failing with an authentication error, run it again and replace the line in `.env`.

**Backups:** `sqlite3 ~/investment/data/market.db ".backup ~/market-backup.db"` (or `uv run python -c "import sqlite3; sqlite3.connect('data/market.db').backup(sqlite3.connect('backup.db'))"`).

## Running digests without the watcher

The digests are ordinary runs: `scripts/run_pipeline.sh morning|preopen|weekly` (sets `PATH`, the timezone and the working directory, unsets `ANTHROPIC_API_KEY`, loads `.env`, logs to `logs/cron.log`). Each scheduled slot runs once per day (`--force` overrides); runs without `--slot` are "manual" and never block a scheduled one. Without the watcher, start them from cron (`scripts/crontab.example`) or, on this PC under WSL, from Windows Task Scheduler (cron inside WSL only fires while WSL is running):

```powershell
$days = "Monday","Tuesday","Wednesday","Thursday","Friday"
$opts = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries
foreach ($job in @(@("morning","07:30"), @("preopen","15:00"))) {
  $action = New-ScheduledTaskAction -Execute "wsl.exe" `
    -Argument "-d Ubuntu -u boobogo -- /home/boobogo/coding/investment/scripts/run_pipeline.sh $($job[0])"
  $trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek $days -At $job[1]
  Register-ScheduledTask -TaskName "Market research ($($job[0]))" -Action $action -Trigger $trigger -Settings $opts
}
```

## Evaluation: is Claude any good at this?

`uv run main.py evaluate` scores every logged verdict (the latest one per ticker and market date) once enough trading days have passed. The outcome is the stock's return **minus SPY's** over 5, 20 and 60 trading days (20 is the analyst's horizon), so a rising market doesn't make every bullish call look smart. A verdict counts as right when:

| Verdict | Right if the excess return over the horizon is ... |
|---|---|
| bullish | above zero |
| bearish | below zero |
| neutral | within ±0.5 sigma |
| watch | beyond ±1 sigma (a big move either way) |

sigma is the stock's typical excess move over that horizon (the standard deviation of its daily excess returns over the previous 60 trading days, scaled to the horizon), so a 5% gap means more for KO than for NVDA. Bands and horizons live under `evaluate:` in `settings.yaml`.

Results are shown by verdict and by confidence (high should be right more often than low, or confidence means nothing), next to two baselines on **the same ticker-dates**: a trend rule (above SMA200 with SMA50 above SMA200 = bullish, both below = bearish, otherwise neutral) and "always bullish". Claude only adds value if it beats them. The report's **Track record** section shows the 20-day scorecard.

How to read it:
- Groups with fewer than 30 scored verdicts are marked "too few to judge". Even past that, daily verdicts on the same stock overlap in time and are not independent, so treat a few points of difference as noise. Months of data are needed before drawing conclusions.
- `--baseline` also runs the trend rule over the full cached history (2 years, every 5th day) as a reference. That history is **survivorship-biased**: the watchlist is today's large, successful companies, so looking back, "always bullish" beats SPY more than half the time. Claude's bullish calls on the same names get the same tailwind, which is exactly why the comparison uses the same dates.
- The screen sends unusual situations, not a random sample, so the hit rates describe "Claude on flagged tickers", not Claude on stocks in general.

## Data and logs

Everything lives in `data/market.db` (SQLite):

| Table | Contents |
|---|---|
| `prices`, `fundamentals` | cached market data |
| `runs` | one row per run: slot, status, model, token totals, API-equivalent cost |
| `packets` | every ticker's packet per run, its conditions, whether it was sent, and its price |
| `llm_calls` | every model call (retries and re-asks included) with the raw response |
| `verdicts` | every verdict with the price and benchmark price it was made at |
| `trigger_log` | which conditions were already sent, so they don't repeat |
| `pending` | tickers carried over to the next run |
| `news_seen` | headlines already shown to the analyst |
| `news_items` | every headline the watcher and digests fetched, with Claude's triage when judged |
| `digest_highlights` | the news highlights Claude picked for each digest |
| `price_events`, `alerts` | everything the detectors found, and every alert sent, held, shadow-recorded or failed (with Telegram message ids) |
| `live_quotes`, `bars_1m` | the watcher's latest prices (written once a minute, only when changed); 1-minute bars only with `watch.save_bars: true` |
| `watch_state` | watcher state: heartbeat, mute, alert memory, Telegram offset |

Logs: `logs/pipeline.log` (rotated daily, 30 days) and `logs/cron.log` (scheduled runs).

Data quirks handled for you:
- For a few hours after 20:00 New York time, Yahoo's daily series can show the newest session with an empty close. The pipeline then rebuilds that day's bar from 5-minute data (within a few hundredths of a percent of the official close), says so in the packet's `data_issues`, and the next run replaces it with the official bar.
- A download that ends earlier than the cache never overwrites newer cached bars.
- A daily bar for a session that hasn't closed yet (a manual run during US trading hours) is ignored.

```bash
sqlite3 data/market.db "SELECT run_date, slot, status, n_sent, output_tokens, cost_usd FROM runs ORDER BY id DESC LIMIT 10"
sqlite3 data/market.db "SELECT as_of, ticker, verdict, confidence, price FROM verdicts ORDER BY id DESC LIMIT 20"
```

(`sudo apt install sqlite3` for the command-line client.)

## Usage and cost

Measured on the first runs: each call has a fixed overhead of about 1,100 prompt tokens; each reviewed ticker adds about 700-800 prompt tokens and about 450 output tokens. A screened run that sends a handful of tickers is a single call of roughly 5-10k tokens. `total_cost_usd` in the logs is what the same calls would cost on the API (about $0.01 per reviewed ticker on Sonnet); on the Pro subscription it counts against your usage limits instead. When a limit is hit, the run stops calling, marks itself `partial`, and the next run picks up the remaining tickers.

To use the Anthropic API instead: `uv sync --extra api`, set `ANTHROPIC_API_KEY`, and change `llm.backend` to `api`.

## Development

```bash
uv run pytest            # unit and whole-run tests (no network, no Claude calls)
uv add <package>         # add a dependency
```

Layout: `main.py` (CLI) and `pipeline/` (`fetch`, `indicators`, `news`, `screen`, `packets`, `brain`, `schema`, `storage`, `runner`, `report`, `runctl`, `config`).
