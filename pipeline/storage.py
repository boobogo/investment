"""SQLite storage: the market-data cache and the run log.

One file, data/market.db. Inspect it with `sqlite3 data/market.db` or any
SQLite browser. The run log is what evaluation is built on: every run, every
packet (sent or not), every model call with its raw response, and every
verdict with the price it was given at.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

SCHEMA = """
CREATE TABLE IF NOT EXISTS prices (
    ticker  TEXT NOT NULL,
    date    TEXT NOT NULL,          -- YYYY-MM-DD, exchange date
    open    REAL, high REAL, low REAL, close REAL, volume REAL,
    PRIMARY KEY (ticker, date)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    ticker      TEXT PRIMARY KEY,
    fetched_at  TEXT NOT NULL,      -- UTC ISO timestamp
    data_json   TEXT NOT NULL
);

-- headlines already shown to the analyst; anything not in here counts as new
CREATE TABLE IF NOT EXISTS news_seen (
    ticker         TEXT NOT NULL,
    title_key      TEXT NOT NULL,   -- normalised title
    first_seen_at  TEXT NOT NULL,
    PRIMARY KEY (ticker, title_key)
);

CREATE TABLE IF NOT EXISTS runs (
    id           INTEGER PRIMARY KEY,
    started_at   TEXT NOT NULL,     -- UTC
    finished_at  TEXT,
    run_date     TEXT NOT NULL,     -- local (Prague) date
    slot         TEXT NOT NULL,     -- morning | afternoon | manual
    mode         TEXT NOT NULL,     -- screened | full | dry
    status       TEXT NOT NULL,     -- running | ok | partial | failed | dry
    as_of        TEXT,              -- market date of the latest bar
    backend      TEXT, model TEXT, effort TEXT,
    n_tickers    INTEGER, n_sent INTEGER, n_verdicts INTEGER, n_pending INTEGER,
    input_tokens INTEGER, cache_write_tokens INTEGER, cache_read_tokens INTEGER,
    output_tokens INTEGER, thinking_tokens INTEGER,
    cost_usd     REAL,              -- API-equivalent cost (not billed on the Pro subscription)
    stop_reason  TEXT,
    error        TEXT,
    args_json    TEXT
);
CREATE INDEX IF NOT EXISTS runs_slot ON runs (run_date, slot, status);

-- one row per ticker per run, including tickers the screen did not send
CREATE TABLE IF NOT EXISTS packets (
    id           INTEGER PRIMARY KEY,
    run_id       INTEGER NOT NULL REFERENCES runs(id),
    ticker       TEXT NOT NULL,
    sent         INTEGER NOT NULL,  -- 1 if sent to the analyst
    batch_no     INTEGER,
    signals_json TEXT,
    packet_json  TEXT NOT NULL,
    price        REAL NOT NULL,     -- close on price_date
    price_date   TEXT NOT NULL,
    UNIQUE (run_id, ticker)
);

CREATE TABLE IF NOT EXISTS llm_calls (
    id            INTEGER PRIMARY KEY,
    run_id        INTEGER NOT NULL REFERENCES runs(id),
    batch_no      INTEGER, attempt INTEGER, reask INTEGER, web_search INTEGER,
    tickers_json  TEXT,
    backend       TEXT, model TEXT,
    ok            INTEGER NOT NULL, error TEXT, usage_limited INTEGER,
    duration_ms   INTEGER,
    input_tokens  INTEGER, cache_write_tokens INTEGER, cache_read_tokens INTEGER,
    output_tokens INTEGER, thinking_tokens INTEGER,
    cost_usd      REAL,
    raw           TEXT,             -- full raw response (CLI stdout or API JSON)
    created_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS verdicts (
    id            INTEGER PRIMARY KEY,
    run_id        INTEGER NOT NULL REFERENCES runs(id),
    call_id       INTEGER REFERENCES llm_calls(id),
    ticker        TEXT NOT NULL,
    as_of         TEXT NOT NULL,    -- market date the verdict is based on
    price         REAL NOT NULL,    -- close on as_of: the evaluation baseline
    bench_price   REAL,             -- benchmark close on as_of
    verdict       TEXT NOT NULL,
    confidence    TEXT NOT NULL,
    key_reasons_json TEXT, risks_json TEXT, what_would_change_my_mind TEXT, data_gaps_json TEXT,
    model         TEXT,
    horizon_days  INTEGER,
    created_at    TEXT NOT NULL,
    UNIQUE (run_id, ticker)
);
CREATE INDEX IF NOT EXISTS verdicts_ticker ON verdicts (ticker, as_of);

-- conditions that were active when a ticker was reviewed; stops the same trigger from re-sending
CREATE TABLE IF NOT EXISTS trigger_log (
    ticker  TEXT NOT NULL,
    kind    TEXT NOT NULL,
    key     TEXT NOT NULL,          -- market date, extended-hours session or news timestamp
    run_id  INTEGER NOT NULL REFERENCES runs(id),
    PRIMARY KEY (ticker, kind, key)
);

-- every headline the news watcher has seen, for instant alerts and digest highlights
CREATE TABLE IF NOT EXISTS news_items (
    id             INTEGER PRIMARY KEY,
    provider       TEXT NOT NULL,     -- finnhub | yahoo | google
    provider_id    TEXT NOT NULL,     -- the provider's id, or the url
    category       TEXT,              -- general | merger | company
    published_at   TEXT NOT NULL,     -- UTC ISO
    first_seen_at  TEXT NOT NULL,
    source         TEXT,
    headline       TEXT NOT NULL,
    summary        TEXT,
    url            TEXT,
    tickers_json   TEXT,              -- watchlist tickers it concerns
    title_key      TEXT NOT NULL,     -- normalised headline, for story dedup
    prefilter      REAL,              -- pre-filter score (no model involved)
    triage_json    TEXT,              -- the model's assessment, if it was triaged
    UNIQUE (provider, provider_id)
);
CREATE INDEX IF NOT EXISTS news_items_published ON news_items (published_at);

-- unusual price moves found by the watcher
CREATE TABLE IF NOT EXISTS price_events (
    id           INTEGER PRIMARY KEY,
    detected_at  TEXT NOT NULL,
    symbol       TEXT NOT NULL,
    kind         TEXT NOT NULL,       -- e.g. move_15m, move_day, ext_move, index_15m, vix_jump
    severity     TEXT NOT NULL,       -- info | alert | critical
    move_pct     REAL,
    ratio        REAL,                -- move relative to the symbol's normal move over that window
    price        REAL,
    details_json TEXT,
    explanation  TEXT                 -- filled in when news explains it
);

-- every message the watcher or a digest sent, held back, or would have sent (shadow mode)
CREATE TABLE IF NOT EXISTS alerts (
    id                   INTEGER PRIMARY KEY,
    created_at           TEXT NOT NULL,
    kind                 TEXT NOT NULL,   -- price | news | follow_up | digest | system
    severity             TEXT NOT NULL,   -- info | alert | critical
    symbol               TEXT,
    text                 TEXT NOT NULL,
    status               TEXT NOT NULL,   -- sent | held | shadow | failed
    reason               TEXT,            -- why held or failed
    telegram_message_id  INTEGER,
    reply_to             INTEGER REFERENCES alerts(id),
    event_id             INTEGER REFERENCES price_events(id)
);
CREATE INDEX IF NOT EXISTS alerts_created ON alerts (created_at);

-- latest price per symbol from the live stream; packets and /why read it
CREATE TABLE IF NOT EXISTS live_quotes (
    symbol          TEXT PRIMARY KEY,
    price           REAL NOT NULL,
    prev_close      REAL,
    day_change_pct  REAL,
    session         TEXT,             -- pre | regular | post | closed
    updated_at      TEXT NOT NULL
);

-- 1-minute bars from the live stream (or yfinance fallback), kept a few days for tuning and /why
CREATE TABLE IF NOT EXISTS bars_1m (
    symbol  TEXT NOT NULL,
    ts      INTEGER NOT NULL,         -- minute start, unix seconds UTC
    open    REAL, high REAL, low REAL, close REAL, volume REAL,
    source  TEXT NOT NULL,            -- stream | yfinance
    PRIMARY KEY (symbol, ts)
);

-- small key/value state for the watcher (mute until, Telegram update offset, heartbeat)
CREATE TABLE IF NOT EXISTS watch_state (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TEXT NOT NULL
);

-- the news highlights the model picked for a digest
CREATE TABLE IF NOT EXISTS digest_highlights (
    run_id        INTEGER NOT NULL REFERENCES runs(id),
    rank          INTEGER NOT NULL,
    news_id       INTEGER REFERENCES news_items(id),
    title         TEXT NOT NULL,
    why           TEXT NOT NULL,
    scope         TEXT NOT NULL,      -- market | sector | ticker
    tickers_json  TEXT,
    importance    INTEGER,
    PRIMARY KEY (run_id, rank)
);

-- tickers that should have been reviewed but were not (usage limit, failures); next run picks them up
CREATE TABLE IF NOT EXISTS pending (
    ticker       TEXT PRIMARY KEY,
    reason       TEXT,
    since_run_id INTEGER REFERENCES runs(id),
    since_at     TEXT NOT NULL
);
"""

PRICE_COLS = ["open", "high", "low", "close", "volume"]


def connect(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")  # the watcher and digest runs write concurrently
    # with WAL this is still crash-safe (a power cut may lose the last commits, never corrupts);
    # it flushes at checkpoints instead of on every commit, which spares the Pi's SD card
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    _migrate(conn)
    return conn


# columns added after the first release; existing databases get them on connect
ADDED_COLUMNS = [
    ("llm_calls", "purpose", "TEXT NOT NULL DEFAULT 'review'"),  # review | highlights | triage | why
    ("runs", "extras_json", "TEXT"),  # digest extras: news window, earnings, movers, held alerts
]


def _migrate(conn: sqlite3.Connection) -> None:
    for table, column, decl in ADDED_COLUMNS:
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        if column not in existing:
            with conn:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def replace_prices(conn: sqlite3.Connection, ticker: str, df: pd.DataFrame) -> None:
    """Replace the cached history for one ticker with a freshly downloaded window.

    The whole window is replaced rather than appended so every cached row is on
    the same split/dividend adjustment basis.
    """
    if df.empty:
        return
    rows = [
        (ticker, d.strftime("%Y-%m-%d"), *(None if pd.isna(v) else round(float(v), 4) for v in r))
        for d, r in zip(df.index, df[PRICE_COLS].itertuples(index=False))
    ]
    with conn:
        conn.execute("DELETE FROM prices WHERE ticker = ?", (ticker,))
        conn.executemany("INSERT INTO prices VALUES (?, ?, ?, ?, ?, ?, ?)", rows)


def load_prices(conn: sqlite3.Connection, ticker: str) -> pd.DataFrame:
    df = pd.read_sql_query(
        "SELECT date, open, high, low, close, volume FROM prices WHERE ticker = ? ORDER BY date",
        conn,
        params=(ticker,),
        parse_dates=["date"],
        index_col="date",
    )
    return df


def save_fundamentals(conn: sqlite3.Connection, ticker: str, data: dict) -> None:
    with conn:
        conn.execute(
            "INSERT OR REPLACE INTO fundamentals VALUES (?, ?, ?)",
            (ticker, utcnow(), json.dumps(data, default=str)),
        )


def load_fundamentals(
    conn: sqlite3.Connection, ticker: str, max_age_hours: float | None = None
) -> dict | None:
    row = conn.execute(
        "SELECT fetched_at, data_json FROM fundamentals WHERE ticker = ?", (ticker,)
    ).fetchone()
    if row is None:
        return None
    if max_age_hours is not None:
        age = datetime.now(timezone.utc) - datetime.fromisoformat(row["fetched_at"])
        if age > timedelta(hours=max_age_hours):
            return None
    return json.loads(row["data_json"])


def seen_news_keys(conn: sqlite3.Connection, ticker: str) -> set[str]:
    rows = conn.execute("SELECT title_key FROM news_seen WHERE ticker = ?", (ticker,))
    return {r["title_key"] for r in rows}


def mark_news_seen(conn: sqlite3.Connection, ticker: str, keys: list[str]) -> None:
    now = utcnow()
    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO news_seen VALUES (?, ?, ?)", [(ticker, k, now) for k in keys]
        )


# --- run log ---------------------------------------------------------------------

USAGE_COLS = ["input_tokens", "cache_write_tokens", "cache_read_tokens", "output_tokens", "thinking_tokens"]


def _dumps(x) -> str:
    return json.dumps(x, ensure_ascii=False, default=str)


def start_run(conn: sqlite3.Connection, *, run_date: str, slot: str, mode: str, args: dict) -> int:
    with conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, run_date, slot, mode, status, args_json) VALUES (?, ?, ?, ?, 'running', ?)",
            (utcnow(), run_date, slot, mode, _dumps(args)),
        )
    return cur.lastrowid


def finish_run(conn: sqlite3.Connection, run_id: int, **fields) -> None:
    fields["finished_at"] = utcnow()
    cols = ", ".join(f"{k} = ?" for k in fields)
    with conn:
        conn.execute(f"UPDATE runs SET {cols} WHERE id = ?", (*fields.values(), run_id))


def slot_done(conn: sqlite3.Connection, run_date: str, slot: str) -> bool:
    """True if this scheduled slot already completed today (failed or crashed runs don't count)."""
    row = conn.execute(
        "SELECT 1 FROM runs WHERE run_date = ? AND slot = ? AND status IN ('ok', 'partial') LIMIT 1",
        (run_date, slot),
    ).fetchone()
    return row is not None


def save_packets(conn: sqlite3.Connection, run_id: int, rows: list[dict]) -> None:
    """rows: {ticker, sent, batch_no, signals, packet, price, price_date}"""
    with conn:
        conn.executemany(
            "INSERT INTO packets (run_id, ticker, sent, batch_no, signals_json, packet_json, price, price_date)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (run_id, r["ticker"], int(r["sent"]), r.get("batch_no"), _dumps(r.get("signals", [])),
                 _dumps(r["packet"]), r["price"], r["price_date"])
                for r in rows
            ],  # fmt: skip
        )


def save_call(conn: sqlite3.Connection, run_id: int, call: dict) -> int:
    usage = call.get("usage") or {}
    with conn:
        cur = conn.execute(
            "INSERT INTO llm_calls (run_id, batch_no, attempt, reask, web_search, tickers_json, backend, model,"
            " ok, error, usage_limited, duration_ms, input_tokens, cache_write_tokens, cache_read_tokens,"
            " output_tokens, thinking_tokens, cost_usd, raw, created_at, purpose)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id, call["batch_no"], call["attempt"], int(call["reask"]), int(call["web_search"]),
                _dumps(call["tickers"]), call["backend"], call.get("model"), int(call["ok"]), call.get("error"),
                int(call.get("usage_limited", False)), call.get("duration_ms"),
                *(usage.get(k, 0) or 0 for k in USAGE_COLS), usage.get("cost_usd"), call.get("raw"),
                call.get("created_at") or utcnow(), call.get("purpose", "review"),
            ),  # fmt: skip
        )
    return cur.lastrowid


def save_verdict(conn: sqlite3.Connection, run_id: int, call_id: int | None, v: dict, **ctx) -> None:
    """v: a Verdict as dict; ctx: as_of, price, bench_price, model, horizon_days."""
    with conn:
        conn.execute(
            "INSERT INTO verdicts (run_id, call_id, ticker, as_of, price, bench_price, verdict, confidence,"
            " key_reasons_json, risks_json, what_would_change_my_mind, data_gaps_json, model, horizon_days,"
            " created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                run_id, call_id, v["ticker"], ctx["as_of"], ctx["price"], ctx.get("bench_price"),
                v["verdict"], v["confidence"], _dumps(v["key_reasons"]), _dumps(v["risks"]),
                v["what_would_change_my_mind"], _dumps(v["data_gaps"]), ctx.get("model"),
                ctx.get("horizon_days"), utcnow(),
            ),  # fmt: skip
        )


def last_verdicts(conn: sqlite3.Connection, tickers: list[str], before_run_id: int) -> dict[str, sqlite3.Row]:
    """Latest verdict per ticker from earlier runs."""
    out = {}
    for t in tickers:
        row = conn.execute(
            "SELECT * FROM verdicts WHERE ticker = ? AND run_id < ? ORDER BY run_id DESC LIMIT 1",
            (t, before_run_id),
        ).fetchone()
        if row:
            out[t] = row
    return out


def get_pending(conn: sqlite3.Connection) -> dict[str, str]:
    return {r["ticker"]: r["reason"] for r in conn.execute("SELECT ticker, reason FROM pending")}


def set_pending(conn: sqlite3.Connection, run_id: int, tickers: list[str], reason: str) -> None:
    with conn:
        conn.executemany(
            "INSERT INTO pending VALUES (?, ?, ?, ?) ON CONFLICT (ticker) DO UPDATE SET reason = excluded.reason",
            [(t, reason, run_id, utcnow()) for t in tickers],
        )


def clear_pending(conn: sqlite3.Connection, tickers: list[str]) -> None:
    with conn:
        conn.executemany("DELETE FROM pending WHERE ticker = ?", [(t,) for t in tickers])


def last_trigger_keys(conn: sqlite3.Connection) -> dict[str, dict[str, str]]:
    """ticker -> kind -> the latest key it was reviewed with."""
    out: dict[str, dict[str, str]] = {}
    for r in conn.execute("SELECT ticker, kind, max(key) AS key FROM trigger_log GROUP BY ticker, kind"):
        out.setdefault(r["ticker"], {})[r["kind"]] = r["key"]
    return out


def log_triggers(conn: sqlite3.Connection, run_id: int, rows: list[tuple[str, str, str]]) -> None:
    """rows: (ticker, kind, key)"""
    with conn:
        conn.executemany(
            "INSERT OR IGNORE INTO trigger_log VALUES (?, ?, ?, ?)", [(t, k, key, run_id) for t, k, key in rows]
        )


# --- watcher state and alerts ----------------------------------------------------


def log_alert(conn: sqlite3.Connection, **fields) -> int:
    """fields: kind, severity, text, status, and optionally symbol, reason, telegram_message_id, reply_to, event_id."""
    fields.setdefault("created_at", utcnow())
    cols = ", ".join(fields)
    with conn:
        cur = conn.execute(
            f"INSERT INTO alerts ({cols}) VALUES ({', '.join('?' * len(fields))})", tuple(fields.values())
        )
    return cur.lastrowid


def get_state(conn: sqlite3.Connection, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM watch_state WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


SET_STATE = ("INSERT INTO watch_state VALUES (?, ?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value,"
             " updated_at = excluded.updated_at")


def set_state(conn: sqlite3.Connection, key: str, value: str) -> None:
    with conn:
        conn.execute(SET_STATE, (key, value, utcnow()))


# --- news store and digests ------------------------------------------------------


def upsert_news(conn: sqlite3.Connection, items, new: set[int] | None = None) -> dict[int, int]:
    """Store news items (ignoring ones already stored); returns {id(item): news_items.id}.

    If `new` is given, the ids (Python id()) of items stored for the first time are added to it.
    """
    now = utcnow()
    ids = {}
    with conn:
        for it in items:
            cur = conn.execute(
                "INSERT OR IGNORE INTO news_items (provider, provider_id, category, published_at, first_seen_at,"
                " source, headline, summary, url, tickers_json, title_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (it.provider, it.provider_id or it.url or it.key, it.category, it.published.isoformat(), now,
                 it.source, it.title, it.snippet, it.url, _dumps(it.tickers), it.key),
            )  # fmt: skip
            if cur.rowcount and new is not None:
                new.add(id(it))
            row = conn.execute(
                "SELECT id FROM news_items WHERE provider = ? AND provider_id = ?",
                (it.provider, it.provider_id or it.url or it.key),
            ).fetchone()
            ids[id(it)] = row["id"]
    return ids


def set_triage(conn: sqlite3.Connection, news_id: int, result: dict) -> None:
    with conn:
        conn.execute("UPDATE news_items SET triage_json = ? WHERE id = ?", (_dumps(result), news_id))


def triaged_since(conn: sqlite3.Connection, since: str) -> list[dict]:
    """Headlines the model has already judged since a UTC ISO time (to skip repeats of the same story)."""
    rows = conn.execute(
        "SELECT id, headline, tickers_json, triage_json FROM news_items WHERE triage_json IS NOT NULL"
        " AND first_seen_at >= ?",
        (since,),
    )
    return [{**dict(r), "tickers": json.loads(r["tickers_json"] or "[]"), "triage": json.loads(r["triage_json"])}
            for r in rows]  # fmt: skip


def news_for_event(conn: sqlite3.Connection, since: str, symbol: str | None) -> list[dict]:
    """Stored headlines published since `since`, about `symbol` (or any, for market-wide events)."""
    rows = conn.execute(
        "SELECT * FROM news_items WHERE published_at >= ? ORDER BY published_at", (since,)
    ).fetchall()
    out = [{**dict(r), "tickers": json.loads(r["tickers_json"] or "[]")} for r in rows]
    return [r for r in out if symbol is None or symbol in r["tickers"]]


def watch_run_id(conn: sqlite3.Connection, local_date: str) -> int:
    """One `runs` row per local day for the watcher, so its model calls are logged like any run's."""
    row = conn.execute("SELECT id FROM runs WHERE slot = 'watch' AND run_date = ?", (local_date,)).fetchone()
    if row:
        return row["id"]
    with conn:
        cur = conn.execute(
            "INSERT INTO runs (started_at, run_date, slot, mode, status, args_json) VALUES (?, ?, 'watch', 'watch',"
            " 'watch', '{}')",
            (utcnow(), local_date),
        )
    return cur.lastrowid


def calls_since(conn: sqlite3.Connection, purpose: str, since: str) -> int:
    return conn.execute(
        "SELECT count(*) FROM llm_calls WHERE purpose = ? AND created_at >= ?", (purpose, since)
    ).fetchone()[0]


def previous_digest_start(conn: sqlite3.Connection, before_run_id: int) -> str | None:
    """When the last completed scheduled digest started (UTC ISO): the news window starts there."""
    row = conn.execute(
        "SELECT max(started_at) FROM runs WHERE id < ? AND slot IN ('morning', 'preopen', 'weekly')"
        " AND status IN ('ok', 'partial')",
        (before_run_id,),
    ).fetchone()
    return row[0] if row else None


def save_highlights(conn: sqlite3.Connection, run_id: int, highlights: list[dict]) -> None:
    with conn:
        conn.executemany(
            "INSERT INTO digest_highlights VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [(run_id, i, h.get("news_id"), h["title"], h["why"], h["scope"], _dumps(h.get("tickers", [])),
              h.get("importance")) for i, h in enumerate(highlights, start=1)],
        )  # fmt: skip


def load_highlights(conn: sqlite3.Connection, run_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT h.*, n.url, n.source, n.published_at FROM digest_highlights h"
        " LEFT JOIN news_items n ON n.id = h.news_id WHERE h.run_id = ? ORDER BY h.rank",
        (run_id,),
    )
    return [{**dict(r), "tickers": json.loads(r["tickers_json"] or "[]")} for r in rows]


def held_alerts(conn: sqlite3.Connection, since: str) -> list[dict]:
    """Alerts held back (quiet hours, mute, budget) since a UTC ISO time, oldest first."""
    rows = conn.execute(
        "SELECT * FROM alerts WHERE status = 'held' AND created_at >= ? ORDER BY created_at", (since,)
    )
    return [dict(r) for r in rows]


def save_live(conn: sqlite3.Connection, quotes: list[dict], bars: list[tuple], heartbeat: str) -> None:
    """One watcher cycle in one transaction (one write to the SD card instead of three).

    quotes: {symbol, price, prev_close, day_change_pct, session, updated_at}, only the changed ones;
    bars: (symbol, ts, open, high, low, close, volume) from the stream.
    """
    with conn:
        conn.executemany(
            "INSERT INTO live_quotes VALUES (:symbol, :price, :prev_close, :day_change_pct, :session, :updated_at)"
            " ON CONFLICT (symbol) DO UPDATE SET price = excluded.price, prev_close = excluded.prev_close,"
            " day_change_pct = excluded.day_change_pct, session = excluded.session, updated_at = excluded.updated_at",
            quotes,
        )
        conn.executemany("INSERT OR REPLACE INTO bars_1m VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                         [(*r, "stream") for r in bars])  # fmt: skip
        conn.execute(SET_STATE, ("heartbeat", heartbeat, utcnow()))


def live_quote(conn: sqlite3.Connection, symbol: str) -> dict | None:
    """The watcher's latest quote; `updated_at` is when it last changed (see `watcher_alive`)."""
    row = conn.execute("SELECT * FROM live_quotes WHERE symbol = ?", (symbol,)).fetchone()
    return dict(row) if row else None


def watcher_alive(conn: sqlite3.Connection, max_age_s: float) -> bool:
    """Whether the watcher wrote its heartbeat recently (its quotes are current, even unchanged ones)."""
    beat = get_state(conn, "heartbeat")
    return bool(beat) and (datetime.now(timezone.utc) - datetime.fromisoformat(beat)).total_seconds() <= max_age_s


def prune_bars(conn: sqlite3.Connection, keep_days: int) -> int:
    cutoff = int(datetime.now(timezone.utc).timestamp()) - keep_days * 86400
    with conn:
        return conn.execute("DELETE FROM bars_1m WHERE ts < ?", (cutoff,)).rowcount


def prune_news(conn: sqlite3.Connection, keep_days: int) -> int:
    """Delete headlines older than keep_days (ones a digest highlighted stay, the reports cite them).

    SQLite reuses the freed space, so the file stops growing instead of shrinking (no VACUUM,
    which would rewrite the whole file).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=keep_days)).isoformat(timespec="seconds")
    with conn:
        n = conn.execute(
            "DELETE FROM news_items WHERE published_at < ?"
            " AND id NOT IN (SELECT news_id FROM digest_highlights WHERE news_id IS NOT NULL)",
            (cutoff,),
        ).rowcount
        return n + conn.execute("DELETE FROM news_seen WHERE first_seen_at < ?", (cutoff,)).rowcount
