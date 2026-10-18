"""Estimate how many price alerts the current thresholds would have sent (`main.py alerts --estimate`).

Replays the last ~60 days of 5-minute regular-session bars (yfinance's limit for
that interval) through the same detectors and alert policy the watcher uses,
in a throwaway database. Not simulated: pre-market/after-hours moves, futures
and VIX (no free intraday history), so treat the result as a floor.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import timedelta
from pathlib import Path

import pandas as pd

from pipeline import alerts, config, detect, storage

log = logging.getLogger(__name__)

BARS_PER_WINDOW_MINUTES = 5  # yfinance 5-minute bars


def download_5m(symbols: list[str], days: int = 59) -> dict[str, pd.DataFrame]:
    import yfinance as yf

    raw = yf.download(symbols, period=f"{days}d", interval="5m", prepost=False, auto_adjust=True,
                      group_by="ticker", progress=False, threads=True)  # fmt: skip
    out = {}
    for s in symbols:
        if isinstance(raw.columns, pd.MultiIndex) and s in raw.columns.get_level_values(0):
            df = raw[s].rename(columns=str.lower).dropna(subset=["close"])
            if not df.empty:
                df.index = df.index.tz_convert("UTC")
                out[s] = df
    return out


def replay(
    bars: dict[str, pd.DataFrame], norms: dict[str, detect.Norms], cfg: config.AlertsCfg, index_symbols: list[str]
) -> pd.DataFrame:
    """Every alert the policy would have let through (status shadow or held), one row each."""
    steps = max(1, round(cfg.stock_window_minutes / BARS_PER_WINDOW_MINUTES))
    with tempfile.TemporaryDirectory() as tmp:
        conn = storage.connect(Path(tmp) / "replay.db")
        policy = alerts.AlertPolicy(cfg, conn)
        timeline = []
        for s, df in bars.items():
            ny_day = df.index.tz_convert("America/New_York").date
            close = df["close"]
            day_close = close.groupby(ny_day).last()
            prev_close = day_close.shift(1)
            for i in range(len(df)):
                day = ny_day[i]
                prev = prev_close.get(day)
                same_day = i >= steps and ny_day[i - steps] == day  # windows never span the overnight gap
                window = (close.iloc[i] / close.iloc[i - steps] - 1) * 100 if same_day else None
                day_pct = None if pd.isna(prev) else (close.iloc[i] / prev - 1) * 100
                timeline.append((df.index[i] + timedelta(minutes=5), s, float(close.iloc[i]), window, day_pct))
        timeline.sort(key=lambda r: r[0])
        rows = []
        for now, s, price, window, day_pct in timeline:
            pydt = now.to_pydatetime()
            if s in index_symbols:
                events = detect.index_events(s, "regular", price, window, day_pct, cfg)
            else:
                events = detect.stock_events(s, "regular", price, window, day_pct, norms.get(s), cfg)
            for e in events:
                d = policy.decide(e, pydt, shadow=True)
                if d.status == "repeat":
                    continue
                alerts.record(conn, e, d, e.label, pydt)
                policy.remember(e, pydt)
                rows.append({"time": now, "symbol": s, "kind": e.kind, "move_pct": round(e.move_pct, 2),
                             "status": d.status, "label": e.label})  # fmt: skip
        conn.close()
    return pd.DataFrame(rows)


def estimate(settings: config.Settings, days: int = 59) -> tuple[pd.DataFrame, int]:
    """Returns (alerts, number of trading days replayed)."""
    from pipeline.watcher import build_universe

    watchlist = [t.symbol for t in config.load_tickers(settings.paths.tickers)]
    universe = build_universe(watchlist, settings.watch.etfs, settings.watch.stream_limit)
    symbols = universe.streamed + universe.polled
    conn = storage.connect(settings.paths.db)
    bars = download_5m(symbols, days)
    first_day = min(df.index[0] for df in bars.values()).normalize().tz_localize(None)
    norms = {}
    for s in symbols:
        daily = storage.load_prices(conn, s)
        daily = daily[daily.index < first_day]  # volatility as known before the replay starts
        n = detect.norms_from_daily(daily)
        if n:
            norms[s] = n
    result = replay(bars, norms, settings.alerts, settings.alerts.index_symbols)
    n_days = len({d for df in bars.values() for d in df.index.tz_convert("America/New_York").date})
    return result, n_days
