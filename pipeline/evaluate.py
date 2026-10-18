"""Score past verdicts against what prices did next, and compare with naive baselines.

For each verdict (the latest one per ticker and market date) and each horizon
h in trading days, the outcome is the stock's return minus the benchmark's
return over the same h days ("excess", in percent), measured on today's
adjusted price history. sigma is the stock's typical excess move over h days:
the standard deviation of daily excess returns over the prior
`vol_lookback_days`, scaled by sqrt(h).

A verdict is right when:
  bullish  excess > 0
  bearish  excess < 0
  neutral  |excess| < neutral_band_sigma * sigma
  watch    |excess| > watch_band_sigma * sigma

Baselines, scored the same way on the same ticker-dates:
  trend rule     close > SMA200 and SMA50 > SMA200 -> bullish; both below -> bearish; else neutral
  always bullish the share of ticker-dates where the stock beat the benchmark
"""

from __future__ import annotations

import math
import sqlite3

import numpy as np
import pandas as pd

from pipeline import indicators, storage
from pipeline.config import EvaluateCfg


def is_hit(verdict: str, excess: float, sigma: float, cfg: EvaluateCfg) -> bool:
    if verdict == "bullish":
        return excess > 0
    if verdict == "bearish":
        return excess < 0
    if verdict == "neutral":
        return abs(excess) < cfg.neutral_band_sigma * sigma
    if verdict == "watch":
        return abs(excess) > cfg.watch_band_sigma * sigma
    raise ValueError(verdict)


def trend_rule(frame: pd.DataFrame) -> pd.Series:
    """The naive baseline's verdict per day (missing while SMA200 is not available yet)."""
    close, s50, s200 = frame["close"], frame["sma50"], frame["sma200"]
    out = np.where((close > s200) & (s50 > s200), "bullish",
                   np.where((close < s200) & (s50 < s200), "bearish", "neutral"))  # fmt: skip
    return pd.Series(out, index=frame.index).where(s200.notna())


class History:
    """One ticker's adjusted history, aligned with the benchmark."""

    def __init__(self, df: pd.DataFrame, bench_close: pd.Series, cfg: EvaluateCfg):
        self.cfg = cfg
        self.close = df["close"]
        bench = bench_close.reindex(self.close.index).ffill()
        self.bench = bench
        self.excess_daily = self.close.pct_change() - bench.pct_change()
        self.trend = trend_rule(indicators.indicator_frame(df))

    def index_of(self, as_of: str) -> int | None:
        i = self.close.index.searchsorted(pd.Timestamp(as_of), side="right") - 1
        return int(i) if i >= 0 else None

    def outcome(self, i: int, h: int) -> dict | None:
        """Forward excess return from bar i over h bars, or None if not matured / too little history."""
        lookback = self.cfg.vol_lookback_days
        if i + h >= len(self.close) or i < lookback:
            return None
        ret = self.close.iloc[i + h] / self.close.iloc[i] - 1
        bench_ret = self.bench.iloc[i + h] / self.bench.iloc[i] - 1
        sigma = self.excess_daily.iloc[i - lookback + 1 : i + 1].std() * math.sqrt(h) * 100
        if not np.isfinite(sigma) or not np.isfinite(bench_ret):
            return None
        return {
            "ret": ret * 100,
            "bench_ret": bench_ret * 100,
            "excess": (ret - bench_ret) * 100,
            "sigma": sigma,
            "end": self.close.index[i + h].strftime("%Y-%m-%d"),
        }


def _histories(conn: sqlite3.Connection, tickers, benchmark: str, cfg: EvaluateCfg) -> dict[str, History]:
    bench = storage.load_prices(conn, benchmark)["close"]
    out = {}
    for t in tickers:
        df = storage.load_prices(conn, t)
        if len(df) > cfg.vol_lookback_days and not bench.empty:
            out[t] = History(df, bench, cfg)
    return out


def latest_verdicts(conn: sqlite3.Connection) -> pd.DataFrame:
    """One verdict per (ticker, market date): the last run's, so repeated runs on the same close don't double-count."""
    return pd.read_sql_query(
        """SELECT v.ticker, v.as_of, v.verdict, v.confidence, v.model, v.run_id
           FROM verdicts v
           JOIN (SELECT ticker, as_of, max(run_id) AS run_id FROM verdicts GROUP BY ticker, as_of) last
             USING (ticker, as_of, run_id)
           ORDER BY v.as_of, v.ticker""",
        conn,
    )


def score_verdicts(conn: sqlite3.Connection, cfg: EvaluateCfg, benchmark: str) -> pd.DataFrame:
    """One row per (verdict, horizon) that has matured."""
    verdicts = latest_verdicts(conn)
    hist = _histories(conn, verdicts["ticker"].unique(), benchmark, cfg)
    rows = []
    for v in verdicts.itertuples(index=False):
        h_ = hist.get(v.ticker)
        i = h_.index_of(v.as_of) if h_ else None
        if i is None:
            continue
        for h in cfg.horizons:
            o = h_.outcome(i, h)
            if o is None:
                continue
            base = h_.trend.iloc[i]
            rows.append({
                **v._asdict(), "horizon": h, **o,
                "hit": is_hit(v.verdict, o["excess"], o["sigma"], cfg),
                "baseline": base,
                "baseline_hit": is_hit(base, o["excess"], o["sigma"], cfg) if isinstance(base, str) else None,
            })  # fmt: skip
    return pd.DataFrame(rows)


def baseline_history(
    conn: sqlite3.Connection, tickers, cfg: EvaluateCfg, benchmark: str, step: int = 5
) -> pd.DataFrame:
    """The trend rule scored over the whole cached history (every `step` bars), for a day-one reference."""
    rows = []
    for t, h_ in _histories(conn, tickers, benchmark, cfg).items():
        for i in range(0, len(h_.close), step):
            base = h_.trend.iloc[i]
            if not isinstance(base, str):
                continue
            for h in cfg.horizons:
                o = h_.outcome(i, h)
                if o is not None:
                    rows.append({"ticker": t, "horizon": h, "verdict": base, **o,
                                 "hit": is_hit(base, o["excess"], o["sigma"], cfg)})  # fmt: skip
    return pd.DataFrame(rows)


# --- summaries -------------------------------------------------------------------


def _group(df: pd.DataFrame, by: str) -> pd.DataFrame:
    return df.groupby(by).agg(n=("hit", "size"), hit_rate=("hit", "mean"), avg_excess=("excess", "mean"))


def summarize(scored: pd.DataFrame, horizon: int) -> dict[str, pd.DataFrame]:
    """Tables for one horizon: by verdict, by confidence, and against the baselines on the same dates."""
    d = scored[scored["horizon"] == horizon]
    if d.empty:
        return {}
    base = d.dropna(subset=["baseline_hit"])
    compare = pd.DataFrame(
        {
            "n": [len(d), len(base), len(d)],
            "hit_rate": [d["hit"].mean(), base["baseline_hit"].astype(bool).mean(), (d["excess"] > 0).mean()],
        },
        index=["Claude, all verdicts", "trend rule, same dates", "always bullish, same dates"],
    )
    return {
        "by_verdict": _group(d, "verdict"),
        "by_confidence": _group(d, "confidence").reindex(["high", "med", "low"]).dropna(how="all"),
        "compare": compare,
    }


def first_maturity(conn: sqlite3.Connection, horizon: int) -> str | None:
    """Approximate date the earliest verdict can be scored at this horizon (weekdays; ignores holidays)."""
    row = conn.execute("SELECT min(as_of) FROM verdicts").fetchone()
    if not row or not row[0]:
        return None
    return (pd.Timestamp(row[0]) + pd.offsets.BDay(horizon)).strftime("%Y-%m-%d")
