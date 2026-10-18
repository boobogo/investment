"""Technical and fundamental indicators.

Everything is plain pandas so each formula is visible and unit-tested.
Ratios are reported in percent (keys ending in `_pct`). Values that cannot be
computed are None and show up later as data gaps.
"""

from __future__ import annotations

import math

import pandas as pd

TRADING_DAYS_YEAR = 252


# --- series -----------------------------------------------------------------

def sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def ema(s: pd.Series, n: int) -> pd.Series:
    return s.ewm(span=n, adjust=False).mean()


def wilder(s: pd.Series, n: int) -> pd.Series:
    """Wilder's smoothing (RMA), as used by RSI and ATR."""
    return s.ewm(alpha=1 / n, adjust=False, min_periods=n).mean()


def rsi(close: pd.Series, n: int = 14) -> pd.Series:
    delta = close.diff()
    gain = wilder(delta.clip(lower=0), n)
    loss = wilder(-delta.clip(upper=0), n)
    out = 100 - 100 / (1 + gain / loss)  # loss == 0 -> gain/loss = inf -> RSI 100
    return out.mask((gain == 0) & (loss == 0), 50.0)  # perfectly flat window


def macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> pd.DataFrame:
    line = ema(close, fast) - ema(close, slow)
    sig = ema(line, signal)
    return pd.DataFrame({"macd": line, "macd_signal": sig, "macd_hist": line - sig})


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    prev = df["close"].shift()
    tr = pd.concat(
        [df["high"] - df["low"], (df["high"] - prev).abs(), (df["low"] - prev).abs()], axis=1
    ).max(axis=1)
    return wilder(tr, n)


def indicator_frame(df: pd.DataFrame) -> pd.DataFrame:
    """All indicator series for one OHLCV frame (used by snapshots and the evaluation baseline)."""
    out = pd.DataFrame(index=df.index)
    out["close"] = df["close"]
    out["sma20"] = sma(df["close"], 20)
    out["std20"] = df["close"].rolling(20, min_periods=20).std(ddof=0)  # Bollinger convention
    out["sma50"] = sma(df["close"], 50)
    out["sma200"] = sma(df["close"], 200)
    out["rsi14"] = rsi(df["close"], 14)
    out = out.join(macd(df["close"]))
    out["atr14"] = atr(df, 14)
    return out


# --- snapshot -----------------------------------------------------------------

def _num(x) -> float | None:
    if x is None:
        return None
    x = float(x)
    return None if math.isnan(x) or math.isinf(x) else x


def _pct_change(s: pd.Series, n: int) -> float | None:
    if len(s) <= n:
        return None
    return _num((s.iloc[-1] / s.iloc[-1 - n] - 1) * 100)


def crossed(a: pd.Series, b: pd.Series) -> int:
    """+1 if `a` crossed above `b` on the last bar, -1 if below, else 0."""
    diff = (a - b).iloc[-2:]
    if len(diff) < 2 or diff.isna().any():
        return 0
    prev, last = diff.iloc[0], diff.iloc[1]
    if prev <= 0 < last:
        return 1
    if prev >= 0 > last:
        return -1
    return 0


def technicals(df: pd.DataFrame, bench: pd.DataFrame | None = None) -> dict:
    """Latest-bar technical snapshot for one ticker."""
    f = indicator_frame(df)
    last = f.iloc[-1]
    close = last["close"]
    year = df.iloc[-TRADING_DAYS_YEAR:]
    vol_avg20 = df["volume"].iloc[-21:-1].mean()

    snap = {
        "as_of": df.index[-1].strftime("%Y-%m-%d"),
        "bars": len(df),
        "close": _num(close),
        "chg_1d_pct": _pct_change(df["close"], 1),
        "chg_5d_pct": _pct_change(df["close"], 5),
        "chg_20d_pct": _pct_change(df["close"], 20),
        "sma50": _num(last["sma50"]),
        "sma200": _num(last["sma200"]),
        "px_vs_sma50_pct": _num((close / last["sma50"] - 1) * 100),
        "px_vs_sma200_pct": _num((close / last["sma200"] - 1) * 100),
        "rsi14": _num(last["rsi14"]),
        "z20": _num((close - last["sma20"]) / last["std20"]) if last["std20"] else None,
        "macd": _num(last["macd"]),
        "macd_signal": _num(last["macd_signal"]),
        "macd_hist": _num(last["macd_hist"]),
        "atr_pct": _num(last["atr14"] / close * 100),
        "high_52w": _num(year["high"].max()),
        "low_52w": _num(year["low"].min()),
        "from_52w_high_pct": _num((close / year["high"].max() - 1) * 100),
        "from_52w_low_pct": _num((close / year["low"].min() - 1) * 100),
        "vol_vs_avg20": _num(df["volume"].iloc[-1] / vol_avg20) if vol_avg20 else None,
        "cross_sma50_sma200": crossed(f["sma50"], f["sma200"]),
        "cross_macd_signal": crossed(f["macd"], f["macd_signal"]),
        "cross_px_sma200": crossed(f["close"], f["sma200"]),
        "rel_20d_vs_bench_pct": None,
    }
    if len(df) < TRADING_DAYS_YEAR:
        for key in ("high_52w", "low_52w", "from_52w_high_pct", "from_52w_low_pct"):
            snap[key] = None
        snap["new_52w_high"] = snap["new_52w_low"] = False
    else:
        prior = df.iloc[-TRADING_DAYS_YEAR:-1]  # the year before today's bar
        snap["new_52w_high"] = bool(df["high"].iloc[-1] > prior["high"].max())
        snap["new_52w_low"] = bool(df["low"].iloc[-1] < prior["low"].min())
    if bench is not None and snap["chg_20d_pct"] is not None:
        # align on the ticker's dates so a missing bar on either side cannot skew the window
        b = bench["close"].reindex(df.index).dropna()
        b20 = _pct_change(b, 20)
        if b20 is not None:
            snap["rel_20d_vs_bench_pct"] = snap["chg_20d_pct"] - b20
    return snap


def fundamentals(raw: dict) -> dict:
    """Normalise raw source fundamentals into percent/ratio values."""

    def pct(key):
        v = _num(raw.get(key))
        return None if v is None else v * 100

    mcap = _num(raw.get("market_cap"))
    fcf = _num(raw.get("fcf"))
    de = _num(raw.get("debt_to_equity_pct"))
    return {
        "name": raw.get("name"),
        "industry": raw.get("industry"),
        "market_cap_bn": None if mcap is None else mcap / 1e9,
        "pe": _num(raw.get("pe")),
        "fwd_pe": _num(raw.get("fwd_pe")),
        "ev_ebitda": _num(raw.get("ev_ebitda")),
        # Yahoo's growth figures are latest quarter vs the same quarter a year ago
        "rev_growth_q_yoy_pct": pct("rev_growth"),
        "eps_growth_q_yoy_pct": pct("eps_growth"),
        "gross_margin_pct": pct("gross_margin") or None,  # Yahoo reports 0 for banks; meaningless
        "op_margin_pct": pct("op_margin"),
        "net_margin_pct": pct("net_margin"),
        "debt_to_equity": None if de is None else de / 100,
        "fcf_yield_pct": None if not (mcap and fcf is not None) else fcf / mcap * 100,
        "next_earnings": raw.get("next_earnings"),
    }
