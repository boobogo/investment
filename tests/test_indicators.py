import math

import numpy as np
import pandas as pd
import pytest

from pipeline import indicators as ind
from tests.conftest import make_ohlcv

# --- textbook reference implementations (loops, deliberately independent) ---


def ref_rsi(closes: list[float], n: int = 14) -> float:
    """Wilder's RSI seeded with a simple average of the first n moves."""
    deltas = np.diff(closes)
    gains, losses = np.clip(deltas, 0, None), np.clip(-deltas, 0, None)
    avg_g, avg_l = gains[:n].mean(), losses[:n].mean()
    for g, l in zip(gains[n:], losses[n:]):
        avg_g = (avg_g * (n - 1) + g) / n
        avg_l = (avg_l * (n - 1) + l) / n
    return 100.0 if avg_l == 0 else 100 - 100 / (1 + avg_g / avg_l)


def ref_ema(values, n):
    k = 2 / (n + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


# --- series ---


def test_sma_simple():
    s = pd.Series(range(1, 11), dtype=float)
    out = ind.sma(s, 3)
    assert out.iloc[:2].isna().all()
    assert out.iloc[-1] == pytest.approx(9.0)


def test_rsi_matches_textbook_after_warmup(aapl):
    # pandas RMA is seeded differently from the textbook SMA seed; after 500 bars they agree
    got = ind.rsi(aapl["close"]).iloc[-1]
    assert got == pytest.approx(ref_rsi(aapl["close"].tolist()), abs=1e-6)


def test_rsi_extremes():
    assert ind.rsi(pd.Series(np.arange(1, 40, dtype=float))).iloc[-1] == pytest.approx(100.0)
    assert ind.rsi(pd.Series(np.arange(40, 1, -1, dtype=float))).iloc[-1] == pytest.approx(0.0)
    assert ind.rsi(pd.Series([5.0] * 30)).iloc[-1] == pytest.approx(50.0)
    zigzag = pd.Series([10.0, 11.0] * 50)  # equal up and down moves
    assert ind.rsi(zigzag).iloc[-1] == pytest.approx(50.0, abs=4)


def test_rsi_needs_warmup():
    out = ind.rsi(pd.Series(np.arange(1, 30, dtype=float)), 14)
    assert out.iloc[:14].isna().all() and not math.isnan(out.iloc[14])


def test_macd_matches_reference(aapl):
    closes = aapl["close"].tolist()
    line = np.array(ref_ema(closes, 12)) - np.array(ref_ema(closes, 26))
    sig = ref_ema(list(line), 9)
    got = ind.macd(aapl["close"]).iloc[-1]
    assert got["macd"] == pytest.approx(line[-1])
    assert got["macd_signal"] == pytest.approx(sig[-1])
    assert got["macd_hist"] == pytest.approx(line[-1] - sig[-1])


def test_macd_flat_is_zero():
    out = ind.macd(pd.Series([50.0] * 60)).iloc[-1]
    assert out.abs().max() == pytest.approx(0.0)


def test_atr_constant_range():
    df = make_ohlcv([100.0] * 40, spread=2.0)
    assert ind.atr(df).iloc[-1] == pytest.approx(2.0)


def test_atr_uses_gaps():
    # a gap up larger than the bar range: true range = high - previous close
    df = make_ohlcv([100.0] * 20 + [110.0], spread=2.0)
    tr_last = 111.0 - 100.0
    expected = ind.atr(df.iloc[:-1]).iloc[-1] * 13 / 14 + tr_last / 14
    assert ind.atr(df).iloc[-1] == pytest.approx(expected)


def test_crossed():
    a = pd.Series([1.0, 2.0, 3.0])
    assert ind.crossed(a, pd.Series([2.0, 2.5, 2.5])) == 1
    assert ind.crossed(pd.Series([3.0, 3.0, 2.0]), pd.Series([2.0, 2.0, 2.5])) == -1
    assert ind.crossed(a, pd.Series([0.0, 0.0, 0.0])) == 0
    assert ind.crossed(a, pd.Series([np.nan, np.nan, 1.0])) == 0


# --- snapshot ---


def test_technicals_on_synthetic_trend():
    closes = list(np.linspace(100, 200, 260))
    df = make_ohlcv(closes, spread=2.0)
    df.iloc[-1, df.columns.get_loc("volume")] = 3_000_000
    snap = ind.technicals(df)
    assert snap["close"] == pytest.approx(200.0)
    assert snap["chg_1d_pct"] == pytest.approx((200 / closes[-2] - 1) * 100)
    assert snap["rsi14"] == pytest.approx(100.0)
    assert snap["px_vs_sma50_pct"] > 0 and snap["px_vs_sma200_pct"] > 0
    assert snap["vol_vs_avg20"] == pytest.approx(3.0)
    # 52w high = last high = 201, low over last 252 bars = closes[8] - 1
    assert snap["high_52w"] == pytest.approx(201) and snap["low_52w"] == pytest.approx(closes[8] - 1)
    assert snap["from_52w_high_pct"] == pytest.approx((200 / 201 - 1) * 100)
    assert snap["from_52w_low_pct"] == pytest.approx((200 / (closes[8] - 1) - 1) * 100)


def test_technicals_short_history_gives_gaps():
    snap = ind.technicals(make_ohlcv(list(np.linspace(10, 20, 60))))
    assert snap["sma200"] is None and snap["px_vs_sma200_pct"] is None
    assert snap["from_52w_high_pct"] is None and snap["high_52w"] is None
    assert snap["sma50"] is not None


def test_technicals_relative_to_benchmark(aapl, spy):
    snap = ind.technicals(aapl, spy)
    spy20 = (spy["close"].iloc[-1] / spy["close"].iloc[-21] - 1) * 100
    assert snap["rel_20d_vs_bench_pct"] == pytest.approx(snap["chg_20d_pct"] - spy20)
    assert 0 <= snap["rsi14"] <= 100
    assert snap["as_of"] == aapl.index[-1].strftime("%Y-%m-%d")
    assert all(v is not None for k, v in snap.items())


# --- fundamentals ---


def test_fundamentals_normalisation():
    raw = {
        "market_cap": 2_000e9,
        "pe": 30.0,
        "fwd_pe": None,
        "rev_growth": 0.12,
        "gross_margin": 0.45,
        "debt_to_equity_pct": 150.0,
        "fcf": 50e9,
        "next_earnings": "2026-11-02",
    }
    f = ind.fundamentals(raw)
    assert f["market_cap_bn"] == pytest.approx(2000)
    assert f["rev_growth_q_yoy_pct"] == pytest.approx(12.0)
    assert f["gross_margin_pct"] == pytest.approx(45.0)
    assert f["debt_to_equity"] == pytest.approx(1.5)
    assert f["fcf_yield_pct"] == pytest.approx(2.5)
    assert f["fwd_pe"] is None and f["ev_ebitda"] is None
    assert f["next_earnings"] == "2026-11-02"


def test_fundamentals_zero_gross_margin_is_missing():
    # banks: Yahoo reports gross margin 0, which would mislead the analyst
    assert ind.fundamentals({"gross_margin": 0.0})["gross_margin_pct"] is None


def test_fundamentals_empty():
    f = ind.fundamentals({})
    assert all(v is None for v in f.values())


def test_z20_and_new_52w_extremes():
    closes = [100.0] * 270 + [100.0 + i for i in range(1, 11)]  # flat for a year, then 10 up days
    snap = ind.technicals(make_ohlcv(closes))
    tail = pd.Series(closes[-20:])
    assert snap["z20"] == pytest.approx((closes[-1] - tail.mean()) / tail.std(ddof=0))
    assert snap["new_52w_high"] is True and snap["new_52w_low"] is False
    flat = ind.technicals(make_ohlcv([100.0] * 280))
    assert flat["z20"] is None and flat["new_52w_high"] is False  # no dispersion, no new extreme
