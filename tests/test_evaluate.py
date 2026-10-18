import math

import numpy as np
import pandas as pd
import pytest

from pipeline import evaluate, report, storage
from pipeline.config import EvaluateCfg
from tests.conftest import make_ohlcv
from tests.test_runner import settings  # noqa: F401  (fixture)

CFG = EvaluateCfg(horizons=[5, 20], vol_lookback_days=60)


def test_hit_rules():
    assert evaluate.is_hit("bullish", 0.1, 5, CFG) and not evaluate.is_hit("bullish", -0.1, 5, CFG)
    assert evaluate.is_hit("bearish", -0.1, 5, CFG) and not evaluate.is_hit("bearish", 0.1, 5, CFG)
    assert evaluate.is_hit("neutral", 2.4, 5, CFG) and not evaluate.is_hit("neutral", -2.6, 5, CFG)
    assert evaluate.is_hit("watch", -5.1, 5, CFG) and not evaluate.is_hit("watch", 4.9, 5, CFG)


def test_trend_rule():
    frame = pd.DataFrame({
        "close":  [110, 90, 105, 100],
        "sma50":  [105, 95, 95, 100],
        "sma200": [100, 100, 100, np.nan],
    })  # fmt: skip
    got = evaluate.trend_rule(frame)
    assert list(got[:3]) == ["bullish", "bearish", "neutral"] and pd.isna(got.iloc[3])


def history(closes, bench=None):
    df = make_ohlcv(closes)
    bench = pd.Series(bench if bench is not None else [100.0] * len(closes), index=df.index)
    return evaluate.History(df, bench, CFG)


def test_outcome_excess_and_sigma():
    wiggle = [100 * (1.01 if i % 2 else 0.99) for i in range(70)]  # +-1% around 100: known volatility
    h = history(wiggle + [100.0, 102, 104, 106, 108, 110])
    o = h.outcome(70, 5)
    assert o["excess"] == pytest.approx(10.0) and o["bench_ret"] == pytest.approx(0.0)
    daily = h.excess_daily.iloc[11:71].std()
    assert o["sigma"] == pytest.approx(daily * math.sqrt(5) * 100)
    assert h.outcome(70, 6) is None  # not matured
    assert h.outcome(30, 5) is None  # not enough history for sigma


def add_verdict(conn, run_id, ticker, as_of, verdict, confidence="med"):
    v = {"ticker": ticker, "verdict": verdict, "confidence": confidence, "key_reasons": ["a", "b", "c"],
         "risks": ["r"], "what_would_change_my_mind": "x", "data_gaps": []}  # fmt: skip
    storage.save_verdict(conn, run_id, None, v, as_of=as_of, price=1.0, model="m", horizon_days=20)


def test_score_verdicts_on_fixture_prices(settings, aapl, spy):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    as_of = aapl.index[-30].strftime("%Y-%m-%d")  # 30 bars before the end: 5 and 20 days have matured
    r1 = storage.start_run(conn, run_date="2026-09-01", slot="morning", mode="full", args={})
    r2 = storage.start_run(conn, run_date="2026-09-01", slot="preopen", mode="full", args={})
    add_verdict(conn, r1, "AAPL", as_of, "bearish")
    add_verdict(conn, r2, "AAPL", as_of, "bullish")  # same close, later run: this one counts
    add_verdict(conn, r2, "MSFT", aapl.index[-1].strftime("%Y-%m-%d"), "bullish")  # not matured

    scored = evaluate.score_verdicts(conn, CFG, "SPY")
    assert len(scored) == 2 and set(scored["verdict"]) == {"bullish"} and set(scored["horizon"]) == {5, 20}
    row = scored[scored["horizon"] == 20].iloc[0]
    i = len(aapl) - 30
    expected = (aapl["close"].iloc[i + 20] / aapl["close"].iloc[i] - spy["close"].iloc[i + 20] / spy["close"].iloc[i]) * 100
    assert row["excess"] == pytest.approx(expected)
    assert row["hit"] == (expected > 0)
    assert row["baseline"] in ("bullish", "bearish", "neutral")

    tables = evaluate.summarize(scored, 20)
    assert tables["by_verdict"].loc["bullish", "n"] == 1
    assert list(tables["compare"].index) == [
        "Claude, all verdicts", "trend rule, same dates", "always bullish, same dates"
    ]  # fmt: skip
    assert evaluate.summarize(scored, 60) == {}


def test_baseline_history(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    hist = evaluate.baseline_history(conn, ["AAPL"], CFG, "SPY")
    assert not hist.empty and set(hist["verdict"]) <= {"bullish", "bearish", "neutral"}
    assert set(hist["horizon"]) == {5, 20}


def test_first_maturity(settings):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    assert evaluate.first_maturity(conn, 5) is None
    run = storage.start_run(conn, run_date="2026-10-16", slot="manual", mode="full", args={})
    add_verdict(conn, run, "AAPL", "2026-10-15", "neutral")
    assert evaluate.first_maturity(conn, 5) == "2026-10-22"


def test_report_track_record(settings, aapl):  # noqa: F811
    conn = storage.connect(settings.paths.db)
    run = storage.start_run(conn, run_date="2026-10-16", slot="manual", mode="full", args={})
    storage.finish_run(conn, run, status="ok", as_of="2026-10-15", n_verdicts=1)
    add_verdict(conn, run, "AAPL", "2026-10-15", "neutral")
    track = report.track_record(conn, settings)
    assert track["rows"] == [] and track["first_maturity"] == "2026-11-12"

    add_verdict(conn, run, "MSFT", aapl.index[-30].strftime("%Y-%m-%d"), "watch")
    track = report.track_record(conn, settings)
    assert [r["label"] for r in track["rows"]][0] == "watch"
    path, _ = report.build(conn, run, settings.paths.reports, settings)
    assert "Track record (20 trading days)" in path.read_text()
