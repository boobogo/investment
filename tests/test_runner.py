"""Whole-run tests: temp database seeded with fixture prices, offline, fake model backend."""

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from pipeline import brain, config, runctl, runner, storage
from tests.test_brain import FakeBackend, ok

TZ = ZoneInfo("Europe/Prague")
MORNING = datetime(2026, 10, 16, 7, 30, tzinfo=TZ)
AFTERNOON = datetime(2026, 10, 16, 15, 0, tzinfo=TZ)
LIMIT = brain.LLMResult(ok=False, error="You've hit your limit · resets 3pm", usage_limited=True, reset_hint="3pm")


@pytest.fixture
def settings(tmp_path, aapl, spy):
    tickers = tmp_path / "tickers.yaml"
    tickers.write_text("tickers:\n  - {symbol: AAPL, name: Apple}\n  - {symbol: MSFT, name: Microsoft}\n")
    s = config.Settings(
        paths=config.Paths(
            db=tmp_path / "data/market.db", logs=tmp_path / "logs", reports=tmp_path / "reports", tickers=tickers
        ),
        llm=config.LLMCfg(retries=0, backoff_s=[]),
    )
    conn = storage.connect(s.paths.db)
    storage.replace_prices(conn, "AAPL", aapl)
    storage.replace_prices(conn, "MSFT", aapl)
    storage.replace_prices(conn, "SPY", spy)
    conn.close()
    return s


def execute(settings, *results, now=MORNING, slot="morning", full=True, **opts):
    """Runs with --full unless told otherwise, so logging tests don't depend on what the screen picks."""
    backend = FakeBackend(*results)
    code = runner.execute(
        settings,
        runner.RunOptions(offline=True, slot=slot, full=full, **opts),
        backend_factory=lambda cfg: backend,
        now=now,
    )
    return code, backend


def rows(settings, sql, *params):
    conn = storage.connect(settings.paths.db)
    try:
        return [dict(r) for r in conn.execute(sql, params)]
    finally:
        conn.close()


def test_run_logs_run_packets_calls_and_verdicts(settings, aapl, spy):
    code, backend = execute(settings, ok("AAPL", "MSFT", v="bullish", conf="med"))
    assert code == runner.EXIT_OK and len(backend.calls) == 1

    [run] = rows(settings, "SELECT * FROM runs")
    assert (run["status"], run["slot"], run["mode"], run["run_date"]) == ("ok", "morning", "full", "2026-10-16")
    assert (run["as_of"], run["n_sent"], run["n_verdicts"], run["n_pending"]) == ("2026-10-15", 2, 2, 0)
    assert run["model"] == "fake-model" and run["output_tokens"] == 10

    packets = rows(settings, "SELECT * FROM packets ORDER BY ticker")
    assert [(p["ticker"], p["sent"], p["batch_no"]) for p in packets] == [("AAPL", 1, 1), ("MSFT", 1, 1)]
    assert json.loads(packets[0]["packet_json"])["t"] == "AAPL"

    [call] = rows(settings, "SELECT * FROM llm_calls")
    assert call["ok"] == 1 and json.loads(call["tickers_json"]) == ["AAPL", "MSFT"]

    verdicts = rows(settings, "SELECT * FROM verdicts ORDER BY ticker")
    v = verdicts[0]
    assert (v["ticker"], v["verdict"], v["confidence"], v["as_of"]) == ("AAPL", "bullish", "med", "2026-10-15")
    assert v["price"] == pytest.approx(aapl["close"].iloc[-1])
    assert v["bench_price"] == pytest.approx(spy["close"].iloc[-1])
    assert v["call_id"] == call["id"] and v["horizon_days"] == 20
    assert len(json.loads(v["key_reasons_json"])) == 3


def test_scheduled_slot_runs_once_per_day(settings):
    assert execute(settings, ok("AAPL", "MSFT"))[0] == runner.EXIT_OK
    code, backend = execute(settings)  # no results queued: calling the model would fail the test
    assert code == runner.EXIT_OK and not backend.calls
    assert len(rows(settings, "SELECT id FROM runs")) == 1

    # the pre-open digest is a separate slot
    assert execute(settings, ok("AAPL", "MSFT"), now=AFTERNOON, slot="preopen")[0] == runner.EXIT_OK
    assert [r["slot"] for r in rows(settings, "SELECT slot FROM runs ORDER BY id")] == ["morning", "preopen"]


def test_force_reruns_and_packets_carry_previous_verdict(settings, aapl):
    execute(settings, ok("AAPL", "MSFT", v="bearish", conf="low"))
    code, backend = execute(settings, ok("AAPL", "MSFT"), force=True)
    assert code == runner.EXIT_OK
    since = json.loads(backend.calls[0]["user"].split("<packets>\n")[1].splitlines()[0])["since_last"]
    assert since == {"verdict": "bearish", "confidence": "low", "as_of": "2026-10-15", "px_chg_pct": 0.0}


def test_manual_runs_never_block(settings):
    for _ in range(2):
        assert execute(settings, ok("AAPL"), tickers=["AAPL"], slot=None)[0] == runner.EXIT_OK
    assert [r["slot"] for r in rows(settings, "SELECT slot FROM runs")] == ["manual", "manual"]
    # and they don't mark the scheduled slot as done
    assert execute(settings, ok("AAPL", "MSFT"))[0] == runner.EXIT_OK


def test_usage_limit_is_partial_and_carries_over(settings):
    code, _ = execute(settings, LIMIT)
    assert code == runner.EXIT_PARTIAL
    [run] = rows(settings, "SELECT * FROM runs")
    assert run["status"] == "partial" and run["n_pending"] == 2 and "resets 3pm" in run["stop_reason"]
    assert {r["ticker"] for r in rows(settings, "SELECT ticker FROM pending")} == {"AAPL", "MSFT"}
    [call] = rows(settings, "SELECT * FROM llm_calls")
    assert call["ok"] == 0 and call["usage_limited"] == 1

    # the next run picks them up, flags them, and clears the backlog
    code, backend = execute(settings, ok("AAPL", "MSFT"), now=AFTERNOON, slot="preopen")
    assert code == runner.EXIT_OK
    assert "carried over from an earlier run (usage limit reached (resets 3pm))" in backend.calls[0]["user"]
    assert rows(settings, "SELECT * FROM pending") == []


def test_dry_run_logs_packets_but_no_calls(settings):
    code, backend = execute(settings, dry_run=True)
    assert code == runner.EXIT_OK and not backend.calls
    [run] = rows(settings, "SELECT * FROM runs")
    assert run["status"] == "dry" and run["n_sent"] == 0
    assert rows(settings, "SELECT * FROM llm_calls") == []
    assert [p["sent"] for p in rows(settings, "SELECT sent FROM packets")] == [0, 0]
    assert list((settings.paths.reports / "dry-run").glob("packets-*.json"))
    # a dry run doesn't use up the scheduled slot
    assert execute(settings, ok("AAPL", "MSFT"))[1].calls


def test_live_quote_needs_a_running_watcher(settings):
    conn = storage.connect(settings.paths.db)
    old = "2026-01-01T00:00:00+00:00"
    quote = {"symbol": "AAPL", "price": 250.0, "prev_close": 245.0, "day_change_pct": 2.04, "session": "regular",
             "updated_at": old}  # fmt: skip
    storage.save_live(conn, [quote], [], heartbeat=storage.utcnow())
    # unchanged for a long time, but the watcher is alive: still the current price
    assert runner._live_quote(conn, "AAPL") == {"px": 250.0, "chg_pct": 2.04, "session": "regular", "utc": "01-01 00:00"}
    storage.save_live(conn, [], [], heartbeat=old)
    assert runner._live_quote(conn, "AAPL") is None  # the watcher stopped


def test_overlapping_run_is_refused(settings):
    with runctl.run_lock(settings.paths.db.parent / "run.lock"):
        code, backend = execute(settings, ok("AAPL", "MSFT"))
    assert code == runner.EXIT_LOCKED and not backend.calls
    assert execute(settings, ok("AAPL", "MSFT"))[0] == runner.EXIT_OK  # lock released


def test_crash_marks_run_failed_and_slot_stays_open(settings):
    def broken(cfg):
        raise RuntimeError("boom")

    code = runner.execute(
        settings, runner.RunOptions(offline=True, slot="morning", full=True), backend_factory=broken, now=MORNING
    )
    assert code == runner.EXIT_FAILED
    [run] = rows(settings, "SELECT * FROM runs")
    assert run["status"] == "failed" and "RuntimeError: boom" in run["error"]
    assert execute(settings, ok("AAPL", "MSFT"))[0] == runner.EXIT_OK


# --- screening inside a run ---


@pytest.fixture
def aapl_jumps(settings, aapl):
    """AAPL's last bar closes 8% higher (a big move); MSFT is unchanged."""
    jumped = aapl.copy()
    for col in ("open", "high", "low", "close"):
        jumped.iloc[-1, jumped.columns.get_loc(col)] = aapl["close"].iloc[-2] * 1.08
    conn = storage.connect(settings.paths.db)
    storage.replace_prices(conn, "AAPL", jumped)
    conn.close()
    return settings


def test_screened_run_sends_only_triggered_tickers(aapl_jumps):
    settings = aapl_jumps
    code, backend = execute(settings, ok("AAPL"), full=False)
    assert code == runner.EXIT_OK
    assert "in this order: AAPL\n" in backend.calls[0]["user"] and "+8.0% on the day" in backend.calls[0]["user"]
    sent = {r["ticker"]: r["sent"] for r in rows(settings, "SELECT ticker, sent FROM packets")}
    assert sent == {"AAPL": 1, "MSFT": 0}
    logged = {(r["ticker"], r["kind"]) for r in rows(settings, "SELECT * FROM trigger_log")}
    assert ("AAPL", "big_move") in logged


def test_same_trigger_is_not_sent_twice(aapl_jumps):
    settings = aapl_jumps
    execute(settings, ok("AAPL"), full=False)
    code, backend = execute(settings, now=AFTERNOON, slot="preopen", full=False)  # same bar, nothing new
    assert code == runner.EXIT_OK and not backend.calls
    run = rows(settings, "SELECT * FROM runs ORDER BY id DESC")[0]
    assert (run["status"], run["n_sent"]) == ("ok", 0)
    assert (settings.paths.reports / "2026-10-16-preopen.html").exists()  # a report even without a model call


def test_monday_morning_reviews_everything(settings):
    monday = datetime(2026, 10, 19, 7, 30, tzinfo=TZ)
    code, backend = execute(settings, ok("AAPL", "MSFT"), now=monday, full=False)
    assert code == runner.EXIT_OK and "weekly full review" in backend.calls[0]["user"]


def test_max_sends_holds_the_rest_back(aapl_jumps):
    settings = aapl_jumps
    settings.screen.max_sends = 1
    conn = storage.connect(settings.paths.db)
    storage.set_pending(conn, None, ["MSFT"], "test")  # MSFT outranks AAPL (carried over = top priority)
    conn.close()
    code, backend = execute(settings, ok("MSFT"), full=False)
    assert code == runner.EXIT_PARTIAL and "in this order: MSFT\n" in backend.calls[0]["user"]
    assert {r["ticker"] for r in rows(settings, "SELECT ticker FROM pending")} == {"AAPL"}
