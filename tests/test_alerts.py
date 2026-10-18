import asyncio
import math
from datetime import datetime, timedelta, timezone

import pytest

from pipeline import alerts, detect, storage
from pipeline.config import AlertsCfg
from pipeline.detect import Event, Norms
from tests.test_live import FakeFinnhub, FakePrices, FakeStream
from tests.test_runner import settings  # noqa: F401  (fixture)

CFG = AlertsCfg()
CALM = Norms(daily_sigma_pct=1.5, atr_pct=2.0)  # normal 15-min move = 1.5 * sqrt(15/390) = 0.29%
WILD = Norms(daily_sigma_pct=6.0, atr_pct=5.0)  # normal 15-min move = 1.18%


def utc(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


# --- detectors ---


def test_norms_from_daily(aapl):
    n = detect.norms_from_daily(aapl)
    assert 0.5 < n.daily_sigma_pct < 5 and 0.5 < n.atr_pct < 6
    assert detect.norms_from_daily(aapl.iloc[:30]) is None
    assert n.window_sigma_pct(390) == pytest.approx(n.daily_sigma_pct)
    assert n.window_sigma_pct(15) == pytest.approx(n.daily_sigma_pct * math.sqrt(15 / 390))


def test_stock_window_threshold_is_the_larger_of_pct_and_sigma():
    assert detect.stock_events("A", "regular", 100, -2.9, -2.9, CALM, CFG) == []
    [e] = detect.stock_events("A", "regular", 100, -3.2, -3.2, CALM, CFG)
    assert (e.kind, e.severity, e.direction, e.key) == ("window", "alert", "down", "A:window:down")
    assert e.label == "-3.2% in 15 min (10.9x its normal 15-minute move)"  # 3.2 / (1.5 * sqrt(15/390))
    # a volatile stock needs 4 x 1.18% = 4.7% in 15 minutes
    assert detect.stock_events("B", "regular", 100, 4.5, 4.5, WILD, CFG) == []
    assert [e.kind for e in detect.stock_events("B", "regular", 100, 4.8, 4.8, WILD, CFG)] == ["window"]


def test_stock_day_and_extended_hours():
    kinds = lambda *a: [e.kind for e in detect.stock_events(*a)]  # noqa: E731
    assert kinds("A", "regular", 100, None, 5.5, CALM, CFG) == ["day"]
    assert kinds("B", "regular", 100, None, 9.0, WILD, CFG) == []  # needs 2.5 x 5% ATR = 12.5%
    assert kinds("A", "post", 100, None, -6.0, CALM, CFG) == ["ext"]
    assert kinds("A", "pre", 100, None, 4.0, CALM, CFG) == []  # pre-market needs 5%
    assert kinds("A", "closed", 100, None, 9.0, CALM, CFG) == []
    assert detect.stock_events("A", "regular", 100, 9.0, 9.0, None, CFG) == []  # no norms, no judgement


def test_index_events_and_crash():
    [w, d] = detect.index_events("QQQ", "regular", 400, -1.3, -2.6, CFG)
    assert (w.kind, w.severity, d.kind, d.severity) == ("index_window", "alert", "index_day", "alert")
    [crash] = detect.index_events("SPY", "pre", 400, None, -3.4, CFG)
    assert crash.severity == "critical" and crash.label == "-3.4% pre-market"
    [up] = detect.index_events("SPY", "regular", 400, None, 3.4, CFG)
    assert up.severity == "alert"  # a big rally is news, not a crash


def test_macro_events():
    names = {"ES=F": "S&P 500 futures", "NQ=F": "Nasdaq-100 futures"}
    quotes = {"^VIX": {"price": 24.0, "change_pct": 21.4}, "ES=F": {"price": 7600, "change_pct": -1.3},
              "NQ=F": {"price": 29000, "change_pct": -3.2}}
    assert [e.symbol for e in detect.macro_events(quotes, names, "regular", CFG)] == ["^VIX"]
    [nq] = detect.macro_events(quotes, names, "closed", CFG)
    assert nq.severity == "critical" and nq.label == "Nasdaq-100 futures -3.2% since the last settle"


# --- policy ---


def event(kind="window", move=-3.5, severity="alert", symbol="NVDA"):
    return Event(symbol, kind, severity, move, 4.0, 200.0, f"{move:+.1f}% test")


@pytest.fixture
def policy(settings):  # noqa: F811
    return alerts.AlertPolicy(AlertsCfg(), storage.connect(settings.paths.db))


def decide_and_record(policy, e, now, shadow=True):
    d = policy.decide(e, now, shadow=shadow)
    if d.status != "repeat":
        alerts.record(policy.conn, e, d, "text", now)
        policy.remember(e, now)
    return d


DAY = "2026-10-19T14:00"  # 16:00 Prague, US regular session


def test_cooldown_and_escalation(policy):
    t = utc(DAY)
    assert decide_and_record(policy, event(move=-3.5), t).status == "shadow"
    assert decide_and_record(policy, event(move=-4.0), t + timedelta(minutes=10)).status == "repeat"
    assert decide_and_record(policy, event(move=-5.5), t + timedelta(minutes=20)).status == "shadow"  # grew 1.5x
    assert decide_and_record(policy, event(move=-5.6), t + timedelta(minutes=30)).status == "repeat"
    assert decide_and_record(policy, event(move=+3.5), t + timedelta(minutes=31)).status == "shadow"  # other direction
    assert decide_and_record(policy, event(move=-5.6), t + timedelta(minutes=81)).status == "shadow"  # cooldown over


def test_day_level_moves_alert_once_per_us_day(policy):
    t = utc(DAY)
    assert decide_and_record(policy, event("day", -5.5), t).status == "shadow"
    assert decide_and_record(policy, event("day", -6.0), t + timedelta(hours=3)).status == "repeat"
    assert decide_and_record(policy, event("day", -8.5), t + timedelta(hours=4)).status == "shadow"  # grew 1.5x
    assert decide_and_record(policy, event("day", -5.5), t + timedelta(days=1)).status == "shadow"  # next day


def test_quiet_hours_hold_but_crashes_get_through(policy):
    night = utc("2026-10-19T23:30")  # 00:30 Prague (CET)
    assert decide_and_record(policy, event("futures", -2.5, symbol="NQ=F"), night).status == "held"
    d = decide_and_record(policy, event("futures", -3.5, "critical", symbol="ES=F"), night)
    assert d.status == "shadow"
    held = policy.conn.execute("SELECT reason FROM alerts WHERE status = 'held'").fetchone()
    assert held["reason"] == "quiet hours"
    # DST: 22:30 UTC on Oct 24 is 00:30 CEST (quiet); 21:30 UTC on Oct 26 is 22:30 CET (not quiet)
    assert policy.decide(event(symbol="X"), utc("2026-10-24T22:30"), shadow=True).status == "held"
    assert policy.decide(event(symbol="Y"), utc("2026-10-26T21:30"), shadow=True).status == "shadow"


def test_mute_and_daily_budget(policy):
    t = utc(DAY)
    storage.set_state(policy.conn, "mute_until", (t + timedelta(hours=2)).isoformat())
    assert decide_and_record(policy, event(symbol="A"), t).reason == "muted"
    assert decide_and_record(policy, event("index_day", -3.5, "critical", "SPY"), t).status == "shadow"
    storage.set_state(policy.conn, "mute_until", t.isoformat())  # unmuted
    for i in range(CFG.daily_budget - 1):  # the critical one already counted
        assert decide_and_record(policy, event(symbol=f"S{i}"), t + timedelta(minutes=i + 1)).status == "shadow"
    d = decide_and_record(policy, event(symbol="LATE"), t + timedelta(minutes=30))
    assert d.status == "held" and "budget" in d.reason


def test_live_mode_sends_and_memory_survives_restart(policy):
    t = utc(DAY)
    assert policy.decide(event(), t, shadow=False).status == "send"
    policy.remember(event(), t)
    reborn = alerts.AlertPolicy(AlertsCfg(), policy.conn)
    assert reborn.decide(event(), t + timedelta(minutes=5), shadow=False).status == "repeat"


def test_format_alert():
    t = utc(DAY)
    text = alerts.format_alert(event(), t, "Europe/Prague", day_pct=-5.2)
    assert text == "⚡ <b>NVDA</b> -3.5% test\n$200.00 · day -5.2% · 16:00"
    crash = alerts.format_alert(event("futures", -3.4, "critical", "NQ=F"), t, "Europe/Prague")
    assert crash.startswith("🚨 <b>Nasdaq-100 futures</b>") and "ignores quiet hours" in crash
    assert "&lt;b&gt;" in alerts.format_alert(Event("<b>", "day", "alert", 6, 2, 1.0, "x"), t, "Europe/Prague")


# --- through the watcher: scripted trades in, exactly one alert out ---


class FakeTelegram:
    def __init__(self):
        self.sent = []

    def send(self, text, **kw):
        self.sent.append(text)
        return [500 + len(self.sent)]


def test_replay_scripted_selloff_gives_one_alert(settings):  # noqa: F811
    from pipeline import watcher

    settings.watch.etfs = ["SPY"]
    settings.alerts.mode = "live"
    tg = FakeTelegram()
    w = watcher.Watcher(settings, finnhub=FakeFinnhub(), stream_factory=FakeStream, price_source=FakePrices(),
                        macro_fetcher=lambda s: {}, telegram_client=tg)
    asyncio.run(w.refresh_session())
    asyncio.run(w.refresh_reference_data())
    prev = w.book.prev_close["AAPL"]
    start = utc(DAY).timestamp()
    # 30 calm minutes, then AAPL drops 4% within 10 minutes; SPY and MSFT stay calm
    for m in range(45):
        price = prev if m < 30 else prev * (1 - 0.004 * min(m - 29, 10))
        w.book.on_trades([{"s": "AAPL", "p": price, "t": (start + m * 60) * 1000, "v": 100},
                          {"s": "MSFT", "p": prev, "t": (start + m * 60) * 1000, "v": 100},
                          {"s": "SPY", "p": w.book.prev_close["SPY"], "t": (start + m * 60) * 1000, "v": 100}])
        asyncio.run(w.check(datetime.fromtimestamp(start + m * 60 + 30, timezone.utc)))
    # it fires the moment the drop crosses 3%; deepening to -4% is the same situation (not 1.5x bigger)
    assert len(tg.sent) == 1 and tg.sent[0].startswith("⚡ <b>AAPL</b> -3.2% in 15 min")
    row = w.conn.execute("SELECT status, telegram_message_id, event_id FROM alerts").fetchone()
    assert tuple(row)[:2] == ("sent", 501)
    assert [tuple(r) for r in w.conn.execute("SELECT kind, symbol FROM price_events")] == [("window", "AAPL")]


def test_day_move_after_an_extended_hours_alert_is_the_same_story(policy):
    evening = utc("2026-10-19T20:30")  # 16:30 New York, 22:30 Prague: after hours, earnings out
    assert decide_and_record(policy, event("ext", -9.0, symbol="AAPL"), evening).status == "shadow"
    open_ = utc("2026-10-20T13:35")  # next regular open: same drop
    assert decide_and_record(policy, event("day", -9.5, symbol="AAPL"), open_).status == "repeat"
    assert decide_and_record(policy, event("day", -14.0, symbol="AAPL"), open_).status == "shadow"  # grew 1.5x


def test_futures_only_while_they_trade():
    from pipeline import clock

    assert not clock.futures_open(utc("2026-10-18T20:00"))  # Sunday 16:00 New York: closed
    assert clock.futures_open(utc("2026-10-18T22:05"))  # Sunday 18:05 New York: open
    assert not clock.futures_open(utc("2026-10-19T21:30"))  # Monday 17:30 New York: daily break
    assert not clock.futures_open(utc("2026-10-23T21:30"))  # Friday after 17:00
    stale = {"NQ=F": {"price": 29000, "change_pct": -2.7}}
    assert detect.macro_events(stale, {}, "closed", CFG, futures_open=False) == []
