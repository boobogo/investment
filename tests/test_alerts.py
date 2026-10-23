from datetime import timedelta

import pytest

from pipeline import alerts
from pipeline.config import AlertsCfg
from pipeline.state import State
from tests.conftest import quote, utc

CFG = AlertsCfg(mode="live")  # day 2.5 sigma, hour 2.0 sigma, critical 4.0 / 3.0
NOON = utc("2026-10-23T12:00")  # 14:00 Prague: outside quiet hours


@pytest.fixture
def policy(tmp_path):
    return alerts.Policy(CFG, State(tmp_path / "state.json"))


def test_thresholds_scale_with_each_instruments_typical_move():
    quotes = [
        quote("ES=F", day=-2.1, sigma=0.8),  # 2.6 sigma: alert
        quote("CL=F", "WTI oil", day=5.0, sigma=3.3),  # 1.5 sigma: normal for oil
        quote("NQ=F", "Nasdaq 100 fut", hour=-2.5, sigma=1.2),  # 2.1 sigma within an hour: alert
        quote("GC=F", "Gold", day=-9.0, sigma=1.8, live=False),  # closed market: never alerts
        quote("XLV", "Health care", day=5.0, sigma=None),  # no history yet
    ]
    hits = alerts.find_hits(quotes, CFG)
    assert [(h.quote.symbol, h.window) for h in hits] == [("ES=F", "day"), ("NQ=F", "hour")]
    assert hits[0].ratio == pytest.approx(2.625) and not hits[0].critical


def test_critical_only_for_very_large_moves_on_critical_symbols():
    big = alerts.find_hits([quote("ES=F", day=-3.3, sigma=0.8)], CFG)  # 4.1 sigma
    assert big[0].critical
    crypto = alerts.find_hits([quote("BTC-USD", "Bitcoin", day=-12.0, sigma=2.4)], CFG)  # 5 sigma, not critical
    assert crypto and not crypto[0].critical


def test_repeats_are_suppressed_until_the_move_grows_or_time_passes(policy):
    first = alerts.find_hits([quote(day=-2.1, sigma=0.8)], CFG)
    assert policy.new_hits(first, NOON) == first
    policy.remember(first, NOON)
    same = alerts.find_hits([quote(day=-2.6, sigma=0.8)], CFG)
    assert policy.new_hits(same, NOON + timedelta(hours=2)) == []  # 2.6 < 1.5 x 2.1
    bigger = alerts.find_hits([quote(day=-3.2, sigma=0.8)], CFG)
    assert policy.new_hits(bigger, NOON + timedelta(hours=2)) == bigger  # grew 1.5x
    assert policy.new_hits(same, NOON + timedelta(hours=21)) == same  # repeat_hours_day passed
    opposite = alerts.find_hits([quote(day=2.1, sigma=0.8)], CFG)
    assert policy.new_hits(opposite, NOON + timedelta(hours=2)) == opposite  # other direction


def test_hour_moves_repeat_sooner(policy):
    hit = alerts.find_hits([quote(hour=-1.7, sigma=0.8)], CFG)
    policy.remember(hit, NOON)
    assert policy.new_hits(hit, NOON + timedelta(hours=2)) == []
    assert policy.new_hits(hit, NOON + timedelta(hours=3, minutes=1)) == hit


def test_gate_quiet_hours_mute_budget_and_shadow(policy):
    normal = alerts.find_hits([quote(day=-2.1, sigma=0.8)], CFG)
    critical = alerts.find_hits([quote(day=-3.5, sigma=0.8)], CFG)
    night = utc("2026-10-23T23:30")  # 01:30 Prague
    assert policy.gate(normal, night, shadow=False).reason == "quiet hours"
    assert policy.gate(critical, night, shadow=False).status == "send"
    assert policy.gate(normal, NOON, shadow=True).status == "shadow"

    policy.state.set("mute_until", (NOON + timedelta(hours=1)).isoformat())
    assert policy.gate(normal, NOON, shadow=False).reason == "muted"
    assert policy.gate(critical, NOON, shadow=False).status == "send"
    policy.state.set("mute_until", NOON.isoformat())

    for i in range(CFG.daily_budget):
        policy.state.append("alerts", {"at": (NOON - timedelta(minutes=i)).isoformat(), "status": "sent"})
    assert policy.gate(normal, NOON, shadow=False).reason.startswith("daily budget")
    assert policy.gate(normal, utc("2026-10-24T08:00"), shadow=False).status == "send"  # a new Prague day


def test_message_groups_hits_by_instrument_strongest_first():
    quotes = [
        quote("ES=F", "S&P 500 fut", price=7600, day=-2.2, hour=-1.0, sigma=0.8),
        quote("NQ=F", "Nasdaq 100 fut", price=29050, day=-3.4, hour=-2.6, sigma=1.2),
        quote("^VIX", "VIX", price=24.1, day=15.0, sigma=8.0),  # 1.9 sigma: context only
        quote("GC=F", "Gold", price=4040, day=1.1, sigma=1.8),
    ]
    hits = alerts.find_hits(quotes, CFG)
    lines = alerts.hit_lines(hits)
    assert lines[0] == "📉 Nasdaq 100 fut -2.6% in 1h, -3.4% in 24h · at 29,050 · 2.8× a normal day"
    assert lines[1].startswith("📉 S&P 500 fut -2.2% in 24h")
    text = alerts.format_alert(hits, quotes, NOON, CFG)
    assert text.startswith("⚡ <b>Unusual market move</b> · 14:00")
    assert "<i>24h: VIX 24.10 (+15.0%) · Gold +1.1%</i>" in text  # context skips the instruments already listed


def test_message_escapes_names():
    hits = alerts.find_hits([quote(name="A<b>&", day=-3.0, sigma=0.8)], CFG)
    assert "A&lt;b&gt;&amp;" in alerts.format_alert(hits, [], NOON, CFG)
