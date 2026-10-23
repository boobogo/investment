"""The watcher end to end, offline: fake prices, a fake Claude and a fake Telegram."""

import asyncio
import time
from datetime import datetime, timedelta

import pandas as pd
import pytest

from pipeline import brain, config
from pipeline.config import ROOT
from pipeline.state import State
from pipeline.watcher import Watcher
from tests.test_brain import FakeBackend, ok
from tests.conftest import utc

THU_1500 = utc("2026-10-22T13:00")  # Thursday 15:00 Prague: the pre-open digest time
FRI_1600 = utc("2026-10-23T14:00")  # Friday 16:00 Prague: markets open, no digest near
EXPLAINED = {"cause": "Chip export ban", "confidence": "high", "details": "d", "sources": [], "watch": "w"}
DIGEST = {"headline": "H", "markets": ["m"], "stories": [], "stretched": [], "calendar": [], "watch": "w"}
FAILED = brain.LLMResult(ok=False, error="You've hit your limit", usage_limited=True)


class FakeTelegram:
    chat_id = "42"

    def __init__(self):
        self.sent, self.next_id = [], 100

    def send(self, text, reply_to=None, silent=False):
        self.next_id += 1
        self.sent.append({"text": text, "reply_to": reply_to, "id": self.next_id})
        return [self.next_id]


def tape(now: datetime, last_hour: list[float], base: float = 100.0) -> pd.Series:
    """Flat at `base` for a day, then the last hour's 12 closes; the last bar starts 5 minutes before now."""
    values = [base] * (24 * 12 + 1) + last_hour
    start = now - timedelta(minutes=5 * len(values))
    return pd.Series(values, index=pd.date_range(start, periods=len(values), freq="5min"), dtype=float)


class Market:
    """What the fake Yahoo returns: NQ=F follows `nq`, everything else is flat."""

    def __init__(self):
        self.nq = [100.0] * 12
        self.now = FRI_1600

    def closes(self, symbols):
        return {s: tape(self.now, self.nq if s == "NQ=F" else [100.0] * 12) for s in symbols}

    def sigmas(self, symbols, days):
        return {s: 1.0 for s in symbols}


@pytest.fixture
def market():
    return Market()


@pytest.fixture
def make(tmp_path, market):
    def build(*results, shadow=False, **claude):
        settings = config.Settings(
            paths=config.Paths(state=tmp_path / "state.json", prompts=ROOT / "prompts"),
            alerts=config.AlertsCfg(mode="live"),
            claude=config.ClaudeCfg(retries=0, **claude),
        )
        return Watcher(settings, shadow=shadow, backend=FakeBackend(*results), telegram_client=FakeTelegram(),
                       state=State(tmp_path / "state.json"), fetch_closes=market.closes, fetch_sigmas=market.sigmas)  # fmt: skip

    return build


def run(coro):
    return asyncio.run(coro)


async def check_and_wait(w, now):
    record = await w.check(now)
    await asyncio.gather(*w.background)
    return record


def test_quiet_market_sends_nothing_but_beats(make):
    w = make()
    assert run(w.check(FRI_1600)) is None
    assert w.telegram.sent == [] and w.state.get("heartbeat") == FRI_1600.isoformat()


def test_big_move_alerts_then_claude_explains_in_the_thread(make, market):
    market.nq = [100 - 0.25 * i for i in range(1, 13)]  # -3% within the hour, 3 typical days
    w = make(ok(EXPLAINED))
    record = run(check_and_wait(w, FRI_1600))
    alert, reply = w.telegram.sent
    assert "Nasdaq 100 fut -3.0% in 1h" in alert["text"] and "Asking Claude" in alert["text"]
    assert reply["reply_to"] == alert["id"] and "Chip export ban" in reply["text"]
    assert record["status"] == "sent" and record["explained"] == "Chip export ban"
    call = w.backend.calls[0]
    assert call["web"] and "Nasdaq 100 fut -3.0% in 1h" in call["user"] and "on call" in call["system"]
    assert [c["kind"] for c in w.state.get("claude_calls")] == ["explain"]

    # the same move 15 minutes later is a repeat: nothing new
    assert run(check_and_wait(w, FRI_1600 + timedelta(minutes=15))) is None
    assert len(w.telegram.sent) == 2


def test_critical_move_comes_through_quiet_hours(make, market):
    market.now = night = utc("2026-10-22T23:30")
    market.nq = [100 - 0.25 * i for i in range(1, 13)]  # -3%: 3 typical days within an hour
    w = make(ok(EXPLAINED))
    record = run(check_and_wait(w, night))
    assert record["status"] == "sent" and record["critical"]
    assert w.telegram.sent[0]["text"].startswith("🚨 <b>Market shock</b>")


def test_quiet_hours_hold_the_alert_and_skip_claude(make, market):
    market.now = night = utc("2026-10-22T23:30")  # 01:30 Prague
    market.nq = [100 - 0.2 * i for i in range(1, 13)]  # -2.4%: unusual, not critical (3 typical days would be)
    w = make()
    record = run(check_and_wait(w, night))
    assert record["status"] == "held" and w.telegram.sent == [] and w.backend.calls == []


def test_shadow_mode_records_only(make, market):
    market.nq = [100 - 0.25 * i for i in range(1, 13)]
    w = make(shadow=True)
    assert run(check_and_wait(w, FRI_1600))["status"] == "shadow"
    assert w.telegram.sent == [] and w.backend.calls == []


def test_claude_budget_reserves_the_scheduled_digests(make):
    w = make()
    assert w.scheduled_today(FRI_1600) == 2 and w.extra_left(FRI_1600) == 2
    assert w.scheduled_today(utc("2026-10-25T12:00")) == 1  # Sunday: the weekly digest only
    assert w.extra_left(utc("2026-10-24T12:00")) == 4  # Saturday: no digests
    for minutes in (0, 150):
        w.state.append("claude_calls", {"at": (FRI_1600 - timedelta(hours=5) + timedelta(minutes=minutes)).isoformat(),
                                        "kind": "explain"})  # fmt: skip
    w.state.append("claude_calls", {"at": (FRI_1600 - timedelta(hours=6)).isoformat(), "kind": "morning"})
    assert w.extra_left(FRI_1600) == 0
    assert "used up" in w.explain_blocker(FRI_1600)


def test_explanations_are_spaced_and_skipped_before_a_digest(make):
    w = make()
    assert w.explain_blocker(FRI_1600) is None
    w.state.append("claude_calls", {"at": (FRI_1600 - timedelta(minutes=30)).isoformat(), "kind": "explain"})
    assert w.explain_blocker(FRI_1600) == "one ran at 15:30 (see above)"
    before_preopen = utc("2026-10-23T12:30")  # 14:30 Prague, digest at 15:00
    assert w.explain_blocker(before_preopen) == "the preopen digest at 15:00 covers it"


def test_alert_without_claude_says_why(make, market):
    market.nq = [100 - 0.25 * i for i in range(1, 13)]
    w = make()
    w.state.append("claude_calls", {"at": (FRI_1600 - timedelta(minutes=30)).isoformat(), "kind": "explain"})
    run(check_and_wait(w, FRI_1600))
    assert "No Claude check: one ran at 15:30 (see above)." in w.telegram.sent[0]["text"]
    assert w.backend.calls == []


def test_scheduled_digest_runs_once_with_its_window(make, market):
    market.now = THU_1500
    w = make(ok(DIGEST))
    w.state.append("alerts", {"at": utc("2026-10-22T08:00").isoformat(), "status": "sent",
                              "lines": ["📉 DAX -2.7% in 24h"], "explained": "Tariffs"})  # fmt: skip
    assert run(w.run_due_digests(THU_1500 + timedelta(minutes=1))) == ["preopen"]
    assert run(w.run_due_digests(THU_1500 + timedelta(minutes=2))) == []  # done
    user = w.backend.calls[0]["user"]
    assert "Digest kind: preopen" in user and "news since Thursday 22 October 2026, 07:30 Prague" in user
    assert "10:00 (sent): 📉 DAX -2.7% in 24h | Claude's explanation then: Tariffs" in user
    assert "Followed stocks" not in user  # none configured in this test
    [msg] = w.telegram.sent
    assert msg["text"].startswith("📰 <b>Pre-open digest</b>") and "<b>Snapshot</b>" in msg["text"]
    assert w.state.get("digests_done") == {"preopen": THU_1500.isoformat()}


def test_failed_digest_retries_once_then_sends_the_snapshot(make, market):
    market.now = THU_1500
    w = make(FAILED, FAILED)
    run(w.run_due_digests(THU_1500))
    assert w.telegram.sent == [] and not w.state.get("digests_done")
    assert run(w.run_due_digests(THU_1500 + timedelta(minutes=5))) == []  # waits 10 minutes
    run(w.run_due_digests(THU_1500 + timedelta(minutes=11)))
    [msg] = w.telegram.sent
    assert "couldn't write the news part" in msg["text"] and "<b>Snapshot</b>" in msg["text"]
    assert w.state.get("digests_done") == {"preopen": THU_1500.isoformat()}


def test_missed_digest_runs_late_within_grace_only(make, market):
    w = make(ok(DIGEST))
    market.now = THU_1500 + timedelta(hours=2)
    assert run(w.run_due_digests(THU_1500 + timedelta(hours=2))) == ["preopen"]
    w2 = make()
    assert run(w2.run_due_digests(THU_1500 + timedelta(hours=3, minutes=1))) == []


def test_checks_every_15_minutes_while_markets_are_open_else_30(make):
    w = make()
    assert w.next_check(utc("2026-10-23T14:03")) == utc("2026-10-23T14:15:30")  # Friday 16:03 Prague
    assert w.next_check(utc("2026-10-23T21:03")) == utc("2026-10-23T21:30:30")  # Friday 23:03 Prague
    assert w.next_check(utc("2026-10-24T10:40")) == utc("2026-10-24T11:00:30")  # Saturday


# --- commands ------------------------------------------------------------------------------


def command(w, text, chat="42", age_s=0):
    update = {"update_id": 1, "message": {"chat": {"id": chat}, "text": text, "date": time.time() - age_s}}
    run(w.handle_update(update))
    return w.telegram.sent[-1]["text"] if w.telegram.sent else None


def test_commands_answer_only_the_configured_chat(make):
    w = make()
    assert command(w, "/status", chat="999") is None
    assert command(w, "/status", age_s=3600) is None  # queued while the watcher was down
    assert "Watcher" in command(w, "/status")
    assert "Unknown command /nope" in command(w, "/nope")
    assert "/markets" in command(w, "/help")


def test_mute_unmute_markets_and_alerts(make, market):
    w = make()
    assert "muted until" in command(w, "/mute 2h") and w.state.get("mute_until")
    assert "Usage" in command(w, "/mute forever")
    assert "back on" in command(w, "/unmute")
    assert "No prices yet" in command(w, "/markets")
    run(w.check(datetime.now().astimezone()))
    assert "S&amp;P 500 fut" in command(w, "/markets")
    assert command(w, "/alerts") == "No alerts today."


def test_digest_command_respects_the_budget(make):
    w = make(daily_calls=2)  # a weekday: both calls are reserved for the scheduled digests
    assert "No Claude calls left" in run(w.commands.handle("/digest", FRI_1600))
    w = make(ok(DIGEST))

    async def go():
        reply = await w.commands.handle("/digest", FRI_1600)
        await asyncio.gather(*w.background)
        return reply

    assert "Researching" in run(go())
    assert w.telegram.sent[-1]["text"].startswith("📰 <b>Market digest</b>")
    assert [c["kind"] for c in w.state.get("claude_calls")] == ["manual"]
    assert "news since Friday 23 October 2026, 15:00 Prague" in w.backend.calls[0]["user"]
