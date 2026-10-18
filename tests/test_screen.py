from datetime import date, datetime, timedelta, timezone

from pipeline import screen
from pipeline.config import ScreenCfg
from pipeline.news import NewsItem, NewsResult

CFG = ScreenCfg()
TODAY = date(2026, 10, 16)
QUIET = {
    "as_of": "2026-10-15", "rsi14": 55.0, "z20": 0.5, "new_52w_high": False, "new_52w_low": False,
    "chg_1d_pct": 0.4, "atr_pct": 2.0, "vol_vs_avg20": 1.1,
    "cross_sma50_sma200": 0, "cross_px_sma200": 0, "cross_macd_signal": 0,
}  # fmt: skip


def kinds(tech=None, fund=None, news=None, ext=None, **kw):
    conds = screen.conditions_for({**QUIET, **(tech or {})}, fund or {}, news, ext, CFG, TODAY, **kw)
    return {c.kind: c for c in conds}


def fresh_news(n=1):
    now = datetime(2026, 10, 15, 22, 0, tzinfo=timezone.utc)
    items = [NewsItem(title=f"story {i}", source="Reuters", published=now - timedelta(hours=i), fresh=True, new=True)
             for i in range(n)]  # fmt: skip
    return NewsResult(items)


def test_quiet_ticker_has_no_conditions():
    assert kinds() == {}


def test_rsi_extremes_use_80_20():
    assert "rsi_extreme" not in kinds({"rsi14": 79.0})
    c = kinds({"rsi14": 83.4})["rsi_extreme"]
    assert c.label == "RSI 83: extreme overbought (≥80)" and c.state and c.priority == 90
    assert kinds({"rsi14": 18.0})["rsi_extreme"].label == "RSI 18: extreme oversold (≤20)"


def test_other_extremes():
    got = kinds({"z20": -2.7, "new_52w_low": True, "from_52w_low_pct": 0.4})
    assert got["z20_extreme"].label == "close 2.7 std devs below its 20-day mean"
    assert got["new_52w_low"].label == "new 52-week low" and not got["new_52w_low"].state
    faded = kinds({"new_52w_high": True, "from_52w_high_pct": -2.2})["new_52w_high"]
    assert faded.label == "new 52-week high intraday, closed 2.2% below it"


def test_big_move_needs_both_percent_and_atr():
    assert "big_move" in kinds({"chg_1d_pct": -3.5, "atr_pct": 2.0})  # 3.5 >= max(3, 3.0)
    assert "big_move" not in kinds({"chg_1d_pct": 3.5, "atr_pct": 3.0})  # volatile stock: needs 4.5
    assert kinds({"chg_1d_pct": 4.6, "atr_pct": 3.0})["big_move"].label == "+4.6% on the day (1.5x ATR)"


def test_extended_hours_move_keyed_by_session():
    ext = {"chg_pct": -5.0, "session": "post", "key": "2026-10-15:post", "px": 1.0, "utc": "x"}
    c = kinds(ext=ext)["ext_move"]
    assert c.label == "-5.0% after hours vs last close" and c.key == "2026-10-15:post"
    assert "ext_move" not in kinds(ext={**ext, "chg_pct": 1.0})


def test_crossovers_and_volume():
    got = kinds({"cross_sma50_sma200": 1, "cross_px_sma200": -1, "cross_macd_signal": 1, "vol_vs_avg20": 3.0})
    assert got["sma_cross"].label.startswith("golden cross")
    assert got["px_sma200_cross"].label == "price crossed below SMA200"
    assert "macd_cross" not in got  # off by default
    assert got["volume_spike"].label == "volume 3.0x its 20-day average"


def test_news_only_counts_with_a_move():
    assert "news" not in kinds(news=fresh_news(3))
    got = kinds({"vol_vs_avg20": 2.6}, news=fresh_news(3))
    assert got["news"].label == "3 new headlines in 24h" and got["news"].key == "2026-10-15T22:00"
    stale = NewsResult([NewsItem(title="old", source="x", published=datetime.now(timezone.utc), fresh=True, new=False)])
    assert "news" not in kinds({"vol_vs_avg20": 2.6}, news=stale)


def test_earnings_and_held():
    assert kinds(fund={"next_earnings": "2026-10-19"})["earnings_soon"].label == "earnings in 3 days"
    assert "earnings_soon" not in kinds(fund={"next_earnings": "2026-10-30"})
    assert "held_check" in kinds(held=True, days_since_review=None)
    assert "held_check" in kinds(held=True, days_since_review=8)
    assert "held_check" not in kinds(held=True, days_since_review=2)


def cond(kind, key, state=False, priority=50):
    return screen.Condition(kind, kind, priority, key, state)


def test_mark_fresh_events_once_per_session():
    c = [cond("big_move", "2026-10-15")]
    screen.mark_fresh(c, {}, 3)
    assert c[0].fresh
    screen.mark_fresh(c, {"big_move": "2026-10-15"}, 3)
    assert not c[0].fresh
    screen.mark_fresh(c, {"big_move": "2026-10-14"}, 3)
    assert c[0].fresh


def test_mark_fresh_states_repeat_every_n_days():
    c = [cond("rsi_extreme", "2026-10-15", state=True)]
    screen.mark_fresh(c, {"rsi_extreme": "2026-10-13"}, 3)
    assert not c[0].fresh
    screen.mark_fresh(c, {"rsi_extreme": "2026-10-12"}, 3)
    assert c[0].fresh


def test_decide_orders_caps_and_forces():
    conds = {
        "A": [cond("big_move", "d", priority=85)],
        "B": [cond("rsi_extreme", "d", state=True, priority=90)],
        "C": [],
        "D": [cond("volume_spike", "d", priority=60)],
    }
    cfg = ScreenCfg(max_sends=2)
    got = screen.decide({k: list(v) for k, v in conds.items()}, {}, cfg, pending={"C": "usage limit"})
    assert [(d.ticker, d.send) for d in got] == [("C", True), ("B", True), ("A", False), ("D", False)]
    assert got[2].note.startswith("held back")

    everyone = screen.decide({k: list(v) for k, v in conds.items()}, {}, cfg, pending={}, review_all="weekly full review")
    assert all(d.send for d in everyone) and not any(d.note for d in everyone)
    assert everyone[-1].conditions[-1].label == "weekly full review"
