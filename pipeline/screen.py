"""Pre-filter: decide which tickers are worth a model call this run.

Every ticker gets its list of active *conditions*; they go into the packet as
`signals` and into the report. A ticker is sent to the analyst when at least
one condition is *fresh*:
  - events (a big move, an extended-hours move, a crossover, a new 52-week
    extreme, a volume spike, news together with a move) are fresh once per
    market session;
  - states (RSI or z-score extremes, earnings coming up) are fresh when first
    seen, then again at most every `state_repeat_days` while they last.
What has been sent is remembered in the trigger_log table.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

from pipeline.config import ScreenCfg
from pipeline.news import NewsResult

# conditions the report lists under "At extremes"
EXTREMES = ("rsi_extreme", "z20_extreme", "new_52w_high", "new_52w_low")
MOVES = ("big_move", "ext_move", "volume_spike")


@dataclass
class Condition:
    kind: str
    label: str
    priority: int  # higher is more urgent; orders the batches and the max_sends cut
    key: str  # the session it belongs to: a market date, an extended-hours session, a news timestamp
    state: bool = False
    fresh: bool = False


@dataclass
class Decision:
    ticker: str
    conditions: list[Condition] = field(default_factory=list)
    send: bool = False
    note: str = ""  # why a triggered ticker was held back

    @property
    def priority(self) -> int:
        return max((c.priority for c in self.conditions if c.fresh), default=0)


def conditions_for(
    tech: dict,
    fund: dict,
    news: NewsResult | None,
    ext: dict | None,
    cfg: ScreenCfg,
    today: date,
    *,
    held: bool = False,
    days_since_review: int | None = None,
) -> list[Condition]:
    as_of = tech["as_of"]
    out: list[Condition] = []

    def add(kind, label, priority, key=as_of, state=False):
        out.append(Condition(kind, label, priority, key, state))

    # extremes
    rsi = tech.get("rsi14")
    if rsi is not None and rsi >= cfg.rsi_extreme_high:
        add("rsi_extreme", f"RSI {rsi:.0f}: extreme overbought (≥{cfg.rsi_extreme_high:g})", 90, state=True)
    elif rsi is not None and rsi <= cfg.rsi_extreme_low:
        add("rsi_extreme", f"RSI {rsi:.0f}: extreme oversold (≤{cfg.rsi_extreme_low:g})", 90, state=True)
    z = tech.get("z20")
    if z is not None and abs(z) >= cfg.z20_extreme:
        side = "above" if z > 0 else "below"
        add("z20_extreme", f"close {abs(z):.1f} std devs {side} its 20-day mean", 80, state=True)
    # the new extreme is set by the day's high/low; a close well away from it can be an early reversal sign
    if cfg.new_52w_extremes and tech.get("new_52w_high"):
        off = tech.get("from_52w_high_pct") or 0
        add("new_52w_high", "new 52-week high" if off > -1 else f"new 52-week high intraday, closed {-off:.1f}% below it", 70)
    if cfg.new_52w_extremes and tech.get("new_52w_low"):
        off = tech.get("from_52w_low_pct") or 0
        add("new_52w_low", "new 52-week low" if off < 1 else f"new 52-week low intraday, closed {off:.1f}% above it", 70)

    # moves
    atr = tech.get("atr_pct")
    threshold = max(cfg.big_move_pct, cfg.big_move_atr_mult * atr) if atr else cfg.big_move_pct
    ret = tech.get("chg_1d_pct")
    if ret is not None and abs(ret) >= threshold:
        add("big_move", f"{ret:+.1f}% on the day ({abs(ret) / atr:.1f}x ATR)" if atr else f"{ret:+.1f}% on the day", 85)
    if ext and abs(ext["chg_pct"]) >= max(cfg.ext_move_pct, cfg.big_move_atr_mult * (atr or 0)):
        session = "after hours" if ext["session"] == "post" else "pre-market"
        add("ext_move", f"{ext['chg_pct']:+.1f}% {session} vs last close", 88, key=ext["key"])
    vol = tech.get("vol_vs_avg20")
    if vol is not None and vol >= cfg.volume_spike:
        add("volume_spike", f"volume {vol:.1f}x its 20-day average", 60)

    # crossovers on the last bar
    if cfg.golden_death_cross and tech.get("cross_sma50_sma200"):
        add("sma_cross", "golden cross: SMA50 above SMA200" if tech["cross_sma50_sma200"] > 0
            else "death cross: SMA50 below SMA200", 70)  # fmt: skip
    if cfg.price_sma200_cross and tech.get("cross_px_sma200"):
        add("px_sma200_cross", f"price crossed {'above' if tech['cross_px_sma200'] > 0 else 'below'} SMA200", 55)
    if cfg.macd_cross and tech.get("cross_macd_signal"):
        add("macd_cross", f"MACD crossed {'above' if tech['cross_macd_signal'] > 0 else 'below'} its signal line", 30)

    # news only counts when the market reacted (mega-caps have fresh headlines every day)
    fresh_new = [i for i in (news.items if news else []) if i.fresh and i.new]
    if fresh_new and (not cfg.news_needs_move or any(c.kind in MOVES for c in out)):
        newest = max(i.published for i in fresh_new)
        add("news", f"{len(fresh_new)} new headline{'s' if len(fresh_new) > 1 else ''} in 24h", 75,
            key=newest.strftime("%Y-%m-%dT%H:%M"))  # fmt: skip

    # calendar and portfolio
    if fund.get("next_earnings"):
        days = (date.fromisoformat(fund["next_earnings"]) - today).days
        if 0 <= days <= cfg.earnings_within_days:
            add("earnings_soon", f"earnings in {days} day{'s' if days != 1 else ''}", 45, state=True)
    if held and (days_since_review is None or days_since_review >= cfg.held_review_days):
        add("held_check", "held position due for a check", 20)
    return out


def _days_between(a: str, b: str) -> int:
    return (date.fromisoformat(b[:10]) - date.fromisoformat(a[:10])).days


def mark_fresh(conditions: list[Condition], last_keys: dict[str, str], repeat_days: int) -> None:
    """last_keys: kind -> latest key this ticker was sent with (from trigger_log)."""
    for c in conditions:
        last = last_keys.get(c.kind)
        if last is None:
            c.fresh = True
        elif c.state:
            c.fresh = _days_between(last, c.key) >= repeat_days
        else:
            c.fresh = c.key > last


def decide(
    conditions: dict[str, list[Condition]],
    last_keys: dict[str, dict[str, str]],
    cfg: ScreenCfg,
    *,
    pending: dict[str, str],
    review_all: str | None = None,
) -> list[Decision]:
    """Turn per-ticker conditions into send decisions, most urgent first.

    review_all: a label ("full review", "weekly full review") to send every ticker regardless.
    Screened runs send at most cfg.max_sends tickers; the rest are marked held back.
    """
    decisions = []
    for ticker, conds in conditions.items():
        any_key = conds[0].key if conds else ""
        mark_fresh(conds, last_keys.get(ticker, {}), cfg.state_repeat_days)
        if ticker in pending:
            conds.insert(0, Condition("carried_over", f"carried over from an earlier run ({pending[ticker]})", 100,
                                      any_key, fresh=True))  # fmt: skip
        if review_all:
            conds.append(Condition("full_review", review_all, 1, any_key, fresh=True))
        decisions.append(Decision(ticker, conds, send=any(c.fresh for c in conds)))

    decisions.sort(key=lambda d: (-d.priority, d.ticker))
    if not review_all:
        sent = [d for d in decisions if d.send]
        for d in sent[cfg.max_sends :]:
            d.send, d.note = False, f"held back: more than max_sends={cfg.max_sends} triggered"
    return decisions
