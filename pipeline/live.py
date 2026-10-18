"""Live prices: trades from the stream become 1-minute bars per symbol, in memory.

The detectors read windows from here (e.g. the change over the last 15
minutes); the watcher copies changed quotes to the live_quotes table every
minute, so other processes (digests, /why) can see them.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass


@dataclass
class Bar:
    ts: int  # minute start, unix seconds
    open: float
    high: float
    low: float
    close: float
    volume: float

    def add(self, price: float, volume: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.volume += volume


class Tape:
    """One symbol's recent 1-minute bars (oldest first)."""

    def __init__(self, max_minutes: int = 24 * 60):
        self.bars: deque[Bar] = deque(maxlen=max_minutes)
        self.last_price: float | None = None
        self.last_ts: float | None = None  # unix seconds of the newest trade
        self.trades = 0

    def add(self, price: float, ts: float, volume: float = 0.0) -> None:
        minute = int(ts // 60) * 60
        if self.bars and minute < self.bars[-1].ts:
            # a late trade: update its bar if we still have it, but it doesn't move the "last" price
            for bar in reversed(self.bars):
                if bar.ts == minute:
                    bar.add(price, volume)
                    break
                if bar.ts < minute:
                    break
            return
        if self.bars and self.bars[-1].ts == minute:
            self.bars[-1].add(price, volume)
        else:
            self.bars.append(Bar(minute, price, price, price, price, volume))
        self.last_price, self.last_ts = price, ts
        self.trades += 1

    def bar_at(self, ts: float) -> Bar | None:
        """The latest bar that started at or before `ts` (None if we have nothing that old)."""
        for bar in reversed(self.bars):
            if bar.ts <= ts:
                return bar
        return None

    def change_pct(self, minutes: float, now: float) -> float | None:
        """Change of the last price versus `minutes` ago.

        None when the window has no solid start: the tape doesn't reach back that far,
        the reference bar is more than one window older than the window start (an
        overnight or halt gap, which is not an intraday move), or the last trade is
        older than the window itself.
        """
        window = minutes * 60
        if self.last_price is None or self.last_ts is None or now - self.last_ts > window:
            return None
        ref = self.bar_at(now - window)
        if ref is None or (now - window) - ref.ts > window:
            return None
        return (self.last_price / ref.close - 1) * 100

    def volume_since(self, ts: float) -> float:
        return sum(b.volume for b in self.bars if b.ts >= ts)

    def finished_bars(self, now: float, after_ts: int) -> list[Bar]:
        """Bars that ended before `now` and started after `after_ts` (the last one persisted)."""
        current = int(now // 60) * 60
        return [b for b in self.bars if after_ts < b.ts < current]


class LiveBook:
    """All streamed symbols' tapes plus their previous closes."""

    def __init__(self, symbols: list[str]):
        self.tapes: dict[str, Tape] = {s: Tape() for s in symbols}
        self.prev_close: dict[str, float] = {}
        self.seed: dict[str, float] = {}  # last quote before any trade arrived: shown, never used in windows
        self.persisted_until: dict[str, int] = {s: 0 for s in symbols}

    def on_trades(self, trades: list[dict]) -> None:
        """Finnhub trade messages: {s: symbol, p: price, t: unix ms, v: volume, c: conditions}."""
        for t in trades:
            tape = self.tapes.get(t.get("s"))
            if tape is not None and t.get("p"):
                tape.add(float(t["p"]), t["t"] / 1000, float(t.get("v") or 0))

    def price(self, symbol: str) -> float | None:
        tape = self.tapes.get(symbol)
        return tape.last_price if tape and tape.last_price is not None else self.seed.get(symbol)

    def day_change_pct(self, symbol: str) -> float | None:
        price, prev = self.price(symbol), self.prev_close.get(symbol)
        if price is None or not prev:
            return None
        return (price / prev - 1) * 100

    def quote_rows(self, session: str, now_iso: str) -> list[dict]:
        rows = []
        for s in self.tapes:
            price = self.price(s)
            if price is not None:
                change = self.day_change_pct(s)
                rows.append({
                    "symbol": s, "price": price, "prev_close": self.prev_close.get(s),
                    "day_change_pct": None if change is None else round(change, 3),
                    "session": session, "updated_at": now_iso,
                })  # fmt: skip
        return rows

    def take_finished_bars(self, now: float) -> list[tuple]:
        """Finished bars not yet handed out, as storage rows; marks them handed out."""
        rows = []
        for s, tape in self.tapes.items():
            bars = tape.finished_bars(now, self.persisted_until[s])
            if bars:
                rows += [(s, b.ts, b.open, b.high, b.low, b.close, b.volume) for b in bars]
                self.persisted_until[s] = bars[-1].ts
        return rows

    def seconds_since_trade(self, now: float) -> float | None:
        latest = max((t.last_ts for t in self.tapes.values() if t.last_ts), default=None)
        return None if latest is None else now - latest
