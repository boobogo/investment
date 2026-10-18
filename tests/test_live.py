import asyncio
import json

import pandas as pd
import pytest

from pipeline import storage, watcher
from pipeline.finnhub import TradeStream
from pipeline.live import LiveBook, Tape
from tests.test_runner import settings  # noqa: F401  (fixture)

T0 = 1_792_000_000 - (1_792_000_000 % 60)  # a minute boundary


# --- tape and book ---


def test_tape_builds_minute_bars():
    tape = Tape()
    tape.add(100.0, T0 + 1, 10)
    tape.add(101.0, T0 + 30, 5)
    tape.add(99.5, T0 + 59, 1)
    tape.add(102.0, T0 + 61, 2)
    assert len(tape.bars) == 2
    b = tape.bars[0]
    assert (b.open, b.high, b.low, b.close, b.volume) == (100.0, 101.0, 99.5, 99.5, 16)
    assert tape.last_price == 102.0 and tape.trades == 4


def test_late_trade_updates_its_bar_but_not_last_price():
    tape = Tape()
    tape.add(100.0, T0 + 10)
    tape.add(101.0, T0 + 70)
    tape.add(90.0, T0 + 50)  # arrives late, belongs to the first minute
    assert tape.bars[0].low == 90.0 and tape.last_price == 101.0


def test_change_pct_ignores_overnight_gaps():
    tape = Tape()
    tape.add(100.0, T0)  # yesterday's last trade
    tape.add(110.0, T0 + 17 * 3600)  # first trade today, a 10% gap
    assert tape.change_pct(15, T0 + 17 * 3600 + 30) is None  # a gap is not a 15-minute move


def test_change_pct_windows():
    tape = Tape()
    for i in range(20):
        tape.add(100.0 + i, T0 + i * 60 + 5)  # +1 per minute
    now = T0 + 19 * 60 + 30
    assert tape.change_pct(15, now) == pytest.approx((119 / 104 - 1) * 100)
    assert tape.change_pct(30, now) is None  # the tape doesn't reach back 30 minutes
    assert tape.change_pct(15, now + 20 * 60) is None  # last trade older than the window: stale


def test_book_quotes_and_finished_bars():
    book = LiveBook(["AAPL", "SPY"])
    book.prev_close["AAPL"] = 100.0
    book.on_trades([{"s": "AAPL", "p": 103.0, "t": (T0 + 5) * 1000, "v": 10},
                    {"s": "NOPE", "p": 1.0, "t": T0 * 1000, "v": 1}])
    assert book.day_change_pct("AAPL") == pytest.approx(3.0) and book.day_change_pct("SPY") is None
    [row] = book.quote_rows("regular", "2026-10-19T14:00:00+00:00")
    assert (row["symbol"], row["price"], row["day_change_pct"]) == ("AAPL", 103.0, 3.0)
    assert book.take_finished_bars(T0 + 30) == []  # the current minute isn't finished
    assert book.take_finished_bars(T0 + 61) == [("AAPL", T0, 103.0, 103.0, 103.0, 103.0, 10.0)]
    assert book.take_finished_bars(T0 + 200) == []  # not handed out twice


def test_build_universe():
    u = watcher.build_universe(["AAPL", "SPY", "KO"], ["SPY", "QQQ", "IWM", "XLK"], limit=4)
    assert u.streamed == ["SPY", "QQQ", "AAPL", "KO"] and u.polled == ["IWM", "XLK"]


# --- trade stream ---


class FakeWS:
    def __init__(self, messages, fail_after=False):
        self.messages, self.sent, self.fail_after = messages, [], fail_after

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def send(self, msg):
        self.sent.append(json.loads(msg))

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for m in self.messages:
            yield json.dumps(m)
        raise ConnectionResetError("connection to wss://ws.finnhub.io?token=SECRET lost")


def test_trade_stream_subscribes_dispatches_and_reconnects(caplog):
    got, sleeps, sockets = [], [], []

    def connect(url, **kw):
        assert url.endswith("token=SECRET")
        ws = FakeWS([{"type": "ping"}, {"type": "trade", "data": [{"s": "AAPL", "p": 1, "t": 0, "v": 1}]},
                     {"type": "error", "msg": "Subscribing to too many symbols"}])
        sockets.append(ws)
        return ws

    async def sleep(s):
        sleeps.append(s)
        if len(sleeps) == 3:
            raise asyncio.CancelledError

    stream = TradeStream("SECRET", ["AAPL", "SPY"], got.extend, connect=connect, sleep=sleep)
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(stream.run())
    assert sockets[0].sent == [{"type": "subscribe", "symbol": "AAPL"}, {"type": "subscribe", "symbol": "SPY"}]
    assert len(got) == 3 and stream.connects == 3 and stream.errors[0] == "Subscribing to too many symbols"
    assert sleeps == [1.0, 1.0, 1.0]  # each connect worked, so the backoff resets
    assert "SECRET" not in caplog.text


def test_trade_stream_backs_off_when_connects_fail():
    sleeps = []

    def connect(url, **kw):
        raise OSError("refused")

    async def sleep(s):
        sleeps.append(s)
        if len(sleeps) == 8:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        asyncio.run(TradeStream("K", ["A"], lambda d: None, connect=connect, sleep=sleep).run())
    assert sleeps == [1, 2, 4, 8, 16, 32, 60, 60]


# --- watcher pieces ---


class FakeFinnhub:
    key = "K"

    def __init__(self, session="regular", holiday=None):
        self.status = {"isOpen": session == "regular", "session": session, "holiday": holiday}

    def market_status(self):
        return self.status

    def quote(self, symbol):
        return {"c": 105.0, "pc": 100.0, "dp": 5.0, "t": T0}


class FakeStream:
    connected = True

    def __init__(self, key, symbols, on_trades):
        self.symbols, self.on_trades = symbols, on_trades

    async def run(self):
        await asyncio.sleep(3600)


class FakePrices:
    def history(self, symbols):
        raise ConnectionError("offline test: keep the cached history")

    def intraday(self, symbols):
        idx = pd.DatetimeIndex([pd.Timestamp(T0, unit="s", tz="UTC"), pd.Timestamp(T0 + 300, unit="s", tz="UTC")])
        df = pd.DataFrame({"open": [1.0, 2.0], "high": [1.0, 2.0], "low": [1.0, 2.0], "close": [10.0, 11.0],
                           "volume": [100.0, 50.0]}, index=idx)
        return {s: df for s in symbols}


def make_watcher(settings, **kw):  # noqa: F811
    settings.watch.etfs = ["SPY"]
    return watcher.Watcher(settings, finnhub=kw.pop("fh", FakeFinnhub()), stream_factory=FakeStream,
                           price_source=FakePrices(), macro_fetcher=lambda syms: {
                               "^VIX": {"price": 20.0, "prev_close": 16.0, "change_pct": 25.0}}, **kw)


def test_watcher_session_prev_close_macro_and_persist(settings):  # noqa: F811
    w = make_watcher(settings)
    assert w.universe.streamed == ["SPY", "AAPL", "MSFT"]

    async def go():
        await w.refresh_session()
        await w.refresh_reference_data()
        await w.poll_macro()

    asyncio.run(go())
    assert w.session() == "regular"
    aapl_close = float(storage.load_prices(w.conn, "AAPL")["close"].iloc[-1])  # fixture history ends 2026-10-15
    assert w.book.prev_close == {"SPY": pytest.approx(771.79), "AAPL": aapl_close, "MSFT": aapl_close}
    w.book.on_trades([{"s": "AAPL", "p": aapl_close * 1.1, "t": (T0 + 120) * 1000, "v": 1}])
    w.persist()
    quotes = {r["symbol"]: r for r in (dict(x) for x in w.conn.execute("SELECT * FROM live_quotes"))}
    assert quotes["AAPL"]["day_change_pct"] == pytest.approx(10.0)
    assert quotes["^VIX"]["day_change_pct"] == 25.0 and quotes["SPY"]["session"] == "regular"
    assert storage.get_state(w.conn, "heartbeat")
    assert w.conn.execute("SELECT count(*) FROM bars_1m WHERE symbol = 'AAPL'").fetchone()[0] == 1  # the trade's bar
    assert {r["symbol"]: r["price"] for r in w.book.quote_rows("regular", "x")}["MSFT"] == 105.0  # seed quote shown
    assert "session regular" in w.status_line() and "^VIX 20.00 (+25.0%)" in w.status_line()


def test_holiday_means_closed(settings):  # noqa: F811
    w = make_watcher(settings, fh=FakeFinnhub(session=None, holiday="Thanksgiving Day"))
    asyncio.run(w.refresh_session())
    assert w.session() == "closed"


def test_extended_fallback_feeds_silent_tapes(settings):  # noqa: F811
    w = make_watcher(settings)
    asyncio.run(w.extended_fallback(now=T0 + 3600))
    tape = w.book.tapes["AAPL"]
    assert tape.last_price == 11.0 and tape.last_ts == T0 + 300 + 299


def test_watcher_run_stops_cleanly(settings):  # noqa: F811
    w = make_watcher(settings)
    asyncio.run(w.run(duration_s=0.2))
    assert storage.get_state(w.conn, "heartbeat")  # final persist ran


def five_min(points):
    """[('2026-10-16 16:55', price), ...] in New York time -> UTC-indexed closes."""
    idx = pd.DatetimeIndex([pd.Timestamp(t, tz="America/New_York") for t, _ in points]).tz_convert("UTC")
    return pd.Series([p for _, p in points], index=idx)


def test_macro_quote_futures_use_the_previous_session_close():
    closes = five_min([("2026-10-15 16:55", 30202.5), ("2026-10-16 09:00", 30000.0), ("2026-10-16 16:55", 29391.5),
                       ("2026-10-18 18:05", 29400.0), ("2026-10-18 19:00", 29100.0)])
    q = watcher.macro_quote(closes, futures=True)
    assert q["prev_close"] == 29391.5 and q["price"] == 29100.0  # vs Friday's last trade, not Thursday's
    assert q["change_pct"] == pytest.approx((29100 / 29391.5 - 1) * 100)
    friday = watcher.macro_quote(closes.iloc[:3], futures=True)
    assert friday["prev_close"] == 30202.5  # during Friday's session: vs Thursday's last trade


def test_macro_quote_index_uses_the_previous_day():
    closes = five_min([("2026-10-15 16:10", 16.46), ("2026-10-16 09:30", 17.0), ("2026-10-16 16:10", 19.98)])
    q = watcher.macro_quote(closes, futures=False)
    assert (q["prev_close"], q["price"]) == (16.46, 19.98) and q["time"].hour == 20  # 16:10 New York in UTC
    assert watcher.macro_quote(closes.iloc[1:], futures=False) is None  # no previous day: no reference


def test_stale_macro_quotes_are_not_acted_on(settings):  # noqa: F811
    from datetime import datetime, timedelta, timezone

    w = make_watcher(settings)
    now = datetime(2026, 10, 18, 22, 5, tzinfo=timezone.utc)  # Sunday 18:05 New York, futures just reopened
    w.macro = {"NQ=F": {"price": 29391.5, "prev_close": 30202.5, "change_pct": -2.7, "time": now - timedelta(days=2)}}
    assert [e for e in w.detect(now) if e.symbol == "NQ=F"] == []
    w.macro["NQ=F"]["time"] = now - timedelta(minutes=10)
    assert [e.symbol for e in w.detect(now) if e.symbol == "NQ=F"] == ["NQ=F"]
