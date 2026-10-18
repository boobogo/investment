"""Finnhub REST client for the free tier: quotes, news, earnings calendar, market status.

Free-tier facts (checked 2026-10-18): 60 calls/minute; /stock/candle, /press-releases
and index symbols like ^VIX are not included. The client stays under the limit on
its own and never puts the API key in errors or logs.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from collections import deque
from datetime import date

import requests
import websockets

from pipeline import config

log = logging.getLogger(__name__)

BASE = "https://finnhub.io/api/v1"
KEY_ENV = "FINNHUB_API_KEY"


class FinnhubError(RuntimeError):
    pass


class RateLimiter:
    """At most `per_minute` calls in any 60-second window (thread-safe)."""

    def __init__(self, per_minute: int, clock=time.monotonic, sleep=time.sleep):
        self.per_minute, self.clock, self.sleep = per_minute, clock, sleep
        self.calls: deque[float] = deque()
        self.lock = threading.Lock()

    def wait(self) -> None:
        with self.lock:
            now = self.clock()
            while self.calls and now - self.calls[0] >= 60:
                self.calls.popleft()
            if len(self.calls) >= self.per_minute:
                pause = 60 - (now - self.calls[0])
                self.sleep(pause)
                now = self.clock()
                self.calls.popleft()
            self.calls.append(now)


class Finnhub:
    def __init__(self, key: str, per_minute: int = 55, timeout: float = 15, session=None, limiter=None, sleep=time.sleep):
        self.key, self.timeout = key, timeout
        self.session = session or requests.Session()
        self.limiter = limiter or RateLimiter(per_minute)
        self.sleep = sleep

    @classmethod
    def from_env(cls) -> Finnhub:
        return cls(config.secret(KEY_ENV))

    def get(self, path: str, **params):
        for attempt in range(2):
            self.limiter.wait()
            try:
                r = self.session.get(f"{BASE}{path}", params={**params, "token": self.key}, timeout=self.timeout)
            except requests.RequestException as e:
                raise FinnhubError(f"{path}: {type(e).__name__}") from None  # the message would include the key
            if r.status_code == 429 and attempt == 0:
                self.sleep(5)
                continue
            if r.status_code in (401, 403):
                raise FinnhubError(f"{path}: not available with this API key/plan (HTTP {r.status_code})")
            if r.status_code != 200:
                raise FinnhubError(f"{path}: HTTP {r.status_code}")
            try:
                body = r.json()
            except ValueError:
                raise FinnhubError(f"{path}: response was not JSON") from None
            if isinstance(body, dict) and body.get("error"):
                raise FinnhubError(f"{path}: {body['error']}")
            return body
        raise FinnhubError(f"{path}: rate limited")

    # --- endpoints -------------------------------------------------------------

    def quote(self, symbol: str) -> dict:
        """{c: price, d: change, dp: change %, h, l, o, pc: previous close, t: unix time}"""
        return self.get("/quote", symbol=symbol)

    def market_news(self, category: str = "general", min_id: int = 0) -> list[dict]:
        """Newest first: {id, datetime (unix), headline, source, summary, url, related, category}"""
        params = {"category": category}
        if min_id:
            params["minId"] = min_id
        return self.get("/news", **params)

    def company_news(self, symbol: str, start: date, end: date) -> list[dict]:
        return self.get("/company-news", symbol=symbol, **{"from": start.isoformat(), "to": end.isoformat()})

    def earnings_calendar(self, start: date, end: date, symbol: str | None = None) -> list[dict]:
        """{date, symbol, hour ('bmo' before open | 'amc' after close | ''), epsEstimate, revenueEstimate, ...}"""
        params = {"from": start.isoformat(), "to": end.isoformat()}
        if symbol:
            params["symbol"] = symbol
        return self.get("/calendar/earnings", **params).get("earningsCalendar", [])

    def market_status(self, exchange: str = "US") -> dict:
        """{isOpen, session ('pre-market' | 'regular' | 'post-market' | None), holiday, t, timezone}"""
        return self.get("/stock/market-status", exchange=exchange)


# --- live trade stream -------------------------------------------------------------

STREAM_URL = "wss://ws.finnhub.io?token={key}"


class TradeStream:
    """Finnhub's websocket trade stream with automatic reconnect.

    Messages: {"type": "trade", "data": [{s, p, t, v, c}, ...]}, {"type": "ping"},
    {"type": "error", "msg": ...}. The free tier accepts about 50 symbols per key.
    Cancel the `run()` task to stop.
    """

    def __init__(self, key: str, symbols: list[str], on_trades, connect=None, sleep=None):
        self.key, self.symbols, self.on_trades = key, list(symbols), on_trades
        self.connect = connect or websockets.connect
        self.sleep = sleep or asyncio.sleep
        self.connected = False
        self.connects = 0
        self.errors: list[str] = []

    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                async with self.connect(STREAM_URL.format(key=self.key), open_timeout=15,
                                        ping_interval=20, ping_timeout=20) as ws:  # fmt: skip
                    for s in self.symbols:
                        await ws.send(json.dumps({"type": "subscribe", "symbol": s}))
                    self.connected, backoff = True, 1.0
                    self.connects += 1
                    log.info("stream: connected, %d symbols subscribed", len(self.symbols))
                    async for raw in ws:
                        msg = json.loads(raw)
                        kind = msg.get("type")
                        if kind == "trade":
                            self.on_trades(msg.get("data") or [])
                        elif kind == "error":
                            self.errors.append(str(msg.get("msg")))
                            log.warning("stream: server error: %s", msg.get("msg"))
            except Exception as e:  # network errors, server closes, bad frames: always reconnect
                # exception text can include the URL, and with it the key
                log.warning("stream: disconnected (%s); reconnecting in %.0fs", type(e).__name__, backoff)
            self.connected = False
            await self.sleep(backoff)
            backoff = min(backoff * 2, 60)
