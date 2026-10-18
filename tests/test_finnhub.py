from datetime import date

import pytest

from pipeline import finnhub


class Resp:
    def __init__(self, status, body):
        self.status_code, self.body = status, body

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.gets = list(responses), []

    def get(self, url, params=None, timeout=None):
        self.gets.append((url, params))
        return self.responses.pop(0)


class NoLimit:
    def wait(self):
        pass


def client(*responses):
    sleeps = []
    return finnhub.Finnhub("SECRET", session=FakeSession(*responses), limiter=NoLimit(), sleep=sleeps.append), sleeps


def test_rate_limiter_waits_when_window_is_full():
    t = [0.0]
    sleeps = []

    def sleep(s):
        sleeps.append(s)
        t[0] += s

    rl = finnhub.RateLimiter(3, clock=lambda: t[0], sleep=sleep)
    for _ in range(3):
        rl.wait()
        t[0] += 1
    assert sleeps == []
    rl.wait()  # 4th call at t=3: must wait until t=60, when the first call leaves the window
    assert sleeps == [57.0]


def test_endpoints_and_params():
    fh, _ = client(Resp(200, {"c": 10.0, "pc": 9.0}), Resp(200, [{"id": 1}]), Resp(200, {"earningsCalendar": [{"symbol": "KO"}]}))
    assert fh.quote("AAPL")["c"] == 10.0
    url, params = fh.session.gets[0]
    assert url.endswith("/quote") and params == {"symbol": "AAPL", "token": "SECRET"}
    assert fh.company_news("NVDA", date(2026, 10, 1), date(2026, 10, 2)) == [{"id": 1}]
    assert fh.session.gets[1][1]["from"] == "2026-10-01"
    assert fh.earnings_calendar(date(2026, 10, 19), date(2026, 10, 23)) == [{"symbol": "KO"}]


def test_errors():
    fh, _ = client(Resp(403, {"error": "You don't have access to this resource."}))
    with pytest.raises(finnhub.FinnhubError, match="not available") as e:
        fh.get("/stock/candle")
    assert "SECRET" not in str(e.value)
    fh, sleeps = client(Resp(429, {}), Resp(200, {"ok": 1}))
    assert fh.get("/quote", symbol="X") == {"ok": 1} and sleeps == [5]
    fh, _ = client(Resp(200, {"error": "Market data subscription required for CFD indices."}))
    with pytest.raises(finnhub.FinnhubError, match="subscription"):
        fh.quote("^VIX")
