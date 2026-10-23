import pytest

from pipeline import telegram


class Resp:
    def __init__(self, status, body):
        self.status_code, self.body = status, body

    def json(self):
        return self.body


class FakeSession:
    def __init__(self, *responses):
        self.responses, self.posts = list(responses), []

    def post(self, url, data=None, files=None, timeout=None):
        self.posts.append({"url": url, "data": data, "files": files})
        r = self.responses.pop(0)
        if isinstance(r, Exception):
            raise r
        return r


def ok(message_id=1):
    return Resp(200, {"ok": True, "result": {"message_id": message_id}})


def client(*responses):
    sleeps = []
    return telegram.Telegram("SECRET-TOKEN", "42", session=FakeSession(*responses), sleep=sleeps.append), sleeps


def test_esc():
    assert telegram.esc("AT&T <b> 5 > 4") == "AT&amp;T &lt;b&gt; 5 &gt; 4"


def test_split_message():
    assert telegram.split_message("short") == ["short"]
    text = "\n".join(f"line {i:03d} " + "x" * 40 for i in range(200))  # ~10k chars
    chunks = telegram.split_message(text, limit=4096)
    assert all(len(c) <= 4096 for c in chunks) and "\n".join(chunks) == text
    long_line = "y" * 9000
    assert [len(c) for c in telegram.split_message(long_line)] == [4096, 4096, 808]


def test_send_message_and_reply():
    tg, _ = client(ok(7))
    assert tg.send("<b>hi</b>", reply_to=5) == [7]
    data = tg.session.posts[0]["data"]
    assert data["chat_id"] == "42" and data["parse_mode"] == "HTML" and data["reply_to_message_id"] == 5
    assert tg.session.posts[0]["url"].endswith("/sendMessage")


def test_rate_limit_retry_after():
    tg, sleeps = client(Resp(429, {"ok": False, "parameters": {"retry_after": 3}}), ok(9))
    assert tg.send("x") == [9] and sleeps == [3.0]


def test_errors_never_contain_the_token():
    import requests

    tg, _ = client(requests.ConnectionError("https://api.telegram.org/botSECRET-TOKEN/sendMessage"),
                   requests.ConnectionError("botSECRET-TOKEN"), requests.ConnectionError("botSECRET-TOKEN"))
    with pytest.raises(telegram.TelegramError) as e:
        tg.send("x")
    assert "SECRET-TOKEN" not in str(e.value) and "ConnectionError" in str(e.value)

    tg, _ = client(Resp(400, {"ok": False, "description": "Bad Request: chat not found"}))
    with pytest.raises(telegram.TelegramError, match="chat not found"):
        tg.send("x")
    assert len(tg.session.posts) == 1  # client errors are not retried
