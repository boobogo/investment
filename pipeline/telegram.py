"""Telegram Bot API client: send messages and files, read commands.

Messages use Telegram's HTML mode, so any text that comes from outside
(headlines, model output) must go through `esc()`. The bot token is never
logged: errors are reported without the request URL.
"""

from __future__ import annotations

import html
import logging
import time
from pathlib import Path

import requests

from pipeline import config

log = logging.getLogger(__name__)

API = "https://api.telegram.org/bot{token}/{method}"
MAX_LEN = 4096  # Telegram's limit per message
TOKEN_ENV, CHAT_ENV = "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"


class TelegramError(RuntimeError):
    pass


def esc(text: object) -> str:
    return html.escape(str(text), quote=False)


def split_message(text: str, limit: int = MAX_LEN) -> list[str]:
    """Split on blank lines, then lines, then hard cuts; never inside a line unless it alone is too long.

    Our messages keep each HTML tag within one line, so line-based splits never break markup.
    """
    if len(text) <= limit:
        return [text]
    chunks, current = [], ""
    for line in text.split("\n"):
        while len(line) > limit:  # a single overlong line: hard cut
            if current:
                chunks.append(current)
                current = ""
            chunks.append(line[:limit])
            line = line[limit:]
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


class Telegram:
    def __init__(self, token: str, chat_id: str, timeout: float = 20, session=None, sleep=time.sleep):
        self.token, self.chat_id, self.timeout = token, str(chat_id), timeout
        self.session = session or requests.Session()
        self.sleep = sleep

    @classmethod
    def from_env(cls, cfg: config.TelegramCfg) -> Telegram:
        return cls(config.secret(TOKEN_ENV), config.secret(CHAT_ENV), timeout=cfg.timeout_s)

    def _call(self, method: str, *, data: dict | None = None, files=None, timeout: float | None = None):
        url = API.format(token=self.token, method=method)
        last = "no attempt"
        for attempt in range(3):
            try:
                r = self.session.post(url, data=data, files=files, timeout=timeout or self.timeout)
            except requests.RequestException as e:
                last = type(e).__name__  # the exception text would contain the URL, and with it the token
                self.sleep(2 * (attempt + 1))
                continue
            try:
                body = r.json()
            except ValueError:
                body = {}
            if body.get("ok"):
                return body["result"]
            last = f"HTTP {r.status_code}: {body.get('description', 'no description')}"
            if r.status_code == 429:
                self.sleep(float((body.get("parameters") or {}).get("retry_after", 5)))
            elif r.status_code >= 500:
                self.sleep(2 * (attempt + 1))
            else:
                break  # a client error won't fix itself
        raise TelegramError(f"{method} failed: {last}")

    def send(self, text: str, *, reply_to: int | None = None, silent: bool = False) -> list[int]:
        """Send an HTML message (split if needed); returns the message ids."""
        ids = []
        for i, chunk in enumerate(split_message(text)):
            data = {
                "chat_id": self.chat_id,
                "text": chunk,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
                "disable_notification": "true" if silent else "false",
            }
            if reply_to and i == 0:
                data["reply_to_message_id"] = reply_to
                data["allow_sending_without_reply"] = "true"
            ids.append(self._call("sendMessage", data=data)["message_id"])
        return ids

    def send_document(self, path: Path, caption: str | None = None, *, silent: bool = True) -> int:
        with open(path, "rb") as f:
            data = {"chat_id": self.chat_id, "disable_notification": "true" if silent else "false"}
            if caption:
                data.update(caption=caption[:1024], parse_mode="HTML")
            return self._call("sendDocument", data=data, files={"document": (path.name, f)}, timeout=60)["message_id"]

    def get_updates(self, offset: int | None, wait_s: int = 25) -> list[dict]:
        data = {"timeout": wait_s, "allowed_updates": '["message"]'}
        if offset is not None:
            data["offset"] = offset
        return self._call("getUpdates", data=data, timeout=wait_s + 10)

    def get_me(self) -> dict:
        return self._call("getMe")
