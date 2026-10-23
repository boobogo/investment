"""The watcher's memory: one small JSON file instead of a database.

Only what must survive a restart is kept: the heartbeat, the Telegram update
offset, mute, which digests ran, recent Claude calls (for the daily cap), recent
alerts (for repeats, /alerts and the next digest). Prices are never stored.
The file is a few KB and is rewritten a few times an hour at most, which is
nothing for the Pi's SD card. Writes go to a temporary file that then replaces
the old one, so a power cut leaves either the old or the new version.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

KEEP = timedelta(days=8)  # Claude calls and alerts older than this are dropped
LISTS = ("claude_calls", "alerts")


class State:
    def __init__(self, path: Path, readonly: bool = False):
        self.path, self.readonly = path, readonly  # readonly: changes stay in memory (a second process)
        self.data: dict = {}
        if path.exists():
            try:
                self.data = json.loads(path.read_text())
            except (OSError, ValueError) as e:
                log.error("state file %s unreadable (%s); starting empty", path, e)

    def get(self, key: str, default=None):
        return self.data.get(key, default)

    def set(self, key: str, value) -> None:
        self.data[key] = value
        self.save()

    def append(self, key: str, item: dict) -> None:
        """Add to a dated list (each item has an ISO 'at'); old items are dropped."""
        self.data.setdefault(key, []).append(item)
        self.save()

    def items_since(self, key: str, since: datetime) -> list[dict]:
        return [i for i in self.data.get(key, []) if datetime.fromisoformat(i["at"]) >= since]

    def save(self) -> None:
        self._prune()
        if self.readonly:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, indent=1, sort_keys=True))
        os.replace(tmp, self.path)

    def _prune(self) -> None:
        if not any(self.data.get(k) for k in LISTS):
            return
        newest = max(datetime.fromisoformat(i["at"]) for k in LISTS for i in self.data.get(k, []))
        cutoff = newest - KEEP
        for k in LISTS:
            if k in self.data:
                self.data[k] = [i for i in self.data[k] if datetime.fromisoformat(i["at"]) >= cutoff]
        memory = self.data.get("alert_memory", {})
        for key in [k for k, v in memory.items() if datetime.fromisoformat(v["at"]) < cutoff]:
            del memory[key]
