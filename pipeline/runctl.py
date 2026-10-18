"""Run control: logging setup, the run lock, and schedule slots."""

from __future__ import annotations

import fcntl
import logging
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from zoneinfo import ZoneInfo

SLOTS = ("morning", "preopen", "weekly", "manual")


class AlreadyRunning(Exception):
    pass


@contextmanager
def run_lock(path: Path):
    """Exclusive, non-blocking lock so two runs never overlap (released even if the process dies)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            f.seek(0)
            raise AlreadyRunning(f"another run holds {path} (pid {f.read().strip() or '?'})") from None
        f.seek(0)
        f.truncate()
        f.write(str(os.getpid()))
        f.flush()
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def local_now(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))


def setup_logging(logs_dir: Path, verbose: bool = False) -> None:
    logs_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    file_h = TimedRotatingFileHandler(logs_dir / "pipeline.log", when="midnight", backupCount=30)
    file_h.setFormatter(fmt)
    file_h.setLevel(logging.DEBUG)

    console_h = logging.StreamHandler(sys.stderr)
    console_h.setFormatter(fmt)
    console_h.setLevel(logging.DEBUG if verbose else logging.INFO)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.handlers[:] = [file_h, console_h]
    for noisy in ("yfinance", "urllib3", "peewee"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
