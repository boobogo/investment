"""Load and validate config/settings.yaml and secrets from .env.

Unknown keys are rejected so a typo in YAML fails loudly instead of silently
falling back to a default. Secret values are never logged or printed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

ROOT = Path(__file__).resolve().parent.parent
SETTINGS_PATH = ROOT / "config" / "settings.yaml"
ENV_PATH = ROOT / ".env"

WEEKDAYS = ["mon", "tue", "wed", "thu", "fri"]


class MissingSecret(RuntimeError):
    pass


def load_env(path: Path = ENV_PATH) -> None:
    """Load KEY=VALUE lines from .env into the environment; variables already set win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip().removeprefix("export ").strip()
        os.environ.setdefault(key, value.strip().strip("'\""))


def secret(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise MissingSecret(f"{name} is not set (add it to {ENV_PATH})")
    return value


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Paths(_Strict):
    state: Path = Path("data/state.json")
    prompts: Path = Path("prompts")

    @field_validator("*")
    @classmethod
    def _absolute(cls, v: Path) -> Path:
        return v if v.is_absolute() else ROOT / v


DayName = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
HHMM = Annotated[str, StringConstraints(pattern=r"^\d{2}:\d{2}$")]


class TimeWindow(_Strict):
    start: HHMM
    end: HHMM
    timezone: str
    days: list[DayName] = ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class WatchCfg(_Strict):
    # the instruments, by group: {group: {yahoo symbol: short name}}
    instruments: dict[str, dict[str, str]] = {"US stocks": {"ES=F": "S&P 500 fut", "NQ=F": "Nasdaq 100 fut"}}
    check_minutes_open: float = Field(15, gt=0)
    check_minutes_closed: float = Field(30, gt=0)
    market_hours: list[TimeWindow] = [
        TimeWindow(start="09:00", end="17:30", timezone="Europe/Berlin", days=WEEKDAYS),
        TimeWindow(start="09:30", end="16:00", timezone="America/New_York", days=WEEKDAYS),
    ]
    max_quote_age_minutes: float = 45
    sigma_days: int = Field(250, ge=20)

    def names(self) -> dict[str, str]:
        return {s: n for group in self.instruments.values() for s, n in group.items()}

    def groups(self) -> dict[str, str]:
        return {s: g for g, members in self.instruments.items() for s in members}


class AlertsCfg(_Strict):
    mode: Literal["shadow", "live"] = "shadow"
    day_sigma: float = 2.5
    hour_sigma: float = 2.0
    critical_day_sigma: float = 4.0
    critical_hour_sigma: float = 3.0
    repeat_hours_day: float = 20
    repeat_hours_hour: float = 3
    escalation: float = 1.5
    critical_symbols: list[str] = ["ES=F", "NQ=F", "YM=F", "RTY=F", "^VIX", "CL=F", "BZ=F", "GC=F"]
    context: list[str] = ["ES=F", "NQ=F", "^VIX", "CL=F", "GC=F", "BTC-USD"]
    quiet_hours: TimeWindow = TimeWindow(start="23:00", end="07:00", timezone="Europe/Prague")
    daily_budget: int = 6


class ClaudeCfg(_Strict):
    model: str = "claude-sonnet-5-5"
    effort: Literal["low", "medium", "high", "xhigh", "max"] | None = None
    timeout_s: float = 600
    retries: int = 1
    backoff_s: list[float] = [60]
    daily_calls: int = Field(4, ge=0)
    explain_min_gap_minutes: float = 120
    explain_skip_before_digest_minutes: float = 45


class DigestTime(_Strict):
    time: HHMM
    timezone: str
    days: list[DayName]


class DigestsCfg(_Strict):
    morning: DigestTime | None = DigestTime(time="07:30", timezone="Europe/Prague", days=WEEKDAYS)
    preopen: DigestTime | None = DigestTime(time="09:00", timezone="America/New_York", days=WEEKDAYS)
    weekly: DigestTime | None = DigestTime(time="19:00", timezone="Europe/Prague", days=["sun"])
    follow: list[str] = []
    timezone: str = "Europe/Prague"

    def scheduled(self) -> dict[str, DigestTime]:
        return {k: v for k in ("morning", "preopen", "weekly") if (v := getattr(self, k)) is not None}


class TelegramCfg(_Strict):
    timeout_s: float = 20


class Settings(_Strict):
    paths: Paths = Paths()
    watch: WatchCfg = WatchCfg()
    alerts: AlertsCfg = AlertsCfg()
    claude: ClaudeCfg = ClaudeCfg()
    digests: DigestsCfg = DigestsCfg()
    telegram: TelegramCfg = TelegramCfg()


def load_settings(path: Path = SETTINGS_PATH) -> Settings:
    with open(path) as f:
        return Settings.model_validate(yaml.safe_load(f) or {})
