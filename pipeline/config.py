"""Load and validate config/settings.yaml, config/tickers.yaml and secrets from .env.

Unknown keys are rejected so a typo in YAML fails loudly instead of silently
falling back to a default. Secret values are never logged or printed.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

ROOT = Path(__file__).resolve().parent.parent
SETTINGS_PATH = ROOT / "config" / "settings.yaml"
ENV_PATH = ROOT / ".env"


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
    db: Path = Path("data/market.db")
    logs: Path = Path("logs")
    reports: Path = Path("reports")
    prompt: Path = Path("prompts/analyst.md")
    tickers: Path = Path("config/tickers.yaml")

    @field_validator("*")
    @classmethod
    def _absolute(cls, v: Path) -> Path:
        return v if v.is_absolute() else ROOT / v


class DataCfg(_Strict):
    source: Literal["yfinance"] = "yfinance"
    history_period: str = "2y"
    benchmark: str = "SPY"
    fundamentals_max_age_hours: float = 20
    extended_hours: bool = True


class NewsCfg(_Strict):
    lookback_hours: float = 72
    fresh_hours: float = 24
    max_items: int = 5
    snippet_chars: int = 200
    dedup_similarity: float = 0.85
    dedup_min_shared_words: int = 2
    dedup_word_overlap: float = 0.4
    timeout_s: float = 15
    workers: int = 6
    preferred_sources: list[str] = []
    blocked_sources: list[str] = []
    skip_title_patterns: list[str] = []


class LLMCfg(_Strict):
    backend: Literal["claude_cli", "api"] = "claude_cli"
    model: str = "claude-sonnet-5-5"
    effort: Literal["low", "medium", "high", "xhigh", "max"] | None = None
    batch_size: int = Field(15, ge=1, le=30)
    timeout_s: float = 300
    retries: int = 2
    backoff_s: list[float] = [30, 120]
    web_search_max_uses: int = 3


class ScheduleCfg(_Strict):
    timezone: str = "Europe/Prague"


class ScreenCfg(_Strict):
    rsi_extreme_high: float = 80
    rsi_extreme_low: float = 20
    z20_extreme: float = 2.5
    new_52w_extremes: bool = True
    big_move_pct: float = 3.0
    big_move_atr_mult: float = 1.5
    ext_move_pct: float = 3.0
    volume_spike: float = 2.5
    golden_death_cross: bool = True
    price_sma200_cross: bool = True
    macd_cross: bool = False
    news_needs_move: bool = True
    earnings_within_days: int = 5
    held_review_days: int = 7
    state_repeat_days: int = 3
    full_review_weekday: int | None = Field(0, ge=0, le=6)
    max_sends: int = Field(30, ge=1)


class EvaluateCfg(_Strict):
    horizons: list[int] = [5, 20, 60]
    neutral_band_sigma: float = 0.5
    watch_band_sigma: float = 1.0
    vol_lookback_days: int = 60
    min_sample: int = 30


class ReportCfg(_Strict):
    delivery: Literal["none", "telegram", "email"] = "none"


DayName = Literal["mon", "tue", "wed", "thu", "fri", "sat", "sun"]


class DigestTime(_Strict):
    time: str = Field(pattern=r"^\d{2}:\d{2}$")
    timezone: str
    days: list[DayName]
    trading_days_only: bool = False


class DigestsCfg(_Strict):
    morning: DigestTime = DigestTime(time="07:30", timezone="Europe/Prague", days=["mon", "tue", "wed", "thu", "fri"])
    preopen: DigestTime = DigestTime(
        time="09:00", timezone="America/New_York", days=["mon", "tue", "wed", "thu", "fri"], trading_days_only=True
    )
    weekly: DigestTime = DigestTime(time="19:00", timezone="Europe/Prague", days=["sun"])
    highlights: int = Field(8, ge=0, le=15)


class NewsWatchCfg(_Strict):
    finnhub_general: bool = True
    finnhub_company: bool = True
    candidates_max: int = 40
    min_window_hours: float = 12
    max_window_hours: float = 72
    live: bool = True
    general_minutes: float = 2
    company_minutes: float = 10
    candidate_max_age_minutes: float = 45
    skip_sources: list[str] = []
    low_quality_sources: list[str] = []
    clickbait_patterns: list[str] = []
    impact_patterns: list[str] = []
    macro_patterns: list[str] = []


class WatchCfg(_Strict):
    etfs: list[str] = ["SPY", "QQQ", "IWM", "DIA"]
    stream_limit: int = Field(50, ge=1)
    macro: dict[str, str] = {"^VIX": "VIX", "ES=F": "S&P 500 futures", "NQ=F": "Nasdaq-100 futures"}
    poll_minutes: dict[Literal["regular", "extended", "closed"], float] = {"regular": 5, "extended": 10, "closed": 30}
    extended_fallback_minutes: float = 10
    persist_seconds: float = 60
    save_bars: bool = False
    bar_retention_days: int = 10
    news_retention_days: int = Field(90, ge=1)
    report_retention_days: int = Field(60, ge=1)
    log_max_mb: float = Field(5, gt=0)


class TimeWindow(_Strict):
    start: str = Field(pattern=r"^\d{2}:\d{2}$")
    end: str = Field(pattern=r"^\d{2}:\d{2}$")
    timezone: str


class AlertsCfg(_Strict):
    mode: Literal["shadow", "live"] = "shadow"
    check_seconds: float = 30
    stock_window_minutes: float = 15
    stock_window_pct: float = 3.0
    stock_window_sigma: float = 4.0
    stock_day_pct: float = 5.0
    stock_day_atr: float = 2.5
    ext_pct: float = 5.0
    ext_atr: float = 2.0
    index_symbols: list[str] = ["SPY", "QQQ"]
    index_window_pct: float = 1.2
    index_day_pct: float = 2.5
    vix_jump_pct: float = 20
    futures_pct: float = 2.0
    crash_pct: float = 3.0
    quiet_hours: TimeWindow = TimeWindow(start="23:00", end="07:00", timezone="Europe/Prague")
    cooldown_minutes: float = 60
    escalation_factor: float = 1.5
    daily_budget: int = 6


class TriageCfg(_Strict):
    enabled: bool = True
    min_interval_minutes: float = 3
    max_calls_per_hour: int = 6
    max_calls_per_day: int = 40
    max_items_per_call: int = 10
    alert_min_impact: int = Field(4, ge=1, le=5)
    follow_up_minutes: list[float] = [5, 15, 30, 60]


class TelegramCfg(_Strict):
    timeout_s: float = 20


class Settings(_Strict):
    paths: Paths = Paths()
    data: DataCfg = DataCfg()
    news: NewsCfg = NewsCfg()
    llm: LLMCfg = LLMCfg()
    screen: ScreenCfg = ScreenCfg()
    schedule: ScheduleCfg = ScheduleCfg()
    report: ReportCfg = ReportCfg()
    evaluate: EvaluateCfg = EvaluateCfg()
    telegram: TelegramCfg = TelegramCfg()
    digests: DigestsCfg = DigestsCfg()
    news_watch: NewsWatchCfg = NewsWatchCfg()
    watch: WatchCfg = WatchCfg()
    alerts: AlertsCfg = AlertsCfg()
    triage: TriageCfg = TriageCfg()


class Ticker(_Strict):
    symbol: str
    type: Literal["stock"] = "stock"
    name: str | None = None
    aliases: list[str] = []
    news_exclude: list[str] = []
    sector: str | None = None
    notes: str | None = None
    held: bool = False
    cost_basis: float | None = None

    @field_validator("symbol")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.strip().upper()


class _TickerFile(_Strict):
    tickers: list[Ticker]


def load_settings(path: Path = SETTINGS_PATH) -> Settings:
    with open(path) as f:
        return Settings.model_validate(yaml.safe_load(f) or {})


def load_tickers(path: Path, only: list[str] | None = None) -> list[Ticker]:
    with open(path) as f:
        tickers = _TickerFile.model_validate(yaml.safe_load(f)).tickers
    symbols = [t.symbol for t in tickers]
    if len(symbols) != len(set(symbols)):
        raise ValueError(f"duplicate symbols in {path}")
    if only:
        wanted = {s.strip().upper() for s in only}
        unknown = wanted - set(symbols)
        if unknown:
            raise ValueError(f"not in {path.name}: {', '.join(sorted(unknown))}")
        tickers = [t for t in tickers if t.symbol in wanted]
    return tickers
