from datetime import datetime, timedelta, timezone

import pandas as pd

from pipeline.markets import Quote


def utc(s: str) -> datetime:
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def bars(start: str, values, minutes: int = 5) -> pd.Series:
    """5-minute closes from `start` (UTC), one per value."""
    idx = pd.date_range(utc(start), periods=len(values), freq=f"{minutes}min")
    return pd.Series([float(v) for v in values], index=idx)


def quote(symbol="ES=F", name="S&P 500 fut", group="US stocks", price=100.0, day=0.0, hour=0.0, sigma=1.0,
          live=True, at="2026-10-23T14:00") -> Quote:  # fmt: skip
    ref = price / (1 + day / 100) if day is not None else None
    return Quote(symbol, name, group, price, utc(at) - timedelta(minutes=5), day, hour, ref, sigma, live)
