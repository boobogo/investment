from pathlib import Path

import pandas as pd
import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def load_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(FIXTURES / name, parse_dates=["date"], index_col="date")


@pytest.fixture
def aapl() -> pd.DataFrame:
    return load_csv("aapl_daily.csv")


@pytest.fixture
def spy() -> pd.DataFrame:
    return load_csv("spy_daily.csv")


def make_ohlcv(closes, spread=2.0, volume=1_000_000) -> pd.DataFrame:
    """Synthetic bars: high/low = close +/- spread/2, open = close."""
    closes = pd.Series(closes, dtype=float)
    idx = pd.bdate_range("2024-01-01", periods=len(closes))
    return pd.DataFrame(
        {
            "open": closes.values,
            "high": closes.values + spread / 2,
            "low": closes.values - spread / 2,
            "close": closes.values,
            "volume": float(volume),
        },
        index=idx,
    )
