"""Deterministic synthetic data provider.

Generates realistic-looking OHLCV bars from a seeded random walk, with no
network access. The same symbol and window always produce the same bars, so
it is useful for demos, offline development, and tests. The data is fake and
must never be used for analysis.
"""

from __future__ import annotations

import zlib
from datetime import date, datetime, time, timedelta

import numpy as np
import pandas as pd

from finsight.datasets.ohlcv import PROVIDER_COLUMNS
from finsight.errors import SymbolNotFoundError
from finsight.models import Adjustment, Interval, SymbolRequest, earliest_bar_date
from finsight.providers.base import DatasetSupport, IntervalSupport, Provider

_EXCHANGE_TZ = "America/New_York"
_SESSION_OPEN = time(9, 30)
_SESSION_CLOSE = time(16, 0)
_EPOCH = date(2000, 1, 3)


class SyntheticProvider(Provider):
    """Seeded random-walk OHLCV for offline use. Not real market data."""

    name = "synthetic"
    title = "Synthetic (offline)"
    description = "Deterministic fake OHLCV for demos and tests. No network, not real data."
    terms = "Generated locally. Not market data."

    def __init__(self, missing_symbols: frozenset[str] | set[str] = frozenset({"NOTFOUND"})):
        #: Symbols that behave like unknown tickers (useful for exercising error paths).
        self.missing_symbols = frozenset(missing_symbols)

    @property
    def capabilities(self) -> tuple[DatasetSupport, ...]:
        return (
            DatasetSupport(
                dataset="ohlcv",
                intervals=tuple(IntervalSupport(interval) for interval in Interval),
                adjustments=(Adjustment.RAW, Adjustment.ADJUSTED),
            ),
        )

    def version(self) -> str | None:
        return "1"

    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        if request.symbol in self.missing_symbols:
            raise SymbolNotFoundError(f"symbol not found: {request.symbol}", symbol=request.symbol)

        stamps = _timestamps(request.start, request.end, request.interval)
        if len(stamps) == 0:
            return pd.DataFrame(columns=PROVIDER_COLUMNS)

        # Seed from the symbol and the absolute bar position so overlapping
        # requests return identical values for the bars they share.
        seed = zlib.crc32(f"{request.symbol}|{request.interval.value}".encode())
        base_price = 20 + seed % 480
        steps = _steps_since_epoch(stamps, request.interval)
        days = np.asarray((stamps - pd.Timestamp(_EPOCH, tz="UTC")) / timedelta(days=1))
        rng_offsets = np.array([_unit_noise(seed, int(step)) for step in steps])

        drift = 0.0003 * days
        wave = 0.15 * np.sin(days / 40.0)
        close = base_price * np.exp(drift + wave + 0.02 * rng_offsets)
        open_ = close * (1 + 0.004 * np.array([_unit_noise(seed + 1, int(s)) for s in steps]))
        spread = np.abs(close * 0.01 * np.array([_unit_noise(seed + 2, int(s)) for s in steps]))
        high = np.maximum(open_, close) + spread
        low = np.minimum(open_, close) - spread
        volume = (1_000_000 + (seed % 5_000_000) * (1 + np.abs(rng_offsets))).astype("int64")

        local = stamps.tz_convert(_EXCHANGE_TZ)
        return pd.DataFrame(
            {
                "ts": stamps.tz_convert("UTC").as_unit("us"),
                "date": [ts.date() for ts in local],
                "open": open_.round(4),
                "high": high.round(4),
                "low": low.round(4),
                "close": close.round(4),
                "adj_close": close.round(4),
                "volume": volume,
            }
        )


def _unit_noise(seed: int, step: int) -> float:
    """Deterministic value in [-1, 1) for a (seed, step) pair."""
    return (zlib.crc32(f"{seed}:{step}".encode()) / 2**32) * 2 - 1


def _timestamps(start: date, end: date, interval: Interval) -> pd.DatetimeIndex:
    first = earliest_bar_date(start, interval)
    if interval == Interval.D1:
        stamps = pd.bdate_range(start, end)
    elif interval == Interval.W1:
        stamps = pd.date_range(first, end, freq="W-MON")
    elif interval == Interval.MO1:
        stamps = pd.date_range(first, end, freq="MS")
    else:
        freq = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "1h": "60min"}
        sessions = [
            pd.date_range(
                datetime.combine(d.date(), _SESSION_OPEN),
                datetime.combine(d.date(), _SESSION_CLOSE),
                freq=freq[interval.value],
                inclusive="left",
            )
            for d in pd.bdate_range(start, end)
        ]
        if not sessions:
            return pd.DatetimeIndex([], tz="UTC")
        intraday = pd.DatetimeIndex(sessions[0].append(sessions[1:]))
        return intraday.tz_localize(_EXCHANGE_TZ).tz_convert("UTC")
    return pd.DatetimeIndex(stamps).tz_localize(_EXCHANGE_TZ).tz_convert("UTC")


def _steps_since_epoch(stamps: pd.DatetimeIndex, interval: Interval) -> np.ndarray:
    epoch = pd.Timestamp(_EPOCH, tz="UTC")
    unit = interval.period if interval.is_intraday else timedelta(days=1)
    return np.asarray((stamps - epoch) // unit, dtype="int64")
