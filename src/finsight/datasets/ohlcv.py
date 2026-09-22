"""OHLCV (open/high/low/close/volume) price bars."""

from __future__ import annotations

from typing import ClassVar

import pandas as pd
import pyarrow as pa

from finsight.datasets.base import ConformResult, Dataset
from finsight.models import FetchRequest, earliest_bar_date
from finsight.quality import Check, Severity, not_null, rule, unique

PRICE_COLUMNS = ["open", "high", "low", "close"]

#: Columns every provider must return for OHLCV, in order.
PROVIDER_COLUMNS = ["ts", "date", "open", "high", "low", "close", "adj_close", "volume"]

# Relative tolerance for OHLC consistency checks; absorbs float rounding in
# adjusted series without hiding genuinely broken bars.
_OHLC_TOLERANCE = 1e-6


class OHLCVDataset(Dataset):
    name = "ohlcv"
    title = "OHLCV price bars"
    description = "Open, high, low, close, adjusted close, and volume per symbol and interval."

    provider_schema = pa.schema(
        [
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("date", pa.date32()),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("adj_close", pa.float64()),
            pa.field("volume", pa.int64()),
        ]
    )
    dimensions = ("interval", "adjustment")
    schema = pa.schema(
        [
            pa.field("symbol", pa.string()),
            pa.field("date", pa.date32()),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("open", pa.float64()),
            pa.field("high", pa.float64()),
            pa.field("low", pa.float64()),
            pa.field("close", pa.float64()),
            pa.field("adj_close", pa.float64()),
            pa.field("volume", pa.int64()),
            pa.field("interval", pa.string()),
            pa.field("adjustment", pa.string()),
            pa.field("provider", pa.string()),
            pa.field("run_id", pa.string()),
            pa.field("ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    primary_key = ("provider", "symbol", "interval", "adjustment", "ts")
    partition_by = ("provider", "interval", "adjustment", "symbol")
    order_by = ("symbol", "ts")
    column_docs: ClassVar[dict[str, str]] = {
        "symbol": "Ticker symbol in provider notation (e.g. AAPL, BRK-B, ^GSPC).",
        "date": "Trading date in the exchange's local timezone.",
        "ts": "Bar start time in UTC. For daily bars this is local midnight.",
        "open": "Opening price.",
        "high": "Highest price.",
        "low": "Lowest price.",
        "close": "Closing price.",
        "adj_close": "Close adjusted for splits and dividends.",
        "volume": "Shares traded (as-traded for raw, split-adjusted for adjusted).",
        "interval": "Bar size: 1m, 5m, 15m, 30m, 1h, 1d, 1wk, 1mo.",
        "adjustment": "raw (as traded) or adjusted (split- and dividend-adjusted OHLC).",
        "provider": "Provider the bar came from.",
        "run_id": "Run that last wrote this bar.",
        "ingested_at": "When the bar was fetched from the provider (UTC).",
    }

    def prepare_provider_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        frame["ts"] = pd.to_datetime(frame["ts"], utc=True).dt.floor("us").dt.as_unit("us")
        for column in [*PRICE_COLUMNS, "adj_close"]:
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float64")
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").round().astype("Int64")
        return frame

    def conform(self, bronze: pd.DataFrame) -> ConformResult:
        input_rows = len(bronze)
        frame = bronze.copy()

        # Providers sometimes emit placeholder rows (holidays, halted sessions)
        # with no prices at all. They carry no information, so drop them.
        empty = frame[PRICE_COLUMNS].isna().all(axis=1)
        frame = frame.loc[~empty]

        # Keep the most recently fetched version of each bar.
        frame = frame.sort_values("fetched_at", kind="stable")
        duplicates = frame.duplicated(subset=list(self.primary_key), keep="last")
        frame = frame.loc[~duplicates]

        frame = frame.rename(columns={"fetched_at": "ingested_at"})
        frame = frame.sort_values(list(self.order_by), kind="stable").reset_index(drop=True)
        return ConformResult(
            frame=frame[self.schema.names],
            input_rows=input_rows,
            dropped_rows=int(empty.sum()),
            duplicate_rows=int(duplicates.sum()),
        )

    def checks(self, request: FetchRequest) -> list[Check]:
        # Weekly and monthly bars are labelled with the period start, which can
        # precede the requested start date.
        earliest = earliest_bar_date(request.start, request.interval)
        latest = request.end

        def outside_window(df: pd.DataFrame) -> pd.Series:
            dates = pd.to_datetime(df["date"]).dt.date
            return (dates < earliest) | (dates > latest)

        def outside_range(df: pd.DataFrame) -> pd.Series:
            tol = df["high"].abs() * _OHLC_TOLERANCE
            above = (df[["open", "close"]].max(axis=1) - df["high"]) > tol
            below = (df["low"] - df[["open", "close"]].min(axis=1)) > tol
            return above | below

        return [
            not_null(["symbol", "date", "ts", "close"]),
            unique(self.primary_key),
            rule(
                "positive_prices",
                "open, high, low, and close must be greater than zero",
                lambda df: (df[PRICE_COLUMNS] <= 0).any(axis=1),
            ),
            rule(
                "non_negative_volume",
                "volume must not be negative",
                lambda df: df["volume"] < 0,
            ),
            rule(
                "within_requested_window",
                f"date must fall between {earliest} and {latest}",
                outside_window,
            ),
            rule(
                "high_gte_low",
                "high must be greater than or equal to low",
                lambda df: df["high"] < df["low"],
                severity=Severity.WARNING,
            ),
            rule(
                "open_close_within_range",
                "open and close must lie between low and high",
                outside_range,
                severity=Severity.WARNING,
            ),
        ]
