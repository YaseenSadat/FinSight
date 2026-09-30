"""Core request types shared by providers, datasets, and the pipeline."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import Any

from finsight.errors import RequestError
from finsight.symbols import parse_symbols


class Interval(StrEnum):
    """Bar size for time-series datasets."""

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    D1 = "1d"
    W1 = "1wk"
    MO1 = "1mo"

    @property
    def is_intraday(self) -> bool:
        return self in _INTRADAY

    @property
    def period(self) -> timedelta:
        """Approximate length of one bar, used for window tolerance checks."""
        return _PERIODS[self]


_INTRADAY = {Interval.M1, Interval.M5, Interval.M15, Interval.M30, Interval.H1}
_PERIODS = {
    Interval.M1: timedelta(minutes=1),
    Interval.M5: timedelta(minutes=5),
    Interval.M15: timedelta(minutes=15),
    Interval.M30: timedelta(minutes=30),
    Interval.H1: timedelta(hours=1),
    Interval.D1: timedelta(days=1),
    Interval.W1: timedelta(days=7),
    Interval.MO1: timedelta(days=31),
}


class Adjustment(StrEnum):
    """How prices are adjusted for corporate actions.

    ``raw``: open/high/low/close/volume as traded, not adjusted for splits or
    dividends. ``adj_close`` carries the split- and dividend-adjusted close.

    ``adjusted``: open/high/low/close adjusted for splits and dividends, so
    the series is continuous for return calculations. ``adj_close == close``.
    """

    RAW = "raw"
    ADJUSTED = "adjusted"


def today_utc() -> date:
    return datetime.now(UTC).date()


def earliest_bar_date(start: date, interval: Interval) -> date:
    """Earliest bar label that still overlaps a window beginning at ``start``.

    Weekly and monthly bars are labelled with the period's first day, so the
    bar covering ``start`` can carry an earlier date.
    """
    if interval == Interval.W1:
        return start - timedelta(days=start.weekday())
    if interval == Interval.MO1:
        return start.replace(day=1)
    return start


def _coerce_date(value: date | datetime | str, name: str, problems: list[str]) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value).strip())
    except ValueError:
        problems.append(f"{name} must be a date in YYYY-MM-DD format, got {value!r}")
        return None


def _coerce_enum(enum: type[StrEnum], value: Any, name: str, problems: list[str]) -> Any:
    try:
        return enum(str(value).strip().lower() if isinstance(value, str) else value)
    except ValueError:
        allowed = ", ".join(member.value for member in enum)
        problems.append(f"{name} must be one of {allowed}, got {value!r}")
        return None


@dataclass(frozen=True)
class FetchRequest:
    """A request for one dataset, from one provider, for many symbols.

    ``start`` and ``end`` are inclusive calendar dates. Build instances with
    :meth:`create`, which normalises input and reports every problem at once.
    """

    dataset: str
    provider: str
    symbols: tuple[str, ...]
    start: date
    end: date
    interval: Interval
    adjustment: Adjustment

    @classmethod
    def create(
        cls,
        symbols: str | Iterable[str],
        start: date | datetime | str,
        end: date | datetime | str | None = None,
        *,
        interval: Interval | str = Interval.D1,
        adjustment: Adjustment | str = Adjustment.RAW,
        provider: str = "yahoo",
        dataset: str = "ohlcv",
    ) -> FetchRequest:
        """Validate and normalise user input into a request.

        ``end`` defaults to today (UTC) and is clamped to today if it lies in
        the future, since no provider can return bars that do not exist yet.

        Raises:
            RequestError: listing every problem with the input.
        """
        problems: list[str] = []

        try:
            parsed_symbols = parse_symbols(symbols)
        except ValueError as exc:
            problems.append(str(exc))
            parsed_symbols = []
        if not parsed_symbols and not problems:
            problems.append("at least one symbol is required")

        today = today_utc()
        start_date = _coerce_date(start, "start", problems)
        end_date = today if end is None else _coerce_date(end, "end", problems)
        if end_date and end_date > today:
            end_date = today
        if start_date and end_date and start_date > end_date:
            problems.append(f"start ({start_date}) must be on or before end ({end_date})")

        parsed_interval = _coerce_enum(Interval, interval, "interval", problems)
        parsed_adjustment = _coerce_enum(Adjustment, adjustment, "adjustment", problems)

        if problems:
            raise RequestError(problems)
        assert start_date is not None  # narrowed by the problem checks above
        assert end_date is not None
        return cls(
            dataset=dataset.strip().lower(),
            provider=provider.strip().lower(),
            symbols=tuple(parsed_symbols),
            start=start_date,
            end=end_date,
            interval=parsed_interval,
            adjustment=parsed_adjustment,
        )

    def for_symbol(self, symbol: str) -> SymbolRequest:
        return SymbolRequest(
            dataset=self.dataset,
            symbol=symbol,
            start=self.start,
            end=self.end,
            interval=self.interval,
            adjustment=self.adjustment,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataset": self.dataset,
            "provider": self.provider,
            "symbols": list(self.symbols),
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "interval": self.interval.value,
            "adjustment": self.adjustment.value,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FetchRequest:
        """Rebuild a request serialised with :meth:`to_dict` (no re-validation)."""
        return cls(
            dataset=data["dataset"],
            provider=data["provider"],
            symbols=tuple(data["symbols"]),
            start=date.fromisoformat(data["start"]),
            end=date.fromisoformat(data["end"]),
            interval=Interval(data["interval"]),
            adjustment=Adjustment(data["adjustment"]),
        )


@dataclass(frozen=True)
class SymbolRequest:
    """The slice of a :class:`FetchRequest` a provider serves in one call."""

    dataset: str
    symbol: str
    start: date
    end: date
    interval: Interval
    adjustment: Adjustment

    @property
    def earliest_bar_date(self) -> date:
        return earliest_bar_date(self.start, self.interval)
