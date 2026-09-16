"""Provider interface.

A provider adapts one upstream data source (an API, a library, a file drop)
to FinSight's dataset contracts. Providers declare what they support up
front via :attr:`Provider.capabilities`; requests are checked against those
declarations *before* any network call, so unsupported combinations fail
fast with a clear message instead of an empty result.

To add a provider, subclass :class:`Provider`, implement ``capabilities`` and
``fetch``, and register it (see :mod:`finsight.providers.registry`).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date, timedelta
from typing import ClassVar

import pandas as pd

from finsight.models import Adjustment, FetchRequest, Interval, SymbolRequest, today_utc


@dataclass(frozen=True)
class IntervalSupport:
    """Limits a provider imposes on one interval.

    Attributes:
        interval: The supported bar size.
        max_lookback_days: How far back from today data is available, or
            ``None`` for full history.
        max_span_days: Longest window the upstream API serves per call. The
            provider is responsible for splitting larger windows into chunks;
            this is informational for users.
    """

    interval: Interval
    max_lookback_days: int | None = None
    max_span_days: int | None = None


@dataclass(frozen=True)
class DatasetSupport:
    """What a provider can serve for one dataset."""

    dataset: str
    intervals: tuple[IntervalSupport, ...]
    adjustments: tuple[Adjustment, ...]

    def interval_support(self, interval: Interval) -> IntervalSupport | None:
        return next((s for s in self.intervals if s.interval == interval), None)


class Provider(ABC):
    """Base class for market data providers."""

    #: Registry key, used in requests and storage paths. Lower-case, no spaces.
    name: ClassVar[str]
    #: Human-readable name.
    title: ClassVar[str]
    #: One-line description shown by ``finsight providers``.
    description: ClassVar[str]
    #: Link to the upstream source.
    homepage: ClassVar[str | None] = None
    #: Short note on usage terms users must respect.
    terms: ClassVar[str | None] = None

    @property
    @abstractmethod
    def capabilities(self) -> tuple[DatasetSupport, ...]:
        """Datasets, intervals, and adjustments this provider supports."""

    @abstractmethod
    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        """Return data for one symbol, shaped to the dataset's provider contract.

        Implementations must return an empty frame (not raise) when the symbol
        exists but has no data in the window, raise
        :class:`~finsight.errors.SymbolNotFoundError` for unknown symbols, and
        raise :class:`~finsight.errors.TransientProviderError` (or a subclass)
        for failures worth retrying.
        """

    def version(self) -> str | None:
        """Version of the upstream client library, recorded in run manifests."""
        return None

    def support_for(self, dataset: str) -> DatasetSupport | None:
        return next((s for s in self.capabilities if s.dataset == dataset), None)

    def check(self, request: FetchRequest, today: date | None = None) -> list[str]:
        """Return every reason ``request`` cannot be served (empty if it can)."""
        support = self.support_for(request.dataset)
        if support is None:
            offered = ", ".join(s.dataset for s in self.capabilities)
            return [
                f"provider {self.name!r} does not offer dataset {request.dataset!r} "
                f"(offers: {offered})"
            ]

        problems: list[str] = []
        interval = support.interval_support(request.interval)
        if interval is None:
            offered = ", ".join(s.interval.value for s in support.intervals)
            problems.append(
                f"provider {self.name!r} does not support interval "
                f"{request.interval.value!r} for {request.dataset} (supported: {offered})"
            )
        elif interval.max_lookback_days is not None:
            earliest = (today or today_utc()) - timedelta(days=interval.max_lookback_days)
            if request.start < earliest:
                problems.append(
                    f"{request.interval.value} data from {self.name!r} only covers the last "
                    f"{interval.max_lookback_days} days; start must be on or after {earliest}"
                )

        if request.adjustment not in support.adjustments:
            offered = ", ".join(a.value for a in support.adjustments)
            problems.append(
                f"provider {self.name!r} does not support adjustment "
                f"{request.adjustment.value!r} (supported: {offered})"
            )
        return problems
