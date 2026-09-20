from __future__ import annotations

from collections.abc import Callable, Iterator
from pathlib import Path

import pandas as pd
import pytest

from finsight.config import Settings
from finsight.models import Adjustment, FetchRequest, Interval, SymbolRequest
from finsight.pipeline import Pipeline
from finsight.providers import DatasetSupport, IntervalSupport, Provider, register_provider
from finsight.providers.registry import unregister_provider
from finsight.storage import Storage


@pytest.fixture(autouse=True)
def _isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Keep tests independent of the developer's environment and .env file."""
    for name in list(__import__("os").environ):
        if name.startswith(("FINSIGHT_", "SNOWFLAKE_")):
            monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FINSIGHT_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", max_workers=2, max_retries=2)


@pytest.fixture
def pipeline(settings: Settings) -> Pipeline:
    return Pipeline(settings, retry_backoff=0)


@pytest.fixture
def storage(settings: Settings) -> Storage:
    return Storage.from_settings(settings)


def make_request(
    symbols: str | list[str] = "AAPL,MSFT",
    start: str = "2024-01-01",
    end: str = "2024-01-31",
    *,
    provider: str = "synthetic",
    interval: Interval | str = Interval.D1,
    adjustment: Adjustment | str = Adjustment.RAW,
) -> FetchRequest:
    return FetchRequest.create(
        symbols, start, end, provider=provider, interval=interval, adjustment=adjustment
    )


class StubProvider(Provider):
    """Provider whose behaviour is scripted per test."""

    name = "stub"
    title = "Stub"
    description = "Scripted provider for tests."

    def __init__(self, handler: Callable[[SymbolRequest], pd.DataFrame]):
        self.handler = handler
        self.calls: list[SymbolRequest] = []

    @property
    def capabilities(self) -> tuple[DatasetSupport, ...]:
        return (
            DatasetSupport(
                dataset="ohlcv",
                intervals=(IntervalSupport(Interval.D1), IntervalSupport(Interval.M5, 59)),
                adjustments=(Adjustment.RAW,),
            ),
        )

    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        self.calls.append(request)
        return self.handler(request)


def bars(dates: list[str], **overrides: list[float]) -> pd.DataFrame:
    """Build a provider-contract OHLCV frame for the given dates."""
    count = len(dates)
    stamps = pd.to_datetime(dates).tz_localize("America/New_York")
    frame = pd.DataFrame(
        {
            "ts": stamps.tz_convert("UTC"),
            "date": [ts.date() for ts in stamps],
            "open": [100.0 + i for i in range(count)],
            "high": [102.0 + i for i in range(count)],
            "low": [99.0 + i for i in range(count)],
            "close": [101.0 + i for i in range(count)],
            "adj_close": [100.5 + i for i in range(count)],
            "volume": [1_000 * (i + 1) for i in range(count)],
        }
    )
    for column, values in overrides.items():
        frame[column] = values
    return frame


@pytest.fixture
def stub_provider() -> Iterator[Callable[[Callable[[SymbolRequest], pd.DataFrame]], StubProvider]]:
    """Register a scripted provider under the name 'stub' for one test."""

    def install(handler: Callable[[SymbolRequest], pd.DataFrame]) -> StubProvider:
        provider = StubProvider(handler)
        register_provider("stub", provider, replace=True)
        return provider

    yield install
    unregister_provider("stub")
