"""Yahoo provider tests with yfinance mocked out (no network)."""

from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd
import pytest
import yfinance as yf

from finsight.errors import (
    ProviderError,
    RateLimitError,
    SymbolNotFoundError,
    TransientProviderError,
)
from finsight.models import Adjustment, Interval, SymbolRequest
from finsight.providers.yahoo import YahooProvider, _chunks, unadjust_splits

NY = "America/New_York"


def history_frame(
    days: list[str], close: list[float], adj: list[float] | None = None
) -> pd.DataFrame:
    index = pd.DatetimeIndex(pd.to_datetime(days)).tz_localize(NY)
    index.name = "Date"
    frame = pd.DataFrame(
        {
            "Open": close,
            "High": [c + 1 for c in close],
            "Low": [c - 1 for c in close],
            "Close": close,
            "Volume": [4_000] * len(close),
        },
        index=index,
    )
    if adj is not None:
        frame["Adj Close"] = adj
    return frame


class FakeTicker:
    def __init__(self, history: Any, splits: pd.Series | None = None):
        self._history = history
        self.splits = splits if splits is not None else pd.Series(dtype=float)
        self.calls: list[dict[str, Any]] = []

    def history(self, **kwargs: Any) -> pd.DataFrame:
        self.calls.append(kwargs)
        if isinstance(self._history, Exception):
            raise self._history
        if callable(self._history):
            return self._history(**kwargs)
        return self._history


def request(
    interval: Interval = Interval.D1,
    adjustment: Adjustment = Adjustment.RAW,
    start: date = date(2020, 8, 27),
    end: date = date(2020, 9, 1),
) -> SymbolRequest:
    return SymbolRequest("ohlcv", "AAPL", start, end, interval, adjustment)


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch):
    instance = YahooProvider()

    def install(ticker: FakeTicker) -> YahooProvider:
        monkeypatch.setattr(instance._yf, "Ticker", lambda symbol: ticker)
        return instance

    return install


def test_raw_mode_reverses_split_adjustment(provider) -> None:
    # Yahoo's Close is split-adjusted: pre-split bars appear at post-split scale.
    history = history_frame(
        ["2020-08-27", "2020-08-28", "2020-08-31", "2020-09-01"],
        close=[125.0, 124.8, 129.0, 134.2],
        adj=[121.0, 120.9, 125.0, 130.0],
    )
    splits = pd.Series([4.0], index=pd.DatetimeIndex([pd.Timestamp("2020-08-31 09:30", tz=NY)]))
    ticker = FakeTicker(history, splits)

    bars = provider(ticker).fetch(request())

    assert list(bars["close"]) == [500.0, 499.2, 129.0, 134.2]
    assert list(bars["volume"]) == [1_000, 1_000, 4_000, 4_000]
    assert list(bars["adj_close"]) == [121.0, 120.9, 125.0, 130.0]
    assert ticker.calls[0]["auto_adjust"] is False
    assert ticker.calls[0]["end"] == "2020-09-02"  # inclusive end -> exclusive for yfinance
    assert str(bars["ts"].dt.tz) == "UTC"
    assert bars["date"].tolist()[0] == date(2020, 8, 27)


def test_adjusted_mode_uses_auto_adjust(provider) -> None:
    history = history_frame(["2020-08-27", "2020-08-28"], close=[121.0, 120.9])
    ticker = FakeTicker(history)
    bars = provider(ticker).fetch(request(adjustment=Adjustment.ADJUSTED))
    assert ticker.calls[0]["auto_adjust"] is True
    assert list(bars["adj_close"]) == list(bars["close"])


def test_trims_rows_outside_window(provider) -> None:
    history = history_frame(["2020-08-26", "2020-08-27", "2020-09-02"], close=[1.0, 2.0, 3.0])
    bars = provider(FakeTicker(history)).fetch(request(adjustment=Adjustment.ADJUSTED))
    assert bars["date"].tolist() == [date(2020, 8, 27)]


def test_intraday_1m_requests_are_chunked(provider) -> None:
    def history(**kwargs: Any) -> pd.DataFrame:
        return history_frame([kwargs["start"]], close=[10.0])

    ticker = FakeTicker(history)
    bars = provider(ticker).fetch(
        request(Interval.M1, Adjustment.ADJUSTED, date(2024, 1, 1), date(2024, 1, 20))
    )
    assert [c["start"] for c in ticker.calls] == ["2024-01-01", "2024-01-08", "2024-01-15"]
    assert len(bars) == 3


def test_chunks_cover_window_without_overlap() -> None:
    chunks = _chunks(date(2024, 1, 1), date(2024, 1, 10), 4)
    assert chunks == [
        (date(2024, 1, 1), date(2024, 1, 4)),
        (date(2024, 1, 5), date(2024, 1, 8)),
        (date(2024, 1, 9), date(2024, 1, 10)),
    ]
    assert _chunks(date(2024, 1, 1), date(2024, 1, 10), None) == [
        (date(2024, 1, 1), date(2024, 1, 10))
    ]


def test_missing_prices_returns_empty_frame(provider) -> None:
    ticker = FakeTicker(yf.exceptions.YFPricesMissingError("AAPL", "no data"))
    assert provider(ticker).fetch(request()).empty


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (yf.exceptions.YFRateLimitError(), RateLimitError),
        (yf.exceptions.YFTickerMissingError("AAPL", "delisted"), SymbolNotFoundError),
        (ConnectionError("reset"), TransientProviderError),
        (RuntimeError("HTTP Error 404: Not Found"), SymbolNotFoundError),
        (RuntimeError("boom"), ProviderError),
    ],
)
def test_error_translation(provider, error: Exception, expected: type[Exception]) -> None:
    with pytest.raises(expected) as info:
        provider(FakeTicker(error)).fetch(request())
    assert info.type is expected


def test_unadjust_splits_handles_multiple_splits() -> None:
    frame = pd.DataFrame(
        {
            "date": [date(2020, 1, 1), date(2021, 1, 1), date(2022, 1, 1)],
            "open": [10.0, 10.0, 10.0],
            "high": [10.0, 10.0, 10.0],
            "low": [10.0, 10.0, 10.0],
            "close": [10.0, 10.0, 10.0],
            "volume": [600, 600, 600],
        }
    )
    splits = [(date(2020, 6, 1), 2.0), (date(2021, 6, 1), 3.0)]
    out = unadjust_splits(frame, splits)
    assert out["close"].tolist() == [60.0, 30.0, 10.0]
    assert out["volume"].tolist() == [100, 200, 600]
