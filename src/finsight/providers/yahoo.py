"""Yahoo Finance provider, backed by the ``yfinance`` library.

Price adjustment
----------------
Yahoo's ``Close`` series is already adjusted for stock splits, and ``Adj Close``
is adjusted for splits *and* dividends. To serve true as-traded prices for
``adjustment="raw"``, this provider multiplies each bar by the cumulative
ratio of every split that happened after it (and divides volume by the same
factor), using the symbol's full split history. ``adj_close`` keeps Yahoo's
fully adjusted close.

For ``adjustment="adjusted"``, yfinance's ``auto_adjust`` mode is used so that
open/high/low/close are all split- and dividend-adjusted.

Usage terms
-----------
yfinance is an unofficial client for Yahoo's public endpoints. Yahoo's terms
permit personal use only; do not redistribute data fetched through it.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any

import pandas as pd

from finsight.datasets.ohlcv import PROVIDER_COLUMNS
from finsight.errors import (
    ProviderError,
    RateLimitError,
    SymbolNotFoundError,
    TransientProviderError,
)
from finsight.models import Adjustment, Interval, SymbolRequest
from finsight.providers.base import DatasetSupport, IntervalSupport, Provider

logger = logging.getLogger(__name__)

_COLUMN_MAP = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}
_PRICE_COLUMNS = ["open", "high", "low", "close"]


class YahooProvider(Provider):
    """Historical OHLCV bars from Yahoo Finance."""

    name = "yahoo"
    title = "Yahoo Finance"
    description = "Free historical OHLCV for global equities, ETFs, indices, FX, and crypto."
    homepage = "https://finance.yahoo.com"
    terms = (
        "Unofficial access via yfinance. Yahoo permits personal use only; "
        "do not redistribute fetched data."
    )

    # Yahoo's documented limits, with a one-day safety margin on lookbacks.
    _capabilities = (
        DatasetSupport(
            dataset="ohlcv",
            intervals=(
                IntervalSupport(Interval.M1, max_lookback_days=29, max_span_days=7),
                IntervalSupport(Interval.M5, max_lookback_days=59),
                IntervalSupport(Interval.M15, max_lookback_days=59),
                IntervalSupport(Interval.M30, max_lookback_days=59),
                IntervalSupport(Interval.H1, max_lookback_days=729),
                IntervalSupport(Interval.D1),
                IntervalSupport(Interval.W1),
                IntervalSupport(Interval.MO1),
            ),
            adjustments=(Adjustment.RAW, Adjustment.ADJUSTED),
        ),
    )

    def __init__(self) -> None:
        import yfinance as yf

        self._yf = yf
        # Raise typed exceptions instead of logging and returning empty frames,
        # so "unknown symbol" and "no data in window" can be told apart.
        yf.config.debug.hide_exceptions = False
        logging.getLogger("yfinance").setLevel(logging.CRITICAL)

    @property
    def capabilities(self) -> tuple[DatasetSupport, ...]:
        return self._capabilities

    def version(self) -> str | None:
        return str(getattr(self._yf, "__version__", "unknown"))

    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        if request.dataset != "ohlcv":
            raise ProviderError(f"unsupported dataset {request.dataset!r}", symbol=request.symbol)

        support = self._capabilities[0].interval_support(request.interval)
        span = support.max_span_days if support else None
        ticker = self._yf.Ticker(request.symbol)

        frames = [
            self._history(ticker, request, chunk_start, chunk_end)
            for chunk_start, chunk_end in _chunks(request.start, request.end, span)
        ]
        frames = [frame for frame in frames if not frame.empty]
        if not frames:
            return pd.DataFrame(columns=PROVIDER_COLUMNS)

        history = pd.concat(frames)
        history = history[~history.index.duplicated(keep="last")].sort_index()
        bars = _to_contract(history)

        if request.adjustment == Adjustment.RAW:
            splits = self._call(request.symbol, lambda: ticker.splits)
            bars = unadjust_splits(bars, _split_events(splits))

        in_window = (bars["date"] >= request.earliest_bar_date) & (bars["date"] <= request.end)
        return bars.loc[in_window, PROVIDER_COLUMNS].reset_index(drop=True)

    def _history(self, ticker: Any, request: SymbolRequest, start: date, end: date) -> pd.DataFrame:
        def call() -> pd.DataFrame:
            return ticker.history(
                start=start.isoformat(),
                end=(end + timedelta(days=1)).isoformat(),  # yfinance's end is exclusive
                interval=request.interval.value,
                auto_adjust=request.adjustment == Adjustment.ADJUSTED,
                actions=False,
            )

        return self._call(request.symbol, call)

    def _call(self, symbol: str, fn: Any) -> Any:
        """Invoke a yfinance call and translate its exceptions."""
        exceptions = self._yf.exceptions
        try:
            return fn()
        except exceptions.YFPricesMissingError:
            return pd.DataFrame()
        except exceptions.YFRateLimitError as exc:
            raise RateLimitError(f"rate limited by Yahoo: {exc}", symbol=symbol) from exc
        except (exceptions.YFTickerMissingError, exceptions.YFTzMissingError) as exc:
            raise SymbolNotFoundError(f"symbol not found: {symbol}", symbol=symbol) from exc
        except Exception as exc:
            if _http_status(exc) == 404:
                raise SymbolNotFoundError(f"symbol not found: {symbol}", symbol=symbol) from exc
            if _is_network_error(exc):
                raise TransientProviderError(f"network error: {exc}", symbol=symbol) from exc
            raise ProviderError(f"{type(exc).__name__}: {exc}", symbol=symbol) from exc


def _chunks(start: date, end: date, span_days: int | None) -> list[tuple[date, date]]:
    """Split the inclusive window [start, end] into pieces of at most ``span_days``."""
    if not span_days:
        return [(start, end)]
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=span_days - 1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _to_contract(history: pd.DataFrame) -> pd.DataFrame:
    """Map a yfinance history frame to the OHLCV provider contract."""
    frame = history.rename(columns=_COLUMN_MAP)
    index = pd.DatetimeIndex(history.index)
    if index.tz is None:  # yfinance normally returns exchange-local tz-aware stamps
        index = index.tz_localize("UTC")
    frame = frame.reset_index(drop=True)
    frame["ts"] = index.tz_convert("UTC").as_unit("us")
    frame["date"] = [ts.date() for ts in index]  # trading date in the exchange's timezone
    if "adj_close" not in frame:
        frame["adj_close"] = frame["close"]
    if "volume" not in frame:
        frame["volume"] = pd.NA
    return frame[PROVIDER_COLUMNS]


def _split_events(splits: Any) -> list[tuple[date, float]]:
    """Convert yfinance's split series into ``(effective_date, ratio)`` pairs."""
    if splits is None or len(splits) == 0:
        return []
    events = []
    for stamp, ratio in splits.items():
        if ratio and ratio > 0 and ratio != 1:
            events.append((pd.Timestamp(stamp).date(), float(ratio)))
    return events


def unadjust_splits(bars: pd.DataFrame, splits: list[tuple[date, float]]) -> pd.DataFrame:
    """Reverse split adjustment on split-adjusted OHLCV bars.

    A bar dated before a split's effective date is multiplied by that split's
    ratio (e.g. 4.0 for a 4-for-1 split); volume is divided by it.
    """
    if not splits or bars.empty:
        return bars
    factors = pd.Series(1.0, index=bars.index)
    for effective, ratio in splits:
        factors[bars["date"] < effective] *= ratio
    if (factors == 1.0).all():
        return bars

    out = bars.copy()
    for column in _PRICE_COLUMNS:
        out[column] = (out[column] * factors).round(6)
    volume = pd.to_numeric(out["volume"], errors="coerce") / factors
    out["volume"] = volume.round().astype("Int64")
    return out


def _http_status(exc: BaseException) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    return 404 if "404" in str(exc) else None


def _is_network_error(exc: BaseException) -> bool:
    if isinstance(exc, ConnectionError | TimeoutError):
        return True
    module = type(exc).__module__
    return module.startswith(("curl_cffi", "requests", "urllib3"))
