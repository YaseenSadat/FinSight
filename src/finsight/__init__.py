"""FinSight: a self-serve, reproducible market data platform.

Quick start::

    import finsight

    run = finsight.fetch(["AAPL", "MSFT"], start="2020-01-01", interval="1d")
    prices = finsight.load("ohlcv", symbols=["AAPL"])
    finsight.query("SELECT symbol, max(close) FROM ohlcv GROUP BY symbol")
"""

from finsight._version import __version__
from finsight.api import catalog, fetch, load, query
from finsight.config import Settings
from finsight.models import Adjustment, FetchRequest, Interval

__all__ = [
    "Adjustment",
    "FetchRequest",
    "Interval",
    "Settings",
    "__version__",
    "catalog",
    "fetch",
    "load",
    "query",
]
