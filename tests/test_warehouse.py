from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

import finsight
from finsight.config import Settings
from finsight.pipeline import Pipeline
from finsight.warehouse import duckdb as warehouse
from tests.conftest import make_request


@pytest.fixture
def populated(pipeline: Pipeline) -> Pipeline:
    pipeline.run(make_request(["AAPL", "MSFT", "^GSPC"], "2024-01-01", "2024-01-31"))
    pipeline.run(make_request("AAPL", "2024-01-01", "2024-01-31", adjustment="adjusted"))
    return pipeline


def test_empty_warehouse_is_queryable(pipeline: Pipeline) -> None:
    assert warehouse.catalog(pipeline.storage).empty
    frame = warehouse.query(pipeline.storage, "SELECT * FROM ohlcv")
    assert frame.empty
    assert "close" in frame.columns


def test_catalog_summarises_series(populated: Pipeline) -> None:
    catalog = warehouse.catalog(populated.storage)
    assert len(catalog) == 4
    assert set(catalog.columns) >= {"symbol", "interval", "adjustment", "rows", "first_date"}


def test_timestamps_are_utc(populated: Pipeline) -> None:
    frame = warehouse.query(populated.storage, "SELECT ts FROM ohlcv LIMIT 1")
    assert str(frame["ts"].dt.tz) == "UTC"


def test_persistent_catalog_database(populated: Pipeline) -> None:
    path = Path(populated.layout.catalog_file())
    assert path.exists()
    con = duckdb.connect(str(path), read_only=True)
    try:
        count = con.execute("SELECT count(*) FROM ohlcv").fetchone()
    finally:
        con.close()
    assert count is not None
    assert count[0] > 0


def test_load_filters(populated: Pipeline, settings: Settings) -> None:
    frame = finsight.load(
        symbols="AAPL",
        start="2024-01-10",
        end="2024-01-12",
        adjustment="raw",
        interval="1d",
        settings=settings,
    )
    assert set(frame["symbol"]) == {"AAPL"}
    assert set(frame["adjustment"]) == {"raw"}
    assert frame["date"].min().date().isoformat() >= "2024-01-10"
    assert frame["date"].max().date().isoformat() <= "2024-01-12"


def test_api_query_and_catalog(populated: Pipeline, settings: Settings) -> None:
    result = finsight.query("SELECT count(DISTINCT symbol) AS n FROM ohlcv", settings=settings)
    assert result["n"][0] == 3
    assert len(finsight.catalog(settings=settings)) == 4


def test_api_fetch(settings: Settings) -> None:
    manifest = finsight.fetch(
        "AAPL", "2024-01-01", "2024-01-05", provider="synthetic", settings=settings
    )
    assert manifest.status.value == "succeeded"


def test_gold_directory_is_readable_by_plain_parquet_readers(populated: Pipeline) -> None:
    import pandas as pd

    gold = pd.read_parquet(populated.layout.gold_dir("ohlcv"))
    expected = warehouse.query(populated.storage, "SELECT count(*) AS n FROM ohlcv")["n"][0]
    assert len(gold) == expected
    assert set(gold["symbol"]) == {"AAPL", "MSFT", "^GSPC"}  # no file silently skipped
