"""Spark engine parity test. Runs only where pyspark and Java 17/21 are available."""

from __future__ import annotations

import shutil

import pytest

from finsight.pipeline import Pipeline, RunStatus
from finsight.warehouse import duckdb as warehouse
from tests.conftest import make_request

pytestmark = pytest.mark.spark
pytest.importorskip("pyspark")
if shutil.which("java") is None:
    pytest.skip("java not available", allow_module_level=True)


def test_spark_engine_matches_pandas_engine(pipeline: Pipeline) -> None:
    # ^GSPC and EURUSD=X exercise path encoding: Spark skips files starting with "_" or ".".
    request = make_request(["AAPL", "^GSPC", "EURUSD=X"], "2024-01-01", "2024-02-29")
    columns = "symbol, date, ts, open, high, low, close, adj_close, volume, interval, adjustment"

    spark_run = pipeline.run(request, engine="spark")
    assert spark_run.status == RunStatus.SUCCEEDED
    assert spark_run.engine == "spark"
    assert all(r.silver_rows for r in spark_run.symbols.values())
    from_spark = warehouse.query(
        pipeline.storage, f"SELECT {columns} FROM ohlcv ORDER BY symbol, ts"
    )

    shutil.rmtree(pipeline.storage.root)
    pipeline.run(request, engine="pandas")
    from_pandas = warehouse.query(
        pipeline.storage, f"SELECT {columns} FROM ohlcv ORDER BY symbol, ts"
    )

    assert from_spark.equals(from_pandas)
