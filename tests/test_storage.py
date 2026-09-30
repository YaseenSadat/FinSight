from __future__ import annotations

from pathlib import Path

import fsspec
import pyarrow as pa
import pytest

from finsight.config import Settings
from finsight.pipeline import Pipeline, RunStatus
from finsight.storage import Layout, Storage
from finsight.warehouse import duckdb as warehouse
from tests.conftest import make_request


def test_local_storage_writes_atomically(storage: Storage) -> None:
    path = storage.path("nested", "file.txt")
    storage.write_text(path, "hello")
    assert storage.read_text(path) == "hello"
    leftovers = [p for p in Path(storage.root, "nested").iterdir() if p.name.endswith(".tmp")]
    assert leftovers == []


def test_write_table_reports_hash(storage: Storage) -> None:
    table = pa.table({"x": [1, 2, 3]})
    written = storage.write_table(table, storage.path("t.parquet"))
    assert written.bytes > 0
    assert len(written.sha256) == 64
    assert storage.read_table(written.path).equals(table)


def test_layout_encodes_symbols(storage: Storage) -> None:
    layout = Layout(storage)
    path = layout.gold_file(
        "ohlcv", {"provider": "yahoo", "interval": "1d", "adjustment": "raw", "symbol": "^GSPC"}
    )
    assert path.endswith("gold/ohlcv/yahoo/1d/raw/~5EGSPC.parquet")
    bronze = layout.bronze_file("ohlcv", "r1", "EURUSD=X")
    assert bronze.endswith("bronze/ohlcv/r1/EURUSD~3DX.parquet")


def test_uris(storage: Storage) -> None:
    assert storage.uri("/a/b", spark=True) == "file:///a/b"
    s3 = Storage(fsspec.filesystem("memory"), "bucket/prefix", scheme="s3")
    assert s3.uri("bucket/prefix/x", spark=True) == "s3a://bucket/prefix/x"
    assert s3.uri("bucket/prefix/x") == "s3://bucket/prefix/x"


def test_pipeline_runs_on_a_remote_style_filesystem(settings: Settings) -> None:
    """The in-memory fsspec backend exercises the non-local code paths used for S3."""
    fs = fsspec.filesystem("memory")
    fs.store.clear()
    storage = Storage(fs, "/finsight-test", scheme="memory")
    pipeline = Pipeline(settings, storage=storage, retry_backoff=0)

    manifest = pipeline.run(make_request(["AAPL", "^GSPC"]))

    assert manifest.status == RunStatus.SUCCEEDED
    catalog = warehouse.catalog(storage)
    assert sorted(catalog["symbol"]) == ["AAPL", "^GSPC"]
    assert not Path("data").exists()  # nothing leaked to local disk


def test_s3_storage_requires_bucket_setting() -> None:
    from finsight.errors import ConfigError

    with pytest.raises(ConfigError):
        Settings(storage="s3")
