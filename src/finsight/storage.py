"""Storage backends and the on-disk layout.

FinSight stores everything under one root, which is a local directory by
default or an S3-compatible bucket prefix (AWS S3, MinIO, ...). All access
goes through :mod:`fsspec`, so the pipeline code is identical for both.

Layout::

    <root>/
      bronze/<dataset>/<run>/<SYMBOL>.parquet          immutable provider output
      silver/<dataset>/<run>/part-<SYMBOL>.parquet     conformed, per run
      gold/<dataset>/<provider>/<interval>/<adjustment>/<SYMBOL>.parquet
                                                       validated, deduplicated history
      runs/<run>.json                                  run manifests
      finsight.duckdb                                  DuckDB catalog (local only)

Directory names are plain values rather than Hive-style ``key=value`` pairs.
Every column is stored inside the files, so any Parquet reader (pandas,
pyarrow, DuckDB, Spark, Polars) can read a directory without inventing
conflicting partition columns.
"""

from __future__ import annotations

import hashlib
import io
import posixpath
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from finsight.config import Settings
from finsight.errors import ConfigError
from finsight.symbols import encode_symbol


@dataclass(frozen=True)
class WrittenFile:
    path: str
    bytes: int
    sha256: str


class Storage:
    """A filesystem rooted at the FinSight data location."""

    def __init__(self, fs: fsspec.AbstractFileSystem, root: str, *, scheme: str = "file"):
        self.fs = fs
        self.root = root.rstrip("/")
        self.scheme = scheme

    @classmethod
    def from_settings(cls, settings: Settings) -> Storage:
        if settings.storage == "local":
            local_root = settings.data_dir.expanduser().resolve()
            return cls(fsspec.filesystem("file"), local_root.as_posix(), scheme="file")

        try:
            import s3fs  # noqa: F401
        except ImportError as exc:
            raise ConfigError(
                "S3 storage requires the s3 extra: pip install 'finsight[s3]'"
            ) from exc
        client_kwargs: dict[str, Any] = {"region_name": settings.s3_region}
        if settings.s3_endpoint_url:
            client_kwargs["endpoint_url"] = settings.s3_endpoint_url
        fs = fsspec.filesystem(
            "s3",
            key=settings.aws_access_key_id,
            secret=settings.aws_secret_access_key,
            client_kwargs=client_kwargs,
        )
        root = posixpath.join(settings.s3_bucket or "", settings.s3_prefix).rstrip("/")
        return cls(fs, root, scheme="s3")

    # Paths ----------------------------------------------------------------------------

    def path(self, *parts: str) -> str:
        return posixpath.join(self.root, *parts)

    def uri(self, path: str, *, spark: bool = False) -> str:
        """Fully qualified URI for ``path`` (``s3a://`` when ``spark`` is set)."""
        if self.scheme == "file":
            return f"file://{path}" if spark else path
        if self.scheme == "s3":
            return f"{'s3a' if spark else 's3'}://{path}"
        return f"{self.scheme}://{path}"

    @property
    def is_local(self) -> bool:
        return self.scheme == "file"

    # Basic operations ------------------------------------------------------------------

    def ensure_root(self) -> None:
        """Create the root directory, or the bucket for S3 storage."""
        if self.scheme == "s3":
            bucket = self.root.split("/", 1)[0]
            if not self.fs.exists(bucket):
                self.fs.mkdir(bucket)
        else:
            self.fs.makedirs(self.root, exist_ok=True)

    def exists(self, path: str) -> bool:
        return bool(self.fs.exists(path))

    def glob(self, pattern: str) -> list[str]:
        return sorted(str(p) for p in self.fs.glob(pattern))

    def read_bytes(self, path: str) -> bytes:
        return bytes(self.fs.cat_file(path))

    def write_bytes(self, path: str, data: bytes) -> None:
        """Write ``data`` to ``path``, atomically on local disk."""
        if self.is_local:
            target = Path(path)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
            tmp.write_bytes(data)
            tmp.replace(target)
        else:
            parent = posixpath.dirname(path)
            if parent and self.scheme != "s3":
                self.fs.makedirs(parent, exist_ok=True)
            self.fs.pipe_file(path, data)

    def delete(self, path: str, *, recursive: bool = False) -> None:
        if self.exists(path):
            self.fs.rm(path, recursive=recursive)

    # Parquet ---------------------------------------------------------------------------

    def write_table(self, table: pa.Table, path: str) -> WrittenFile:
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression="zstd")
        data = buffer.getvalue()
        self.write_bytes(path, data)
        return WrittenFile(path=path, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    def read_table(self, path: str) -> pa.Table:
        return pq.read_table(io.BytesIO(self.read_bytes(path)))

    def read_tables(self, paths: list[str], schema: pa.Schema) -> pa.Table:
        """Read and concatenate parquet files, cast to ``schema``."""
        if not paths:
            return schema.empty_table()
        tables = [self.read_table(p).select(schema.names).cast(schema) for p in paths]
        return pa.concat_tables(tables)

    # JSON ------------------------------------------------------------------------------

    def write_text(self, path: str, text: str) -> None:
        self.write_bytes(path, text.encode("utf-8"))

    def read_text(self, path: str) -> str:
        return self.read_bytes(path).decode("utf-8")


class Layout:
    """Relative paths for each layer. Kept separate so the Spark engine can reuse them."""

    def __init__(self, storage: Storage):
        self.storage = storage

    def bronze_dir(self, dataset: str, run_id: str) -> str:
        return self.storage.path("bronze", dataset, run_id)

    def bronze_file(self, dataset: str, run_id: str, symbol: str) -> str:
        return posixpath.join(self.bronze_dir(dataset, run_id), f"{encode_symbol(symbol)}.parquet")

    def silver_dir(self, dataset: str, run_id: str) -> str:
        return self.storage.path("silver", dataset, run_id)

    def silver_file(self, dataset: str, run_id: str, symbol: str) -> str:
        name = f"part-{encode_symbol(symbol)}.parquet"
        return posixpath.join(self.silver_dir(dataset, run_id), name)

    def gold_dir(self, dataset: str) -> str:
        return self.storage.path("gold", dataset)

    def gold_file(self, dataset: str, partition: dict[str, str]) -> str:
        """Path of one gold partition; the last partition value names the file."""
        *dirs, leaf = [encode_symbol(str(value)) for value in partition.values()]
        return posixpath.join(self.gold_dir(dataset), *dirs, f"{leaf}.parquet")

    def gold_files(self, dataset: str) -> list[str]:
        return self.storage.glob(posixpath.join(self.gold_dir(dataset), "**", "*.parquet"))

    def runs_dir(self) -> str:
        return self.storage.path("runs")

    def manifest_file(self, run_id: str) -> str:
        return posixpath.join(self.runs_dir(), f"{run_id}.json")

    def catalog_file(self) -> str:
        return self.storage.path("finsight.duckdb")
