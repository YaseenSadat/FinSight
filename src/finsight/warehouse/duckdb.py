"""DuckDB access to the gold layer.

Every dataset is exposed as a view named after the dataset (``ohlcv``), so the
gold layer can be queried with plain SQL whether it lives on local disk or in
S3/MinIO.

For local storage, FinSight also maintains ``<data_dir>/finsight.duckdb``, a
small database containing only views over the gold Parquet files. Open it
from any DuckDB client (CLI, DBeaver, notebooks) to query the data directly::

    duckdb data/finsight.duckdb -c "SELECT * FROM ohlcv LIMIT 5"
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd
import pyarrow.dataset as ds
from pyarrow.fs import FSSpecHandler, PyFileSystem

from finsight.datasets import available_datasets
from finsight.storage import Layout, Storage

logger = logging.getLogger(__name__)


def connect(storage: Storage) -> duckdb.DuckDBPyConnection:
    """Open an in-memory DuckDB connection with one view per dataset."""
    con = duckdb.connect(":memory:")
    con.execute("SET TimeZone = 'UTC'")
    layout = Layout(storage)
    filesystem = PyFileSystem(FSSpecHandler(storage.fs))
    for dataset in available_datasets():
        files = layout.gold_files(dataset.name)
        if files:
            arrow = ds.dataset(
                files, schema=dataset.schema, format="parquet", filesystem=filesystem
            )
        else:
            arrow = dataset.empty_table()
        con.register(f"_{dataset.name}_source", arrow)
        con.execute(f'CREATE VIEW "{dataset.name}" AS SELECT * FROM "_{dataset.name}_source"')
    return con


def query(storage: Storage, sql: str) -> pd.DataFrame:
    """Run ``sql`` against the gold layer and return a DataFrame."""
    con = connect(storage)
    try:
        return con.execute(sql).fetch_df()
    finally:
        con.close()


def catalog(storage: Storage) -> pd.DataFrame:
    """Summarise what the gold layer contains, one row per series."""
    return query(
        storage,
        """
        SELECT provider, symbol, interval, adjustment,
               count(*)        AS rows,
               min(date)       AS first_date,
               max(date)       AS last_date,
               max(ingested_at) AS last_ingested_at
        FROM ohlcv
        GROUP BY ALL
        ORDER BY provider, symbol, interval, adjustment
        """,
    )


def refresh_catalog(storage: Storage) -> str | None:
    """(Re)create the persistent DuckDB catalog of views for local storage.

    Returns the database path, or ``None`` if it could not be updated (for
    example because another process holds a write lock on it).
    """
    if not storage.is_local:
        return None
    layout = Layout(storage)
    path = layout.catalog_file()
    try:
        con = duckdb.connect(path)
    except duckdb.IOException as exc:
        logger.warning("could not update DuckDB catalog %s: %s", path, exc)
        return None
    try:
        for dataset in available_datasets():
            if not layout.gold_files(dataset.name):
                continue
            pattern = f"{layout.gold_dir(dataset.name)}/**/*.parquet".replace("'", "''")
            con.execute(
                f'CREATE OR REPLACE VIEW "{dataset.name}" AS '
                f"SELECT * FROM read_parquet('{pattern}', hive_partitioning = false)"
            )
    finally:
        con.close()
    return path
