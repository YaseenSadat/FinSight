"""High-level Python API.

These functions are thin wrappers around :class:`~finsight.pipeline.Pipeline`
and the DuckDB warehouse, and are what the CLI, UI, and Airflow DAGs call.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime

import pandas as pd

from finsight.config import Settings
from finsight.datasets import get_dataset
from finsight.errors import RunNotFoundError
from finsight.models import Adjustment, FetchRequest, Interval
from finsight.pipeline import Engine, Pipeline, ProgressCallback, RunManifest
from finsight.storage import Storage
from finsight.warehouse import duckdb as warehouse
from finsight.warehouse.snowflake import LoadResult

DateLike = date | datetime | str


def _storage(settings: Settings | None) -> Storage:
    return Storage.from_settings(settings or Settings.from_env())


def fetch(
    symbols: str | Iterable[str],
    start: DateLike,
    end: DateLike | None = None,
    *,
    interval: Interval | str = Interval.D1,
    adjustment: Adjustment | str = Adjustment.RAW,
    provider: str = "yahoo",
    dataset: str = "ohlcv",
    engine: Engine = "pandas",
    settings: Settings | None = None,
    progress: ProgressCallback | None = None,
) -> RunManifest:
    """Fetch, validate, and publish data; return the run manifest.

    ``start`` and ``end`` are inclusive. ``end`` defaults to today.

    Raises:
        RequestError: if the input is invalid or unsupported by the provider.
    """
    request = FetchRequest.create(
        symbols,
        start,
        end,
        interval=interval,
        adjustment=adjustment,
        provider=provider,
        dataset=dataset,
    )
    return Pipeline(settings).run(request, engine=engine, progress=progress)


def query(sql: str, *, settings: Settings | None = None) -> pd.DataFrame:
    """Run SQL against the gold layer (one view per dataset, e.g. ``ohlcv``)."""
    return warehouse.query(_storage(settings), sql)


def catalog(*, settings: Settings | None = None) -> pd.DataFrame:
    """One row per stored series with row counts and date coverage."""
    return warehouse.catalog(_storage(settings))


def load(
    dataset: str = "ohlcv",
    *,
    symbols: str | Iterable[str] | None = None,
    start: DateLike | None = None,
    end: DateLike | None = None,
    interval: Interval | str | None = None,
    adjustment: Adjustment | str | None = None,
    provider: str | None = None,
    settings: Settings | None = None,
) -> pd.DataFrame:
    """Load gold data into a DataFrame, filtered by any of the given fields."""
    contract = get_dataset(dataset)
    clauses: list[str] = []
    params: list[object] = []

    if symbols is not None:
        from finsight.symbols import parse_symbols

        wanted = parse_symbols(symbols)
        clauses.append(f"symbol IN ({', '.join('?' for _ in wanted)})")
        params.extend(wanted)
    if start is not None:
        clauses.append("date >= ?")
        params.append(_as_date(start))
    if end is not None:
        clauses.append("date <= ?")
        params.append(_as_date(end))
    for column, value in (
        ("interval", interval),
        ("adjustment", adjustment),
        ("provider", provider),
    ):
        if value is not None:
            clauses.append(f'"{column}" = ?')
            params.append(str(getattr(value, "value", value)))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    con = warehouse.connect(_storage(settings))
    try:
        order = ", ".join(contract.order_by)
        sql = f'SELECT * FROM "{contract.name}" {where} ORDER BY {order}'
        return con.execute(sql, params).fetch_df()
    finally:
        con.close()


def sync_to_snowflake(run_id: str | None = None, *, settings: Settings | None = None) -> LoadResult:
    """Upsert the rows a run published (default: the latest run) into Snowflake."""
    from finsight.pipeline import ManifestStore, SymbolStatus
    from finsight.warehouse.snowflake import SnowflakeLoader

    settings = settings or Settings.from_env()
    storage = Storage.from_settings(settings)
    manifests = ManifestStore(storage)
    manifest = manifests.load(run_id) if run_id else manifests.latest()
    if manifest is None:
        raise RunNotFoundError("no runs found; fetch some data first")

    contract = get_dataset(manifest.request.dataset)
    published = manifest.symbols_with(SymbolStatus.PUBLISHED)
    paths = [r.gold_path for r in published if r.gold_path]
    frame = storage.read_tables(paths, contract.schema).to_pandas()
    frame = frame.loc[frame["run_id"] == manifest.run_id]

    loader = SnowflakeLoader(settings.snowflake)
    try:
        return loader.load(contract, frame)
    finally:
        loader.close()


def _as_date(value: DateLike) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)
