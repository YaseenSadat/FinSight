"""Optional Snowflake sink.

Loads gold data into a Snowflake table with ``MERGE`` semantics keyed on the
dataset's primary key, so re-loading a run never creates duplicates. Rows are
first bulk-loaded into a temporary staging table with ``write_pandas``.

Configure with ``SNOWFLAKE_ACCOUNT``, ``SNOWFLAKE_USER``,
``SNOWFLAKE_PASSWORD``, ``SNOWFLAKE_WAREHOUSE`` and optionally
``SNOWFLAKE_DATABASE``, ``SNOWFLAKE_SCHEMA``, ``SNOWFLAKE_ROLE``.
Requires ``pip install 'finsight[snowflake]'``.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

import pandas as pd
import pyarrow as pa

from finsight.config import SnowflakeSettings
from finsight.datasets import Dataset
from finsight.errors import ConfigError, FinSightError

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")

_TYPE_MAP = {
    pa.string(): "VARCHAR",
    pa.float64(): "DOUBLE",
    pa.int64(): "NUMBER(38,0)",
    pa.date32(): "DATE",
    pa.timestamp("us", tz="UTC"): "TIMESTAMP_TZ",
}


def _identifier(name: str) -> str:
    """Validate and quote a Snowflake identifier (upper-cased)."""
    if not _IDENTIFIER.fullmatch(name):
        raise ConfigError(f"invalid Snowflake identifier {name!r}")
    return f'"{name.upper()}"'


def table_name(dataset: Dataset) -> str:
    return dataset.name.upper()


def create_table_sql(dataset: Dataset, database: str, schema: str) -> str:
    columns = []
    for field in dataset.schema:
        sql_type = _TYPE_MAP.get(field.type)
        if sql_type is None:
            raise FinSightError(f"no Snowflake type mapping for {field.name}: {field.type}")
        columns.append(f"  {_identifier(field.name)} {sql_type}")
    key = ", ".join(_identifier(c) for c in dataset.primary_key)
    target = ".".join(_identifier(p) for p in (database, schema, table_name(dataset)))
    body = ",\n".join([*columns, f"  PRIMARY KEY ({key})"])
    return f"CREATE TABLE IF NOT EXISTS {target} (\n{body}\n)"


def merge_sql(dataset: Dataset, database: str, schema: str, staging: str) -> str:
    target = ".".join(_identifier(p) for p in (database, schema, table_name(dataset)))
    source = _identifier(staging)
    columns = [_identifier(f.name) for f in dataset.schema]
    key = [_identifier(c) for c in dataset.primary_key]
    on = " AND ".join(f"t.{c} = s.{c}" for c in key)
    updates = ", ".join(f"{c} = s.{c}" for c in columns if c not in key)
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join(f"s.{c}" for c in columns)
    return (
        f"MERGE INTO {target} t USING {source} s ON {on}\n"
        f"WHEN MATCHED THEN UPDATE SET {updates}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
    )


@dataclass(frozen=True)
class LoadResult:
    table: str
    rows_loaded: int


class SnowflakeLoader:
    """Upserts gold data into Snowflake."""

    def __init__(self, settings: SnowflakeSettings, connection: Any | None = None):
        if connection is None and not settings.configured:
            raise ConfigError(
                "Snowflake is not configured; set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, "
                "and SNOWFLAKE_PASSWORD"
            )
        self.settings = settings
        self._connection = connection

    def _connect(self) -> Any:
        if self._connection is None:
            try:
                import snowflake.connector
            except ImportError as exc:
                raise ConfigError(
                    "the Snowflake sink requires: pip install 'finsight[snowflake]'"
                ) from exc
            self._connection = snowflake.connector.connect(
                account=self.settings.account,
                user=self.settings.user,
                password=self.settings.password,
                warehouse=self.settings.warehouse,
                role=self.settings.role,
                application="FinSight",
            )
        return self._connection

    def load(self, dataset: Dataset, frame: pd.DataFrame) -> LoadResult:
        """Create the target table if needed and upsert ``frame`` into it."""
        from snowflake.connector.pandas_tools import write_pandas

        database, schema = self.settings.database, self.settings.schema
        target = f"{database}.{schema}.{table_name(dataset)}"
        conn = self._connect()
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {_identifier(database)}")
            cur.execute(
                f"CREATE SCHEMA IF NOT EXISTS {_identifier(database)}.{_identifier(schema)}"
            )
            cur.execute(f"USE SCHEMA {_identifier(database)}.{_identifier(schema)}")
            cur.execute(create_table_sql(dataset, database, schema))
            if frame.empty:
                return LoadResult(table=target, rows_loaded=0)

            staging = f"{table_name(dataset)}_STAGING"
            upload = frame[dataset.schema.names].copy()
            upload.columns = [c.upper() for c in upload.columns]
            ok, _, rows, _ = write_pandas(
                conn,
                upload,
                table_name=staging,
                database=database.upper(),
                schema=schema.upper(),
                auto_create_table=True,
                table_type="temporary",
                overwrite=True,
                use_logical_type=True,
            )
            if not ok:
                raise FinSightError(f"write_pandas failed while staging {len(upload)} rows")
            cur.execute(merge_sql(dataset, database, schema, staging))
            logger.info("merged %d rows into %s", rows, target)
            return LoadResult(table=target, rows_loaded=int(rows))
        finally:
            cur.close()

    def close(self) -> None:
        if self._connection is not None:
            self._connection.close()
            self._connection = None
