from __future__ import annotations

from typing import Any

import pandas as pd
import pytest

from finsight.config import SnowflakeSettings
from finsight.datasets import OHLCVDataset
from finsight.errors import ConfigError
from finsight.warehouse import snowflake as sf

DATASET = OHLCVDataset()


def test_create_table_sql_maps_every_column() -> None:
    sql = sf.create_table_sql(DATASET, "FINSIGHT", "MARKET_DATA")
    assert sql.startswith('CREATE TABLE IF NOT EXISTS "FINSIGHT"."MARKET_DATA"."OHLCV"')
    for column in DATASET.schema.names:
        assert f'"{column.upper()}"' in sql
    assert '"TS" TIMESTAMP_TZ' in sql
    assert '"VOLUME" NUMBER(38,0)' in sql
    assert 'PRIMARY KEY ("PROVIDER", "SYMBOL", "INTERVAL", "ADJUSTMENT", "TS")' in sql


def test_merge_sql_upserts_on_primary_key() -> None:
    sql = sf.merge_sql(DATASET, "FINSIGHT", "MARKET_DATA", "OHLCV_STAGING")
    assert 'MERGE INTO "FINSIGHT"."MARKET_DATA"."OHLCV" t USING "OHLCV_STAGING" s' in sql
    assert 't."SYMBOL" = s."SYMBOL"' in sql
    assert 'WHEN MATCHED THEN UPDATE SET "DATE" = s."DATE"' in sql
    assert '"SYMBOL" = s."SYMBOL",' not in sql.split("UPDATE SET")[1]  # keys are not updated
    assert "WHEN NOT MATCHED THEN INSERT" in sql


def test_identifiers_are_validated() -> None:
    with pytest.raises(ConfigError, match="invalid Snowflake identifier"):
        sf.create_table_sql(DATASET, 'X"; DROP TABLE users; --', "S")


def test_loader_requires_configuration() -> None:
    with pytest.raises(ConfigError, match="not configured"):
        sf.SnowflakeLoader(SnowflakeSettings())


class FakeCursor:
    def __init__(self, log: list[str]):
        self.log = log

    def execute(self, sql: str) -> None:
        self.log.append(sql)

    def close(self) -> None:
        pass


class FakeConnection:
    def __init__(self) -> None:
        self.log: list[str] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.log)

    def close(self) -> None:
        pass


def test_loader_stages_then_merges(monkeypatch: pytest.MonkeyPatch) -> None:
    pytest.importorskip("snowflake.connector")
    staged: dict[str, Any] = {}

    def fake_write_pandas(
        conn: Any, df: pd.DataFrame, **kwargs: Any
    ) -> tuple[bool, int, int, list]:
        staged.update(kwargs, columns=list(df.columns), rows=len(df))
        return True, 1, len(df), []

    monkeypatch.setattr("snowflake.connector.pandas_tools.write_pandas", fake_write_pandas)
    connection = FakeConnection()
    loader = sf.SnowflakeLoader(SnowflakeSettings(), connection=connection)
    frame = DATASET.empty_table().to_pandas()
    frame.loc[0] = [
        "AAPL",
        pd.Timestamp("2024-01-02").date(),
        pd.Timestamp("2024-01-02", tz="UTC"),
        *(1.0, 2.0, 0.5, 1.5, 1.5, 100),
        *("1d", "raw", "yahoo", "run"),
        pd.Timestamp("2024-01-03", tz="UTC"),
    ]

    result = loader.load(DATASET, frame)

    assert result.rows_loaded == 1
    assert result.table == "FINSIGHT.MARKET_DATA.OHLCV"
    assert staged["table_type"] == "temporary"
    assert staged["columns"][0] == "SYMBOL"
    assert connection.log[0] == 'CREATE DATABASE IF NOT EXISTS "FINSIGHT"'
    assert connection.log[-1].startswith("MERGE INTO")
