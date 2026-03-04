"""
Snowflake load helpers.

Provides a single function that writes a pandas DataFrame into the Snowflake
gold table using the snowflake-connector write_pandas utility. The target
database, schema, and table are read from the shared config module.
"""
from __future__ import annotations
import pandas as pd
from airflow.exceptions import AirflowFailException # type: ignore
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook # type: ignore
from snowflake.connector.pandas_tools import write_pandas

from . import config


def load_dataframe(df: pd.DataFrame) -> None:
    """
    Write a DataFrame to the Snowflake gold table.

    Sets the warehouse, database, and schema context before writing, then uses
    write_pandas for efficient bulk loading. Raises AirflowFailException if the
    context setup fails or if no rows are written.

    Args:
        df: DataFrame with uppercase column names matching STOCKS.CURATED.EOD_PRICES.
    """
    hook = SnowflakeHook(snowflake_conn_id=config.SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute(f"USE DATABASE {config.SNOWFLAKE_DB}")
        cur.execute(f"USE SCHEMA {config.SNOWFLAKE_SCHEMA}")
    except Exception as e:
        raise AirflowFailException(f"Failed to set Snowflake context. Error: {e}")

    ok, chunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=config.SNOWFLAKE_TABLE,
        database=config.SNOWFLAKE_DB,
        schema=config.SNOWFLAKE_SCHEMA,
        quote_identifiers=False,
    )
    if not ok or nrows == 0:
        raise AirflowFailException(f"write_pandas failed, chunks={chunks}, rows={nrows}")
