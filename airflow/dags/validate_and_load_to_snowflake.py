from __future__ import annotations
from datetime import datetime
import io
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from lib import config
from lib.validation import validate_curated
from lib.snowflake_loader import load_dataframe

DAG_ID = "validate_and_load_to_snowflake"


def _validate(**context):
    params = context.get("params", {}) or {}
    df, date_used = validate_curated(load_date=params.get("load_date"))
    # XCom payload as JSON string
    return {"payload": df.to_json(orient="records"), "load_date": date_used}


def _load(ti, **_):
    result = ti.xcom_pull(task_ids="validate_curated")
    if not result or not result.get("payload"):
        raise AirflowFailException("No data from validate_curated")

    df = pd.read_json(io.StringIO(result["payload"]), orient="records")
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df.rename(
        columns={
            "ticker": "TICKER",
            "date": "DATE",
            "open": "OPEN",
            "high": "HIGH",
            "low": "LOW",
            "close": "CLOSE",
            "volume": "VOLUME",
            "load_ts": "LOAD_TS",
        }
    )[["TICKER", "DATE", "OPEN", "HIGH", "LOW", "CLOSE", "VOLUME", "LOAD_TS"]]

    load_dataframe(df)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=config.DEFAULT_DAG_ARGS,
    tags=["stocks", "minio", "snowflake"],
    doc_md="Validate latest curated partition in MinIO, then load to Snowflake",
) as dag:
    t_validate = PythonOperator(
        task_id="validate_curated",
        python_callable=_validate,
    )
    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=_load,
    )
    t_validate >> t_load
