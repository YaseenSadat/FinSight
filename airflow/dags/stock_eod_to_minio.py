from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib import bronze_ingest, config, tickers

DAG_ID = "stock_eod_to_minio"


def _fetch_and_upload(**context):
    params = context.get("params", {}) or {}
    result = bronze_ingest.upload_batch(
        tickers_param=params.get("tickers"),
        start_date=params.get("start_date", "2024-01-01"),
        end_date=params.get("end_date", "2025-01-01"),
        interval=params.get("interval", "1d"),
    )
    return {
        "load_date": result.load_date,
        "uploaded": result.uploaded,
        "load_ts": result.load_ts.isoformat(),
    }


def _default_params():
    return {
        "tickers": tickers.DEFAULT_TICKERS,
        "start_date": "2010-10-08",
        "end_date": "2015-10-09",
        "interval": "1d",
    }


with DAG(
    dag_id=DAG_ID,
    default_args=config.DEFAULT_DAG_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "stocks", "minio", "parquet"],
    params=_default_params(),
) as dag:
    PythonOperator(
        task_id="fetch_and_upload",
        python_callable=_fetch_and_upload,
        provide_context=True,
    )
