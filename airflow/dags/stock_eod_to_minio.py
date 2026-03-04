"""
Bronze ingestion DAG.

Pulls EOD stock prices from Yahoo Finance and writes parquet files to MinIO.
Trigger params: tickers, start_date, end_date, interval (all optional).
"""
from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

from lib import bronze_ingest, config, tickers

DAG_ID = "stock_eod_to_minio"


# Task callable 
# Reads trigger params from the DAG run context and kicks off the bronze
# ingestion batch. Returns a summary dict that Airflow stores in XCom.
def _fetch_and_upload(**context):
    """Read DAG run params and delegate to bronze_ingest.upload_batch."""
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


# Default trigger parameters
# Used when the DAG is triggered without explicit params. Targets a 5-year
# historical range across the full S&P 500 ticker list.
def _default_params():
    """Return the default DAG run parameters."""
    return {
        "tickers": tickers.DEFAULT_TICKERS,
        "start_date": "2010-10-08",
        "end_date": "2015-10-09",
        "interval": "1d",
    }


# DAG definition
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
