"""
Silver transformation DAG.

Runs a Spark job to read raw bronze parquet files from MinIO, clean the schema,
and write a curated partition back to MinIO. Defaults to the latest partition.
Trigger param: load_date (optional).
"""
from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore

from lib import config
from lib.spark_job import transform_raw_to_curated

DAG_ID = "transform_raw_to_curated"


def _run_spark(**context):
    """Read the load_date param and invoke the Spark transform."""
    params = context.get("params", {}) or {}
    used_date = transform_raw_to_curated(load_date=params.get("load_date"))
    return {"load_date": used_date}


with DAG(
    dag_id=DAG_ID,
    default_args=config.DEFAULT_DAG_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "minio", "parquet", "stocks"],
) as dag:
    PythonOperator(
        task_id="run_spark",
        python_callable=_run_spark,
    )
