from __future__ import annotations
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from lib import config
from lib.spark_job import transform_raw_to_curated

DAG_ID = "transform_raw_to_curated"


def _run_spark(**context):
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
