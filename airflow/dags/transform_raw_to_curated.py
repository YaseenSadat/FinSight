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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator # type: ignore

from lib import config
from lib.spark_job import transform_raw_to_curated

DAG_ID = "transform_raw_to_curated"


def _run_spark(**context):
    """Read the load_date from trigger conf or params, then invoke the Spark transform."""
    conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
    params = context.get("params", {}) or {}
    load_date = conf.get("load_date") or params.get("load_date")
    used_date = transform_raw_to_curated(load_date=load_date)
    return {"load_date": used_date}


with DAG(
    dag_id=DAG_ID,
    default_args=config.DEFAULT_DAG_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "minio", "parquet", "stocks"],
) as dag:
    t_spark = PythonOperator(
        task_id="run_spark",
        python_callable=_run_spark,
    )

    t_trigger = TriggerDagRunOperator(
        task_id="trigger_validate",
        trigger_dag_id="validate_and_load_to_snowflake",
        conf={"load_date": "{{ ti.xcom_pull('run_spark')['load_date'] }}"},
        wait_for_completion=False,
    )

    t_spark >> t_trigger
