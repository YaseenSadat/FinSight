"""Shared configuration helpers for DAGs."""
from airflow.models import Variable

AWS_CONN_ID = "minio_s3"
SNOWFLAKE_CONN_ID = "snowflake_conn"
SNOWFLAKE_DB = "STOCKS"
SNOWFLAKE_SCHEMA = "CURATED"
SNOWFLAKE_TABLE = "EOD_PRICES"
DEFAULT_DAG_ARGS = {"owner": "airflow", "retries": 0}


def bucket() -> str:
    return Variable.get("S3_BUCKET", default_var="stock-etl")


def raw_prefix() -> str:
    return Variable.get("S3_PREFIX", default_var="raw/eod")


def curated_prefix() -> str:
    return "curated/eod"
