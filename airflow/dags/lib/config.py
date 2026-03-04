"""
Shared configuration for all DAGs and lib modules.

Connection IDs and Snowflake target identifiers are defined here as constants.
Bucket and prefix values are read from Airflow Variables at runtime so they can
be changed without redeploying code.
"""
from airflow.models import Variable # type: ignore

# Airflow connection IDs
AWS_CONN_ID = "minio_s3"
SNOWFLAKE_CONN_ID = "snowflake_conn"

# Snowflake target
SNOWFLAKE_DB = "STOCKS"
SNOWFLAKE_SCHEMA = "CURATED"
SNOWFLAKE_TABLE = "EOD_PRICES"

# Default DAG arguments applied to all DAGs
DEFAULT_DAG_ARGS = {"owner": "airflow", "retries": 0}


def bucket() -> str:
    """Return the MinIO bucket name (Airflow Variable S3_BUCKET, default: stock-etl)."""
    return Variable.get("S3_BUCKET", default_var="stock-etl")


def raw_prefix() -> str:
    """Return the S3 prefix for raw bronze data (Airflow Variable S3_PREFIX, default: raw/eod)."""
    return Variable.get("S3_PREFIX", default_var="raw/eod")


def curated_prefix() -> str:
    """Return the S3 prefix for curated silver data."""
    return "curated/eod"
