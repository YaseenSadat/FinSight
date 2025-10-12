# validate_and_load_to_snowflake.py
from __future__ import annotations
from datetime import datetime
import re
import pandas as pd

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# MinIO creds from Airflow connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
# Snowflake
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas

# -------- Config you may change --------
AWS_CONN_ID = "minio_s3"                 # your existing MinIO connection
BUCKET = Variable.get("S3_BUCKET", default_var="stock-etl")
CURATED_PREFIX = "curated/eod"           # where transform writes
SNOWFLAKE_CONN_ID = "snowflake_conn"
DB = "STOCKS"
SCHEMA = "CURATED"
TABLE = "EOD_PRICES"
REQUIRED_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "load_ts"]
# --------------------------------------

def _minio_storage_opts():
    """s3fs-style storage options for pandas to talk to MinIO."""
    hook = AwsBaseHook(aws_conn_id=AWS_CONN_ID, client_type="s3")
    creds = hook.get_credentials()
    extra = hook.get_connection(hook.aws_conn_id).extra_dejson
    endpoint_url = extra.get("endpoint_url")  # e.g. http://minio:9000
    return {
        "key": creds.access_key,
        "secret": creds.secret_key,
        "client_kwargs": {"endpoint_url": endpoint_url},
    }

def _find_latest_partition():
    """List s3://<bucket>/curated/eod/, pick the newest load_date=YYYY-MM-DD."""
    import s3fs
    fs = s3fs.S3FileSystem(**_minio_storage_opts())
    root = f"{BUCKET}/{CURATED_PREFIX}"
    # Example entries: curated/eod/load_date=2025-10-10/...
    entries = fs.ls(root)
    dates = []
    for p in entries:
        m = re.search(r"load_date=(\d{4}-\d{2}-\d{2})/?$", p)
        if m:
            dates.append(m.group(1))
    if not dates:
        raise AirflowFailException("No curated partitions found under s3://%s/%s" % (BUCKET, CURATED_PREFIX))
    return sorted(dates)[-1]

def validate_task(**_):
    """Read latest curated partition, run checks, return JSON for XCom."""
    storage_opts = _minio_storage_opts()
    latest = _find_latest_partition()
    path = f"s3://{BUCKET}/{CURATED_PREFIX}/load_date={latest}/"
    df = pd.read_parquet(path, storage_options=storage_opts)

    # Columns
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise AirflowFailException(f"Missing columns: {missing}")

    # Key null checks
    for c in ["ticker", "date", "close"]:
        if df[c].isna().any():
            raise AirflowFailException(f"Nulls in {c}: {int(df[c].isna().sum())}")

    # Duplicate business key
    dups = int(df.duplicated(subset=["ticker", "date"]).sum())
    if dups > 0:
        raise AirflowFailException(f"Duplicate rows on (ticker,date): {dups}")

    # Normalize types
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    # Keep only required columns
    df = df[REQUIRED_COLUMNS].copy()

    # Pass via XCom as JSON
    return df.to_json(orient="records")

def load_task(ti, **_):
    import io
    from airflow.exceptions import AirflowFailException
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    from snowflake.connector.pandas_tools import write_pandas
    import pandas as pd

    payload = ti.xcom_pull(task_ids="validate_curated")
    if not payload:
        raise AirflowFailException("No data from validate_curated")

    # Avoid the FutureWarning by using StringIO
    df = pd.read_json(io.StringIO(payload), orient="records")

    # Convert date column properly - it comes back as timestamp from JSON
    df["date"] = pd.to_datetime(df["date"]).dt.date
    
    # Map to Snowflake target columns
    df = df.rename(columns={
        "ticker": "TICKER",
        "date": "DATE",
        "open": "OPEN",
        "high": "HIGH",
        "low": "LOW",
        "close": "CLOSE",
        "volume": "VOLUME",
        "load_ts": "LOAD_TS",
    })[["TICKER","DATE","OPEN","HIGH","LOW","CLOSE","VOLUME","LOAD_TS"]]

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    # Set context with correct warehouse name
    try:
        cur.execute("USE WAREHOUSE COMPUTE_WH")
        cur.execute(f"USE DATABASE {DB}")
        cur.execute(f"USE SCHEMA {SCHEMA}")
    except Exception as e:
        raise AirflowFailException(
            f"Failed to set Snowflake context. Error: {str(e)}"
        )

    ok, chunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=TABLE,
        database=DB,
        schema=SCHEMA,
        quote_identifiers=False
    )
    if not ok or nrows == 0:
        raise AirflowFailException(f"write_pandas failed, chunks={chunks}, rows={nrows}")

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id="validate_and_load_to_snowflake",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["stocks","minio","snowflake"],
    doc_md="Validate latest curated partition in MinIO, then load to Snowflake",
):
    t_validate = PythonOperator(
        task_id="validate_curated",
        python_callable=validate_task,
    )
    t_load = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_task,
    )
    t_validate >> t_load