from datetime import datetime, timezone
from pathlib import Path
import json
import tempfile

import pandas as pd
import yfinance as yf

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3

DAG_ID = "stock_eod_to_minio"

default_args = {"owner": "airflow", "retries": 0}

def _fetch_and_upload(**context):
    tickers_param = context["params"].get("tickers", "AAPL,MSFT")
    period_param = context["params"].get("period", "1y")
    interval_param = context["params"].get("interval", "1d")

    tickers = [t.strip().upper() for t in tickers_param.split(",") if t.strip()]
    if not tickers:
        raise ValueError("No tickers provided")

    bucket = Variable.get("S3_BUCKET", default_var="stock-etl")
    prefix = Variable.get("S3_PREFIX", default_var="raw/eod")

    hook = AwsBaseHook(aws_conn_id="minio_s3", client_type="s3")
    creds = hook.get_credentials()
    extra = hook.get_connection(hook.aws_conn_id).extra_dejson
    endpoint_url = extra.get("endpoint_url")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds.access_key,
        aws_secret_access_key=creds.secret_key,
        aws_session_token=creds.token,
        endpoint_url=endpoint_url,
        region_name=extra.get("region_name", "us-east-1"),
    )

    load_ts = datetime.now(timezone.utc)
    load_date = load_ts.strftime("%Y-%m-%d")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)

        for t in tickers:
            df = yf.Ticker(t).history(period=period_param, interval=interval_param, auto_adjust=True)
            if df.empty:
                print(f"[WARN] No data for {t}")
                continue

            df = df.reset_index().rename(columns={
                "Date": "date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume"
            })
            df["ticker"] = t
            df["load_ts"] = load_ts.isoformat()

            local_parquet = tmp_path / f"{t}.parquet"
            # FIX: Use coerce_timestamps to microseconds for Spark compatibility
            df.to_parquet(
                local_parquet,
                index=False,
                engine="pyarrow",
                coerce_timestamps="ms",  # Convert to milliseconds
                allow_truncated_timestamps=True
            )

            key = f"{prefix}/ticker={t}/load_date={load_date}/{t}.parquet"
            print(f"Uploading {local_parquet} to s3://{bucket}/{key}")
            s3.upload_file(str(local_parquet), bucket, key)

    manifest = {
        "tickers": tickers,
        "period": period_param,
        "interval": interval_param,
        "bucket": bucket,
        "prefix": prefix,
        "load_ts": load_ts.isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=f"{prefix}/_manifests/{load_date}.json",
        Body=json.dumps(manifest).encode("utf-8"),
        ContentType="application/json",
    )

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dev", "stocks", "minio", "parquet"],
    params={"tickers": "AAPL,MSFT", "period": "1y", "interval": "1d"},
) as dag:
    fetch_and_upload = PythonOperator(
        task_id="fetch_and_upload",
        python_callable=_fetch_and_upload,
        provide_context=True,
    )