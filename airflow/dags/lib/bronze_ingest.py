"""
Bronze ingestion helpers.

Fetches end-of-day OHLCV price history from Yahoo Finance using yfinance and
uploads the results as parquet files to MinIO. Each ticker gets its own
partition keyed by ticker symbol and load date. A JSON manifest is written at
the end of every batch run to record what was uploaded.
"""
from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
import tempfile
from typing import List

import pandas as pd
import yfinance as yf

from . import config, tickers
from .s3 import get_client, put_json


class IngestResult:
    """Holds the outcome of a completed ingestion batch."""

    def __init__(self, load_ts: datetime, load_date: str, uploaded: List[str]):
        self.load_ts = load_ts       # UTC timestamp of the run
        self.load_date = load_date   # YYYY-MM-DD string used as the partition key
        self.uploaded = uploaded     # List of ticker symbols successfully uploaded


def fetch_history(ticker: str, start_date: str, end_date: str, interval: str) -> pd.DataFrame | None:
    """
    Download price history for a single ticker from Yahoo Finance.

    Returns a normalised DataFrame with lowercase column names and a ticker
    column, or None if no data is available for the requested range.
    """
    df = yf.Ticker(ticker).history(
        start=start_date,
        end=end_date,
        interval=interval,
        auto_adjust=True,
    )
    if df.empty:
        return None
    df = df.reset_index().rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df["ticker"] = ticker
    return df


def upload_batch(
    tickers_param: str | None,
    start_date: str,
    end_date: str,
    interval: str,
) -> IngestResult:
    """
    Fetch and upload a batch of tickers to MinIO.

    For each ticker, downloads price history, writes a local parquet file,
    and uploads it to:

        s3://<bucket>/raw/eod/ticker=<TICKER>/load_date=<DATE>/<TICKER>.parquet

    After all uploads, writes a manifest JSON at:

        s3://<bucket>/raw/eod/_manifests/<DATE>.json

    Args:
        tickers_param:  Comma-separated ticker string, or None to use the default list.
        start_date:     History start date (YYYY-MM-DD).
        end_date:       History end date   (YYYY-MM-DD).
        interval:       Price interval (e.g. "1d").

    Returns:
        IngestResult with the load timestamp, date, and list of uploaded tickers.
    """
    s3, _, _ = get_client()
    bucket = config.bucket()
    prefix = config.raw_prefix()

    load_ts = datetime.now(timezone.utc)
    load_date = load_ts.strftime("%Y-%m-%d")

    requested = tickers.parse(tickers_param)
    uploaded: List[str] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        for t in requested:
            df = fetch_history(t, start_date, end_date, interval)
            if df is None:
                print(f"[WARN] No data for {t}")
                continue
            df["load_ts"] = load_ts.isoformat()

            local_parquet = tmp_path / f"{t}.parquet"
            df.to_parquet(
                local_parquet,
                index=False,
                engine="pyarrow",
                coerce_timestamps="ms",
                allow_truncated_timestamps=True,
            )

            key = f"{prefix}/ticker={t}/load_date={load_date}/{t}.parquet"
            print(f"Uploading {local_parquet} to s3://{bucket}/{key}")
            s3.upload_file(str(local_parquet), bucket, key)
            uploaded.append(t)

    manifest = {
        "tickers": requested,
        "uploaded": uploaded,
        "start_date": start_date,
        "end_date": end_date,
        "interval": interval,
        "bucket": bucket,
        "prefix": prefix,
        "load_ts": load_ts.isoformat(),
    }
    put_json(
        client=s3,
        bucket=bucket,
        key=f"{prefix}/_manifests/{load_date}.json",
        payload=manifest,
    )

    return IngestResult(load_ts=load_ts, load_date=load_date, uploaded=uploaded)
