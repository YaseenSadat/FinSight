"""
Curated data validation helpers.

Reads a silver partition from MinIO and applies a set of data quality checks
before it is loaded into Snowflake. Raises AirflowFailException on any failure
so the DAG task is marked as failed rather than silently passing bad data.

Checks performed:
    - All required columns are present.
    - No nulls in ticker, date, or close.
    - No duplicate (ticker, date) pairs.
"""
from __future__ import annotations
import re
from typing import List, Optional

import pandas as pd
from airflow.exceptions import AirflowFailException # type: ignore

from . import config
from .s3 import storage_options

REQUIRED_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "load_ts"]


def _find_latest_partition(fs) -> str:
    """
    Scan the curated prefix in MinIO and return the most recent load_date.

    Raises AirflowFailException if no partitions are found.
    """
    root = f"{config.bucket()}/{config.curated_prefix()}"
    entries = fs.ls(root)  # list all objects under the curated prefix
    dates: List[str] = []
    for p in entries:
        m = re.search(r"load_date=(\d{4}-\d{2}-\d{2})/?$", p)  # extract date from partition folder name
        if m:
            dates.append(m.group(1))
    if not dates:
        raise AirflowFailException(
            f"No curated partitions found under s3://{config.bucket()}/{config.curated_prefix()}"
        )
    return sorted(dates)[-1]  # return the most recent date


def read_curated(load_date: Optional[str]) -> tuple[pd.DataFrame, str]:
    """
    Read a curated partition from MinIO into a pandas DataFrame.

    Args:
        load_date:  Partition date (YYYY-MM-DD), or None to use the latest.

    Returns:
        Tuple of (DataFrame, load_date string used).
    """
    import s3fs

    fs = s3fs.S3FileSystem(**storage_options())  # s3fs client for listing partitions
    target_date = load_date or _find_latest_partition(fs)  # fall back to latest if not specified
    path = f"s3://{config.bucket()}/{config.curated_prefix()}/load_date={target_date}/"
    df = pd.read_parquet(path, storage_options=storage_options())  # read the partition into a DataFrame
    return df, target_date


def validate_curated(load_date: Optional[str]) -> tuple[pd.DataFrame, str]:
    """
    Read and validate a curated partition.

    Runs data quality checks and returns the cleaned DataFrame. Raises
    AirflowFailException if any check fails.

    Args:
        load_date:  Partition date (YYYY-MM-DD), or None to use the latest.

    Returns:
        Tuple of (validated DataFrame, load_date string used).
    """
    df, date_used = read_curated(load_date)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]  # check all expected columns exist
    if missing:
        raise AirflowFailException(f"Missing columns: {missing}")

    for c in ["ticker", "date", "close"]:  # check no nulls in critical fields
        if df[c].isna().any():
            raise AirflowFailException(f"Nulls in {c}: {int(df[c].isna().sum())}")

    dups = int(df.duplicated(subset=["ticker", "date"]).sum())  # check no duplicate rows
    if dups > 0:
        raise AirflowFailException(f"Duplicate rows on (ticker,date): {dups}")

    df["date"] = pd.to_datetime(df["date"]).dt.date  # normalise date type
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")  # coerce volume to int
    df = df[REQUIRED_COLUMNS].copy()  # return only the expected columns in order
    return df, date_used
