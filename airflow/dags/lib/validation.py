"""Validation helpers for curated data."""
from __future__ import annotations
import re
from typing import List, Optional

import pandas as pd
from airflow.exceptions import AirflowFailException

from . import config
from .s3 import storage_options

REQUIRED_COLUMNS = ["date", "ticker", "open", "high", "low", "close", "volume", "load_ts"]


def _find_latest_partition(fs) -> str:
    root = f"{config.bucket()}/{config.curated_prefix()}"
    entries = fs.ls(root)
    dates: List[str] = []
    for p in entries:
        m = re.search(r"load_date=(\d{4}-\d{2}-\d{2})/?$", p)
        if m:
            dates.append(m.group(1))
    if not dates:
        raise AirflowFailException(
            f"No curated partitions found under s3://{config.bucket()}/{config.curated_prefix()}"
        )
    return sorted(dates)[-1]


def read_curated(load_date: Optional[str]) -> tuple[pd.DataFrame, str]:
    import s3fs

    fs = s3fs.S3FileSystem(**storage_options())
    target_date = load_date or _find_latest_partition(fs)
    path = f"s3://{config.bucket()}/{config.curated_prefix()}/load_date={target_date}/"
    df = pd.read_parquet(path, storage_options=storage_options())
    return df, target_date


def validate_curated(load_date: Optional[str]) -> tuple[pd.DataFrame, str]:
    df, date_used = read_curated(load_date)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise AirflowFailException(f"Missing columns: {missing}")

    for c in ["ticker", "date", "close"]:
        if df[c].isna().any():
            raise AirflowFailException(f"Nulls in {c}: {int(df[c].isna().sum())}")

    dups = int(df.duplicated(subset=["ticker", "date"]).sum())
    if dups > 0:
        raise AirflowFailException(f"Duplicate rows on (ticker,date): {dups}")

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype("int64")
    df = df[REQUIRED_COLUMNS].copy()
    return df, date_used
