"""Spark transform helpers for raw -> curated."""
from __future__ import annotations
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max as spark_max, lit

from . import config
from .s3 import get_client


def build_spark() -> SparkSession:
    """Create a SparkSession configured for MinIO S3A access."""
    _, extra, creds = get_client()
    endpoint_url = extra.get("endpoint_url") or ""

    return (
        SparkSession.builder.appName("stocks_transform")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            endpoint_url.replace("http://", "").replace("https://", ""),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.access.key", creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.int96AsTimestamp", "true")
        .getOrCreate()
    )


def transform_raw_to_curated(load_date: Optional[str]) -> str:
    """Read raw parquet, filter by load_date (or latest), write curated, return date used."""
    spark = build_spark()
    bucket = config.bucket()
    raw_prefix = config.raw_prefix()
    curated_prefix = config.curated_prefix()

    all_path = f"s3a://{bucket}/{raw_prefix}/"
    df_all = spark.read.option("basePath", all_path).parquet(f"{all_path}ticker=*/load_date=*/*.parquet")

    if load_date:
        date_to_use = load_date
    else:
        latest_row = (
            df_all.select(to_date(col("load_date")).alias("ld"))
            .agg(spark_max("ld").alias("max_ld"))
            .collect()[0]
        )
        latest = latest_row["max_ld"]
        if latest is None:
            spark.stop()
            raise ValueError("No raw data found under raw/eod/")
        date_to_use = latest.strftime("%Y-%m-%d")

    df = df_all.where(col("load_date") == lit(date_to_use))

    out = (
        df.withColumn("date", to_date(col("date")))
        .select("date", "ticker", "open", "high", "low", "close", "volume", "load_ts")
        .orderBy("ticker", "date")
    )

    out_path = f"s3a://{bucket}/{curated_prefix}/load_date={date_to_use}/"
    out.write.mode("overwrite").parquet(out_path)

    spark.stop()
    return date_to_use
