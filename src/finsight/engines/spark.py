"""Spark engine for the silver (transform) stage.

Produces exactly the same silver output as the default pandas engine, using
the DataFrame API only (no Python UDFs), so Spark executors never need
FinSight or Python packages installed. Use it to spread large backfills
across a cluster::

    FINSIGHT_SPARK_MASTER=spark://spark-master:7077 finsight fetch ... --engine spark

Requires ``pip install 'finsight[spark]'`` and a Java runtime supported by
your Spark version. S3/MinIO storage also needs the ``hadoop-aws`` jars on the
classpath; point ``FINSIGHT_SPARK_JARS_DIR`` at a directory containing them.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

import pyarrow as pa

from finsight.config import Settings
from finsight.errors import ConfigError

if TYPE_CHECKING:
    from finsight.datasets import Dataset
    from finsight.pipeline.manifest import RunManifest
    from finsight.pipeline.runner import Pipeline
    from finsight.storage import Storage

logger = logging.getLogger(__name__)


def build_session(settings: Settings, storage: Storage) -> Any:
    """Create a SparkSession configured for FinSight's storage backend."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise ConfigError("the Spark engine requires: pip install 'finsight[spark]'") from exc

    builder = (
        SparkSession.builder.appName("finsight-transform")
        .master(settings.spark_master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.sources.partitionOverwriteMode", "static")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if settings.spark_driver_host:
        builder = builder.config("spark.driver.host", settings.spark_driver_host).config(
            "spark.driver.bindAddress", "0.0.0.0"
        )
    elif settings.spark_master.startswith("local"):
        # Avoid binding to the machine's hostname, which often fails on laptops/VPNs.
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    if settings.spark_jars_dir:
        classpath = f"{settings.spark_jars_dir.rstrip('/')}/*"
        builder = builder.config("spark.driver.extraClassPath", classpath).config(
            "spark.executor.extraClassPath", classpath
        )
    if storage.scheme == "s3":
        endpoint = settings.s3_endpoint_url or f"https://s3.{settings.s3_region}.amazonaws.com"
        secure = urlparse(endpoint).scheme == "https"
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.s3_region)
            .config(
                "spark.hadoop.fs.s3a.path.style.access", str(bool(settings.s3_endpoint_url)).lower()
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(secure).lower())
        )
        if settings.aws_access_key_id and settings.aws_secret_access_key:
            builder = (
                builder.config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                )
                .config("spark.hadoop.fs.s3a.access.key", settings.aws_access_key_id)
                .config("spark.hadoop.fs.s3a.secret.key", settings.aws_secret_access_key)
            )
    return builder.getOrCreate()


def _spark_type(arrow_type: pa.DataType) -> Any:
    from pyspark.sql import types as T

    mapping = {
        pa.string(): T.StringType(),
        pa.float64(): T.DoubleType(),
        pa.int64(): T.LongType(),
        pa.date32(): T.DateType(),
    }
    if pa.types.is_timestamp(arrow_type):
        return T.TimestampType()
    if arrow_type not in mapping:
        raise ConfigError(f"no Spark type mapping for {arrow_type}")
    return mapping[arrow_type]


def transform_run(
    pipeline: Pipeline, manifest: RunManifest, dataset: Dataset
) -> dict[str, tuple[int, int, int]]:
    """Conform a run's bronze files into silver with Spark.

    Mirrors :meth:`finsight.datasets.ohlcv.OHLCVDataset.conform`: drop rows
    with no prices, keep the latest fetch of each primary key, and order the
    output.

    Returns:
        ``{symbol: (silver_rows, dropped_rows, duplicate_rows)}``.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    storage = pipeline.storage
    bronze = [r.bronze_path for r in manifest.symbols.values() if r.bronze_path]
    if not bronze:
        return {}

    spark = build_session(pipeline.settings, storage)
    try:
        raw = spark.read.parquet(*[storage.uri(path, spark=True) for path in bronze])
        prices = [c for c in ("open", "high", "low", "close") if c in raw.columns]
        all_null = F.lit(True)
        for column in prices:
            all_null = all_null & F.col(column).isNull()
        nonempty = raw.filter(~all_null).cache()

        latest_first = Window.partitionBy(*dataset.primary_key).orderBy(F.col("fetched_at").desc())
        conformed = (
            nonempty.withColumn("_rank", F.row_number().over(latest_first))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
            .withColumnRenamed("fetched_at", "ingested_at")
            .select(
                *[F.col(f.name).cast(_spark_type(f.type)).alias(f.name) for f in dataset.schema]
            )
            .cache()
        )

        output = storage.uri(pipeline.layout.silver_dir(dataset.name, manifest.run_id), spark=True)
        conformed.orderBy(*dataset.order_by).write.mode("overwrite").parquet(output)

        def counts(frame: Any) -> dict[str, int]:
            return {
                row["symbol"]: row["count"] for row in frame.groupBy("symbol").count().collect()
            }

        before, kept, final = counts(raw), counts(nonempty), counts(conformed)
        return {
            symbol: (
                final.get(symbol, 0),
                total - kept.get(symbol, 0),
                kept.get(symbol, 0) - final.get(symbol, 0),
            )
            for symbol, total in before.items()
        }
    finally:
        spark.stop()
