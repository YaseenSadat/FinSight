from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, max as spark_max, lit

DAG_ID = "transform_raw_to_curated"

def _run_spark(**context):
    # ---- Inputs & Airflow Variables
    params = context.get("params", {}) or {}
    load_date = params.get("load_date")  # optional
    bucket = Variable.get("S3_BUCKET", default_var="stock-etl")
    raw_prefix = Variable.get("S3_PREFIX", default_var="raw/eod")
    curated_prefix = "curated/eod"

    # ---- Airflow AWS/MinIO connection
    hook = AwsBaseHook(aws_conn_id="minio_s3", client_type="s3")
    creds = hook.get_credentials()
    extra = hook.get_connection(hook.aws_conn_id).extra_dejson
    endpoint_url = extra.get("endpoint_url")  # e.g. http://minio:9000

    # ---- Build Spark session with S3A support (UPDATED PACKAGES)
    spark = (
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

    # ---- Read ALL raw partitions with partition discovery enabled
    all_path = f"s3a://{bucket}/{raw_prefix}/"
    df_all = spark.read.option("basePath", all_path).parquet(f"{all_path}ticker=*/load_date=*/*.parquet")

    # Determine load_date (use param if set; otherwise discover latest)
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

    # ---- Simple cleaning/selection
    out = (
        df.withColumn("date", to_date(col("date")))
        .select("date", "ticker", "open", "high", "low", "close", "volume", "load_ts")
        .orderBy("ticker", "date")
    )

    # ---- Write curated partition
    out_path = f"s3a://{bucket}/{curated_prefix}/load_date={date_to_use}/"
    out.write.mode("overwrite").parquet(out_path)

    spark.stop()

default_args = {"owner": "airflow", "retries": 0}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # on-demand
    catchup=False,
    tags=["dev", "minio", "parquet", "stocks"],
) as dag:
    PythonOperator(
        task_id="run_spark",
        python_callable=_run_spark,
    )