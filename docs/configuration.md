# Configuration

FinSight reads environment variables. A `.env` file in the working directory is loaded
automatically, and variables already set in the environment take precedence. Every setting is
optional.

Run `finsight config` to see the resolved configuration, with secrets masked.

## Storage

| Variable | Default | Description |
|---|---|---|
| `FINSIGHT_STORAGE` | `local` | `local` (a directory) or `s3` (S3, MinIO, or another S3-compatible store) |
| `FINSIGHT_DATA_DIR` | `./data` | Data directory for `local` storage |
| `FINSIGHT_S3_BUCKET` | – | Bucket for `s3` storage (required when `FINSIGHT_STORAGE=s3`) |
| `FINSIGHT_S3_PREFIX` | empty | Key prefix inside the bucket |
| `FINSIGHT_S3_ENDPOINT_URL` | – | Custom endpoint, e.g. `http://localhost:9000` for MinIO. Omit for AWS |
| `FINSIGHT_S3_REGION` | `us-east-1` | Region |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | – | Credentials. If unset, the standard AWS credential chain is used |

S3 storage requires `pip install "finsight[s3]"`.

## Pipeline

| Variable | Default | Description |
|---|---|---|
| `FINSIGHT_MAX_WORKERS` | `4` | Symbols fetched in parallel. Keep this modest to respect provider rate limits |
| `FINSIGHT_MAX_RETRIES` | `3` | Retries for rate limits and network errors, with exponential backoff |

## Spark engine

| Variable | Default | Description |
|---|---|---|
| `FINSIGHT_SPARK_MASTER` | `local[*]` | Spark master URL |
| `FINSIGHT_SPARK_DRIVER_HOST` | – | Hostname executors use to reach the driver (set in Docker) |
| `FINSIGHT_SPARK_JARS_DIR` | – | Directory of extra jars (Hadoop S3A) added to driver and executor classpaths |

The Spark engine requires `pip install "finsight[spark]"` and Java 17 or 21. For S3 storage,
Spark also needs `hadoop-aws` and the AWS SDK bundle on its classpath. The platform stack
provides them.

## Snowflake sink

| Variable | Default | Description |
|---|---|---|
| `SNOWFLAKE_ACCOUNT` | – | Account identifier, e.g. `orgname-accountname` |
| `SNOWFLAKE_USER` | – | User |
| `SNOWFLAKE_PASSWORD` | – | Password |
| `SNOWFLAKE_WAREHOUSE` | – | Warehouse used for loading |
| `SNOWFLAKE_DATABASE` | `FINSIGHT` | Created if missing |
| `SNOWFLAKE_SCHEMA` | `MARKET_DATA` | Created if missing |
| `SNOWFLAKE_ROLE` | – | Role (optional) |

Requires `pip install "finsight[snowflake]"`.

## Platform stack (docker compose)

These variables configure `docker-compose.yml` only:

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `finsight` / `finsight-secret` | MinIO credentials. Also used by Airflow tasks |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | `admin` / `admin` | Airflow login |
| `AIRFLOW_JWT_SECRET` | development value | Secret for Airflow's internal API tokens |
| `AIRFLOW_FERNET_KEY` | empty | Encrypts Airflow connection secrets |
| `FINSIGHT_S3_BUCKET` | `finsight` | Bucket used by the stack |
| `FINSIGHT_DAILY_UNIVERSE` | `dow30` | Universe refreshed by `finsight_daily` |

## Programmatic configuration

Every API function accepts a `Settings` object, which takes precedence over the environment:

```python
from pathlib import Path

import finsight
from finsight import Settings

settings = Settings(data_dir=Path("/mnt/market-data"), max_workers=8)
finsight.fetch("AAPL", start="2024-01-01", settings=settings)
```
