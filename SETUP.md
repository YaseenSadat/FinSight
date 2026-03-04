# FinSight Setup Guide

Complete walkthrough to get FinSight running from scratch.

---

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- A [Snowflake](https://signup.snowflake.com/) account (free trial works)

---

## 1. Clone the Repo

```bash
git clone <repo-url>
cd FinSight
```

---

## 2. Create the `.env` File

Create a `.env` file in the project root (it is gitignored, so you must create it manually):

```env
AIRFLOW_UID=50000
AIRFLOW_GID=0

MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
```

You can use any username/password for MinIO — just keep them consistent here.

---

## 3. Build and Start the Stack

```bash
docker compose build --no-cache
```

Then start Postgres and MinIO first:

```bash
docker compose up -d postgres minio
```

Initialize the Airflow database and create the admin user:

```bash
docker compose run --rm airflow-init bash -c "airflow db init && airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin || true"
```

Start the MinIO bucket setup:

```bash
docker compose up minio-setup
```

Finally, start Airflow:

```bash
docker compose up -d airflow-webserver airflow-scheduler
```

---

## 4. Verify Services

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9090 | your .env values |

MinIO should have a `stock-etl` bucket created automatically. If it's missing, run `docker compose up minio-setup` again.

---

## 5. Set Up Snowflake

Log into your Snowflake account and run the following in a worksheet:

```sql
CREATE DATABASE IF NOT EXISTS STOCKS;
CREATE SCHEMA IF NOT EXISTS STOCKS.CURATED;
CREATE OR REPLACE TABLE STOCKS.CURATED.EOD_PRICES (
    TICKER    VARCHAR,
    DATE      DATE,
    OPEN      FLOAT,
    HIGH      FLOAT,
    LOW       FLOAT,
    CLOSE     FLOAT,
    VOLUME    BIGINT,
    LOAD_TS   TIMESTAMP
);
```

Your **account identifier** is the subdomain in your Snowflake URL:
```
https://<account-identifier>.snowflakecomputing.com
```
For new-style URLs (`https://app.snowflake.com/orgname/accountname/`), combine them as `orgname-accountname`.

---

## 6. Configure Airflow Connections

Go to **Airflow UI → Admin → Connections → "+" (Add new)**.

### MinIO (S3)

| Field | Value |
|---|---|
| Conn ID | `minio_s3` |
| Conn Type | `Amazon Web Services` |
| AWS Access Key ID | your `MINIO_ROOT_USER` |
| AWS Secret Access Key | your `MINIO_ROOT_PASSWORD` |
| Extra | `{"endpoint_url": "http://minio:9000"}` |

### Snowflake

| Field | Value |
|---|---|
| Conn ID | `snowflake_conn` |
| Conn Type | `Snowflake` |
| Login | your Snowflake username |
| Password | your Snowflake password |
| Account | your account identifier (e.g. `abc123-xy45678`) |
| Warehouse | `COMPUTE_WH` |
| Database | `STOCKS` |
| Schema | *(leave blank)* |
| Role | `ACCOUNTADMIN` |
| Extra | `{"account": "...", "warehouse": "COMPUTE_WH", "database": "STOCKS", "role": "ACCOUNTADMIN"}` |

> **Note:** Leave the Region and Schema fields blank. Putting anything in Schema causes it to be appended to the account URL, breaking the connection.

---

## 7. Run the DAGs

In the Airflow UI, trigger each DAG in order and wait for it to go green before starting the next:

1. **`stock_eod_to_minio`** — fetches EOD stock prices and writes raw parquet to MinIO (Bronze)
2. **`transform_raw_to_curated`** — Spark job that curates the raw data (Silver)
3. **`validate_and_load_to_snowflake`** — validates and loads the final data into Snowflake (Gold)

---

## 8. Verify in Snowflake

```sql
SELECT COUNT(*) FROM STOCKS.CURATED.EOD_PRICES;
SELECT * FROM STOCKS.CURATED.EOD_PRICES LIMIT 10;
```

---

## Restarting After a Shutdown

```bash
docker compose up -d
```

No need to re-initialize the DB or recreate connections — those are persisted in Docker volumes.

To tear everything down completely (including volumes):

```bash
docker compose down -v
```
