# Option 2: Airflow platform

The Docker Compose stack runs FinSight as a small data platform:

- **Airflow 3** for orchestration and scheduling
- **MinIO** as S3-compatible storage for the bronze, silver, and gold layers
- **Postgres** for Airflow's metadata
- optionally a **Spark** master and worker for the transform stage

The DAGs call the same library as the CLI, so everything in the main README applies. Only the
storage location (MinIO instead of `./data`) and the entry point (Airflow instead of your
terminal) change.

> Just want data on your machine? Use [Option 1](../README.md#option-1-local-quick-start-no-infrastructure).
> This stack is for scheduled refreshes, a web-based orchestrator, or exercising the Spark and
> Snowflake paths.

## Requirements

- Docker with Compose v2 (Docker Desktop or Docker Engine)
- At least 4 GB of memory for Docker (6 GB or more with the Spark profile)
- About 3 GB of disk for images. The Spark profile downloads another 650 MB of Hadoop S3A jars
  on first start

## Start

```bash
cp .env.example .env                    # optional: change credentials and settings
docker compose up -d                    # Airflow + MinIO + Postgres
# or
docker compose --profile spark up -d    # ...plus Spark master + worker
```

The first start builds the Airflow image, which takes a few minutes. The one-shot
`airflow-init` service then:

1. migrates Airflow's database
2. creates the admin user
3. creates the MinIO bucket

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` (`AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD`) |
| MinIO console | http://localhost:9001 | `finsight` / `finsight-secret` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`) |
| MinIO S3 API | http://localhost:9000 | same as above |
| Spark master UI | http://localhost:8082 | – (spark profile only) |

No Airflow connections or variables need to be configured. All settings come from environment
variables, set in `docker-compose.yml` and your `.env`.

## DAGs

Both DAGs run the same chain of tasks. Each task is a single call into `finsight.pipeline`, and
tasks can be retried on their own:

```text
plan → ingest → transform → publish → snowflake_enabled → load_snowflake
```

### `finsight_fetch` (on demand)

Trigger it from the UI (**Trigger DAG** opens a parameter form) or from the CLI:

```bash
docker compose exec airflow-scheduler airflow dags unpause finsight_fetch
docker compose exec airflow-scheduler airflow dags trigger finsight_fetch --conf '{
  "symbols": "AAPL,MSFT", "universe": "index-etfs",
  "start": "2024-01-01", "end": null,
  "interval": "1d", "adjustment": "raw",
  "provider": "yahoo", "engine": "pandas", "load_to_snowflake": false
}'
```

| Parameter | Default | Notes |
|---|---|---|
| `symbols` | `AAPL,MSFT` | Comma or space separated |
| `universe` | none | Bundled list added to `symbols` (`sp500`, `dow30`, …) |
| `start` / `end` | `2024-01-01` / today | Inclusive |
| `interval` | `1d` | Must be supported by the provider |
| `adjustment` | `raw` | `raw` or `adjusted` |
| `provider` | `yahoo` | Any registered provider |
| `engine` | `pandas` | `spark` requires the spark profile |
| `load_to_snowflake` | `false` | Requires `SNOWFLAKE_*` settings in `.env` |

A run that delivers some symbols but not others succeeds, and the failures are listed in the
`publish` task log and the run manifest. The DAG run fails only if nothing could be delivered.

### `finsight_daily` (scheduled)

Runs at 22:30 UTC on weekdays, after the US close. It re-fetches the last `lookback_days`
(default 7) of daily bars for a universe (default `dow30`, or `FINSIGHT_DAILY_UNIVERSE`).
Overlapping bars are upserted, not duplicated. Like every DAG, it is **paused on creation**;
enable it in the UI when you want it.

## Inspecting results

From the host, point the CLI at MinIO:

```bash
export FINSIGHT_STORAGE=s3 FINSIGHT_S3_BUCKET=finsight \
       FINSIGHT_S3_ENDPOINT_URL=http://localhost:9000 \
       AWS_ACCESS_KEY_ID=finsight AWS_SECRET_ACCESS_KEY=finsight-secret
pip install -e ".[s3]"
finsight catalog
finsight runs show latest
finsight query "SELECT symbol, max(date) FROM ohlcv GROUP BY 1"
```

Or browse `finsight/bronze|silver|gold|runs` in the MinIO console.

## Spark engine

With `--profile spark`:

- `spark-jars` downloads the Hadoop S3A connector jars once, with checksum verification, into a
  shared volume.
- `spark-master` and `spark-worker` form a standalone cluster.
- Airflow tasks with `engine=spark` run the Spark driver in the scheduler container and
  distribute the transform to the worker.

The Spark engine uses only the DataFrame API. Executors need no Python packages, and its output
is identical to the pandas engine's.

Versions must line up. The Airflow image pins `pyspark==4.1.3` to match the
`apache/spark:4.1.3` cluster image, and both run Java 17.

## Snowflake

Set the `SNOWFLAKE_*` variables in `.env`, restart the stack, and trigger `finsight_fetch` with
`load_to_snowflake: true`. The target table (`FINSIGHT.MARKET_DATA.OHLCV` by default) is
created if needed. Rows are staged in a temporary table and merged on the primary key, so
re-loads never duplicate. From the host, `finsight snowflake sync --run-id <id>` does the same.

## Developing against the stack

`./src` is mounted into the Airflow containers and takes precedence over the installed package.
Library changes apply to the next task run without rebuilding. Rebuild (`docker compose build`)
only after changing dependencies in `pyproject.toml`.

## Operations

```bash
docker compose ps                         # service health
docker compose logs -f airflow-scheduler  # task execution logs
docker compose --profile spark down       # stop (data volumes are kept)
docker compose --profile spark down -v    # stop and delete all data
```

## Troubleshooting

| Symptom | Fix |
|---|---|
| `airflow-init` exits with "waiting for MinIO" | Check `docker compose logs minio`. Credentials must be at least 8 characters |
| Task fails with `Connection refused` to `spark-master` | Start the stack with `--profile spark`, or use `engine=pandas` |
| `ContractError` / `not_found` for a symbol | Check the symbol in Yahoo notation (`BRK-B`, not `BRK.B`) |
| Out-of-memory kills | Give Docker more memory, or lower `FINSIGHT_MAX_WORKERS` |
| Port already in use | Change the host port on the left side of `ports:` in `docker-compose.yml` |

## Security notes

The defaults are for local use only. Before exposing the stack anywhere, set strong values for
`MINIO_ROOT_PASSWORD`, `AIRFLOW_ADMIN_PASSWORD`, `AIRFLOW_JWT_SECRET`, and `AIRFLOW_FERNET_KEY`
in `.env`.
