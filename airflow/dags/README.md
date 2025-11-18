# Airflow DAGs (stocks pipeline)

Modules live in `lib/` and keep DAG files small:
- `lib/config.py` – shared constants and bucket/prefix helpers.
- `lib/bronze_ingest.py` – yfinance fetch + parquet upload + manifest.
- `lib/spark_job.py` – Spark session builder and raw->curated transform.
- `lib/validation.py` – curated data checks and partition discovery.
- `lib/snowflake_loader.py` – Snowflake context + `write_pandas` loader.
- `lib/tickers.py` – single source for default ticker list + parser.

DAGs call these helpers:
- `stock_eod_to_minio.py` – on-demand bronze ingest (params for tickers/date range).
- `transform_raw_to_curated.py` – Spark transform, optional `load_date` param.
- `validate_and_load_to_snowflake.py` – validates curated partition then loads to Snowflake.

Add new logic in `lib/` and keep DAG files focused on scheduling/wiring.
