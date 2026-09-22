# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-09-30

FinSight is reframed from a one-off stock ETL pipeline into a self-serve, reproducible market
data platform.

### Added

- `finsight` Python package with a public API: `fetch`, `load`, `query`, `catalog`.
- `finsight` CLI: `fetch`, `query`, `catalog`, `runs`, `providers`, `datasets`, `universes`,
  `config`, `storage init`, `snowflake sync`, `ui`.
- Streamlit web UI (`finsight ui`) for building requests, charting, SQL, and downloads.
- Provider abstraction with declared capabilities and a plugin entry point
  (`finsight.providers`).
- Yahoo Finance provider: OHLCV from 1-minute to monthly bars, with `raw` (as-traded,
  rebuilt from split history) and `adjusted` modes.
- Offline `synthetic` provider for demos and tests.
- Dataset contracts (`ohlcv`) with schema, primary key, partitioning, and quality checks.
- Idempotent gold layer: upserts on the primary key, so re-runs never duplicate rows.
- Run manifests with the request, versions, per-symbol row accounting, check results, and
  file hashes.
- Local Parquet + DuckDB storage as the default, with S3/MinIO as an option.
- Bundled symbol universes: `sp500`, `dow30`, `mag7`, `sector-etfs`, `index-etfs`.
- Optional Spark engine for the transform stage, with output identical to pandas.
- Optional Snowflake sink using `MERGE`.
- Airflow 3 platform stack: `finsight_fetch` (on demand) and `finsight_daily` (scheduled)
  DAGs, MinIO, and an optional Spark cluster profile. Configured entirely from environment
  variables.
- Test suite, CI (lint, types, tests on Python 3.11–3.13, Spark parity, platform build), and
  documentation.

### Changed

- Airflow 2.9 → 3.3; Spark 3.5 → 4.1; DAG logic moved into the library.
- MinIO image switched to `cgr.dev/chainguard/minio`, because MinIO no longer publishes
  community images to Docker Hub.

### Removed

- Hard-coded tickers, Airflow connections configured by hand, and the mandatory Snowflake
  dependency.
