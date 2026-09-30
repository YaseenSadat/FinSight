"""The bronze -> silver -> gold pipeline.

Each stage is a separate method so that orchestrators (Airflow) can run them
as separate tasks; they communicate only through storage and the run
manifest. :meth:`Pipeline.run` executes all three in-process.

* **ingest** (bronze): fetch each symbol from the provider, check it against
  the dataset contract, and write it unchanged plus lineage columns.
* **transform** (silver): conform bronze records (drop empty rows,
  de-duplicate, order) with the pandas or Spark engine.
* **publish** (gold): run quality checks per symbol; merge passing data into
  the gold partition, upserting on the dataset's primary key.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal

import pandas as pd

from finsight.config import Settings
from finsight.datasets import Dataset, get_dataset
from finsight.errors import (
    ContractError,
    FinSightError,
    ProviderError,
    RequestError,
    SymbolNotFoundError,
)
from finsight.models import FetchRequest
from finsight.pipeline.manifest import (
    ManifestStore,
    RunManifest,
    SymbolResult,
    SymbolStatus,
)
from finsight.providers import Provider, get_provider
from finsight.quality import run_checks
from finsight.storage import Layout, Storage

logger = logging.getLogger(__name__)

Engine = Literal["pandas", "spark"]
ENGINES: tuple[Engine, ...] = ("pandas", "spark")


@dataclass(frozen=True)
class ProgressEvent:
    """Emitted after each symbol finishes a stage."""

    stage: str
    symbol: str | None
    completed: int
    total: int
    status: str | None = None


ProgressCallback = Callable[[ProgressEvent], None]


class Pipeline:
    """Runs data requests through the bronze, silver, and gold layers."""

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        storage: Storage | None = None,
        retry_backoff: float = 2.0,
    ):
        self.settings = settings or Settings.from_env()
        self.storage = storage or Storage.from_settings(self.settings)
        self.layout = Layout(self.storage)
        self.manifests = ManifestStore(self.storage)
        self.retry_backoff = retry_backoff

    # Planning ---------------------------------------------------------------------------

    def plan(self, request: FetchRequest) -> tuple[Provider, Dataset]:
        """Resolve the provider and dataset, and verify the request is supported.

        Raises:
            RequestError: if the provider cannot serve the request.
        """
        dataset = get_dataset(request.dataset)
        provider = get_provider(request.provider)
        problems = provider.check(request)
        if problems:
            raise RequestError(problems)
        return provider, dataset

    def run(
        self,
        request: FetchRequest,
        *,
        engine: Engine = "pandas",
        run_id: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> RunManifest:
        """Run all three stages and return the final manifest."""
        manifest = self.ingest(request, run_id=run_id, progress=progress)
        self.transform(manifest.run_id, engine=engine, progress=progress)
        return self.publish(manifest.run_id, progress=progress)

    # Bronze -----------------------------------------------------------------------------

    def ingest(
        self,
        request: FetchRequest,
        *,
        run_id: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> RunManifest:
        provider, dataset = self.plan(request)
        self.storage.ensure_root()

        manifest = RunManifest.start(request, run_id)
        manifest.provider_version = provider.version()
        manifest.begin_stage("ingest")
        self.manifests.save(manifest)
        logger.info("run %s: ingesting %d symbol(s)", manifest.run_id, len(request.symbols))

        total = len(request.symbols)
        workers = max(1, min(self.settings.max_workers, total))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="finsight-ingest") as pool:
            futures = [
                pool.submit(self._ingest_symbol, provider, dataset, request, manifest.run_id, s)
                for s in request.symbols
            ]
            for completed, future in enumerate(as_completed(futures), start=1):
                result = future.result()
                manifest.symbols[result.symbol] = result
                _emit(progress, "ingest", result.symbol, completed, total, result.status.value)

        manifest.end_stage("ingest", symbols=manifest.counts())
        self.manifests.save(manifest)
        return manifest

    def _ingest_symbol(
        self,
        provider: Provider,
        dataset: Dataset,
        request: FetchRequest,
        run_id: str,
        symbol: str,
    ) -> SymbolResult:
        """Fetch one symbol with retries. Never raises; failures go in the result."""
        result = SymbolResult(symbol=symbol)
        max_attempts = self.settings.max_retries + 1
        while True:
            result.attempts += 1
            try:
                fetched_at = datetime.now(UTC)
                frame = provider.fetch(request.for_symbol(symbol))
                if frame.empty:
                    result.status = SymbolStatus.NO_DATA
                    result.message = "provider returned no data for the requested window"
                    return result
                table = dataset.to_bronze(
                    frame, request=request, symbol=symbol, run_id=run_id, fetched_at=fetched_at
                )
                path = self.layout.bronze_file(request.dataset, run_id, symbol)
                self.storage.write_table(table, path)
                result.status = SymbolStatus.INGESTED
                result.bronze_path = path
                result.bronze_rows = table.num_rows
                result.message = None
                return result
            except SymbolNotFoundError as exc:
                result.status, result.message = SymbolStatus.NOT_FOUND, str(exc)
                return result
            except ProviderError as exc:
                result.status, result.message = SymbolStatus.FAILED, str(exc)
                if not exc.retryable or result.attempts >= max_attempts:
                    return result
                delay = self.retry_backoff * 2 ** (result.attempts - 1)
                logger.warning("%s: %s; retrying in %.1fs", symbol, exc, delay)
                time.sleep(delay)
            except ContractError as exc:
                result.status, result.message = SymbolStatus.FAILED, str(exc)
                return result
            except Exception as exc:  # never let one symbol take down the batch
                logger.exception("%s: unexpected error during ingest", symbol)
                result.status = SymbolStatus.FAILED
                result.message = f"unexpected error: {type(exc).__name__}: {exc}"
                return result

    # Silver -----------------------------------------------------------------------------

    def transform(
        self,
        run_id: str,
        *,
        engine: Engine = "pandas",
        progress: ProgressCallback | None = None,
    ) -> RunManifest:
        if engine not in ENGINES:
            raise FinSightError(f"unknown engine {engine!r} (choose from {', '.join(ENGINES)})")
        manifest = self.manifests.load(run_id)
        dataset = get_dataset(manifest.request.dataset)
        manifest.engine = engine
        manifest.begin_stage("transform")

        # Re-running a stage must be safe (Airflow retries), so start from a clean slate.
        self.storage.delete(self.layout.silver_dir(dataset.name, run_id), recursive=True)
        candidates = [r for r in manifest.symbols.values() if r.bronze_path]

        if engine == "spark":
            from finsight.engines.spark import transform_run

            stats = transform_run(self, manifest, dataset)
            for result in candidates:
                silver_rows, dropped, duplicates = stats.get(result.symbol, (0, 0, 0))
                self._record_transform(result, silver_rows, dropped, duplicates)
        else:
            for completed, result in enumerate(candidates, start=1):
                bronze = self.storage.read_table(result.bronze_path or "").to_pandas()
                conformed = dataset.conform(bronze)
                if len(conformed.frame):
                    path = self.layout.silver_file(dataset.name, run_id, result.symbol)
                    self.storage.write_table(dataset.to_table(conformed.frame), path)
                self._record_transform(
                    result, len(conformed.frame), conformed.dropped_rows, conformed.duplicate_rows
                )
                _emit(progress, "transform", result.symbol, completed, len(candidates))

        manifest.end_stage("transform", engine=engine, symbols=manifest.counts())
        self.manifests.save(manifest)
        return manifest

    @staticmethod
    def _record_transform(result: SymbolResult, rows: int, dropped: int, duplicates: int) -> None:
        result.silver_rows, result.dropped_rows, result.duplicate_rows = rows, dropped, duplicates
        if rows:
            result.status = SymbolStatus.TRANSFORMED
        else:
            result.status = SymbolStatus.NO_DATA
            result.message = "all rows were empty after conforming"

    # Gold -------------------------------------------------------------------------------

    def publish(self, run_id: str, *, progress: ProgressCallback | None = None) -> RunManifest:
        manifest = self.manifests.load(run_id)
        dataset = get_dataset(manifest.request.dataset)
        manifest.begin_stage("publish")

        silver_files = self.storage.glob(
            f"{self.layout.silver_dir(dataset.name, run_id)}/*.parquet"
        )
        silver = self.storage.read_tables(silver_files, dataset.schema).to_pandas()
        checks = dataset.checks(manifest.request)
        candidates = [r for r in manifest.symbols.values() if r.silver_rows]

        for completed, result in enumerate(candidates, start=1):
            frame = silver.loc[silver["symbol"] == result.symbol].reset_index(drop=True)
            report = run_checks(frame, checks)
            result.checks = list(report.results)
            if not report.passed:
                result.status = SymbolStatus.REJECTED
                result.message = f"failed quality checks: {report.summary()}"
            else:
                self._merge_into_gold(dataset, frame, result)
                result.status = SymbolStatus.PUBLISHED
                result.message = f"warnings: {report.summary()}" if report.warnings else None
            _emit(progress, "publish", result.symbol, completed, len(candidates), result.status)

        manifest.end_stage("publish", symbols=manifest.counts())
        manifest.finalize()
        self.manifests.save(manifest)

        if self.storage.is_local:
            from finsight.warehouse.duckdb import refresh_catalog

            refresh_catalog(self.storage)
        logger.info("run %s finished: %s %s", run_id, manifest.status.value, manifest.counts())
        return manifest

    def _merge_into_gold(self, dataset: Dataset, frame: pd.DataFrame, result: SymbolResult) -> None:
        partition = {column: str(frame[column].iloc[0]) for column in dataset.partition_by}
        path = self.layout.gold_file(dataset.name, partition)
        key = list(dataset.primary_key)

        if self.storage.exists(path):
            existing = self.storage.read_table(path).cast(dataset.schema).to_pandas()
            overlap = frame[key].merge(existing[key], on=key, how="inner")
            merged = pd.concat([existing, frame], ignore_index=True)
            merged = merged.drop_duplicates(subset=key, keep="last")
            updated = len(overlap)
        else:
            merged, updated = frame, 0
        merged = merged.sort_values(list(dataset.order_by), kind="stable").reset_index(drop=True)

        written = self.storage.write_table(dataset.to_table(merged), path)
        dates = pd.to_datetime(frame[dataset.date_column])
        result.gold_path = path
        result.gold_sha256 = written.sha256
        result.gold_total_rows = len(merged)
        result.rows_updated = updated
        result.rows_inserted = len(frame) - updated
        result.first_date = dates.min().date().isoformat()
        result.last_date = dates.max().date().isoformat()


def _emit(
    progress: ProgressCallback | None,
    stage: str,
    symbol: str | None,
    completed: int,
    total: int,
    status: str | None = None,
) -> None:
    if progress is not None:
        progress(ProgressEvent(stage, symbol, completed, total, status))
