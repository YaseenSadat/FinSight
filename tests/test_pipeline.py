from __future__ import annotations

import json

import pandas as pd
import pytest

from finsight.errors import (
    FinSightError,
    RequestError,
    RunNotFoundError,
    SymbolNotFoundError,
    TransientProviderError,
)
from finsight.models import SymbolRequest
from finsight.pipeline import Pipeline, RunStatus, SymbolStatus
from finsight.warehouse import duckdb as warehouse
from tests.conftest import bars, make_request


def test_end_to_end_run_writes_every_layer(pipeline: Pipeline) -> None:
    events = []
    manifest = pipeline.run(make_request(["AAPL", "MSFT"]), progress=events.append)

    assert manifest.status == RunStatus.SUCCEEDED
    assert {r.status for r in manifest.symbols.values()} == {SymbolStatus.PUBLISHED}
    aapl = manifest.symbols["AAPL"]
    assert aapl.bronze_rows == aapl.silver_rows == aapl.gold_total_rows == aapl.rows_inserted
    assert aapl.rows_updated == 0
    assert aapl.first_date == "2024-01-01"
    assert aapl.gold_sha256 is not None
    assert len(aapl.gold_sha256) == 64
    assert all(check.passed for check in aapl.checks)
    assert {e.stage for e in events} == {"ingest", "transform", "publish"}

    storage = pipeline.storage
    assert storage.exists(aapl.bronze_path or "")
    assert storage.exists(aapl.gold_path or "")
    assert (aapl.gold_path or "").endswith("gold/ohlcv/synthetic/1d/raw/AAPL.parquet")

    saved = json.loads(storage.read_text(pipeline.layout.manifest_file(manifest.run_id)))
    assert saved["status"] == "succeeded"
    assert saved["request"]["symbols"] == ["AAPL", "MSFT"]
    assert saved["stages"]["publish"]["finished_at"]


def test_rerun_is_idempotent(pipeline: Pipeline) -> None:
    request = make_request("AAPL")
    first = pipeline.run(request)
    second = pipeline.run(request)

    result = second.symbols["AAPL"]
    assert result.rows_inserted == 0
    assert result.rows_updated == first.symbols["AAPL"].rows_inserted
    count = warehouse.query(pipeline.storage, "SELECT count(*) AS n FROM ohlcv")["n"][0]
    assert count == first.symbols["AAPL"].gold_total_rows


def test_overlapping_windows_are_merged(pipeline: Pipeline) -> None:
    pipeline.run(make_request("AAPL", "2024-01-01", "2024-01-31"))
    manifest = pipeline.run(make_request("AAPL", "2024-01-15", "2024-02-29"))

    result = manifest.symbols["AAPL"]
    assert (result.rows_updated or 0) > 0
    assert (result.rows_inserted or 0) > 0
    frame = warehouse.query(pipeline.storage, "SELECT date FROM ohlcv ORDER BY date")
    assert frame["date"].is_unique
    assert str(frame["date"].min().date()) == "2024-01-01"
    assert str(frame["date"].max().date()) == "2024-02-29"


def test_raw_and_adjusted_series_are_stored_separately(pipeline: Pipeline) -> None:
    pipeline.run(make_request("AAPL", adjustment="raw"))
    pipeline.run(make_request("AAPL", adjustment="adjusted"))
    frame = warehouse.catalog(pipeline.storage)
    assert sorted(frame["adjustment"]) == ["adjusted", "raw"]


def test_partial_failure_is_isolated(pipeline: Pipeline) -> None:
    manifest = pipeline.run(make_request(["AAPL", "NOTFOUND"]))
    assert manifest.status == RunStatus.PARTIAL
    assert manifest.symbols["AAPL"].status == SymbolStatus.PUBLISHED
    missing = manifest.symbols["NOTFOUND"]
    assert missing.status == SymbolStatus.NOT_FOUND
    assert "not found" in (missing.message or "")


def test_all_failed(pipeline: Pipeline) -> None:
    assert pipeline.run(make_request("NOTFOUND")).status == RunStatus.FAILED


def test_no_data_is_not_a_failure(pipeline: Pipeline, stub_provider) -> None:
    stub_provider(lambda request: pd.DataFrame())
    manifest = pipeline.run(make_request("AAPL", provider="stub"))
    assert manifest.status == RunStatus.SUCCEEDED
    assert manifest.symbols["AAPL"].status == SymbolStatus.NO_DATA


def test_transient_errors_are_retried(pipeline: Pipeline, stub_provider) -> None:
    attempts = {"n": 0}

    def flaky(request: SymbolRequest) -> pd.DataFrame:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise TransientProviderError("timeout", symbol=request.symbol)
        return bars(["2024-01-02"])

    stub_provider(flaky)
    manifest = pipeline.run(make_request("AAPL", provider="stub"))
    assert manifest.symbols["AAPL"].status == SymbolStatus.PUBLISHED
    assert manifest.symbols["AAPL"].attempts == 3


def test_retries_are_bounded(pipeline: Pipeline, stub_provider) -> None:
    def always_down(request: SymbolRequest) -> pd.DataFrame:
        raise TransientProviderError("down", symbol=request.symbol)

    stub_provider(always_down)
    result = pipeline.run(make_request("AAPL", provider="stub")).symbols["AAPL"]
    assert result.status == SymbolStatus.FAILED
    assert result.attempts == pipeline.settings.max_retries + 1


def test_not_found_is_not_retried(pipeline: Pipeline, stub_provider) -> None:
    def missing(request: SymbolRequest) -> pd.DataFrame:
        raise SymbolNotFoundError("nope", symbol=request.symbol)

    stub = stub_provider(missing)
    pipeline.run(make_request("AAPL", provider="stub"))
    assert len(stub.calls) == 1


def test_unexpected_exceptions_are_contained(pipeline: Pipeline, stub_provider) -> None:
    def broken(request: SymbolRequest) -> pd.DataFrame:
        if request.symbol == "MSFT":
            raise ZeroDivisionError("bug")
        return bars(["2024-01-02"])

    stub_provider(broken)
    manifest = pipeline.run(make_request(["AAPL", "MSFT"], provider="stub"))
    assert manifest.symbols["AAPL"].status == SymbolStatus.PUBLISHED
    assert manifest.symbols["MSFT"].status == SymbolStatus.FAILED
    assert "ZeroDivisionError" in (manifest.symbols["MSFT"].message or "")


def test_contract_violation_fails_symbol(pipeline: Pipeline, stub_provider) -> None:
    stub_provider(lambda request: bars(["2024-01-02"]).drop(columns=["volume"]))
    result = pipeline.run(make_request("AAPL", provider="stub")).symbols["AAPL"]
    assert result.status == SymbolStatus.FAILED
    assert "volume" in (result.message or "")


def test_failed_quality_checks_block_publication(pipeline: Pipeline, stub_provider) -> None:
    stub_provider(lambda request: bars(["2024-01-02", "2024-01-03"], close=[101.0, -3.0]))
    manifest = pipeline.run(make_request("AAPL", provider="stub"))
    result = manifest.symbols["AAPL"]
    assert result.status == SymbolStatus.REJECTED
    assert result.gold_path is None
    assert "positive_prices" in (result.message or "")
    assert manifest.status == RunStatus.FAILED
    assert warehouse.catalog(pipeline.storage).empty


def test_warnings_are_recorded_but_published(pipeline: Pipeline, stub_provider) -> None:
    stub_provider(lambda request: bars(["2024-01-02"], high=[50.0]))
    result = pipeline.run(make_request("AAPL", provider="stub")).symbols["AAPL"]
    assert result.status == SymbolStatus.PUBLISHED
    assert "warnings" in (result.message or "")


def test_unsupported_request_fails_before_fetching(pipeline: Pipeline, stub_provider) -> None:
    stub = stub_provider(lambda request: bars(["2024-01-02"]))
    with pytest.raises(RequestError, match="interval"):
        pipeline.run(make_request("AAPL", provider="stub", interval="1h"))
    assert stub.calls == []


def test_stages_can_run_separately_and_rerun(pipeline: Pipeline) -> None:
    manifest = pipeline.ingest(make_request("AAPL"), run_id="manual-run")
    assert manifest.symbols["AAPL"].status == SymbolStatus.INGESTED
    pipeline.transform("manual-run")
    pipeline.transform("manual-run")  # retry-safe
    final = pipeline.publish("manual-run")
    assert final.status == RunStatus.SUCCEEDED
    assert final.engine == "pandas"


def test_unknown_engine_and_run(pipeline: Pipeline) -> None:
    manifest = pipeline.ingest(make_request("AAPL"))
    with pytest.raises(FinSightError, match="unknown engine"):
        pipeline.transform(manifest.run_id, engine="dask")  # type: ignore[arg-type]
    with pytest.raises(RunNotFoundError):
        pipeline.publish("does-not-exist")


def test_manifest_store_lists_newest_first(pipeline: Pipeline) -> None:
    first = pipeline.run(make_request("AAPL"), run_id="20240101T000000Z-aaaaaa")
    second = pipeline.run(make_request("MSFT"), run_id="20240102T000000Z-bbbbbb")
    assert pipeline.manifests.run_ids() == [second.run_id, first.run_id]
    latest = pipeline.manifests.latest()
    assert latest is not None
    assert latest.run_id == second.run_id
    assert latest.symbols["MSFT"].checks  # check results survive serialisation
