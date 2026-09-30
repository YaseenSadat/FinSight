from __future__ import annotations

from datetime import UTC, date, datetime

import pandas as pd
import pytest

from finsight.datasets import OHLCVDataset, get_dataset
from finsight.errors import ContractError, UnknownDatasetError
from finsight.quality import Severity, run_checks
from tests.conftest import bars, make_request

DATASET = OHLCVDataset()
FETCHED = datetime(2024, 2, 1, 12, tzinfo=UTC)


def bronze(frame: pd.DataFrame, symbol: str = "AAPL", fetched: datetime = FETCHED) -> pd.DataFrame:
    request = make_request(symbol)
    table = DATASET.to_bronze(
        frame, request=request, symbol=symbol, run_id="r1", fetched_at=fetched
    )
    return table.to_pandas()


def test_registry() -> None:
    assert get_dataset("OHLCV").name == "ohlcv"
    with pytest.raises(UnknownDatasetError):
        get_dataset("fundamentals")


def test_bronze_adds_lineage_and_enforces_types() -> None:
    table = DATASET.to_bronze(
        bars(["2024-01-02", "2024-01-03"]),
        request=make_request(),
        symbol="AAPL",
        run_id="r1",
        fetched_at=FETCHED,
    )
    assert table.schema == DATASET.bronze_schema
    row = table.to_pylist()[0]
    assert row["symbol"] == "AAPL"
    assert row["provider"] == "synthetic"
    assert row["interval"] == "1d"
    assert row["adjustment"] == "raw"
    assert row["date"] == date(2024, 1, 2)


def test_bronze_rejects_missing_columns() -> None:
    frame = bars(["2024-01-02"]).drop(columns=["adj_close"])
    with pytest.raises(ContractError, match="adj_close"):
        bronze(frame)


def test_bronze_rejects_uncastable_values() -> None:
    frame = bars(["2024-01-02"])
    frame["date"] = ["not a date"]
    with pytest.raises(ContractError, match="does not match the contract"):
        bronze(frame)


def test_conform_drops_empty_rows_and_keeps_latest_duplicate() -> None:
    first = bronze(bars(["2024-01-02", "2024-01-03"]))
    newer = bronze(bars(["2024-01-03"], close=[555.0]), fetched=datetime(2024, 2, 2, tzinfo=UTC))
    empty = bars(["2024-01-04"])
    empty[["open", "high", "low", "close"]] = float("nan")
    combined = pd.concat([newer, first, bronze(empty)], ignore_index=True)

    result = DATASET.conform(combined)

    assert result.input_rows == 4
    assert result.dropped_rows == 1
    assert result.duplicate_rows == 1
    assert list(result.frame.columns) == DATASET.schema.names
    assert result.frame["close"].tolist() == [101.0, 555.0]
    assert "ingested_at" in result.frame


def _checked(frame: pd.DataFrame, **request_kwargs: str) -> dict[str, int]:
    conformed = DATASET.conform(bronze(frame)).frame
    report = run_checks(conformed, DATASET.checks(make_request(**request_kwargs)))
    return {r.name: r.failed_rows for r in report.results if not r.passed}


def test_clean_data_passes_all_checks() -> None:
    assert _checked(bars(["2024-01-02", "2024-01-03"])) == {}


def test_error_checks() -> None:
    frame = bars(["2024-01-02", "2024-01-03", "2024-03-01"], close=[101.0, -1.0, 103.0])
    frame.loc[0, "volume"] = -5
    failures = _checked(frame)
    assert failures["positive_prices"] == 1
    assert failures["non_negative_volume"] == 1
    assert failures["within_requested_window"] == 1


def test_warning_checks_do_not_block() -> None:
    frame = bars(["2024-01-02"], high=[90.0])
    conformed = DATASET.conform(bronze(frame)).frame
    report = run_checks(conformed, DATASET.checks(make_request()))
    assert report.passed
    assert {r.name for r in report.warnings} == {"high_gte_low", "open_close_within_range"}
    assert all(r.severity == Severity.WARNING for r in report.warnings)
    assert "high_gte_low (warning): 1 rows" in report.summary()


def test_weekly_window_allows_period_start_before_request_start() -> None:
    frame = bars(["2024-01-01"])  # Monday label for the week containing Jan 3
    failures = _checked(frame, start="2024-01-03", interval="1wk")
    assert "within_requested_window" not in failures
