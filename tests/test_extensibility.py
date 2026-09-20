"""The extension example from docs/providers.md, run end to end.

Proves that a new dataset and a provider serving it work through the pipeline,
storage, manifests, and DuckDB without any changes to FinSight itself.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import date

import pandas as pd
import pyarrow as pa
import pytest

from finsight.datasets import ConformResult, Dataset, register_dataset, unregister_dataset
from finsight.models import Adjustment, FetchRequest, Interval, SymbolRequest
from finsight.pipeline import Pipeline, RunStatus, SymbolStatus
from finsight.providers import DatasetSupport, IntervalSupport, Provider, register_provider
from finsight.providers.registry import unregister_provider
from finsight.quality import not_null, rule, unique
from finsight.warehouse import duckdb as warehouse


class DividendsDataset(Dataset):
    name = "dividends"
    title = "Cash dividends"
    description = "Cash dividends per share by ex-date."
    provider_schema = pa.schema([("ex_date", pa.date32()), ("amount", pa.float64())])
    dimensions = ()
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("ex_date", pa.date32()),
            ("amount", pa.float64()),
            ("provider", pa.string()),
            ("run_id", pa.string()),
            ("ingested_at", pa.timestamp("us", tz="UTC")),
        ]
    )
    primary_key = ("provider", "symbol", "ex_date")
    partition_by = ("provider", "symbol")
    order_by = ("symbol", "ex_date")
    date_column = "ex_date"
    column_docs = {"amount": "Cash amount per share."}  # noqa: RUF012

    def conform(self, bronze: pd.DataFrame) -> ConformResult:
        frame = bronze.rename(columns={"fetched_at": "ingested_at"})
        dupes = frame.duplicated(subset=list(self.primary_key), keep="last")
        frame = frame.loc[~dupes].sort_values(list(self.order_by))
        return ConformResult(frame[self.schema.names], len(bronze), 0, int(dupes.sum()))

    def checks(self, request: FetchRequest) -> list:
        return [
            not_null(["symbol", "ex_date", "amount"]),
            unique(self.primary_key),
            rule("positive_amount", "amount must be > 0", lambda df: df["amount"] <= 0),
        ]


class DividendProvider(Provider):
    name = "divs"
    title = "Dividend test provider"
    description = "Serves a fixed dividend schedule."

    @property
    def capabilities(self) -> tuple[DatasetSupport, ...]:
        return (
            DatasetSupport(
                dataset="dividends",
                intervals=(IntervalSupport(Interval.D1),),
                adjustments=(Adjustment.RAW,),
            ),
        )

    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        return pd.DataFrame(
            {"ex_date": [date(2024, 2, 9), date(2024, 5, 10)], "amount": [0.24, 0.25]}
        )


@pytest.fixture
def dividends() -> Iterator[None]:
    register_dataset(DividendsDataset(), replace=True)
    register_provider("divs", DividendProvider(), replace=True)
    yield
    unregister_dataset("dividends")
    unregister_provider("divs")


@pytest.mark.usefixtures("dividends")
def test_new_dataset_and_provider_work_end_to_end(pipeline: Pipeline) -> None:
    request = FetchRequest.create(
        ["AAPL", "MSFT"], "2024-01-01", "2024-12-31", provider="divs", dataset="dividends"
    )

    manifest = pipeline.run(request)
    pipeline.run(request)  # idempotent

    assert manifest.status == RunStatus.SUCCEEDED
    result = manifest.symbols["AAPL"]
    assert result.status == SymbolStatus.PUBLISHED
    assert (result.first_date, result.last_date) == ("2024-02-09", "2024-05-10")
    assert (result.gold_path or "").endswith("gold/dividends/divs/AAPL.parquet")

    frame = warehouse.query(pipeline.storage, "SELECT * FROM dividends ORDER BY symbol, ex_date")
    assert len(frame) == 4
    assert frame["amount"].tolist() == [0.24, 0.25, 0.24, 0.25]
