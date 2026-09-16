"""Dataset contracts.

A dataset defines everything FinSight needs to handle one kind of market data
independently of where it comes from:

* ``provider_schema``: the columns every provider must return (the bronze
  contract).
* ``schema``: the conformed silver/gold table, including lineage columns.
* ``primary_key``: the columns that uniquely identify a row. The gold layer
  upserts on it, so re-running a request never creates duplicates.
* ``partition_by``: how gold files are laid out in storage.
* ``conform``: the bronze-to-silver transformation.
* ``checks``: the quality rules applied before publishing to gold.

Adding a dataset (dividends, splits, fundamentals, ...) means writing one
subclass and teaching at least one provider to serve it.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, ClassVar

import pandas as pd
import pyarrow as pa

from finsight.errors import ContractError
from finsight.models import FetchRequest
from finsight.quality import Check

#: Lineage columns added to every bronze record, in order.
LINEAGE_FIELDS = (
    pa.field("symbol", pa.string()),
    pa.field("provider", pa.string()),
    pa.field("run_id", pa.string()),
    pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
)


@dataclass(frozen=True)
class ConformResult:
    """Output of :meth:`Dataset.conform` plus row accounting for the manifest."""

    frame: pd.DataFrame
    input_rows: int
    dropped_rows: int
    duplicate_rows: int


class Dataset(ABC):
    """Base class for dataset contracts."""

    name: ClassVar[str]
    title: ClassVar[str]
    description: ClassVar[str]
    #: Columns providers must return, with their types.
    provider_schema: ClassVar[pa.Schema]
    #: Request attributes stored as columns (e.g. interval, adjustment).
    dimensions: ClassVar[tuple[str, ...]]
    #: Conformed silver/gold schema.
    schema: ClassVar[pa.Schema]
    primary_key: ClassVar[tuple[str, ...]]
    partition_by: ClassVar[tuple[str, ...]]
    order_by: ClassVar[tuple[str, ...]]
    #: Date column used to report each symbol's coverage in run manifests.
    date_column: ClassVar[str] = "date"
    #: Human-readable column documentation.
    column_docs: ClassVar[dict[str, str]]

    @property
    def bronze_schema(self) -> pa.Schema:
        dims = [pa.field(dim, pa.string()) for dim in self.dimensions]
        return pa.schema([*LINEAGE_FIELDS[:2], *dims, *LINEAGE_FIELDS[2:], *self.provider_schema])

    def to_bronze(
        self,
        frame: pd.DataFrame,
        *,
        request: FetchRequest,
        symbol: str,
        run_id: str,
        fetched_at: datetime,
    ) -> pa.Table:
        """Validate a provider frame against the contract and add lineage.

        Raises:
            ContractError: when columns are missing or cannot be cast.
        """
        expected = self.provider_schema.names
        missing = [column for column in expected if column not in frame.columns]
        if missing:
            raise ContractError(
                f"provider {request.provider!r} returned {self.name} data without "
                f"required column(s): {', '.join(missing)}"
            )

        out = self.prepare_provider_frame(frame[expected].copy())
        dims = request.to_dict()
        lineage: dict[str, Any] = {
            "symbol": symbol,
            "provider": request.provider,
            **{dim: str(dims[dim]) for dim in self.dimensions},
            "run_id": run_id,
            "fetched_at": pd.Timestamp(fetched_at).tz_convert("UTC").as_unit("us"),
        }
        for position, (column, value) in enumerate(lineage.items()):
            out.insert(position, column, value)
        try:
            return pa.Table.from_pandas(out, schema=self.bronze_schema, preserve_index=False)
        except (pa.ArrowInvalid, pa.ArrowTypeError, TypeError, ValueError) as exc:
            raise ContractError(
                f"provider {request.provider!r} returned {self.name} data that does not "
                f"match the contract: {exc}"
            ) from exc

    def prepare_provider_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        """Hook for light type coercion before the contract cast."""
        return frame

    def to_table(self, frame: pd.DataFrame) -> pa.Table:
        """Convert a conformed frame to an Arrow table with the dataset schema."""
        return pa.Table.from_pandas(
            frame[self.schema.names], schema=self.schema, preserve_index=False
        )

    def empty_table(self) -> pa.Table:
        return self.schema.empty_table()

    @abstractmethod
    def conform(self, bronze: pd.DataFrame) -> ConformResult:
        """Transform bronze records into the conformed silver shape."""

    @abstractmethod
    def checks(self, request: FetchRequest) -> list[Check]:
        """Quality rules applied to silver data before it is published."""
