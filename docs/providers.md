# Providers and extending FinSight

## Built-in providers

| Name | Source | Datasets | Notes |
|---|---|---|---|
| `yahoo` | Yahoo Finance via [yfinance](https://github.com/ranaroussi/yfinance) | `ohlcv` | Free, no API key. Personal use only under Yahoo's terms. Intraday history is limited |
| `synthetic` | Generated locally | `ohlcv` | Deterministic random walk for demos and tests. **Not real data.** The symbol `NOTFOUND` behaves like an unknown ticker |

`finsight providers` prints each provider's datasets, intervals, history limits, and adjustment
modes.

### Yahoo interval limits

| Interval | History available | Upstream request span |
|---|---|---|
| `1m` | last 29 days | 7 days (FinSight splits longer windows automatically) |
| `5m`, `15m`, `30m` | last 59 days | – |
| `1h` | last 729 days | – |
| `1d`, `1wk`, `1mo` | full history | – |

Requests outside these limits are rejected before any network call.

## Adding a provider

A provider does four things:

1. **Declare its capabilities:** datasets, intervals (with history limits), and adjustments.
2. **Fetch one symbol** for a `SymbolRequest` and return a DataFrame in the dataset's provider
   contract. For `ohlcv` that is `ts` (tz-aware), `date`, `open`, `high`, `low`, `close`,
   `adj_close`, `volume`.
3. **Signal errors with the right exception**, so the pipeline can retry or classify them:

   | Situation | What to do |
   |---|---|
   | Symbol exists but has no data in the window | Return an empty DataFrame |
   | Unknown symbol | Raise `SymbolNotFoundError` |
   | Rate limit | Raise `RateLimitError` (retried) |
   | Timeout or connection error | Raise `TransientProviderError` (retried) |
   | Anything else | Raise `ProviderError` (not retried) |

4. **Optionally report a version** (`version()`), which is recorded in run manifests.

```python
# my_provider.py
import pandas as pd
import requests

from finsight.errors import RateLimitError, SymbolNotFoundError
from finsight.models import Adjustment, Interval, SymbolRequest
from finsight.providers import DatasetSupport, IntervalSupport, Provider, register_provider


class AcmeProvider(Provider):
    name = "acme"
    title = "Acme Market Data"
    description = "Daily bars from the Acme REST API."
    terms = "Requires an Acme subscription."

    @property
    def capabilities(self):
        return (
            DatasetSupport(
                dataset="ohlcv",
                intervals=(IntervalSupport(Interval.D1),),
                adjustments=(Adjustment.RAW,),
            ),
        )

    def fetch(self, request: SymbolRequest) -> pd.DataFrame:
        response = requests.get(
            f"https://api.acme.example/v1/bars/{request.symbol}",
            params={"from": request.start.isoformat(), "to": request.end.isoformat()},
            timeout=30,
        )
        if response.status_code == 404:
            raise SymbolNotFoundError(f"unknown symbol {request.symbol}", symbol=request.symbol)
        if response.status_code == 429:
            raise RateLimitError("rate limited", symbol=request.symbol)
        response.raise_for_status()

        rows = pd.DataFrame(response.json()["bars"])
        if rows.empty:
            return rows
        ts = pd.to_datetime(rows["date"]).dt.tz_localize("America/New_York")
        return pd.DataFrame(
            {
                "ts": ts.dt.tz_convert("UTC"),
                "date": ts.dt.date,
                "open": rows["o"],
                "high": rows["h"],
                "low": rows["l"],
                "close": rows["c"],
                "adj_close": rows["adj_c"],
                "volume": rows["v"],
            }
        )


register_provider(AcmeProvider.name, AcmeProvider)
```

Then use it like any other provider:

```python
import my_provider  # registers "acme"
import finsight

finsight.fetch("AAPL", start="2024-01-01", provider="acme")
```

### Distributing a provider as a plugin

Providers can live in their own package. Expose them under the `finsight.providers` entry-point
group and FinSight discovers them automatically. Plugin providers then work from the CLI, the
UI, and Airflow:

```toml
# pyproject.toml of your package
[project.entry-points."finsight.providers"]
acme = "finsight_acme:AcmeProvider"
```

### Testing a provider

Reuse the pipeline tests' approach: register your provider, run a small request against a temp
directory, and assert on the manifest.

```python
from finsight.config import Settings
from finsight.models import FetchRequest
from finsight.pipeline import Pipeline, RunStatus


def test_acme(tmp_path):
    pipeline = Pipeline(Settings(data_dir=tmp_path))
    request = FetchRequest.create("AAPL", "2024-01-02", "2024-01-05", provider="acme")
    assert pipeline.run(request).status == RunStatus.SUCCEEDED
```

## Adding a dataset

Subclass `finsight.datasets.Dataset` and register an instance:

| Attribute / method | Purpose |
|---|---|
| `name`, `title`, `description` | Identity, used in requests, storage paths, and the SQL view name |
| `provider_schema` | Arrow schema providers must return (the bronze contract) |
| `dimensions` | Request attributes stored as columns (e.g. `("interval", "adjustment")`, or `()` for event data) |
| `schema` | Arrow schema of silver and gold rows, including lineage columns |
| `primary_key` | Columns gold upserts on |
| `partition_by` | Gold directory layout |
| `order_by` | Row order within files |
| `date_column` | Date column used for coverage in manifests (default `date`) |
| `column_docs` | Documentation shown by `finsight datasets <name>` |
| `conform(bronze)` | Bronze → silver transformation; returns a `ConformResult` with row accounting |
| `checks(request)` | Quality rules (build them with `finsight.quality.not_null`, `unique`, `rule`) |

```python
import pyarrow as pa

from finsight.datasets import ConformResult, Dataset, register_dataset
from finsight.quality import not_null, rule, unique


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
    column_docs = {"amount": "Cash amount per share."}

    def conform(self, bronze):
        frame = bronze.rename(columns={"fetched_at": "ingested_at"})
        dupes = frame.duplicated(subset=list(self.primary_key), keep="last")
        frame = frame.loc[~dupes].sort_values(list(self.order_by))
        return ConformResult(frame[self.schema.names], len(bronze), 0, int(dupes.sum()))

    def checks(self, request):
        return [
            not_null(["symbol", "ex_date", "amount"]),
            unique(self.primary_key),
            rule("positive_amount", "amount must be > 0", lambda df: df["amount"] <= 0),
        ]


register_dataset(DividendsDataset())
```

Then teach a provider to serve it by adding a `DatasetSupport(dataset="dividends", ...)` entry
to its capabilities and handling `request.dataset == "dividends"` in `fetch`. The pipeline,
storage layout, manifests, CLI, and DuckDB view (`SELECT * FROM dividends`) work without
further changes. `tests/test_extensibility.py` runs this exact example end to end.
