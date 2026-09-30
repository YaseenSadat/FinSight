"""FinSight DAGs.

Two DAGs share the same four-stage task chain. Each task is a thin call into
the FinSight library; all logic lives in ``finsight.pipeline``:

    plan -> ingest (bronze) -> transform (silver) -> publish (gold) -> [load_snowflake]

* ``finsight_fetch``: on demand. Trigger with parameters from the UI or with
  ``airflow dags trigger finsight_fetch --conf '{"symbols": "AAPL,MSFT", ...}'``.
* ``finsight_daily``: weekdays after the US close, refreshes the last few
  days of daily bars for a symbol universe so the gold layer stays current.

Stages communicate only through storage (MinIO) and the run manifest, so any
task can be retried on its own. (FinSight's run id is passed as
``finsight_run_id`` because ``run_id`` is reserved by Airflow.)
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from typing import Any

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

from finsight.models import Adjustment, Interval
from finsight.symbols import list_universes

INTERVALS = [interval.value for interval in Interval]
ADJUSTMENTS = [adjustment.value for adjustment in Adjustment]
UNIVERSES = sorted(list_universes())

DEFAULT_ARGS = {"owner": "finsight", "retries": 1, "retry_delay": timedelta(minutes=2)}


def _resolve_symbols(symbols: str | None, universe: str | None) -> list[str]:
    from finsight.symbols import get_universe, parse_symbols

    resolved = parse_symbols(symbols) if symbols else []
    if universe:
        resolved.extend(get_universe(universe).symbols)
    if not resolved:
        raise AirflowFailException("provide 'symbols', a 'universe', or both")
    return resolved


def _pipeline_chain(request_builder: Any) -> None:
    """Wire plan -> ingest -> transform -> publish -> optional Snowflake load."""

    @task
    def plan() -> dict[str, Any]:
        from finsight.errors import RequestError
        from finsight.pipeline import Pipeline, new_run_id

        request = request_builder(get_current_context())
        try:
            Pipeline().plan(request)
        except RequestError as exc:
            raise AirflowFailException(str(exc)) from exc
        return {"finsight_run_id": new_run_id(), "request": request.to_dict()}

    @task
    def ingest(planned: dict[str, Any]) -> str:
        from finsight.models import FetchRequest
        from finsight.pipeline import Pipeline

        request = FetchRequest.from_dict(planned["request"])
        manifest = Pipeline().ingest(request, run_id=planned["finsight_run_id"])
        print(f"ingest finished: {manifest.counts()}")
        return manifest.run_id

    @task
    def transform(finsight_run_id: str) -> str:
        from finsight.pipeline import Pipeline

        engine = get_current_context()["params"].get("engine", "pandas")
        manifest = Pipeline().transform(finsight_run_id, engine=engine)
        print(f"transform finished with {engine}: {manifest.counts()}")
        return finsight_run_id

    @task
    def publish(finsight_run_id: str) -> dict[str, Any]:
        from finsight.pipeline import Pipeline, RunStatus

        manifest = Pipeline().publish(finsight_run_id)
        summary = {
            "finsight_run_id": finsight_run_id,
            "status": manifest.status.value,
            **manifest.counts(),
        }
        print(f"publish finished: {summary}")
        for result in manifest.symbols.values():
            if result.message:
                print(f"  {result.symbol}: {result.status.value}: {result.message}")
        if manifest.status == RunStatus.FAILED:
            raise AirflowFailException(f"no symbols could be delivered: {summary}")
        return summary

    @task.short_circuit
    def snowflake_enabled() -> bool:
        return bool(get_current_context()["params"].get("load_to_snowflake"))

    @task
    def load_snowflake(summary: dict[str, Any]) -> dict[str, Any]:
        from finsight.api import sync_to_snowflake

        result = sync_to_snowflake(summary["finsight_run_id"])
        return {"table": result.table, "rows": result.rows_loaded}

    summary = publish(transform(ingest(plan())))
    gate = snowflake_enabled()
    summary >> gate >> load_snowflake(summary)


# On-demand -------------------------------------------------------------------------------


def _manual_request(context: Any) -> Any:
    from finsight.errors import RequestError
    from finsight.models import FetchRequest

    params = context["params"]
    try:
        return FetchRequest.create(
            _resolve_symbols(params.get("symbols"), params.get("universe")),
            params["start"],
            params.get("end") or None,
            interval=params["interval"],
            adjustment=params["adjustment"],
            provider=params["provider"],
        )
    except (RequestError, ValueError) as exc:
        raise AirflowFailException(str(exc)) from exc


@dag(
    dag_id="finsight_fetch",
    description="Fetch, validate, and publish market data on demand.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=4,
    default_args=DEFAULT_ARGS,
    tags=["finsight", "on-demand"],
    doc_md=__doc__,
    params={
        "symbols": Param(
            "AAPL,MSFT",
            type=["null", "string"],
            description="Comma or space separated symbols, e.g. AAPL, BRK-B, ^GSPC.",
        ),
        "universe": Param(
            None,
            type=["null", "string"],
            enum=[None, *UNIVERSES],
            description="Optional bundled symbol list, added to 'symbols'.",
        ),
        "start": Param("2024-01-01", type="string", format="date", description="Inclusive."),
        "end": Param(
            None,
            type=["null", "string"],
            format="date",
            description="Inclusive. Leave empty for today.",
        ),
        "interval": Param("1d", type="string", enum=INTERVALS),
        "adjustment": Param(
            "raw",
            type="string",
            enum=ADJUSTMENTS,
            description="raw = as traded (adj_close included); adjusted = splits + dividends.",
        ),
        "provider": Param("yahoo", type="string", description="See `finsight providers`."),
        "engine": Param(
            "pandas",
            type="string",
            enum=["pandas", "spark"],
            description="spark requires `docker compose --profile spark up -d`.",
        ),
        "load_to_snowflake": Param(
            False, type="boolean", description="Also MERGE the results into Snowflake."
        ),
    },
)
def finsight_fetch() -> None:
    _pipeline_chain(_manual_request)


finsight_fetch()


# Scheduled -------------------------------------------------------------------------------


def _daily_request(context: Any) -> Any:
    from finsight.models import FetchRequest

    params = context["params"]
    logical = context.get("logical_date")
    end = logical.date() if logical else date.today()
    start = end - timedelta(days=int(params["lookback_days"]))
    return FetchRequest.create(
        _resolve_symbols(None, params["universe"]),
        start,
        end,
        interval=Interval.D1,
        adjustment=params["adjustment"],
        provider=params["provider"],
    )


@dag(
    dag_id="finsight_daily",
    description="Refresh recent daily bars for a symbol universe after the US close.",
    schedule="30 22 * * 1-5",  # 22:30 UTC, after the 16:00 ET close in both EST and EDT
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["finsight", "scheduled"],
    doc_md=__doc__,
    params={
        "universe": Param(
            os.environ.get("FINSIGHT_DAILY_UNIVERSE", "dow30"), type="string", enum=UNIVERSES
        ),
        "lookback_days": Param(
            7,
            type="integer",
            minimum=1,
            maximum=365,
            description="Days re-fetched each run; overlapping bars are upserted, not duplicated.",
        ),
        "adjustment": Param("raw", type="string", enum=ADJUSTMENTS),
        "provider": Param("yahoo", type="string"),
        "engine": Param("pandas", type="string", enum=["pandas", "spark"]),
        "load_to_snowflake": Param(False, type="boolean"),
    },
)
def finsight_daily() -> None:
    _pipeline_chain(_daily_request)


finsight_daily()
