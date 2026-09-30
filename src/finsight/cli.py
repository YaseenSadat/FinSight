"""The ``finsight`` command-line interface."""

from __future__ import annotations

import logging
import subprocess
import sys
from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any

import pandas as pd
import typer
from rich.console import Console
from rich.logging import RichHandler
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn, TimeElapsedColumn
from rich.table import Table

from finsight._version import __version__
from finsight.config import Settings
from finsight.errors import FinSightError, RequestError
from finsight.models import Adjustment, FetchRequest, Interval
from finsight.pipeline import (
    FAILURE_STATUSES,
    ManifestStore,
    Pipeline,
    ProgressEvent,
    RunManifest,
    RunStatus,
    SymbolStatus,
)

app = typer.Typer(
    name="finsight",
    help="Fetch validated, query-ready market data into local Parquet + DuckDB (or S3/MinIO).",
    no_args_is_help=True,
    add_completion=False,
    rich_markup_mode="rich",
)
runs_app = typer.Typer(help="Inspect past runs and their manifests.", no_args_is_help=True)
snowflake_app = typer.Typer(help="Optional Snowflake sink.", no_args_is_help=True)
storage_app = typer.Typer(help="Storage utilities.", no_args_is_help=True)
app.add_typer(runs_app, name="runs")
app.add_typer(snowflake_app, name="snowflake")
app.add_typer(storage_app, name="storage")

console = Console()
err_console = Console(stderr=True)

#: Process exit codes.
EXIT_OK, EXIT_FAILED, EXIT_PARTIAL = 0, 1, 2

_STATUS_STYLE = {
    SymbolStatus.PUBLISHED: "green",
    SymbolStatus.NO_DATA: "yellow",
    SymbolStatus.NOT_FOUND: "red",
    SymbolStatus.FAILED: "red",
    SymbolStatus.REJECTED: "red",
}
_RUN_STYLE = {RunStatus.SUCCEEDED: "green", RunStatus.PARTIAL: "yellow", RunStatus.FAILED: "red"}


class Engine(StrEnum):
    pandas = "pandas"
    spark = "spark"


class OutputFormat(StrEnum):
    table = "table"
    csv = "csv"
    json = "json"
    parquet = "parquet"


def _settings() -> Settings:
    return Settings.from_env()


def _fail(message: str, code: int = EXIT_FAILED) -> typer.Exit:
    err_console.print(f"[bold red]error:[/] {message}")
    return typer.Exit(code)


def _version_callback(value: bool) -> None:
    if value:
        console.print(f"finsight {__version__}")
        raise typer.Exit()


@app.callback()
def _main(
    verbose: Annotated[
        bool, typer.Option("--verbose", "-v", help="Show progress logs from the pipeline.")
    ] = False,
    version: Annotated[
        bool | None,
        typer.Option("--version", callback=_version_callback, is_eager=True, help="Show version."),
    ] = None,
) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(message)s",
        handlers=[RichHandler(console=err_console, show_path=False, rich_tracebacks=True)],
    )
    for noisy in ("py4j", "urllib3", "botocore", "s3fs", "aiobotocore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


# fetch ---------------------------------------------------------------------------------


@app.command()
def fetch(
    start: Annotated[
        str, typer.Option("--start", "-s", help="First date (YYYY-MM-DD), inclusive.")
    ],
    symbols: Annotated[
        list[str] | None,
        typer.Argument(help="Symbols to fetch, e.g. AAPL MSFT or AAPL,MSFT.", show_default=False),
    ] = None,
    end: Annotated[
        str | None,
        typer.Option("--end", "-e", help="Last date (YYYY-MM-DD), inclusive. Defaults to today."),
    ] = None,
    universe: Annotated[
        list[str] | None,
        typer.Option(
            "--universe", "-u", help="Add a bundled symbol list (see `finsight universes`)."
        ),
    ] = None,
    interval: Annotated[Interval, typer.Option("--interval", "-i", help="Bar size.")] = Interval.D1,
    adjustment: Annotated[
        Adjustment,
        typer.Option(
            "--adjustment", "-a", help="raw (as traded) or adjusted (splits + dividends)."
        ),
    ] = Adjustment.RAW,
    provider: Annotated[str, typer.Option("--provider", "-p", help="Data provider.")] = "yahoo",
    dataset: Annotated[str, typer.Option(help="Dataset to fetch.")] = "ohlcv",
    engine: Annotated[Engine, typer.Option(help="Engine for the transform stage.")] = Engine.pandas,
    as_json: Annotated[
        bool, typer.Option("--json", help="Print the run manifest as JSON.")
    ] = False,
) -> None:
    """Fetch data, validate it, and publish it to the gold layer.

    Exit code is 0 when every symbol was delivered, 2 when some failed, and 1
    when nothing could be delivered.
    """
    from finsight.symbols import get_universe

    requested: list[str] = list(symbols or [])
    for name in universe or []:
        try:
            requested.extend(get_universe(name).symbols)
        except KeyError as exc:
            raise _fail(str(exc.args[0])) from None
    if not requested:
        raise _fail("give at least one symbol or --universe")

    try:
        request = FetchRequest.create(
            requested,
            start,
            end,
            interval=interval,
            adjustment=adjustment,
            provider=provider,
            dataset=dataset,
        )
        pipeline = Pipeline(_settings())
        pipeline.plan(request)
    except RequestError as exc:
        for problem in exc.problems:
            err_console.print(f"[bold red]error:[/] {problem}")
        raise typer.Exit(EXIT_FAILED) from None
    except FinSightError as exc:
        raise _fail(str(exc)) from None

    manifest = _run_with_progress(pipeline, request, engine.value, quiet=as_json)
    if as_json:
        console.print_json(manifest.to_json())
    else:
        _print_run(manifest, pipeline.settings)
    raise typer.Exit(_exit_code(manifest))


def _run_with_progress(
    pipeline: Pipeline, request: FetchRequest, engine: str, *, quiet: bool
) -> RunManifest:
    progress = Progress(
        TextColumn("[bold]{task.description:<10}"),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn("{task.fields[symbol]}", style="dim"),
        TimeElapsedColumn(),
        console=err_console,
        disable=quiet,
        transient=True,
    )
    tasks: dict[str, Any] = {}

    def on_progress(event: ProgressEvent) -> None:
        if event.stage not in tasks:
            tasks[event.stage] = progress.add_task(event.stage, total=event.total, symbol="")
        progress.update(
            tasks[event.stage],
            total=event.total,
            completed=event.completed,
            symbol=event.symbol or "",
        )

    total = len(request.symbols)
    with progress:
        for stage in ("ingest", "transform", "publish"):
            tasks[stage] = progress.add_task(stage, total=total, symbol="")
        return pipeline.run(request, engine=engine, progress=on_progress)  # type: ignore[arg-type]


def _exit_code(manifest: RunManifest) -> int:
    return {RunStatus.SUCCEEDED: EXIT_OK, RunStatus.PARTIAL: EXIT_PARTIAL}.get(
        manifest.status, EXIT_FAILED
    )


def _print_run(manifest: RunManifest, settings: Settings) -> None:
    request = manifest.request
    table = Table(
        title=(
            f"{request.dataset} · {request.provider} · {request.interval.value} · "
            f"{request.adjustment.value} · {request.start} → {request.end}"
        ),
        title_justify="left",
    )
    table.add_column("Symbol", style="bold")
    table.add_column("Status")
    table.add_column("Rows", justify="right")
    table.add_column("New", justify="right")
    table.add_column("Updated", justify="right")
    table.add_column("Coverage")
    table.add_column("Notes", overflow="fold", max_width=60)

    for result in manifest.symbols.values():
        style = _STATUS_STYLE.get(result.status, "")
        coverage = f"{result.first_date} → {result.last_date}" if result.first_date else ""
        table.add_row(
            result.symbol,
            f"[{style}]{result.status.value}[/]",
            _fmt(result.silver_rows),
            _fmt(result.rows_inserted),
            _fmt(result.rows_updated),
            coverage,
            result.message or "",
        )
    console.print(table)

    style = _RUN_STYLE.get(manifest.status, "")
    counts = ", ".join(f"{count} {status}" for status, count in manifest.counts().items())
    console.print(f"Run [bold]{manifest.run_id}[/] [{style}]{manifest.status.value}[/] ({counts})")
    if any(r.status == SymbolStatus.PUBLISHED for r in manifest.symbols.values()):
        console.print(f"Data: [cyan]{_location(settings)}[/]")
        console.print(
            'Next: [cyan]finsight query "SELECT * FROM ohlcv LIMIT 10"[/]  '
            "or  [cyan]finsight catalog[/]"
        )
    if any(r.status in FAILURE_STATUSES for r in manifest.symbols.values()):
        console.print(f"Details: [cyan]finsight runs show {manifest.run_id}[/]")


def _location(settings: Settings) -> str:
    if settings.storage == "local":
        return str(settings.data_dir.resolve())
    return f"s3://{settings.s3_bucket}/{settings.s3_prefix}".rstrip("/")


def _fmt(value: int | None) -> str:
    return "" if value is None else f"{value:,}"


# query / catalog -----------------------------------------------------------------------


@app.command()
def query(
    sql: Annotated[str, typer.Argument(help='SQL to run, e.g. "SELECT * FROM ohlcv LIMIT 5".')],
    fmt: Annotated[OutputFormat, typer.Option("--format", "-f", help="Output format.")] = (
        OutputFormat.table
    ),
    output: Annotated[
        Path | None, typer.Option("--output", "-o", help="Write results to a file.")
    ] = None,
    max_rows: Annotated[int, typer.Option(help="Rows to display in table format.")] = 50,
) -> None:
    """Query the gold layer with DuckDB SQL. Each dataset is a view (e.g. ohlcv)."""
    from finsight.api import query as run_query

    try:
        frame = run_query(sql, settings=_settings())
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    except Exception as exc:  # DuckDB parser/binder errors
        raise _fail(f"query failed: {exc}") from None
    _emit_frame(frame, fmt, output, max_rows)


@app.command()
def catalog(
    fmt: Annotated[OutputFormat, typer.Option("--format", "-f")] = OutputFormat.table,
) -> None:
    """List every stored series with row counts and date coverage."""
    from finsight.api import catalog as get_catalog

    frame = get_catalog(settings=_settings())
    if frame.empty:
        console.print("No data yet. Try: [cyan]finsight fetch AAPL --start 2024-01-01[/]")
        return
    _emit_frame(frame, fmt, None, max_rows=10_000)


def _emit_frame(frame: pd.DataFrame, fmt: OutputFormat, output: Path | None, max_rows: int) -> None:
    if fmt == OutputFormat.parquet and output is None:
        raise _fail("--format parquet requires --output")
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        if fmt == OutputFormat.parquet:
            frame.to_parquet(output, index=False)
        elif fmt == OutputFormat.json:
            frame.to_json(output, orient="records", date_format="iso", indent=2)
        else:
            frame.to_csv(output, index=False)
        err_console.print(f"Wrote {len(frame):,} rows to [cyan]{output}[/]")
        return
    if fmt == OutputFormat.csv:
        sys.stdout.write(frame.to_csv(index=False))
    elif fmt == OutputFormat.json:
        sys.stdout.write(frame.to_json(orient="records", date_format="iso", indent=2) + "\n")
    else:
        _print_frame(frame, max_rows)


def _print_frame(frame: pd.DataFrame, max_rows: int) -> None:
    table = Table(show_lines=False)
    for column in frame.columns:
        numeric = pd.api.types.is_numeric_dtype(frame[column])
        table.add_column(str(column), justify="right" if numeric else "left")
    for row in frame.head(max_rows).itertuples(index=False):
        table.add_row(*[_cell(v) for v in row])
    console.print(table)
    if len(frame) > max_rows:
        console.print(f"[dim]… showing {max_rows} of {len(frame):,} rows (use --max-rows)[/]")


def _cell(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NaT:
        return ""
    if isinstance(value, float):
        return f"{value:,.4f}"
    if isinstance(value, pd.Timestamp):
        if value.tzinfo is None and value == value.normalize():
            return value.date().isoformat()  # DuckDB DATE columns arrive as midnight timestamps
        return value.isoformat()
    return str(value)


# runs ----------------------------------------------------------------------------------


@runs_app.command("list")
def runs_list(limit: Annotated[int, typer.Option(help="Number of runs to show.")] = 20) -> None:
    """List recent runs, newest first."""
    from finsight.storage import Storage

    store = ManifestStore(Storage.from_settings(_settings()))
    manifests = store.list(limit=limit)
    if not manifests:
        console.print("No runs yet.")
        return
    table = Table()
    for column in ("Run", "Status", "Provider", "Interval", "Window", "Symbols", "Outcome"):
        table.add_column(column)
    for m in manifests:
        style = _RUN_STYLE.get(m.status, "")
        outcome = ", ".join(f"{n} {s}" for s, n in m.counts().items())
        table.add_row(
            m.run_id,
            f"[{style}]{m.status.value}[/]",
            m.request.provider,
            f"{m.request.interval.value}/{m.request.adjustment.value}",
            f"{m.request.start} → {m.request.end}",
            str(len(m.request.symbols)),
            outcome,
        )
    console.print(table)


@runs_app.command("show")
def runs_show(
    run_id: Annotated[str, typer.Argument(help="Run id, or 'latest'.")] = "latest",
    as_json: Annotated[bool, typer.Option("--json", help="Print the raw manifest.")] = False,
) -> None:
    """Show a run's per-symbol results and quality checks."""
    from finsight.storage import Storage

    settings = _settings()
    store = ManifestStore(Storage.from_settings(settings))
    try:
        manifest = store.latest() if run_id == "latest" else store.load(run_id)
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    if manifest is None:
        raise _fail("no runs yet")
    if as_json:
        console.print_json(manifest.to_json())
        return
    _print_run(manifest, settings)
    failed_checks = [
        (r.symbol, c) for r in manifest.symbols.values() for c in r.checks if not c.passed
    ]
    if failed_checks:
        table = Table(title="Failed quality checks", title_justify="left")
        for column in ("Symbol", "Check", "Severity", "Rows", "Rule"):
            table.add_column(column)
        for symbol, check in failed_checks:
            table.add_row(
                symbol, check.name, check.severity.value, _fmt(check.failed_rows), check.description
            )
        console.print(table)


# discovery -----------------------------------------------------------------------------


@app.command()
def providers() -> None:
    """List data providers and what each one supports."""
    from finsight.providers import available_providers

    table = Table(show_lines=True)
    for column in ("Provider", "Dataset", "Intervals", "Adjustments", "Notes"):
        table.add_column(column, overflow="fold")
    for provider in available_providers():
        for support in provider.capabilities:
            intervals = ", ".join(
                s.interval.value
                + (f" ({s.max_lookback_days}d)" if s.max_lookback_days is not None else "")
                for s in support.intervals
            )
            table.add_row(
                f"[bold]{provider.name}[/]\n{provider.title}",
                support.dataset,
                intervals,
                ", ".join(a.value for a in support.adjustments),
                f"{provider.description}\n[dim]{provider.terms or ''}[/]",
            )
    console.print(table)
    console.print("[dim](Nd) = only the last N days are available for that interval.[/]")


@app.command()
def datasets(
    name: Annotated[str | None, typer.Argument(help="Show one dataset's schema.")] = None,
) -> None:
    """List datasets, or show a dataset's schema."""
    from finsight.datasets import available_datasets, get_dataset

    if name is None:
        table = Table()
        for column in ("Dataset", "Description", "Primary key"):
            table.add_column(column)
        for ds in available_datasets():
            table.add_row(f"[bold]{ds.name}[/]", ds.description, ", ".join(ds.primary_key))
        console.print(table)
        return
    try:
        ds = get_dataset(name)
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    table = Table(title=f"{ds.name}: {ds.title}", title_justify="left")
    for column in ("Column", "Type", "Description"):
        table.add_column(column)
    for field in ds.schema:
        key = " [dim](key)[/]" if field.name in ds.primary_key else ""
        table.add_row(f"{field.name}{key}", str(field.type), ds.column_docs.get(field.name, ""))
    console.print(table)


@app.command()
def universes(
    name: Annotated[str | None, typer.Argument(help="Print the symbols in one universe.")] = None,
) -> None:
    """List bundled symbol universes (use with `fetch --universe`)."""
    from finsight.symbols import get_universe, list_universes

    if name:
        try:
            universe = get_universe(name)
        except KeyError as exc:
            raise _fail(str(exc.args[0])) from None
        console.print(" ".join(universe.symbols))
        return
    table = Table()
    for column in ("Universe", "Symbols", "Description", "As of"):
        table.add_column(column)
    for universe in list_universes().values():
        table.add_row(
            f"[bold]{universe.name}[/]",
            str(len(universe.symbols)),
            universe.description,
            universe.as_of or "",
        )
    console.print(table)


@app.command("config")
def show_config() -> None:
    """Show the resolved configuration (secrets masked)."""
    try:
        settings = _settings()
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    table = Table(show_header=False)
    table.add_column(style="bold")
    table.add_column()
    for key, value in settings.describe().items():
        table.add_row(key, "" if value is None else str(value))
    console.print(table)


# storage / snowflake / ui --------------------------------------------------------------


@storage_app.command("init")
def storage_init() -> None:
    """Create the data directory, or the bucket for S3 storage."""
    from finsight.storage import Storage

    try:
        storage = Storage.from_settings(_settings())
        storage.ensure_root()
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    console.print(f"Storage ready at [cyan]{storage.uri(storage.root)}[/]")


@snowflake_app.command("sync")
def snowflake_sync(
    run_id: Annotated[
        str | None, typer.Option("--run-id", help="Run to load. Defaults to the latest run.")
    ] = None,
) -> None:
    """Upsert the rows a run published into Snowflake (MERGE on the primary key)."""
    from finsight.api import sync_to_snowflake

    try:
        result = sync_to_snowflake(run_id, settings=_settings())
    except FinSightError as exc:
        raise _fail(str(exc)) from None
    console.print(f"Merged [bold]{result.rows_loaded:,}[/] rows into [cyan]{result.table}[/]")


@app.command()
def ui(
    port: Annotated[int, typer.Option(help="Port to serve the UI on.")] = 8501,
    open_browser: Annotated[bool, typer.Option("--open/--no-open", help="Open a browser.")] = True,
) -> None:
    """Launch the web UI (requires the ui extra)."""
    try:
        import streamlit  # noqa: F401
    except ImportError:
        raise _fail("the UI requires: pip install 'finsight[ui]'") from None
    app_path = Path(__file__).parent / "ui" / "app.py"
    command = [
        sys.executable, "-m", "streamlit", "run", str(app_path),
        "--server.port", str(port),
        "--server.headless", str(not open_browser).lower(),
        "--browser.gatherUsageStats", "false",
    ]  # fmt: skip
    raise typer.Exit(subprocess.call(command))


def main() -> None:
    """Console-script entry point."""
    app()


if __name__ == "__main__":
    main()
