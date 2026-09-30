"""FinSight web UI.

A point-and-click front end over the same pipeline the CLI uses. Launch with
``finsight ui`` (requires ``pip install 'finsight[ui]'``).
"""

from __future__ import annotations

import io
from datetime import date, timedelta
from typing import Any

import altair as alt
import pandas as pd
import streamlit as st

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
from finsight.providers import available_providers, get_provider
from finsight.storage import Storage
from finsight.symbols import list_universes, parse_symbols
from finsight.warehouse import duckdb as warehouse

st.set_page_config(page_title="FinSight", page_icon="📈", layout="wide")

_STATUS_ICON = {
    SymbolStatus.PUBLISHED: "✅",
    SymbolStatus.NO_DATA: "⚪",
    SymbolStatus.NOT_FOUND: "❓",
    SymbolStatus.FAILED: "❌",
    SymbolStatus.REJECTED: "🚫",
}
_ADJUSTMENT_HELP = {
    Adjustment.RAW: "As traded. Not adjusted for splits or dividends; adj_close is adjusted.",
    Adjustment.ADJUSTED: "Open/high/low/close adjusted for splits and dividends.",
}


# Shared resources -----------------------------------------------------------------------


def settings() -> Settings:
    return Settings.from_env()


def storage() -> Storage:
    return Storage.from_settings(settings())


def load_catalog() -> pd.DataFrame:
    try:
        return warehouse.catalog(storage())
    except FinSightError as exc:
        st.error(str(exc))
        return pd.DataFrame()


# Sidebar: request builder -------------------------------------------------------------


def request_form() -> tuple[FetchRequest | None, str]:
    """Render the request builder. Returns (request, engine) when submitted and valid."""
    st.sidebar.header("New request")

    providers = {p.name: p for p in available_providers()}
    provider_name = st.sidebar.selectbox(
        "Provider",
        options=list(providers),
        index=list(providers).index("yahoo") if "yahoo" in providers else 0,
        format_func=lambda name: providers[name].title,
        help="Where the data comes from. `finsight providers` lists capabilities.",
    )
    provider = providers[provider_name]
    if provider.terms:
        st.sidebar.caption(provider.terms)

    datasets = [s.dataset for s in provider.capabilities]
    dataset = st.sidebar.selectbox("Dataset", datasets)
    support = provider.support_for(dataset)
    assert support is not None

    universes = list_universes()
    chosen_universes = st.sidebar.multiselect(
        "Symbol lists",
        options=list(universes),
        format_func=lambda n: f"{n} ({len(universes[n].symbols)})",
        help="Bundled lists of symbols. Combine with individual symbols below.",
    )
    typed = st.sidebar.text_area(
        "Symbols",
        value="AAPL, MSFT",
        help="Comma or space separated, e.g. AAPL, BRK-B, ^GSPC, EURUSD=X",
        height=80,
    )

    intervals = [s.interval for s in support.intervals]
    lookbacks = {s.interval: s.max_lookback_days for s in support.intervals}

    def interval_label(interval: Interval) -> str:
        days = lookbacks.get(interval)
        return f"{interval.value} (last {days} days)" if days else interval.value

    interval = st.sidebar.selectbox(
        "Interval",
        options=intervals,
        index=intervals.index(Interval.D1) if Interval.D1 in intervals else 0,
        format_func=interval_label,
    )
    adjustment = st.sidebar.radio(
        "Price adjustment",
        options=list(support.adjustments),
        format_func=lambda a: a.value,
        captions=[_ADJUSTMENT_HELP.get(a, "") for a in support.adjustments],
    )

    today = date.today()
    lookback = lookbacks.get(interval)
    earliest = today - timedelta(days=lookback) if lookback else date(1970, 1, 1)
    default_start = max(earliest, today - timedelta(days=365 if not lookback else min(lookback, 7)))
    col_start, col_end = st.sidebar.columns(2)
    start = col_start.date_input("Start", value=default_start, min_value=earliest, max_value=today)
    end = col_end.date_input("End", value=today, min_value=earliest, max_value=today)

    with st.sidebar.expander("Advanced"):
        engine = st.selectbox(
            "Transform engine",
            ["pandas", "spark"],
            help="Spark requires the spark extra and a Java runtime (or a cluster).",
        )

    submitted = st.sidebar.button("Fetch data", type="primary", width="stretch")
    if not submitted:
        return None, engine

    symbols: list[str] = []
    for name in chosen_universes:
        symbols.extend(universes[name].symbols)
    try:
        symbols.extend(parse_symbols(typed))
    except ValueError as exc:
        st.sidebar.error(str(exc))
        return None, engine

    try:
        request = FetchRequest.create(
            symbols,
            start,
            end,
            interval=interval,
            adjustment=adjustment,
            provider=provider_name,
            dataset=dataset,
        )
        problems = get_provider(provider_name).check(request)
        if problems:
            raise RequestError(problems)
    except RequestError as exc:
        for problem in exc.problems:
            st.sidebar.error(problem)
        return None, engine
    return request, engine


# Run tab --------------------------------------------------------------------------------


def execute(request: FetchRequest, engine: str) -> RunManifest | None:
    stages = {
        name: st.progress(0.0, text=f"{name}: waiting")
        for name in ("ingest", "transform", "publish")
    }

    def on_progress(event: ProgressEvent) -> None:
        fraction = event.completed / max(event.total, 1)
        label = f"{event.stage}: {event.completed}/{event.total}"
        if event.symbol:
            label += f" · {event.symbol}"
        stages[event.stage].progress(fraction, text=label)

    try:
        manifest = Pipeline(settings()).run(request, engine=engine, progress=on_progress)  # type: ignore[arg-type]
    except FinSightError as exc:
        st.error(str(exc))
        return None
    for name, bar in stages.items():
        bar.progress(1.0, text=f"{name}: done")
    st.session_state["last_run_id"] = manifest.run_id
    return manifest


def show_run(manifest: RunManifest, key: str) -> None:
    request = manifest.request
    status_text = {
        RunStatus.SUCCEEDED: st.success,
        RunStatus.PARTIAL: st.warning,
        RunStatus.FAILED: st.error,
    }.get(manifest.status, st.info)
    status_text(
        f"Run `{manifest.run_id}` **{manifest.status.value}**: "
        f"{request.dataset} · {request.provider} · {request.interval.value} · "
        f"{request.adjustment.value} · {request.start} → {request.end}"
    )

    results = list(manifest.symbols.values())
    published = [r for r in results if r.status == SymbolStatus.PUBLISHED]
    failed = [r for r in results if r.status in FAILURE_STATUSES]
    cols = st.columns(4)
    cols[0].metric("Symbols published", f"{len(published)}/{len(results)}")
    cols[1].metric("Rows delivered", f"{sum(r.silver_rows or 0 for r in published):,}")
    cols[2].metric("New rows", f"{sum(r.rows_inserted or 0 for r in published):,}")
    cols[3].metric("Failed", len(failed))

    table = pd.DataFrame(
        {
            "": [_STATUS_ICON.get(r.status, "") for r in results],
            "symbol": [r.symbol for r in results],
            "status": [r.status.value for r in results],
            "rows": [r.silver_rows for r in results],
            "new": [r.rows_inserted for r in results],
            "updated": [r.rows_updated for r in results],
            "first": [r.first_date for r in results],
            "last": [r.last_date for r in results],
            "notes": [r.message or "" for r in results],
        }
    )
    st.dataframe(table, hide_index=True, width="stretch")

    failed_checks = [
        {
            "symbol": r.symbol,
            "check": c.name,
            "severity": c.severity.value,
            "rows": c.failed_rows,
            "rule": c.description,
        }
        for r in results
        for c in r.checks
        if not c.passed
    ]
    if failed_checks:
        with st.expander(f"Quality check failures ({len(failed_checks)})", expanded=bool(failed)):
            st.dataframe(pd.DataFrame(failed_checks), hide_index=True, width="stretch")

    with st.expander("Reproduce this run"):
        symbols = " ".join(request.symbols)
        st.code(
            f"finsight fetch {symbols} --start {request.start} --end {request.end} "
            f"--interval {request.interval.value} --adjustment {request.adjustment.value} "
            f"--provider {request.provider}",
            language="bash",
        )
        st.code(
            "import finsight\n\n"
            f"finsight.fetch({list(request.symbols)!r}, start={str(request.start)!r}, "
            f"end={str(request.end)!r},\n"
            f"               interval={request.interval.value!r}, "
            f"adjustment={request.adjustment.value!r}, provider={request.provider!r})",
            language="python",
        )
        st.download_button(
            "Download manifest (JSON)",
            manifest.to_json(),
            file_name=f"finsight-run-{manifest.run_id}.json",
            mime="application/json",
            key=f"{key}-manifest",
        )


# Explore tab ----------------------------------------------------------------------------


def explore_tab() -> None:
    catalog = load_catalog()
    if catalog.empty:
        st.info("No data yet. Build a request in the sidebar and press **Fetch data**.")
        return

    st.dataframe(catalog, hide_index=True, width="stretch")

    series = catalog[["provider", "interval", "adjustment"]].drop_duplicates()
    options = [tuple(row) for row in series.itertuples(index=False)]
    col_series, col_symbols = st.columns([1, 2])
    provider, interval, adjustment = col_series.selectbox(
        "Series", options, format_func=lambda o: f"{o[0]} · {o[1]} · {o[2]}"
    )
    available = catalog.loc[
        (catalog["provider"] == provider)
        & (catalog["interval"] == interval)
        & (catalog["adjustment"] == adjustment),
        "symbol",
    ].tolist()
    symbols = col_symbols.multiselect("Symbols", available, default=available[:3])
    if not symbols:
        return

    placeholders = ", ".join("?" for _ in symbols)
    con = warehouse.connect(storage())
    try:
        frame = con.execute(
            f"""
            SELECT * FROM ohlcv
            WHERE provider = ? AND "interval" = ? AND adjustment = ? AND symbol IN ({placeholders})
            ORDER BY symbol, ts
            """,
            [provider, interval, adjustment, *symbols],
        ).fetch_df()
    finally:
        con.close()
    if frame.empty:
        st.warning("No rows for this selection.")
        return

    frame["ts"] = pd.to_datetime(frame["ts"], utc=True)
    if len(symbols) == 1:
        st.altair_chart(candlestick(frame), width="stretch")
    else:
        normalise = st.toggle("Normalise to 100 at start", value=True)
        st.altair_chart(line_chart(frame, normalise), width="stretch")

    st.dataframe(frame, hide_index=True, width="stretch", height=320)
    downloads(frame, f"finsight-{provider}-{interval}-{adjustment}", key="explore")

    with st.expander("Query this in code"):
        in_list = ", ".join(f"'{s}'" for s in symbols)
        st.code(
            "SELECT * FROM ohlcv\n"
            f"WHERE provider = '{provider}' AND \"interval\" = '{interval}' "
            f"AND adjustment = '{adjustment}'\n  AND symbol IN ({in_list})\nORDER BY symbol, ts;",
            language="sql",
        )
        st.code(
            "import finsight\n\n"
            f"df = finsight.load('ohlcv', symbols={symbols!r}, interval={interval!r}, "
            f"adjustment={adjustment!r}, provider={provider!r})",
            language="python",
        )


def candlestick(frame: pd.DataFrame) -> alt.Chart:
    data = frame[["ts", "open", "high", "low", "close", "volume"]].copy()
    data["direction"] = (data["close"] >= data["open"]).map({True: "up", False: "down"})
    color = alt.Color(
        "direction:N",
        scale=alt.Scale(domain=["up", "down"], range=["#1a9850", "#d73027"]),
        legend=None,
    )
    base = alt.Chart(data).encode(x=alt.X("ts:T", title=None))
    wick = base.mark_rule().encode(
        y=alt.Y("low:Q", title="Price", scale=alt.Scale(zero=False)), y2="high:Q", color=color
    )
    body = base.mark_bar().encode(
        y="open:Q",
        y2="close:Q",
        color=color,
        tooltip=["ts:T", "open:Q", "high:Q", "low:Q", "close:Q", "volume:Q"],
    )
    return (wick + body).properties(height=380).interactive(bind_y=False)


def line_chart(frame: pd.DataFrame, normalise: bool) -> alt.Chart:
    data = frame[["ts", "symbol", "close"]].copy()
    if normalise:
        first = data.groupby("symbol")["close"].transform("first")
        data["close"] = data["close"] / first * 100
    return (
        alt.Chart(data)
        .mark_line()
        .encode(
            x=alt.X("ts:T", title=None),
            y=alt.Y(
                "close:Q",
                title="Close (start = 100)" if normalise else "Close",
                scale=alt.Scale(zero=False),
            ),
            color="symbol:N",
            tooltip=["symbol:N", "ts:T", "close:Q"],
        )
        .properties(height=380)
        .interactive(bind_y=False)
    )


def downloads(frame: pd.DataFrame, stem: str, key: str) -> None:
    export = frame.copy()
    col_csv, col_parquet, _ = st.columns([1, 1, 4])
    col_csv.download_button(
        "Download CSV",
        export.to_csv(index=False),
        file_name=f"{stem}.csv",
        mime="text/csv",
        key=f"{key}-csv",
    )
    buffer = io.BytesIO()
    export.to_parquet(buffer, index=False)
    col_parquet.download_button(
        "Download Parquet",
        buffer.getvalue(),
        file_name=f"{stem}.parquet",
        mime="application/octet-stream",
        key=f"{key}-parquet",
    )


# SQL tab --------------------------------------------------------------------------------


def sql_tab() -> None:
    st.caption("DuckDB SQL over the gold layer. Each dataset is a view, e.g. `ohlcv`.")
    sql = st.text_area(
        "SQL",
        value=(
            "SELECT symbol, interval, adjustment, count(*) AS bars,\n"
            "       min(date) AS first_date, max(date) AS last_date,\n"
            "       round(avg(close), 2) AS avg_close\n"
            "FROM ohlcv\nGROUP BY ALL\nORDER BY symbol"
        ),
        height=160,
    )
    if st.button("Run query", type="primary"):
        try:
            result = warehouse.query(storage(), sql)
        except Exception as exc:
            st.error(f"Query failed: {exc}")
            return
        st.dataframe(result, hide_index=True, width="stretch")
        downloads(result, "finsight-query", key="sql")


# Runs tab -------------------------------------------------------------------------------


def runs_tab() -> None:
    store = ManifestStore(storage())
    manifests = store.list(limit=50)
    if not manifests:
        st.info("No runs yet.")
        return
    rows: list[dict[str, Any]] = [
        {
            "run_id": m.run_id,
            "status": m.status.value,
            "provider": m.request.provider,
            "interval": m.request.interval.value,
            "adjustment": m.request.adjustment.value,
            "window": f"{m.request.start} → {m.request.end}",
            "symbols": len(m.request.symbols),
            "outcome": ", ".join(f"{n} {s}" for s, n in m.counts().items()),
        }
        for m in manifests
    ]
    st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
    run_id = st.selectbox("Inspect run", [m.run_id for m in manifests])
    if run_id:
        show_run(store.load(run_id), key="history")


# Page -----------------------------------------------------------------------------------


def main() -> None:
    try:
        current = settings()
    except FinSightError as exc:
        st.error(f"Configuration error: {exc}")
        st.stop()

    st.title("FinSight")
    location = current.data_dir.resolve() if current.storage == "local" else current.s3_bucket
    st.caption(
        f"Validated, query-ready market data · v{__version__} · storage: "
        f"{current.storage} (`{location}`)"
    )

    request, engine = request_form()
    tab_run, tab_explore, tab_sql, tab_runs = st.tabs(["Run", "Explore", "SQL", "History"])

    with tab_run:
        if request is not None:
            manifest = execute(request, engine)
            if manifest:
                show_run(manifest, key="run")
        elif "last_run_id" in st.session_state:
            show_run(ManifestStore(storage()).load(st.session_state["last_run_id"]), key="run")
        else:
            st.markdown(
                "#### Get market data in three steps\n"
                "1. Pick a **provider**, **symbols** (or a bundled list), an **interval**, "
                "and a **date range** in the sidebar.\n"
                "2. Choose **raw** (as traded) or **adjusted** prices.\n"
                "3. Press **Fetch data**. Every symbol is validated before it is stored.\n\n"
                "Then chart and download it in **Explore**, or query it with SQL."
            )
    with tab_explore:
        explore_tab()
    with tab_sql:
        sql_tab()
    with tab_runs:
        runs_tab()


main()
