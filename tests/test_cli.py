from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from typer.testing import CliRunner

from finsight import __version__
from finsight.cli import EXIT_FAILED, EXIT_OK, EXIT_PARTIAL, app

runner = CliRunner()


def invoke(*args: str) -> tuple[int, str]:
    result = runner.invoke(app, list(args), env={"COLUMNS": "200"})
    return result.exit_code, result.output


def test_version() -> None:
    code, output = invoke("--version")
    assert code == EXIT_OK
    assert __version__ in output


def test_fetch_and_query_round_trip(tmp_path: Path) -> None:
    code, output = invoke(
        "fetch", "AAPL", "MSFT", "-s", "2024-01-01", "-e", "2024-01-31", "-p", "synthetic"
    )
    assert code == EXIT_OK, output
    assert "published" in output
    assert "succeeded" in output

    code, output = invoke(
        "query", "SELECT symbol, count(*) AS n FROM ohlcv GROUP BY 1", "-f", "csv"
    )
    assert code == EXIT_OK
    frame = pd.read_csv(__import__("io").StringIO(output))
    assert sorted(frame["symbol"]) == ["AAPL", "MSFT"]

    out = tmp_path / "export" / "prices.parquet"
    code, _ = invoke("query", "SELECT * FROM ohlcv", "-f", "parquet", "-o", str(out))
    assert code == EXIT_OK
    assert len(pd.read_parquet(out)) == frame["n"].sum()

    code, output = invoke("catalog")
    assert code == EXIT_OK
    assert "AAPL" in output


def test_fetch_with_universe_and_json_output() -> None:
    code, output = invoke(
        "fetch",
        "--universe",
        "mag7",
        "-s",
        "2024-01-02",
        "-e",
        "2024-01-05",
        "-p",
        "synthetic",
        "--json",
    )
    assert code == EXIT_OK
    manifest = json.loads(output)
    assert len(manifest["request"]["symbols"]) == 7
    assert manifest["status"] == "succeeded"


def test_partial_and_failed_exit_codes() -> None:
    code, output = invoke(
        "fetch", "AAPL", "NOTFOUND", "-s", "2024-01-01", "-e", "2024-01-05", "-p", "synthetic"
    )
    assert code == EXIT_PARTIAL
    assert "not_found" in output
    code, _ = invoke("fetch", "NOTFOUND", "-s", "2024-01-01", "-e", "2024-01-05", "-p", "synthetic")
    assert code == EXIT_FAILED


@pytest.mark.parametrize(
    ("args", "message"),
    [
        (["fetch", "-s", "2024-01-01"], "at least one symbol"),
        (["fetch", "AAPL", "-s", "yesterday"], "start must be a date"),
        (["fetch", "AAPL", "-s", "2024-01-01", "-p", "nope"], "unknown provider"),
        (["fetch", "AAPL", "-s", "2020-01-01", "-i", "5m"], "last 59 days"),
        (["fetch", "-u", "nope", "-s", "2024-01-01"], "unknown universe"),
        (["query", "SELECT * FROM missing_table"], "query failed"),
        (["runs", "show", "nope"], "no run with id"),
        (["datasets", "nope"], "unknown dataset"),
    ],
)
def test_errors_are_reported_cleanly(args: list[str], message: str) -> None:
    code, output = invoke(*args)
    assert code != EXIT_OK
    assert message in output
    assert "Traceback" not in output


def test_discovery_commands() -> None:
    for args, expected in [
        (["providers"], "yahoo"),
        (["datasets"], "ohlcv"),
        (["datasets", "ohlcv"], "adj_close"),
        (["universes"], "sp500"),
        (["universes", "mag7"], "NVDA"),
        (["config"], "storage"),
    ]:
        code, output = invoke(*args)
        assert code == EXIT_OK, output
        assert expected in output


def test_runs_commands() -> None:
    invoke("fetch", "AAPL", "-s", "2024-01-01", "-e", "2024-01-05", "-p", "synthetic")
    code, output = invoke("runs", "list")
    assert code == EXIT_OK
    assert "succeeded" in output
    code, output = invoke("runs", "show", "latest", "--json")
    assert code == EXIT_OK
    assert json.loads(output)["request"]["symbols"] == ["AAPL"]


def test_storage_init(tmp_path: Path) -> None:
    code, _ = invoke("storage", "init")
    assert code == EXIT_OK
    assert (tmp_path / "data").is_dir()


def test_snowflake_sync_without_config_fails_cleanly() -> None:
    invoke("fetch", "AAPL", "-s", "2024-01-01", "-e", "2024-01-05", "-p", "synthetic")
    code, output = invoke("snowflake", "sync")
    assert code == EXIT_FAILED
    assert "Snowflake is not configured" in output
