"""Refresh the index-constituent universes bundled with FinSight.

Scrapes the current constituents of the S&P 500 and the Dow Jones Industrial
Average from Wikipedia and rewrites the matching files in
``src/finsight/universes/``. Symbols are converted to Yahoo Finance notation
(``BRK.B`` -> ``BRK-B``).

Usage:
    python scripts/refresh_universes.py            # refresh all
    python scripts/refresh_universes.py sp500      # refresh one

Requires the ``dev`` extra (for ``lxml``).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

UNIVERSE_DIR = Path(__file__).resolve().parents[1] / "src" / "finsight" / "universes"
USER_AGENT = "FinSight universe refresher (https://github.com/YaseenSadat/FinSight)"


@dataclass(frozen=True)
class Source:
    name: str
    description: str
    url: str
    min_size: int


SOURCES = {
    "sp500": Source(
        name="sp500",
        description="S&P 500 constituents",
        url="https://en.wikipedia.org/wiki/List_of_S%26P_500_companies",
        min_size=490,
    ),
    "dow30": Source(
        name="dow30",
        description="Dow Jones Industrial Average constituents",
        url="https://en.wikipedia.org/wiki/List_of_Dow_Jones_Industrial_Average_companies",
        min_size=30,
    ),
}


SYMBOL_COLUMNS = ("Symbol", "Ticker")


def fetch_symbols(source: Source) -> list[str]:
    """Return the constituents from the first table that looks like a member list."""
    response = requests.get(source.url, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    for table in pd.read_html(StringIO(response.text)):
        column = next((c for c in SYMBOL_COLUMNS if c in table.columns), None)
        if column is None or len(table) < source.min_size:
            continue
        symbols = table[column].dropna().astype(str).str.strip()
        symbols = symbols.str.replace(".", "-", regex=False).str.upper()
        return sorted(set(symbols))
    raise RuntimeError(f"no constituent table (>= {source.min_size} rows) at {source.url}")


def write_universe(source: Source, symbols: list[str]) -> Path:
    as_of = datetime.now(UTC).date().isoformat()
    header = [
        f"# description: {source.description}",
        f"# source: {source.url}",
        f"# as_of: {as_of}",
    ]
    path = UNIVERSE_DIR / f"{source.name}.txt"
    path.write_text("\n".join([*header, *symbols]) + "\n", encoding="utf-8")
    return path


def main(names: list[str]) -> int:
    targets = names or list(SOURCES)
    unknown = [name for name in targets if name not in SOURCES]
    if unknown:
        print(f"unknown universe(s): {', '.join(unknown)}; choose from {', '.join(SOURCES)}")
        return 2
    for name in targets:
        source = SOURCES[name]
        symbols = fetch_symbols(source)
        path = write_universe(source, symbols)
        print(f"{name}: wrote {len(symbols)} symbols to {path.relative_to(Path.cwd())}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
