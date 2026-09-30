"""Symbol parsing, validation, and curated symbol universes."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources

# Upper-case tickers with the punctuation used by common listings:
# class shares (BRK-B, BRK.B), indices (^GSPC), currencies/futures (EURUSD=X, ES=F).
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9^][A-Z0-9.\-^=]{0,19}$")

# Characters that may appear in storage paths unchanged. Everything else is
# written as ``~XX`` (hex). ``~`` never appears in a valid symbol, so the
# encoding is unambiguous and reversible. (``_`` would be a poor escape:
# Hadoop, Spark, and pyarrow silently skip files whose names start with it.)
_PATH_SAFE = re.compile(r"[A-Za-z0-9.\-]")


def normalize_symbol(raw: str) -> str:
    """Return the canonical form of ``raw`` or raise :class:`ValueError`."""
    symbol = raw.strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol):
        raise ValueError(f"invalid symbol {raw!r}")
    return symbol


def parse_symbols(values: str | Iterable[str]) -> list[str]:
    """Split, normalise, and de-duplicate symbols while preserving order.

    Accepts a single comma/space separated string or an iterable of strings
    (each of which may itself contain separators).

    Raises:
        ValueError: listing every invalid symbol found.
    """
    items = [values] if isinstance(values, str) else list(values)
    tokens = [token for item in items for token in re.split(r"[,\s]+", item) if token]

    symbols: list[str] = []
    invalid: list[str] = []
    for token in tokens:
        try:
            symbol = normalize_symbol(token)
        except ValueError:
            invalid.append(token)
            continue
        if symbol not in symbols:
            symbols.append(symbol)
    if invalid:
        raise ValueError(f"invalid symbol(s): {', '.join(invalid)}")
    return symbols


def encode_symbol(symbol: str) -> str:
    """Encode a symbol (or any partition value) for use as a file or directory name."""
    return "".join(ch if _PATH_SAFE.fullmatch(ch) else f"~{ord(ch):02X}" for ch in symbol)


def decode_symbol(encoded: str) -> str:
    """Invert :func:`encode_symbol`."""
    return re.sub(r"~([0-9A-F]{2})", lambda m: chr(int(m.group(1), 16)), encoded)


@dataclass(frozen=True)
class Universe:
    """A named, versioned list of symbols shipped with FinSight."""

    name: str
    description: str
    symbols: tuple[str, ...]
    as_of: str | None = None
    source: str | None = None


def _parse_universe(name: str, text: str) -> Universe:
    meta: dict[str, str] = {}
    symbols: list[str] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            key, sep, value = line.lstrip("# ").partition(":")
            if sep:
                meta[key.strip().lower()] = value.strip()
            continue
        symbols.append(normalize_symbol(line))
    return Universe(
        name=name,
        description=meta.get("description", name),
        symbols=tuple(dict.fromkeys(symbols)),
        as_of=meta.get("as_of"),
        source=meta.get("source"),
    )


@lru_cache(maxsize=1)
def list_universes() -> dict[str, Universe]:
    """Return all bundled universes keyed by name."""
    package = resources.files("finsight.universes")
    universes: dict[str, Universe] = {}
    for entry in sorted(package.iterdir(), key=lambda e: e.name):
        if entry.name.endswith(".txt"):
            name = entry.name.removesuffix(".txt")
            universes[name] = _parse_universe(name, entry.read_text(encoding="utf-8"))
    return universes


def get_universe(name: str) -> Universe:
    """Look up a bundled universe by name (case-insensitive)."""
    universes = list_universes()
    key = name.strip().lower()
    if key not in universes:
        known = ", ".join(universes) or "none"
        raise KeyError(f"unknown universe {name!r} (available: {known})")
    return universes[key]
