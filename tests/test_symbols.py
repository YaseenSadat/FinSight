from __future__ import annotations

import pytest

from finsight.symbols import (
    decode_symbol,
    encode_symbol,
    get_universe,
    list_universes,
    normalize_symbol,
    parse_symbols,
)


@pytest.mark.parametrize("raw", ["aapl", "BRK-B", "BRK.B", "^GSPC", "EURUSD=X", "ES=F", "7203.T"])
def test_valid_symbols(raw: str) -> None:
    assert normalize_symbol(raw) == raw.upper()


@pytest.mark.parametrize("raw", ["", "AAPL MSFT", "A/B", "-AAPL", "A" * 21, "AA_PL"])
def test_invalid_symbols(raw: str) -> None:
    with pytest.raises(ValueError, match="invalid symbol"):
        normalize_symbol(raw)


def test_parse_symbols_splits_dedupes_and_preserves_order() -> None:
    assert parse_symbols(["msft, aapl", "MSFT\tnvda"]) == ["MSFT", "AAPL", "NVDA"]


def test_parse_symbols_lists_every_invalid_token() -> None:
    with pytest.raises(ValueError, match=r"a/b, c\$"):
        parse_symbols("AAPL a/b c$")


@pytest.mark.parametrize("symbol", ["AAPL", "BRK-B", "^GSPC", "EURUSD=X", "1d"])
def test_path_encoding_round_trips(symbol: str) -> None:
    encoded = encode_symbol(symbol)
    assert not encoded.startswith(("_", "."))  # hidden to Hadoop/Spark/pyarrow
    assert "/" not in encoded
    assert "=" not in encoded
    assert "^" not in encoded
    assert decode_symbol(encoded) == symbol


def test_bundled_universes() -> None:
    universes = list_universes()
    assert {"sp500", "dow30", "mag7", "sector-etfs", "index-etfs"} <= set(universes)
    assert len(universes["dow30"].symbols) == 30
    assert len(universes["sp500"].symbols) > 490
    assert universes["sp500"].as_of is not None
    assert get_universe("MAG7").symbols[0] == "AAPL"


def test_unknown_universe() -> None:
    with pytest.raises(KeyError, match="available"):
        get_universe("nope")
