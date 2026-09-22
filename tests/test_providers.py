from __future__ import annotations

from datetime import date, timedelta

import pytest

from finsight.errors import SymbolNotFoundError, UnknownProviderError
from finsight.models import Interval
from finsight.providers import SyntheticProvider, get_provider, provider_names, register_provider
from finsight.providers.registry import unregister_provider
from tests.conftest import make_request


def test_builtin_providers_registered() -> None:
    assert {"yahoo", "synthetic"} <= set(provider_names())
    assert get_provider("Synthetic") is get_provider("synthetic")


def test_unknown_provider() -> None:
    with pytest.raises(UnknownProviderError, match="available"):
        get_provider("bloomberg")


def test_duplicate_registration_requires_replace() -> None:
    register_provider("dup-test", SyntheticProvider())
    try:
        with pytest.raises(ValueError, match="already registered"):
            register_provider("dup-test", SyntheticProvider())
        register_provider("dup-test", SyntheticProvider(), replace=True)
    finally:
        unregister_provider("dup-test")


def test_yahoo_capabilities_reject_out_of_range_intraday() -> None:
    yahoo = get_provider("yahoo")
    today = date(2026, 6, 1)
    old = make_request("AAPL", "2026-01-01", "2026-01-05", provider="yahoo", interval="5m")
    problems = yahoo.check(old, today=today)
    assert len(problems) == 1
    assert "last 59 days" in problems[0]

    recent_start = (today - timedelta(days=10)).isoformat()
    recent = make_request("AAPL", recent_start, "2026-05-30", provider="yahoo", interval="5m")
    assert yahoo.check(recent, today=today) == []


def test_capability_check_reports_unsupported_dataset_and_interval(stub_provider) -> None:
    provider = stub_provider(lambda request: None)
    request = make_request(provider="stub", interval="1wk", adjustment="adjusted")
    problems = " | ".join(provider.check(request))
    assert "does not support interval '1wk'" in problems
    assert "does not support adjustment 'adjusted'" in problems

    other = make_request(provider="stub")
    object.__setattr__(other, "dataset", "dividends")
    assert "does not offer dataset 'dividends'" in provider.check(other)[0]


def test_synthetic_is_deterministic_and_consistent_across_windows() -> None:
    provider = SyntheticProvider()
    full = provider.fetch(make_request("AAPL", "2024-01-01", "2024-03-31").for_symbol("AAPL"))
    part = provider.fetch(make_request("AAPL", "2024-02-01", "2024-02-29").for_symbol("AAPL"))
    again = provider.fetch(make_request("AAPL", "2024-01-01", "2024-03-31").for_symbol("AAPL"))

    assert full.equals(again)
    overlap = full[full["date"].between(date(2024, 2, 1), date(2024, 2, 29))].reset_index(drop=True)
    assert overlap.equals(part)
    assert (full["high"] >= full[["open", "close"]].max(axis=1)).all()
    assert (full["low"] <= full[["open", "close"]].min(axis=1)).all()


@pytest.mark.parametrize("interval", list(Interval))
def test_synthetic_supports_every_interval(interval: Interval) -> None:
    provider = SyntheticProvider()
    request = make_request("MSFT", "2024-03-04", "2024-03-29", interval=interval)
    frame = provider.fetch(request.for_symbol("MSFT"))
    assert not frame.empty
    assert frame["ts"].dt.tz is not None
    assert frame["date"].min() >= date(2024, 3, 1)
    assert frame["date"].max() <= date(2024, 3, 29)


def test_synthetic_missing_symbol() -> None:
    with pytest.raises(SymbolNotFoundError):
        SyntheticProvider().fetch(make_request("NOTFOUND").for_symbol("NOTFOUND"))
