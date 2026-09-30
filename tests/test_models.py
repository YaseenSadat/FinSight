from __future__ import annotations

from datetime import date, timedelta

import pytest

from finsight.errors import RequestError
from finsight.models import Adjustment, FetchRequest, Interval, earliest_bar_date, today_utc


def test_create_normalises_input() -> None:
    request = FetchRequest.create(
        " aapl, msft AAPL ", "2024-01-01", "2024-02-01", interval="1D", adjustment="Adjusted"
    )
    assert request.symbols == ("AAPL", "MSFT")
    assert request.start == date(2024, 1, 1)
    assert request.end == date(2024, 2, 1)
    assert request.interval is Interval.D1
    assert request.adjustment is Adjustment.ADJUSTED
    assert request.provider == "yahoo"
    assert request.dataset == "ohlcv"


def test_end_defaults_to_today_and_future_end_is_clamped() -> None:
    assert FetchRequest.create("AAPL", "2024-01-01").end == today_utc()
    future = today_utc() + timedelta(days=30)
    assert FetchRequest.create("AAPL", "2024-01-01", future).end == today_utc()


def test_all_problems_are_reported_together() -> None:
    with pytest.raises(RequestError) as info:
        FetchRequest.create("AAPL, !!", "not-a-date", "2024-01-01", interval="7m", adjustment="x")
    problems = " | ".join(info.value.problems)
    assert "invalid symbol" in problems
    assert "start must be a date" in problems
    assert "interval must be one of" in problems
    assert "adjustment must be one of" in problems


def test_start_after_end_is_rejected() -> None:
    with pytest.raises(RequestError, match="must be on or before"):
        FetchRequest.create("AAPL", "2024-02-01", "2024-01-01")


def test_empty_symbols_rejected() -> None:
    with pytest.raises(RequestError, match="at least one symbol"):
        FetchRequest.create([], "2024-01-01")


def test_round_trip_serialisation() -> None:
    request = FetchRequest.create(["AAPL", "^GSPC"], "2024-01-01", "2024-03-01", interval="1h")
    assert FetchRequest.from_dict(request.to_dict()) == request


def test_for_symbol_carries_window() -> None:
    request = FetchRequest.create("AAPL", "2024-01-01", "2024-01-05")
    sub = request.for_symbol("AAPL")
    assert (sub.symbol, sub.start, sub.end, sub.interval) == (
        "AAPL",
        date(2024, 1, 1),
        date(2024, 1, 5),
        Interval.D1,
    )


def test_interval_properties() -> None:
    assert Interval.M5.is_intraday
    assert not Interval.D1.is_intraday
    assert Interval.W1.period == timedelta(days=7)


@pytest.mark.parametrize(
    ("interval", "start", "expected"),
    [
        (Interval.D1, date(2024, 3, 6), date(2024, 3, 6)),
        (Interval.W1, date(2024, 3, 6), date(2024, 3, 4)),
        (Interval.MO1, date(2024, 3, 6), date(2024, 3, 1)),
    ],
)
def test_earliest_bar_date(interval: Interval, start: date, expected: date) -> None:
    assert earliest_bar_date(start, interval) == expected
