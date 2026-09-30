"""Smoke tests for the Streamlit UI using Streamlit's headless AppTest."""

from __future__ import annotations

import logging

import pytest

pytest.importorskip("streamlit")

from streamlit.testing.v1 import AppTest


@pytest.fixture
def app() -> AppTest:
    logging.getLogger("streamlit").setLevel(logging.ERROR)
    import finsight.ui.app as module

    return AppTest.from_file(module.__file__, default_timeout=60)


def test_initial_render(app: AppTest) -> None:
    app.run()
    assert not app.exception
    assert [tab.label for tab in app.tabs] == ["Run", "Explore", "SQL", "History"]


def test_fetch_from_the_sidebar(app: AppTest) -> None:
    app.run()
    app.sidebar.selectbox[0].select("synthetic").run()
    app.sidebar.text_area[0].input("AAPL MSFT NOTFOUND").run()
    app.sidebar.button[0].click().run()
    assert not app.exception

    metrics = {m.label: m.value for m in app.metric}
    assert metrics["Symbols published"] == "2/3"
    assert metrics["Failed"] == "1"

    app.run()  # Explore tab now has data
    assert not app.exception
    app.tabs[1].multiselect[0].set_value(["AAPL"]).run()
    assert not app.exception
    app.tabs[2].button[0].click().run()
    assert not app.exception
    assert len(app.tabs[2].dataframe[0].value) == 2


def test_invalid_symbols_are_reported(app: AppTest) -> None:
    app.run()
    app.sidebar.text_area[0].input("AAPL !!bad").run()
    app.sidebar.button[0].click().run()
    assert [e.value for e in app.sidebar.error] == ["invalid symbol(s): !!bad"]
