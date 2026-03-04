"""
S&P 500 ticker scraper — get_sp500_tickers.py

Utility script for refreshing the ticker list in tickers.py. Fetches the
current S&P 500 constituents from Wikipedia and prints them as a single
comma-separated string ready to paste into DEFAULT_TICKERS.

Usage:
    python get_sp500_tickers.py
"""

import pandas as pd
import requests


def get_sp500_tickers():
    """
    Scrape the S&P 500 ticker list from Wikipedia.

    Returns:
        Comma-separated string of ticker symbols (e.g. "AAPL,MSFT,GOOG,..."),
        or None if the request fails.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/17.0 Safari/605.1.15"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        tables = pd.read_html(response.text)
        sp500_df = tables[0]

        tickers = sp500_df["Symbol"].tolist()
        # Normalise dot notation to dash (e.g. BRK.B -> BRK-B) for yfinance compatibility
        tickers = [t.replace(".", "-") for t in tickers]

        return ",".join(tickers)

    except Exception as e:
        print(f"[Error] Failed to fetch S&P 500 tickers: {e}")
        return None


if __name__ == "__main__":
    tickers_str = get_sp500_tickers()
    if tickers_str:
        print("Successfully fetched S&P 500 tickers!\n")
        print(tickers_str)
