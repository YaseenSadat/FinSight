"""
=============================================================================
File: get_sp500_tickers.py
Purpose:
    - Fetch the current S&P 500 tickers from Wikipedia
    - Output them as a single comma-separated string in the format:
        "AAPL,MSFT,GOOG,AMZN,..."
=============================================================================
"""

import pandas as pd
import requests

def get_sp500_tickers():
    """
    Scrapes the list of S&P 500 tickers from Wikipedia and returns
    them as a comma-separated string.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                      "Version/17.0 Safari/605.1.15"
    }

    try:
        # Fetch the webpage using requests with a fake browser header
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Parse tables from the HTML
        tables = pd.read_html(response.text)
        sp500_df = tables[0]

        # Extract the Symbol column
        tickers = sp500_df["Symbol"].tolist()

        # Clean up: replace '.' with '-' (e.g., BRK.B -> BRK-B)
        tickers = [t.replace(".", "-") for t in tickers]

        # Join into a single comma-separated string
        formatted = ",".join(tickers)
        return formatted

    except Exception as e:
        print(f"[Error] Failed to fetch S&P 500 tickers: {e}")
        return None


if __name__ == "__main__":
    tickers_str = get_sp500_tickers()
    if tickers_str:
        print("✅ Successfully fetched S&P 500 tickers!\n")
        print(tickers_str)
