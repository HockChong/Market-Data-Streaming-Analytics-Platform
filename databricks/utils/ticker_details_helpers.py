"""Pure-Python ticker detail extraction and retry — importable without the ingestion notebook."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any


def extract_ticker_details(ticker: str, details: Any) -> dict:
    """Extract ticker details from Polygon API response object (duck-typed)."""
    return {
        "symbol": ticker,
        "name": getattr(details, "name", None),
        "type": getattr(details, "type", None),
        "active": getattr(details, "active", None),
        "primary_exchange": getattr(details, "primary_exchange", None),
        "sic_code": getattr(details, "sic_code", None),
        "sic_description": getattr(details, "sic_description", None),
        "market_cap": getattr(details, "market_cap", None),
        "list_date": getattr(details, "list_date", None),
    }


def fetch_ticker_details_with_retry(
    ticker: str,
    get_ticker_details: Callable[[str], Any],
    *,
    max_retries: int = 3,
    backoff_initial: float = 1.0,
    backoff_multiplier: float = 2.0,
):
    """Fetch one ticker with exponential backoff. ``get_ticker_details`` is injectable for tests."""
    last_error = None
    backoff = backoff_initial

    for attempt in range(max_retries):
        try:
            details = get_ticker_details(ticker)
            if details:
                return (extract_ticker_details(ticker, details), None)
            return (None, {"ticker": ticker, "error": "No data returned"})

        except Exception as e:
            last_error = str(e)
            is_rate_limit = "429" in last_error or "rate" in last_error.lower()
            is_server_error = any(code in last_error for code in ["500", "502", "503", "504"])

            if (is_rate_limit or is_server_error) and attempt < max_retries - 1:
                time.sleep(backoff)
                backoff *= backoff_multiplier
            else:
                break

    return (None, {"ticker": ticker, "error": last_error})
