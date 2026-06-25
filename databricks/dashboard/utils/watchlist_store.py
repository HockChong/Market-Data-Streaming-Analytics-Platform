"""Simple JSON-backed persistence for the user's watchlist.

Stored in watchlist.json alongside app.py so it survives page reloads within
the same Databricks App instance without needing a DB table.
"""

import json
from pathlib import Path

_STORE = Path(__file__).parent.parent / "watchlist.json"


def load_watchlist() -> list[str]:
    if not _STORE.exists():
        return []
    try:
        data = json.loads(_STORE.read_text())
        return [str(t) for t in data] if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def save_watchlist(tickers: list[str]) -> None:
    _STORE.write_text(json.dumps(tickers))


def add_ticker(ticker: str) -> None:
    tickers = load_watchlist()
    if ticker not in tickers:
        tickers.append(ticker)
        save_watchlist(tickers)


def remove_ticker(ticker: str) -> None:
    tickers = load_watchlist()
    if ticker in tickers:
        tickers.remove(ticker)
        save_watchlist(tickers)
