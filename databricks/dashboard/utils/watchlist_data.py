"""Data-access helpers for the Watchlist page."""

import pandas as pd
from utils.connection import run_query, run_query_versioned, table


def fetch_all_tickers() -> pd.DataFrame:
    """Return all tickers with company names for the add-stock dropdown."""
    return run_query(f"SELECT symbol, company_name FROM {table('dim_ticker_hc')} ORDER BY symbol")


_QUOTE_COLUMNS = [
    "symbol",
    "sector",
    "price",
    "chg_pct",
    "intraday_pct",
    "gap_pct",
    "rvol",
    "volume",
    "dollar_volume",
    "gain_1w_pct",
    "gain_1m_pct",
    "gain_3m_pct",
]


def fetch_watchlist_quotes(tickers: list[str]) -> pd.DataFrame:
    """Return the latest screener-style metrics for each watchlisted ticker.

    Uses a 150-day look-back window from MAX(date) so LAG(adj_close, 63) for
    the 3-month gain column has a full window of trading days (~63 trading
    days ≈ 92 calendar days), mirroring fetch_screener_base in screener_data.

    Reads split-ADJUSTED columns from fact_daily_market_adjusted_hc so the
    period-return columns stay continuous across stock splits.
    """
    if not tickers:
        return pd.DataFrame(columns=_QUOTE_COLUMNS)
    placeholders = ", ".join("?" * len(tickers))
    return run_query_versioned(
        f"""
        WITH market_context AS (
            SELECT
                symbol,
                date,
                adj_open   AS open,
                adj_close  AS close,
                adj_volume AS volume,
                LAG(adj_close) OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                -- RVOL only once a full 20-day base exists (COUNT = 20), matching
                -- the screener; otherwise NULL so a young ticker reads "—".
                CASE WHEN COUNT(adj_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                     ) = 20
                     THEN adj_volume * 1.0 / NULLIF(AVG(adj_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                     ), 0)
                END AS rvol_20d,
                LAG(adj_close, 5)  OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
                LAG(adj_close, 21) OVER (PARTITION BY symbol ORDER BY date) AS close_21d,
                LAG(adj_close, 63) OVER (PARTITION BY symbol ORDER BY date) AS close_63d,
                ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
            FROM {table("fact_daily_market_adjusted_hc")}
            WHERE symbol IN ({placeholders})
              AND date >= DATE_SUB(
                    (SELECT MAX(date) FROM {table("fact_daily_market_adjusted_hc")}),
                    150
                  )
        )
        SELECT
            c.symbol,
            t.sector,
            c.close                                                             AS price,
            ROUND((c.close - c.prev_close) / NULLIF(c.prev_close, 0) * 100, 2)  AS chg_pct,
            ROUND((c.close - c.open)       / NULLIF(c.open, 0)       * 100, 2)  AS intraday_pct,
            ROUND((c.open  - c.prev_close) / NULLIF(c.prev_close, 0) * 100, 2)  AS gap_pct,
            ROUND(c.rvol_20d, 2)                                                AS rvol,
            c.volume                                                            AS volume,
            ROUND(c.close * c.volume, 2)                                        AS dollar_volume,
            ROUND((c.close - c.close_5d)  / NULLIF(c.close_5d,  0) * 100, 2)    AS gain_1w_pct,
            ROUND((c.close - c.close_21d) / NULLIF(c.close_21d, 0) * 100, 2)    AS gain_1m_pct,
            ROUND((c.close - c.close_63d) / NULLIF(c.close_63d, 0) * 100, 2)    AS gain_3m_pct
        FROM market_context c
        JOIN {table("dim_ticker_hc")} t ON c.symbol = t.symbol
        WHERE c.rn = 1
        """,
        params=tuple(tickers),
    )
