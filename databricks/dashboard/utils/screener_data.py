"""Data-access helpers for the Signal Screener page."""

import pandas as pd
from utils.connection import run_query, run_query_versioned, table


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a query with the current Gold-version cache key."""
    return run_query_versioned(sql, params=params)


def fetch_available_dates() -> pd.DataFrame:
    """Fetch recent available market dates for screener date selection."""
    return _run_query(
        f"""
        SELECT DISTINCT date FROM {table("fact_daily_market_adjusted_hc")}
        WHERE date >= DATE_SUB((SELECT MAX(date) FROM {table("fact_daily_market_adjusted_hc")}), 90)
        ORDER BY date DESC
        """
    )


def fetch_sectors() -> pd.DataFrame:
    """Fetch distinct non-null sectors from ticker dimension."""
    return run_query(f"SELECT DISTINCT sector FROM {table('dim_ticker_hc')} WHERE sector IS NOT NULL ORDER BY sector")


def fetch_screener_base(selected_date_str: str) -> pd.DataFrame:
    """Fetch all screener rows for the selected date with bounded rolling context.

    Lookback is 400 calendar days (~280 trading days) to support LAG(adj_close, 252)
    for the 1-year gain column without a separate join.

    Reads split-ADJUSTED columns from fact_daily_market_adjusted_hc so period-return
    columns (1W–1Y gain) stay continuous across stock splits. The table also carries
    raw `close`/`volume`, so every window function must reference `adj_*` explicitly —
    a bare `close` here would silently use the unadjusted price.
    """
    return _run_query(
        f"""
        WITH market_context AS (
            SELECT
                symbol,
                date,
                adj_open   AS open,
                adj_close  AS close,
                adj_volume AS volume,
                LAG(adj_close)     OVER (PARTITION BY symbol ORDER BY date) AS prev_close,
                -- RVOL only once a full 20-day base exists (COUNT = 20), matching the
                -- terminal's compute_window_stats which requires exactly 20 prior days;
                -- otherwise NULL so a young ticker reads "—" in both surfaces.
                CASE WHEN COUNT(adj_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                     ) = 20
                     THEN adj_volume * 1.0 / NULLIF(AVG(adj_volume) OVER (
                        PARTITION BY symbol
                        ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                     ), 0)
                END AS rvol_20d,
                LAG(adj_close, 5)   OVER (PARTITION BY symbol ORDER BY date) AS close_5d,
                LAG(adj_close, 21)  OVER (PARTITION BY symbol ORDER BY date) AS close_21d,
                LAG(adj_close, 63)  OVER (PARTITION BY symbol ORDER BY date) AS close_63d,
                LAG(adj_close, 126) OVER (PARTITION BY symbol ORDER BY date) AS close_126d,
                LAG(adj_close, 252) OVER (PARTITION BY symbol ORDER BY date) AS close_252d
            FROM {table("fact_daily_market_adjusted_hc")}
            WHERE date >= DATE_SUB(?, 400)
              AND date <= ?
        )
        SELECT
            c.symbol                                                              AS Symbol,
            t.company_name                                                        AS Company,
            t.sector                                                              AS Sector,
            c.close                                                               AS Close,
            ROUND((c.close - c.prev_close)  / NULLIF(c.prev_close, 0)  * 100, 2) AS `Chg %`,
            ROUND((c.close - c.open)        / NULLIF(c.open, 0)        * 100, 2) AS `Intraday %`,
            ROUND((c.open  - c.prev_close)  / NULLIF(c.prev_close, 0)  * 100, 2) AS `Gap %`,
            ROUND(c.rvol_20d, 2)                                                  AS RVOL,
            c.volume                                                              AS Volume,
            ROUND(c.close * c.volume, 2)                                          AS `Dollar Volume`,
            ROUND((c.close - c.close_5d)   / NULLIF(c.close_5d,   0) * 100, 2)  AS `1W Gain %`,
            ROUND((c.close - c.close_21d)  / NULLIF(c.close_21d,  0) * 100, 2)  AS `1M Gain %`,
            ROUND((c.close - c.close_63d)  / NULLIF(c.close_63d,  0) * 100, 2)  AS `3M Gain %`,
            ROUND((c.close - c.close_126d) / NULLIF(c.close_126d, 0) * 100, 2)  AS `6M Gain %`,
            ROUND((c.close - c.close_252d) / NULLIF(c.close_252d, 0) * 100, 2)  AS `1Y Gain %`
        FROM market_context c
        JOIN {table("dim_ticker_hc")} t ON c.symbol = t.symbol
        WHERE c.date = ?
          AND c.close >= 5.0
        """,
        params=(selected_date_str,) * 3,
    )
