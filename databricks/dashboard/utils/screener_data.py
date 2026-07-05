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
    """Fetch all screener rows for the selected date as a single-date point read.

    The rolling context (lag closes close_5d..close_252d and rvol_20d) is
    materialized on fact_daily_market_adjusted_hc by the Gold pipeline
    (databricks/utils/daily_metrics_spark.py), so this query prunes straight to
    one date via the table's (date, symbol) clustering instead of window-scanning
    400 days of history per cache miss. The stored metrics are computed from the
    split-ADJUSTED series, so period-return columns stay continuous across splits.
    """
    return _run_query(
        f"""
        SELECT
            f.symbol                                                                       AS Symbol,
            t.company_name                                                                 AS Company,
            t.sector                                                                       AS Sector,
            f.adj_close                                                                    AS Close,
            ROUND((f.adj_close - f.prev_adj_close) / NULLIF(f.prev_adj_close, 0) * 100, 2) AS `Chg %`,
            ROUND((f.adj_close - f.adj_open)       / NULLIF(f.adj_open, 0)       * 100, 2) AS `Intraday %`,
            ROUND((f.adj_open  - f.prev_adj_close) / NULLIF(f.prev_adj_close, 0) * 100, 2) AS `Gap %`,
            ROUND(f.rvol_20d, 2)                                                           AS RVOL,
            f.adj_volume                                                                   AS Volume,
            ROUND(f.adj_close * f.adj_volume, 2)                                           AS `Dollar Volume`,
            ROUND((f.adj_close - f.close_5d)   / NULLIF(f.close_5d,   0) * 100, 2)         AS `1W Gain %`,
            ROUND((f.adj_close - f.close_21d)  / NULLIF(f.close_21d,  0) * 100, 2)         AS `1M Gain %`,
            ROUND((f.adj_close - f.close_63d)  / NULLIF(f.close_63d,  0) * 100, 2)         AS `3M Gain %`,
            ROUND((f.adj_close - f.close_126d) / NULLIF(f.close_126d, 0) * 100, 2)         AS `6M Gain %`,
            ROUND((f.adj_close - f.close_252d) / NULLIF(f.close_252d, 0) * 100, 2)         AS `1Y Gain %`
        FROM {table("fact_daily_market_adjusted_hc")} f
        JOIN {table("dim_ticker_hc")} t ON f.symbol = t.symbol
        WHERE f.date = ?
          AND f.adj_close >= 5.0
        """,
        params=(selected_date_str,),
    )
