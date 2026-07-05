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

    Reads the rolling metrics (prev_adj_close, close_5d/21d/63d, rvol_20d)
    materialized on fact_daily_market_adjusted_hc by the Gold pipeline
    (databricks/utils/daily_metrics_spark.py) — the same stored columns the
    screener reads, so both surfaces share one definition of each metric.
    ROW_NUMBER picks each symbol's own latest session (a halted ticker still
    shows its last trade); the 150-day bound keeps the scan short while
    preserving the previous inclusion window for stale symbols.

    Metrics are computed from the split-ADJUSTED series, so period-return
    columns stay continuous across stock splits.
    """
    if not tickers:
        return pd.DataFrame(columns=_QUOTE_COLUMNS)
    placeholders = ", ".join("?" * len(tickers))
    return run_query_versioned(
        f"""
        WITH latest AS (
            SELECT
                symbol,
                adj_open   AS open,
                adj_close  AS close,
                adj_volume AS volume,
                prev_adj_close AS prev_close,
                rvol_20d,
                close_5d,
                close_21d,
                close_63d,
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
        FROM latest c
        JOIN {table("dim_ticker_hc")} t ON c.symbol = t.symbol
        WHERE c.rn = 1
        """,
        params=tuple(tickers),
    )
