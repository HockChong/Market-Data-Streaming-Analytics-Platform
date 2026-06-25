"""Data-access helpers for the Stock Terminal page."""

import pandas as pd
import streamlit as st
from utils.connection import run_query, run_query_uncached, run_query_versioned, table


def _run_query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Execute a query with the current Gold-version cache key."""
    return run_query_versioned(sql, params=params)


def fetch_active_tickers() -> pd.DataFrame:
    """Return active ticker symbols and names."""
    return run_query(
        f"SELECT symbol, company_name FROM {table('dim_ticker_hc')} WHERE is_active = true ORDER BY symbol"
    )


def fetch_daily_market_data(symbol: str, fetch_days: int) -> pd.DataFrame:
    """Fetch daily split-ADJUSTED OHLCV + return columns for a symbol over a window.

    Reads `fact_daily_market_adjusted_hc` and exposes the adjusted columns under the
    raw names so the chart shows a continuous series across splits. Returns are
    computed from `adj_close`/`prev_adj_close`; the raw `price_change_pct` is NOT used
    because it reflects the mechanical price drop on a split day (a fake crash).
    """
    return _run_query(
        f"""
        SELECT
            date, adj_open AS open, adj_high AS high, adj_low AS low,
            adj_close AS close, adj_volume AS volume,
            prev_adj_close AS prev_close,
            CASE
                WHEN prev_adj_close > 0 THEN (adj_close - prev_adj_close) / prev_adj_close * 100
            END AS price_change_pct
        FROM {table("fact_daily_market_adjusted_hc")}
        WHERE symbol = ?
          AND date >= DATE_SUB(CURRENT_DATE(), {fetch_days})
        ORDER BY date
        """,
        params=(symbol,),
    )


@st.cache_data(ttl=300, show_spinner=False)
def fetch_latest_minute_date(symbol: str, minute_version: str = ""):
    """Fetch the latest date available in the minute fact table for a symbol.

    `minute_version` joins the cache key (see connection.get_minute_version): the
    result busts as soon as a new 1-min bar lands. The ttl is only a safety net.
    """
    df = run_query_uncached(
        f"SELECT MAX(date) AS max_date FROM {table('fact_minute_market_adjusted_hc')} WHERE symbol = ?",
        params=(symbol,),
    )
    if df.empty:
        return None
    return df["max_date"].iloc[0]


def fetch_prev_session_close(symbol: str, before_date) -> float | None:
    """Return the split-adjusted close of the most recent session before `before_date`.

    Anchors the intraday "Previous close" reference line to the session actually
    shown in the intraday chart (the latest minute date), not the daily date-range
    picker. Reading `adj_close` keeps the reference split-safe and consistent with
    the adjusted intraday bars.
    """
    if before_date is None or pd.isna(before_date):
        return None
    df = _run_query(
        f"""
        SELECT adj_close
        FROM {table("fact_daily_market_adjusted_hc")}
        WHERE symbol = ?
          AND date < ?
        ORDER BY date DESC
        LIMIT 1
        """,
        params=(symbol, str(before_date)),
    )
    if df.empty or pd.isna(df["adj_close"].iloc[0]):
        return None
    return float(df["adj_close"].iloc[0])


@st.cache_data(ttl=300, show_spinner=False)
def fetch_intraday(symbol: str, minute_date, minute_version: str = "") -> pd.DataFrame:
    """Fetch 1-minute intraday split-ADJUSTED OHLCV bars for a symbol/date.

    Reads `fact_minute_market_adjusted_hc` and exposes the adjusted columns under the
    raw names. Within a single session the adjustment factor is constant, so the chart
    is unchanged for the common case; the reason for adjusted is the 2-day intraday
    horizon, which concatenates two sessions and can straddle a split — raw bars would
    show a mechanical price cliff at the session boundary.

    `minute_version` joins the cache key (see connection.get_minute_version): the
    result busts as soon as a new 1-min bar lands. The ttl is only a safety net.
    """
    if minute_date is None or pd.isna(minute_date):
        return pd.DataFrame()
    return run_query_uncached(
        f"""
        SELECT
            start_timestamp,
            adj_open AS open, adj_high AS high, adj_low AS low,
            adj_close AS close, adj_volume AS volume
        FROM {table("fact_minute_market_adjusted_hc")}
        WHERE symbol = ?
          AND date = ?
        ORDER BY start_timestamp
        """,
        params=(symbol, str(minute_date)),
    )


def fetch_daily_date_range(symbol: str) -> tuple:
    """Return (min_date, max_date) of available daily data for a symbol as date objects."""
    df = _run_query(
        f"""
        SELECT MIN(date) AS min_date, MAX(date) AS max_date
        FROM {table("fact_daily_market_adjusted_hc")}
        WHERE symbol = ?
        """,
        params=(symbol,),
    )
    if df.empty or pd.isna(df["min_date"].iloc[0]):
        return None, None
    return pd.to_datetime(df["min_date"].iloc[0]).date(), pd.to_datetime(df["max_date"].iloc[0]).date()


def fetch_recent_news(symbol: str, since_date: str, limit: int = 50) -> pd.DataFrame:
    """Fetch recent news rows for the ticker sidebar/news panel.

    `since_date` is an inclusive lower bound (YYYY-MM-DD string) anchored to the
    actual data window rather than CURRENT_DATE(), so news appears even when
    ingestion lags behind the calendar.
    """
    return _run_query(
        f"""
        SELECT
            published_date,
            title,
            publisher_name,
            article_url
        FROM {table("fact_news_hc")}
        WHERE symbol = ?
          AND published_date >= ?
        ORDER BY published_date DESC, published_utc DESC
        LIMIT {limit}
        """,
        params=(symbol, since_date),
    )
