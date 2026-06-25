"""Split / reverse-split price adjustment (Option A — full recompute).

Pure DataFrame transforms shared by the Gold DLT pipeline (``dim_split_dlt.py``)
and the Spark tests. No DLT/Databricks imports so the logic is unit-testable.

Source of the factor (Polygon ``/stocks/v1/splits``):
    ``historical_adjustment_factor`` is the CUMULATIVE factor — it already folds in
    every split from a given event forward to today. We do NOT recompute it.

Vendor rule (verbatim): for a price on date D, find the FIRST split whose
``execution_date`` is after D and multiply the unadjusted price by that split's
``historical_adjustment_factor``. A price on/after the most recent split keeps
factor 1.0 (today's share basis is canonical). Volume is rescaled by the inverse.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import broadcast, coalesce, col, lag, lit
from pyspark.sql.functions import round as spark_round


def latest_splits_snapshot_to_dim_df(df: DataFrame) -> DataFrame:
    """Project a Bronze ``splits`` snapshot to the ``dim_split_hc`` layout.

    Drops vendor rows that would make a factor unusable (null/non-positive),
    so a single bad row can never corrupt every adjusted price downstream.
    """
    return df.select(
        col("symbol"),
        col("execution_date").cast("date").alias("execution_date"),
        col("split_from").cast("double").alias("split_from"),
        col("split_to").cast("double").alias("split_to"),
        col("adjustment_type"),
        col("historical_adjustment_factor").cast("double").alias("historical_adjustment_factor"),
    ).where(col("historical_adjustment_factor").isNotNull() & (col("historical_adjustment_factor") > 0))


def adjustment_factor_segments(splits_df: DataFrame) -> DataFrame:
    """Turn split events into non-overlapping date segments, one factor each.

    For splits at ascending dates e1 < e2 < ... < en, a price on date D in
    ``[e_(k-1), e_k)`` is rescaled by split k's factor (the first split after D).
    So each split row owns the half-open range ``[valid_from, valid_to)`` where
    ``valid_to`` = its own ``execution_date`` and ``valid_from`` = the previous
    split's ``execution_date`` (NULL = unbounded past). Prices on/after the last
    split match no segment and fall through to factor 1.0 at apply time.
    """
    prev = Window.partitionBy("symbol").orderBy(col("execution_date").asc())
    return splits_df.select(
        col("symbol"),
        lag("execution_date").over(prev).alias("valid_from"),
        col("execution_date").alias("valid_to"),
        col("historical_adjustment_factor").alias("price_factor"),
        (lit(1.0) / col("historical_adjustment_factor")).alias("volume_factor"),
    )


def add_split_adjusted_columns(prices_df: DataFrame, segments_df: DataFrame, price_scale: int = 6) -> DataFrame:
    """Join price bars to factor segments and emit adjusted OHLCV beside raw.

    Grain-agnostic: the match is on ``date`` only, so it works for both daily and
    1-minute bars. Left join so symbols with no split (and rows after the latest
    split) keep factor 1.0 and pass through unchanged. Segments are non-overlapping
    per symbol, so each price row matches at most one segment.

    Does NOT add ``prev_adj_close`` — that is a daily-only, cross-session concept
    (``lag`` over ``date`` is ill-defined at minute grain where many rows share a
    date). Daily callers use ``apply_split_adjustment``, which wraps this.
    """
    cond = (
        (prices_df["symbol"] == segments_df["symbol"])
        & (prices_df["date"] < segments_df["valid_to"])
        & (segments_df["valid_from"].isNull() | (prices_df["date"] >= segments_df["valid_from"]))
    )
    price_factor = coalesce(segments_df["price_factor"], lit(1.0))
    volume_factor = coalesce(segments_df["volume_factor"], lit(1.0))

    # Broadcast the segments side: it is always small (one row per split event per
    # symbol), while prices_df can be ~19.5M minute rows over the Gold lookback. The
    # join is symbol-equi plus a date-range residual; without the hint Spark may fall
    # back to a sort-merge join keyed on symbol, where hot tickers (AAPL/TSLA) skew a
    # single reducer. A map-side broadcast removes that skew. Daily callers share this
    # path and benefit equally — segments is tiny in both cases.
    return prices_df.join(broadcast(segments_df), cond, "left").select(
        prices_df["*"],
        spark_round(prices_df["open"] * price_factor, price_scale).alias("adj_open"),
        spark_round(prices_df["high"] * price_factor, price_scale).alias("adj_high"),
        spark_round(prices_df["low"] * price_factor, price_scale).alias("adj_low"),
        spark_round(prices_df["close"] * price_factor, price_scale).alias("adj_close"),
        spark_round(prices_df["volume"] * volume_factor, 0).cast("bigint").alias("adj_volume"),
        spark_round(price_factor, 8).alias("price_factor"),
    )


def apply_split_adjustment(prices_df: DataFrame, segments_df: DataFrame, price_scale: int = 6) -> DataFrame:
    """Daily bars: split-adjusted columns plus ``prev_adj_close``.

    Thin wrapper over ``add_split_adjusted_columns`` that appends ``prev_adj_close``
    for the Gold "unexplained gap" data-quality check — a large day-over-day move on
    the ADJUSTED series usually means a split we don't have. Daily grain only.
    """
    adjusted = add_split_adjusted_columns(prices_df, segments_df, price_scale)
    prev_close_window = Window.partitionBy("symbol").orderBy("date")
    return adjusted.withColumn("prev_adj_close", lag("adj_close").over(prev_close_window))
