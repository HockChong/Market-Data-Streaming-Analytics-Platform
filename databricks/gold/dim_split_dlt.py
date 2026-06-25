"""
Gold Layer: Split Dimension + Adjusted Fact Tables — Delta Live Tables

dim_split_hc — Grain: one row per split event (symbol, execution_date).
  Source: latest Bronze splits snapshot. Filters out unusable factors (null/non-positive).

fact_daily_market_adjusted_hc — fact_daily_market_hc (raw daily bars) left-joined to
  dim_split_hc (split factors); emits adj_* columns beside the raw OHLCV.
  Full recompute each run (a split rewrites a symbol's entire price history).

fact_minute_market_adjusted_hc — fact_minute_market_hc + dim_split_hc, same factor
  logic as the daily table but at 1-minute grain.
"""

import sys
from datetime import date, timedelta

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from gold_config import GoldConfig

_config = GoldConfig()

import dlt
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import max as spark_max
from split_adjust_spark import (
    add_split_adjusted_columns,
    adjustment_factor_segments,
    apply_split_adjustment,
    latest_splits_snapshot_to_dim_df,
)

BRONZE_SPLITS_PATH = _config.get_bronze_path("splits")


@dlt.table(
    name="dim_split_hc",
    comment="Split dimension — latest Bronze splits snapshot (Polygon /stocks/v1/splits). Grain: one row per split event.",
    schema="""
        symbol STRING,
        execution_date DATE,
        split_from DOUBLE,
        split_to DOUBLE,
        adjustment_type STRING,
        historical_adjustment_factor DOUBLE
    """,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol",
    },
)
@dlt.expect_all_or_fail(
    {
        "valid_symbol": "symbol IS NOT NULL",
        "valid_execution_date": "execution_date IS NOT NULL",
        "positive_factor": "historical_adjustment_factor > 0",
    }
)
def dim_split_hc():
    bronze = spark.read.format("delta").load(BRONZE_SPLITS_PATH)
    latest = bronze.agg(spark_max("snapshot_date").alias("d")).collect()[0]["d"]
    if latest is None:
        raise ValueError(f"splits at {BRONZE_SPLITS_PATH} has no rows — run splits_ingestion first")
    if latest > date.today() + timedelta(days=7):
        raise ValueError(f"snapshot_date {latest} is more than 7 days in the future — Bronze data may be corrupt")
    snap = bronze.filter(col("snapshot_date") == lit(latest).cast("date"))
    return latest_splits_snapshot_to_dim_df(snap)


@dlt.table(
    name="fact_daily_market_adjusted_hc",
    comment="Daily OHLCV with split-adjusted columns beside raw; built from fact_daily_market_hc + dim_split_hc. Grain: one row per (symbol, date).",
    schema="""
        symbol STRING,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        processing_timestamp TIMESTAMP,
        correlation_id STRING,
        adj_open DOUBLE,
        adj_high DOUBLE,
        adj_low DOUBLE,
        adj_close DOUBLE,
        adj_volume BIGINT,
        price_factor DOUBLE,
        prev_adj_close DOUBLE
    """,
    cluster_by=["date", "symbol"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_fail("valid_symbol", "symbol IS NOT NULL")
@dlt.expect_or_fail("valid_date", "date IS NOT NULL")
# DQ sanity check (warn, don't drop): a large overnight move on the ADJUSTED close
# usually means a split we are missing. Surfaces in the DLT event log for review.
@dlt.expect(
    "no_unexplained_gap",
    "adj_close IS NULL OR prev_adj_close IS NULL OR prev_adj_close <= 0 OR abs(adj_close / prev_adj_close - 1) < 0.40",
)
def fact_daily_market_adjusted_hc():
    """Gold table — fact_daily_market_hc (raw daily bars, untouched) left-joined to dim_split_hc (split factors).

    Full recompute on every run (Option A): a split rewrites a symbol's entire
    history, so append-only cannot express it. At ~2.9M daily rows the recompute
    is seconds. Both inputs live in this Gold pipeline, so dlt.read() wires the
    dependency order automatically (dim_split_hc and fact_daily_market_hc first).
    """
    bars = dlt.read("fact_daily_market_hc")
    segments = adjustment_factor_segments(dlt.read("dim_split_hc"))
    return apply_split_adjustment(bars, segments)


@dlt.table(
    name="fact_minute_market_adjusted_hc",
    comment="1-minute OHLCV with split-adjusted columns beside raw; built from fact_minute_market_hc + dim_split_hc. Grain: one row per (symbol, date, start_timestamp).",
    schema="""
        symbol STRING,
        date DATE,
        start_timestamp BIGINT,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        processing_timestamp TIMESTAMP,
        correlation_id STRING,
        adj_open DOUBLE,
        adj_high DOUBLE,
        adj_low DOUBLE,
        adj_close DOUBLE,
        adj_volume BIGINT,
        price_factor DOUBLE
    """,
    cluster_by=["date", "symbol", "start_timestamp"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "8",
    },
)
@dlt.expect_or_fail("valid_symbol", "symbol IS NOT NULL")
@dlt.expect_or_fail("valid_timestamp", "start_timestamp IS NOT NULL")
def fact_minute_market_adjusted_hc():
    """Gold table — fact_minute_market_hc (raw 1-minute bars, untouched) left-joined to dim_split_hc (split factors).

    Mirrors fact_daily_market_adjusted_hc but at minute grain. Reuses the same
    grain-agnostic add_split_adjusted_columns() so adjusted prices line up across a
    split. A single intraday session never spans a split, so this matters only for
    multi-day intraday spans (e.g. a multi-session VWAP, or the dashboard's 2-day
    intraday horizon): the dashboard reads this table's adj_* columns so concatenating
    two sessions across a split shows no mechanical price cliff at the boundary.

    No prev_adj_close / unexplained-gap check: those are daily, cross-session concepts
    (see add_split_adjusted_columns). Full recompute on every run, like the daily
    adjusted table — a split rewrites a symbol's history, which append-only cannot express.
    Both inputs live in this Gold pipeline, so dlt.read() wires the dependency order.
    """
    bars = dlt.read("fact_minute_market_hc")
    segments = adjustment_factor_segments(dlt.read("dim_split_hc"))
    return add_split_adjusted_columns(bars, segments)
