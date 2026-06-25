"""
Gold Layer: Daily Market Fact Table — Delta Live Tables

Grain: one row per (symbol, date).
Key: (symbol, date) — FK to dim_ticker_hc and dim_date_hc.
Idempotency: full recompute from Silver ohlcv_daily_silver_hc each Gold run.
The Silver daily table is a materialized view (serverless incremental refresh) on
(symbol, date); Gold replaces its fact partition each run from the current Silver snapshot.

Reads pre-aggregated daily bars from Silver (~2.9M rows) instead of
scanning ~420M minute rows directly. Cross-pipeline read via
spark.read.table() — ensure Silver pipeline completes before Gold.
"""

import sys

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

import uuid

import dlt
from gold_config import GoldConfig
from pyspark.sql.functions import col, current_timestamp, expr, lit

_config = GoldConfig()

# One UUID per DLT pipeline execution — all rows written in the same run share
# this value, making it trivial to correlate Gold rows back to a single batch.
_PIPELINE_CORRELATION_ID = str(uuid.uuid4())


@dlt.table(
    name="fact_daily_market_hc",
    comment="Daily OHLCV. FK: symbol->dim_ticker.symbol, date->dim_date.date",
    schema="""
        symbol STRING,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        processing_timestamp TIMESTAMP,
        correlation_id STRING
    """,
    cluster_by=["date", "symbol"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_fail("valid_symbol", "symbol IS NOT NULL")
@dlt.expect_or_fail("valid_date", "date IS NOT NULL")
def fact_daily_market():
    """Gold table — raw daily OHLCV.

    Reads pre-aggregated daily bars from ohlcv_daily_silver_hc (~2.9M rows)
    instead of scanning ~420M minute rows directly. Silver maintains that
    table as a materialized view with serverless incremental refresh (see
    ohlcv_silver_dlt.py); during market hours today's daily row is a rolling
    snapshot until session close.

    Prices here are raw (unadjusted). Daily returns are NOT computed on this
    table: a split-day move on raw close is a mechanical drop, not an economic
    return. The split-safe return lives in fact_daily_market_adjusted_hc as
    prev_adj_close (and the dashboard derives the % from adj_close).

    OHLCV validity (prices, OHLC relationships, volume) is enforced at Silver
    via the WAP pattern (ohlcv_silver_quarantine_hc). Gold enforces PK columns
    only — a null symbol or date indicates a pipeline misconfiguration, not a
    data issue.

    Cross-pipeline read: ohlcv_daily_silver_hc is owned by the Silver DLT
    pipeline. dlt.read() cannot reference tables from a different pipeline, so
    spark.read.table() is required. Layer 3 orchestration: trigger Gold only
    after the Silver OHLCV DLT pipeline update succeeds (see DEPLOYMENT_GUIDE.md).
    """
    daily = spark.read.table(_config.get_fully_qualified_table("ohlcv_daily_silver_hc"))
    return daily.filter(
        col("date") >= expr(f"current_date() - interval {_config.daily_fact_lookback_days} days")
    ).select(
        "symbol",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        current_timestamp().alias("processing_timestamp"),
        lit(_PIPELINE_CORRELATION_ID).alias("correlation_id"),
    )
