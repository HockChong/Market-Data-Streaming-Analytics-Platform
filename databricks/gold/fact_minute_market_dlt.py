"""
Gold Layer: Minute Market Fact Table — Delta Live Tables

Grain: one row per (symbol, date, start_timestamp).
Key: (symbol, start_timestamp) — FK to dim_ticker_hc.
Idempotency: full recompute from Silver ohlcv_silver_hc each run.

Bounded to a rolling window (default 5 calendar days) to control scan cost.
Cross-pipeline read via spark.read.table().
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

# Minimum lookback: dashboard queries use a few days of 1-min data; keep margin for replays.
_MIN_MINUTE_LOOKBACK_DAYS = 5
if _config.minute_lookback_days < _MIN_MINUTE_LOOKBACK_DAYS:
    raise ValueError(
        f"minute_lookback_days={_config.minute_lookback_days} is too short. "
        f"Requires >= {_MIN_MINUTE_LOOKBACK_DAYS} calendar days."
    )


@dlt.table(
    name="fact_minute_market_hc",
    comment="1-minute OHLCV fact table. FK: symbol->dim_ticker.symbol. Quality enforced at Silver (WAP pattern).",
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
        correlation_id STRING
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
def fact_minute_market():
    """Gold table — market-hours 1-minute bars promoted from Silver.

    Silver WAP (ohlcv_silver_quarantine_hc + wap_audit_log_hc) enforces OHLC
    validity before rows reach this table. Gold adds expect_or_fail on PK
    columns only — a null here indicates a Silver pipeline misconfiguration.

    Cross-pipeline read: ohlcv_silver_hc is owned by the Silver DLT pipeline.
    dlt.read() cannot reference tables from a different pipeline, so
    spark.read.table() is required. Ensure the Silver pipeline task completes
    before this Gold pipeline is triggered in Databricks job orchestration.
    """
    return (
        spark.read.table(_config.get_fully_qualified_table("ohlcv_silver_hc"))
        .filter(col("date") >= expr(f"current_date() - interval {_config.minute_lookback_days} days"))
        .select(
            col("symbol"),
            col("date"),
            col("start_timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            current_timestamp().alias("processing_timestamp"),
            lit(_PIPELINE_CORRELATION_ID).alias("correlation_id"),
        )
    )
