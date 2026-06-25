"""
Gold Layer: Ticker Dimension — Delta Live Tables

Grain: one row per symbol (SCD Type 1 — full refresh each run).
Key: symbol (PK).
Source: latest Bronze ticker_details snapshot (max snapshot_date).

Maps SIC codes to sectors, classifies market cap tiers, and derives
industry descriptions from security type for non-SIC instruments (ETFs, ADRs).
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
from ticker_details_dim_spark import ticker_details_snapshot_to_dim_df

BRONZE_TICKER_DETAILS_PATH = _config.get_bronze_path("ticker_details")


@dlt.table(
    name="dim_ticker_hc",
    comment="Ticker dimension — latest Bronze ticker_details snapshot, Type 1 (full refresh each run)",
    schema="""
        symbol STRING,
        company_name STRING,
        type STRING,
        sector STRING,
        industry STRING,
        exchange STRING,
        market_cap_category STRING,
        is_active BOOLEAN,
        list_date DATE
    """,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol",
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "4",
    },
)
@dlt.expect_all_or_fail({"valid_symbol": "symbol IS NOT NULL AND LENGTH(symbol) >= 1 AND LENGTH(symbol) <= 8"})
# Keep (don't drop) null-exchange rows: delisted names enriched into Bronze may
# lack primary_exchange, and dropping them would re-orphan the very tickers we add
# to fix survivorship bias. Violations are still logged in the DLT event log.
@dlt.expect_all({"valid_exchange": "exchange IS NOT NULL"})
@dlt.expect_all({"has_company_name": "company_name IS NOT NULL"})
@dlt.expect_all({"is_active_known": "is_active IS NOT NULL"})
def dim_ticker_hc():
    bronze = spark.read.format("delta").load(BRONZE_TICKER_DETAILS_PATH)
    latest = bronze.agg(spark_max("snapshot_date").alias("d")).collect()[0]["d"]
    if latest is None:
        raise ValueError(
            f"ticker_details at {BRONZE_TICKER_DETAILS_PATH} has no rows — run ticker_details_ingestion first"
        )
    if latest > date.today() + timedelta(days=7):
        raise ValueError(f"snapshot_date {latest} is more than 7 days in the future — Bronze data may be corrupt")
    snap = bronze.filter(col("snapshot_date") == lit(latest).cast("date"))
    return ticker_details_snapshot_to_dim_df(snap, _config.SIC_SECTOR_MAPPINGS)
