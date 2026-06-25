"""
Shared utilities for Bronze layer ingestion notebooks.

Centralizes functions and schemas used by both historical and incremental
flat-file ingestion to eliminate duplication.
"""

from datetime import datetime

import pandas as pd
from base_config import BaseConfig
from pyspark.sql.functions import col, current_timestamp, lit, row_number, xxhash64
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

# =============================================================================
# Polygon flat-file CSV schema (single source of truth)
# Reference: https://polygon.io/flat-files
# =============================================================================

FLATFILES_CSV_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), False),
        StructField("volume", DoubleType(), False),  # DoubleType: Polygon sometimes emits fractional volume
        StructField("open", DoubleType(), False),
        StructField("close", DoubleType(), False),
        StructField("high", DoubleType(), False),
        StructField("low", DoubleType(), False),
        StructField("window_start", LongType(), False),  # Bar start time (nanoseconds since epoch)
        StructField("transactions", IntegerType(), True),
    ]
)

NANOS_TO_MILLIS = 1_000_000
MINUTE_IN_MILLIS = 60_000


def generate_date_paths(start_date_str, end_date_str, bucket, prefix):
    """
    Generate s3a:// paths for each business day in a date range.

    Skips weekends automatically via pandas.bdate_range.  Holidays and
    non-trading days are NOT skipped here; callers handle missing files at
    read time (e.g. by catching exceptions per-file or using ignoreMissingFiles).

    Args:
        start_date_str: Start date string in YYYY-MM-DD format.
        end_date_str:   End date string in YYYY-MM-DD format.
        bucket:         S3-compatible bucket name (e.g. "flatfiles").
        prefix:         Object-key prefix within the bucket
                        (e.g. "us_stocks_sip/minute_aggs_v1").
                        If the prefix was stored with the bucket name prepended
                        (e.g. "flatfiles/us_stocks_sip/..."), the bucket portion
                        is stripped automatically before building the path.

    Returns:
        List[str]: One s3a:// path per business day, following the Polygon
        flat-file layout: s3a://<bucket>/<prefix>/<YYYY>/<MM>/<YYYY-MM-DD>.csv.gz
    """
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    business_days = pd.bdate_range(start_date, end_date)

    paths = []
    for day in business_days:
        year, month = day.strftime("%Y"), day.strftime("%m")
        date_str = day.strftime("%Y-%m-%d")
        clean_prefix = prefix.replace(f"{bucket}/", "")
        paths.append(f"s3a://{bucket}/{clean_prefix}/{year}/{month}/{date_str}.csv.gz")
    return paths


def transform_flatfile_ohlcv(raw_df, correlation_id: str, file_ingestion_ts_ms: int):
    """Apply the standard Bronze schema mapping to a raw Polygon flat-file DataFrame.

    Shared by historical and incremental ingestion; the only post-transform
    difference between those callers is that incremental adds dropDuplicates.

    Args:
        file_ingestion_ts_ms: S3 last-modified time for the source file (epoch ms).
            Stored as ``ingestion_timestamp`` so it reflects when Polygon made the
            data available, not when the operator ran the notebook.  Two re-runs of
            the same file produce the same ``ingestion_timestamp``, making the
            Silver dedup sequence deterministic across retries.
    """
    return (
        raw_df.withColumnRenamed("ticker", "symbol")
        .withColumn("start_timestamp", (col("window_start") / NANOS_TO_MILLIS).cast("long"))
        .withColumn("end_timestamp", col("start_timestamp") + MINUTE_IN_MILLIS)
        .withColumn("event_type", lit("AM"))
        .withColumn("otc", lit(None).cast("boolean"))
        .withColumn("ingestion_timestamp", (lit(file_ingestion_ts_ms) / lit(1000)).cast("timestamp"))
        .withColumn("processing_timestamp", current_timestamp())
        .withColumn("correlation_id", lit(correlation_id))
        .withColumn("source", lit("polygon_flatfiles_s3"))
        .withColumn("ts_unit", lit(BaseConfig.TS_UNIT_MILLISECONDS))
        .withColumn("date", BaseConfig.timestamp_to_date(col("start_timestamp"), col("ts_unit")))
        .withColumn("volume", col("volume").cast(LongType()))
        .select(
            "symbol",
            "event_type",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "start_timestamp",
            "end_timestamp",
            "ts_unit",
            "transactions",
            "otc",
            "ingestion_timestamp",
            "processing_timestamp",
            "correlation_id",
            "source",
            "date",
        )
    )


def dedupe_incremental_bronze_keys(df):
    """Deterministically keep one Bronze row per (symbol, start_timestamp).

    Incremental loads can contain conflicting duplicates for the same key in a
    single batch. This helper defines a stable winner policy:
      1) latest ingestion_timestamp
      2) latest processing_timestamp
      3) highest correlation_id (deterministic tie-break)
      4) highest source (deterministic tie-break)
      5) stable full-row hash fallback
    """
    ordering = [
        col("ingestion_timestamp").desc_nulls_last(),
        col("processing_timestamp").desc_nulls_last(),
        col("correlation_id").desc_nulls_last(),
        col("source").desc_nulls_last(),
        xxhash64(*[col(c) for c in df.columns]).desc(),
    ]
    window = Window.partitionBy("symbol", "start_timestamp").orderBy(*ordering)
    return df.withColumn("_dedup_rank", row_number().over(window)).filter(col("_dedup_rank") == 1).drop("_dedup_rank")
