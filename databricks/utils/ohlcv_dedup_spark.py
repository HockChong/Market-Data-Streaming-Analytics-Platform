"""Silver OHLCV ``source_priority`` and ``_dedup_sequence`` — shared with DLT and tests."""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql.functions import col, lit, struct, when, xxhash64


def with_silver_source_priority(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "source_priority",
        when(col("source") == "polygon_flatfiles_s3", lit(2))
        .when(col("source") == "polygon_kafka_delayed_streaming", lit(1))
        .otherwise(lit(0)),
    )


def silver_ohlcv_payload_hash_long() -> Column:
    return (
        xxhash64(
            col("symbol"),
            col("start_timestamp"),
            col("end_timestamp"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("source"),
        )
        .bitwiseAND(lit(0x7FFFFFFFFFFFFFFF))  # strip sign bit so cast to signed long is always non-negative
        .cast("long")
    )


def silver_ohlcv_dedup_sequence_struct() -> Column:
    return struct(
        col("source_priority").cast("int"),
        col("ingestion_timestamp").cast("timestamp"),
        silver_ohlcv_payload_hash_long(),
    )


def with_silver_ohlcv_dedup_sequence(df: DataFrame) -> DataFrame:
    """Append ``_dedup_sequence``; expects ``source_priority`` and ``ingestion_timestamp``."""
    return df.withColumn("_dedup_sequence", silver_ohlcv_dedup_sequence_struct())
