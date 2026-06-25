"""
Spark helpers for Silver OHLCV WAP quarantine (testable without DLT runtime).

Used by ``databricks/silver/ohlcv_silver_dlt.py`` so dedup / rejection_reason
logic can be exercised in local Spark integration tests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr, lit, row_number, when, xxhash64
from pyspark.sql.window import Window


def dedupe_quarantine_invalid_rows(invalid_df: DataFrame) -> DataFrame:
    """One row per (symbol, start_timestamp, source) with deterministic winner.

    Invalid rows only — caller must pre-filter with WAP validation. Tie-break:
    descending ``xxhash64`` over
    (symbol, start_timestamp, source, open, high, low, close, volume) so the
    surviving row is a function of payload content, not batch order.
    """
    _quarantine_rank = xxhash64(
        col("symbol"),
        col("start_timestamp"),
        col("source"),
        col("open"),
        col("high"),
        col("low"),
        col("close"),
        col("volume"),
    )
    _quarantine_window = Window.partitionBy("symbol", "start_timestamp", "source").orderBy(_quarantine_rank.desc())
    return invalid_df.withColumn("_qrn", row_number().over(_quarantine_window)).filter(col("_qrn") == 1).drop("_qrn")


def with_quarantine_rejection_reason(df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Attach ``rejection_reason`` using the same precedence as ``ohlcv_silver_quarantine``."""
    is_valid_price_positive = expr(rules["valid_price_positive"])
    is_valid_ohlc_logic = expr(rules["valid_ohlc_logic"])
    is_valid_volume = expr(rules["valid_volume"])
    return df.withColumn(
        "rejection_reason",
        when(~is_valid_price_positive, lit("invalid_price_positive"))
        .when(~is_valid_ohlc_logic, lit("invalid_ohlc_logic"))
        .when(~is_valid_volume, lit("invalid_volume"))
        .otherwise(lit("unknown")),
    )
