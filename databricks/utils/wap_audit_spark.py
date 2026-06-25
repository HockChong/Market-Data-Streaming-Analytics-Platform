"""WAP audit daily aggregates — shared by Silver ``wap_audit_log`` and tests."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import coalesce, col, count, expr, lit, when
from pyspark.sql.functions import max as max_
from pyspark.sql.functions import sum as sum_


def aggregate_bronze_wap_counts_by_date(bronze_with_date_df: DataFrame, rules: dict[str, str]) -> DataFrame:
    """Group pre-dedup Bronze (already has ``date``) into daily WAP counts.

    ``rules`` must provide keys: valid_price_positive, valid_ohlc_logic, valid_volume
    (same as ``SilverConfig.get_wap_validation_rules()``).
    """
    is_valid_price_positive = expr(rules["valid_price_positive"])
    is_valid_ohlc_logic = expr(rules["valid_ohlc_logic"])
    is_valid_volume = expr(rules["valid_volume"])
    is_all_valid = is_valid_price_positive & is_valid_ohlc_logic & is_valid_volume

    return bronze_with_date_df.groupBy("date").agg(
        count("*").alias("total_count"),
        sum_(when(~is_all_valid, 1).otherwise(0)).alias("rejected_count"),
        sum_(when(~is_valid_price_positive, 1).otherwise(0)).alias("rejected_price_positive"),
        sum_(when(is_valid_price_positive & ~is_valid_ohlc_logic, 1).otherwise(0)).alias("rejected_ohlc_logic"),
        sum_(when(is_valid_price_positive & is_valid_ohlc_logic & ~is_valid_volume, 1).otherwise(0)).alias(
            "rejected_volume"
        ),
    )


def aggregate_session_bars_by_date(silver_df: DataFrame) -> DataFrame:
    """Per-date fullest-session bar count — a coarse market-wide-gap signal.

    ``silver_df`` must be deduped, market-hours-filtered Silver (``ohlcv_silver_hc``):
    one row per (symbol, start_timestamp) already inside ``[9:30, close)`` ET. Reading
    deduped Silver (not pre-dedup Bronze) means a Kafka replay can't inflate the count
    and hide a gap, and extended-hours bars are excluded so the count reflects the
    regular session.

    ``session_bars`` = the most bars any single symbol reached that day (~390 on a
    normal session, ~210 on an early-close half-day). A value far below that on a
    trading day means even the most active symbol barely traded — i.e. a market-wide
    ingestion gap. Checked against ``EXPECTED_BARS_PER_DAY`` in the ``session_complete``
    warn gate. Non-trading days have no rows and never appear.

    Returns one row per date: ``session_bars``.
    """
    per_symbol_day = silver_df.groupBy("date", "symbol").agg(count("*").alias("bars"))
    return per_symbol_day.groupBy("date").agg(max_("bars").alias("session_bars"))


def finalize_wap_audit_metrics(counts_df: DataFrame, wap_thresholds: dict) -> DataFrame:
    """Add valid_count, rejection_rate_pct, and quality gate flags (matches DLT)."""
    return (
        counts_df.withColumn("total_count", coalesce(col("total_count"), lit(0)))
        .withColumn("rejected_count", coalesce(col("rejected_count"), lit(0)))
        .withColumn("valid_count", col("total_count") - col("rejected_count"))
        .withColumn(
            "rejection_rate_pct",
            when(col("total_count") > 0, (col("rejected_count") / col("total_count")) * 100).otherwise(0.0),
        )
        .withColumn("quality_gate_passed", col("rejection_rate_pct") < wap_thresholds["rejection_rate_critical"])
        .withColumn("quality_gate_warning", col("rejection_rate_pct") >= wap_thresholds["rejection_rate_warning"])
    )
