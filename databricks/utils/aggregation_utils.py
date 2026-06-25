"""
Aggregation utilities for Silver/Gold pipelines.

- aggregate_minute_to_daily: minute bars -> daily OHLCV
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct
from pyspark.sql.functions import max as _max
from pyspark.sql.functions import min as _min
from pyspark.sql.functions import sum as _sum


def aggregate_minute_to_daily(df: DataFrame) -> DataFrame:
    """Aggregate minute-level OHLCV to daily bars.

    Picks first-bar open (argmin over start_timestamp) and last-bar close
    (argmax over start_timestamp) via min/max of a (start_timestamp, price)
    struct: the struct compares start_timestamp first, so min/max selects the
    earliest/latest bar and carries that bar's price along. (symbol,
    start_timestamp) is unique per group after Silver dedup, so there are no
    timestamp ties and this is identical to the prior row_number() approach —
    but as a pure groupBy of associative aggregates it skips the per-group sort.

    Being a pure groupBy of associative aggregates is also what lets the daily
    materialized view (``ohlcv_daily_silver_hc``) refresh incrementally on
    serverless — only changed (symbol, date) groups are recomputed.

    Args:
        df: columns symbol, date, start_timestamp, open, high, low, close, volume.

    Returns:
        Daily OHLCV: symbol, date, open, high, low, close, volume.
    """
    result = df.groupBy("symbol", "date").agg(
        _min(struct(col("start_timestamp"), col("open"))).alias("_first"),
        _max("high").alias("high"),
        _min("low").alias("low"),
        _max(struct(col("start_timestamp"), col("close"))).alias("_last"),
        _sum("volume").alias("volume"),
    )
    return result.select(
        "symbol",
        "date",
        col("_first.open").alias("open"),
        "high",
        "low",
        col("_last.close").alias("close"),
        "volume",
    )
