"""
News quarantine deduplication helpers for the Silver news DLT pipeline.

Mirrors ohlcv_quarantine_spark.py: keeps the window-based dedup pattern out of
the pipeline file so it can be tested and reasoned about independently.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, xxhash64
from pyspark.sql.window import Window


def dedupe_news_quarantine_invalid_rows(df: DataFrame, key_col: str = "article_id") -> DataFrame:
    """Deduplicate invalid news rows before writing to quarantine.

    Deterministic winner: descending xxhash64 of key fields so the surviving
    row is stable across replays and pipeline restarts — no watermark state
    store, no eviction.

    Args:
        df: DataFrame of invalid news rows (pre-filtered to records that failed
            validation).
        key_col: Column to partition by for deduplication (default: "article_id").

    Returns:
        Deduplicated DataFrame with one row per key_col value.
    """
    _win = Window.partitionBy(key_col).orderBy(
        xxhash64(col(key_col), col("title"), col("article_url"), col("ingestion_timestamp")).desc()
    )
    return df.withColumn("_qrn", row_number().over(_win)).filter(col("_qrn") == 1).drop("_qrn")
