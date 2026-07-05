"""Rolling daily metric columns for the Gold adjusted daily fact.

Pure DataFrame transform shared by the Gold DLT pipeline (``dim_split_dlt.py``)
and the Spark tests. No DLT/Databricks imports so the logic is unit-testable.

Moves the screener/watchlist window math (lag closes + 20-day relative volume)
from query time into the Gold build: the dashboard's Signal Screener and
Watchlist previously re-derived these with window functions over a 400-day scan
on every cache miss; storing them beside the adjusted bars turns both reads
into single-date point queries with one shared definition of each metric.

Semantics are preserved exactly: the windows run over each symbol's full
retained history ordered by date, which is the same row set the old query-time
CTEs saw for any selectable date (the table's retention window bounds both).
The RVOL COUNT=20 gate is kept so a young ticker reads NULL ("—") in both
dashboard surfaces, matching the previous behaviour.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import avg, col, count, lag, when

# Trading-day offsets for the period-return anchor closes (1W/1M/3M/6M/1Y).
LAG_CLOSE_OFFSETS: dict[str, int] = {
    "close_5d": 5,
    "close_21d": 21,
    "close_63d": 63,
    "close_126d": 126,
    "close_252d": 252,
}

# RVOL baseline: average adj_volume over exactly this many prior trading days.
RVOL_BASELINE_DAYS = 20


def add_daily_metric_columns(adjusted_df: DataFrame) -> DataFrame:
    """Append lag-close anchors and 20-day relative volume to daily adjusted bars.

    Expects at least ``symbol``, ``date``, ``adj_close``, ``adj_volume`` columns
    (the output of ``apply_split_adjustment``). Deterministic: windows order by
    ``date``, which is unique per symbol at daily grain, so re-runs are
    byte-identical and a full recompute after a split refreshes every value.

    ``rvol_20d`` is NULL unless a full RVOL_BASELINE_DAYS-day base of prior
    volume exists (COUNT gate) and that base average is positive — mirroring the
    NULLIF(avg, 0) guard the screener SQL used at query time.
    """
    ordered = Window.partitionBy("symbol").orderBy("date")
    baseline = ordered.rowsBetween(-RVOL_BASELINE_DAYS, -1)

    df = adjusted_df
    for name, offset in LAG_CLOSE_OFFSETS.items():
        df = df.withColumn(name, lag("adj_close", offset).over(ordered))

    baseline_count = count("adj_volume").over(baseline)
    baseline_avg = avg("adj_volume").over(baseline)
    return df.withColumn(
        "rvol_20d",
        when(
            (baseline_count == RVOL_BASELINE_DAYS) & (baseline_avg > 0),
            col("adj_volume") / baseline_avg,
        ),
    )
