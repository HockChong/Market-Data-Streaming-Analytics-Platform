"""
Market cap tier labels for the Gold dim_ticker transform (Spark).

Thresholds match Bronze ticker snapshot → dim_ticker path: Mega >$200B, Large >$10B,
Mid >$2B, Small >$300M, Micro >$50M, else Nano; null → Unknown.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql.functions import lit, when


def market_cap_category_column(cap_col: Column) -> Column:
    """Native Spark when/otherwise mapping a market-cap column to its tier label."""
    return (
        when(cap_col.isNull(), lit("Unknown"))
        .when(cap_col > 200_000_000_000, lit("Mega Cap"))
        .when(cap_col > 10_000_000_000, lit("Large Cap"))
        .when(cap_col > 2_000_000_000, lit("Mid Cap"))
        .when(cap_col > 300_000_000, lit("Small Cap"))
        .when(cap_col > 50_000_000, lit("Micro Cap"))
        .otherwise(lit("Nano Cap"))
    )
