"""Map Bronze ``ticker_details`` snapshot rows to Gold ``dim_ticker`` layout.

Shared by ``dim_ticker_dlt.py`` and Spark integration tests. SIC→sector logic
matches the former ``stocks_metadata_cdc_generator`` notebook (without CDC).
"""

from __future__ import annotations

from market_cap_classification import market_cap_category_column
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when


def sector_column_from_sic_mappings(sic_sector_mappings: dict):
    """Build a Spark column: SIC prefix → sector, with ETF/type fallbacks."""
    sector_expr = None
    for sic_prefix, sector_name in sic_sector_mappings.items():
        condition = col("sic_code").startswith(sic_prefix)
        if sector_expr is None:
            sector_expr = when(condition, lit(sector_name))
        else:
            sector_expr = sector_expr.when(condition, lit(sector_name))
    return sector_expr.otherwise(
        when(
            col("type").isin(
                "ETF",
                "ETN",
                "ETV",
                "FUND",
                "UNIT",
                "SP",
                "WARRANT",
                "RIGHT",
                "PFD",
                "ADRC",
                "ADRP",
                "ADRW",
                "ADRT",
                "OS",
            ),
            lit("Financial Services"),
        ).otherwise(lit("Other"))
    )


def industry_column():
    """Industry description from SIC or security type (same as legacy CDC path)."""
    return (
        when(col("sic_description").isNotNull(), col("sic_description"))
        .when(col("type") == "ETF", lit("Exchange Traded Fund"))
        .when(col("type") == "ETN", lit("Exchange Traded Note"))
        .when(col("type") == "ETV", lit("Exchange Traded Vehicle"))
        .when(col("type").isin("ADRC", "ADRP", "ADRW", "ADRT"), lit("American Depositary Receipt"))
        .when(col("type") == "UNIT", lit("Unit Investment Trust"))
        .when(col("type") == "RIGHT", lit("Rights"))
        .when(col("type") == "PFD", lit("Preferred Stock"))
        .when(col("type") == "FUND", lit("Closed-End Fund"))
        .when(col("type") == "SP", lit("Structured Product"))
        .when(col("type") == "WARRANT", lit("Warrant"))
        .when(col("type") == "OS", lit("Ordinary Shares"))
        .otherwise(lit(None))
        .alias("industry")
    )


def ticker_details_snapshot_to_dim_df(df: DataFrame, sic_sector_mappings: dict) -> DataFrame:
    """Project Bronze ticker_details rows (one snapshot partition) to dim_ticker columns."""
    sector_expr = sector_column_from_sic_mappings(sic_sector_mappings)
    return df.select(
        col("symbol"),
        col("name").alias("company_name"),
        col("type"),
        sector_expr.alias("sector"),
        industry_column(),
        col("primary_exchange").alias("exchange"),
        market_cap_category_column(col("market_cap")).alias("market_cap_category"),
        col("active").alias("is_active"),
        col("list_date").cast("date"),
    )
