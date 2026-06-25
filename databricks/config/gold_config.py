"""
Gold Layer Configuration Module

Centralized configuration for Gold layer Delta Live Tables (DLT) pipelines.
Inherits common settings from BaseConfig.

Gold Layer Tables (Star Schema):
- Dimensions: `dim_date_hc`, `dim_ticker_hc`, `dim_split_hc`
- Facts: `fact_daily_market_hc`, `fact_daily_market_adjusted_hc`, `fact_minute_market_hc`, `fact_minute_market_adjusted_hc`, `fact_news_hc`
"""

from datetime import date

from base_config import BaseConfig


class GoldConfig(BaseConfig):
    """
    Configuration manager for Gold layer DLT pipelines.

    Inherits from BaseConfig for shared constants (paths, thresholds, market hours).

    Star Schema Tables:
    - dim_date_hc: Date dimension for time-based analytics
    - dim_ticker_hc: Ticker reference dimension
    - dim_split_hc: Stock split events with cumulative adjustment factor
    - fact_daily_market_hc: Raw daily OHLCV (returns live in fact_daily_market_adjusted_hc)
    - fact_daily_market_adjusted_hc: fact_daily_market_hc + dim_split_hc -> daily OHLCV with split-adjusted columns beside raw
    - fact_minute_market_hc: 1-minute OHLCV (market hours)
    - fact_minute_market_adjusted_hc: fact_minute_market_hc + dim_split_hc -> 1-minute OHLCV with split-adjusted columns beside raw
    - fact_news_hc: News articles with article-ticker grain
    """

    def __init__(self):
        super().__init__()

        # ===== Gold-Specific Table List =====
        self.dimension_tables = ["dim_date_hc", "dim_ticker_hc", "dim_split_hc"]
        self.fact_tables = [
            "fact_daily_market_hc",
            "fact_daily_market_adjusted_hc",
            "fact_minute_market_hc",
            "fact_minute_market_adjusted_hc",
            "fact_news_hc",
        ]

        # ===== Daily Fact Lookback =====
        # fact_daily_market reads ohlcv_daily_silver_hc (a Silver materialized
        # view at (symbol, date) grain).
        self.daily_fact_lookback_days = BaseConfig.DAILY_AGGREGATION_LOOKBACK_DAYS

        # ===== Minute Fact Lookback =====
        # Bounded window over ohlcv_silver_hc (market hours). 5 calendar days is the
        # floor that still retains the prior trading session across a weekend + holiday
        # for the dashboard's 2-day intraday horizon, while halving the per-run scan
        # under the 5-minute trigger cadence.
        self.minute_lookback_days = 5

        # ===== Date Dimension Configuration =====
        self.dim_date_start = "2020-01-01"
        # Dynamic end date: current year + 5 years to avoid silent join failures
        self.dim_date_end = f"{date.today().year + 5}-12-31"

    def get_checkpoint_path(self, pipeline_name: str) -> str:
        """Get checkpoint path for DLT pipeline."""
        return super().get_checkpoint_path("gold", pipeline_name)

    # Note: get_gold_path() is inherited from BaseConfig

    def print_config(self):
        """Print configuration summary."""
        print("=" * 60)
        print("Gold Layer Configuration - Star Schema")
        print("=" * 60)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"Volume Base Path: {self.volume_base_path}")
        print()
        print("Dimension Tables:")
        for i, table in enumerate(self.dimension_tables, 1):
            print(f"  {i}. {table}")
        print()
        print("Fact Tables:")
        for i, table in enumerate(self.fact_tables, 1):
            print(f"  {i}. {table}")
        print()
        print("Fact Lookback Windows:")
        print(f"  - Daily:    {self.daily_fact_lookback_days} calendar days")
        print(f"  - Minute:   {self.minute_lookback_days} calendar days")
        print()
        print("Date Dimension Range:")
        print(f"  - Start: {self.dim_date_start}")
        print(f"  - End: {self.dim_date_end}")
        print()
        print("Symbol Validation:")
        print(f"  - Length: {self.SYMBOL_MIN_LENGTH}-{self.SYMBOL_MAX_LENGTH} chars")
        print("=" * 60)


if __name__ == "__main__":
    config = GoldConfig()
    config.print_config()
