"""
Silver Layer Configuration Module

Centralized configuration for Silver layer Delta Live Tables (DLT) pipelines.
Inherits common settings from BaseConfig.
Handles path management, quality thresholds, and pipeline settings.
"""

from base_config import BaseConfig


class SilverConfig(BaseConfig):
    """
    Configuration manager for Silver layer DLT pipelines.

    Inherits from BaseConfig for shared constants (paths, thresholds, market hours).
    Provides paths, quality thresholds, and settings for cleaning
    and validating data from Bronze to Silver layer.
    """

    # News richness threshold — drives the has_description boolean enrichment
    # flag in news_silver_dlt.py. This is a quality *signal*, not a validation
    # gate: the valid_title gate only requires a non-empty title (see
    # get_news_expectations), so non-Latin headlines are not rejected on a raw
    # character count.
    NEWS_MIN_DESCRIPTION_LENGTH = 20

    def __init__(self):
        super().__init__()

        # Silver layer specific targets
        self.duplicate_rate_threshold = 0.005  # <0.5% duplicate rate

    # Note: get_bronze_path(), get_silver_path(), get_gold_path() are inherited from BaseConfig

    def get_checkpoint_path(self, pipeline_name: str) -> str:
        """
        Get checkpoint path for DLT pipeline.

        Args:
            pipeline_name: Name of the DLT pipeline

        Returns:
            Checkpoint path in Unity Catalog Volume
        """
        return super().get_checkpoint_path("silver", pipeline_name)

    def get_news_expectations(self):
        """
        Get shared SQL rules used by News Silver validation and quarantine logic.

        Returns:
            Dictionary of rule name -> SQL condition

        Note: Critical keys are used in expect_or_fail decorators; others are reused
        by filter/quarantine logic in news_silver_dlt.py.
        """
        return {
            # CRITICAL: wired to expect_or_fail in news_silver_dlt.py
            "valid_article_id": "article_id IS NOT NULL AND LENGTH(article_id) > 0",
            "valid_published_date": "published_utc IS NOT NULL",
            # WAP validation rules: invalid rows are routed to quarantine for audit.
            # Non-empty title only (script-neutral): a fixed character-count floor
            # would reject valid CJK headlines that pack a full headline into <10
            # characters. TRIM guards against whitespace-only titles.
            "valid_title": "title IS NOT NULL AND LENGTH(TRIM(title)) > 0",
            "valid_url": "article_url IS NOT NULL AND article_url RLIKE '^https?://[^ ]+$'",
            # Cast published_utc (STRING) to TIMESTAMP before comparing.
            # Without the cast, Spark's implicit STRING→TIMESTAMP coercion returns
            # NULL for malformed dates, which makes the comparison NULL → the row
            # silently passes the check instead of being routed to quarantine.
            # The IS NOT NULL guard catches unparseable strings explicitly.
            "valid_timestamp_order": "to_timestamp(published_utc) IS NOT NULL AND to_timestamp(published_utc) <= ingestion_timestamp",
        }

    def get_wap_config(self):
        """
        Get WAP (Write-Audit-Publish) configuration.

        Returns:
            Dictionary with WAP thresholds
        """
        return {
            # Quality gate thresholds (from BaseConfig)
            "rejection_rate_warning": self.WAP_REJECTION_RATE_WARNING,
            "rejection_rate_critical": self.WAP_REJECTION_RATE_CRITICAL,
            "duplicate_rate_max": self.duplicate_rate_threshold * 100,  # As percentage
            # Retention (from BaseConfig)
            "audit_retention_days": self.WAP_AUDIT_RETENTION_DAYS,
        }

    def get_wap_validation_rules(self):
        """
        Get WAP validation rules for OHLCV data.

        These rules determine which records go to quarantine vs production.
        Invalid records are captured in quarantine for audit trail instead
        of being silently dropped.

        Returns:
            Dictionary of rule name -> SQL condition
        """
        return {
            "valid_price_positive": "close > 0 AND open > 0 AND high > 0 AND low > 0",
            "valid_ohlc_logic": ("high >= low AND high >= open AND high >= close AND low <= open AND low <= close"),
            "valid_volume": "volume >= 0",
        }

    def print_config(self):
        """Print safe configuration summary."""
        print("=" * 80)
        print("Silver Layer Configuration")
        print("=" * 80)
        print(f"Volume Base Path: {self.volume_base_path}")
        print()
        print("Quality Thresholds (from BaseConfig):")
        print(f"  Warning Threshold: {self.QUALITY_THRESHOLD_WARNING * 100}%")
        print(f"  Critical Threshold: {self.QUALITY_THRESHOLD_CRITICAL * 100}%")
        print(f"  Duplicate Rate Threshold: {self.duplicate_rate_threshold * 100}%")
        print()
        print("Paths:")
        print(f"  Bronze Streaming: {self.get_bronze_path('streaming')}")
        print(f"  Bronze Historical: {self.get_bronze_path('historical')}")
        print(f"  Bronze News: {self.get_bronze_path('news')}")
        print(f"  Bronze Ticker Details: {self.get_bronze_path('ticker_details')}")
        print(f"  Silver OHLCV: {self.get_silver_path('ohlcv_silver')}")
        print(f"  Silver News: {self.get_silver_path('news_silver')}")
        print()
        print("Symbol Validation:")
        print(f"  Length: {self.SYMBOL_MIN_LENGTH}-{self.SYMBOL_MAX_LENGTH} chars")
        print()
        print("WAP (Write-Audit-Publish) Configuration:")
        print(f"  Rejection Rate Warning: {self.WAP_REJECTION_RATE_WARNING}%")
        print(f"  Rejection Rate Critical: {self.WAP_REJECTION_RATE_CRITICAL}%")
        print(f"  Audit Retention: {self.WAP_AUDIT_RETENTION_DAYS} days")
        print("=" * 80)


if __name__ == "__main__":
    config = SilverConfig()
    config.print_config()
