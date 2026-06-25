"""
Silver Layer: News Articles - Delta Live Tables Pipeline
=========================================================

WHAT IT DOES:
    Cleans, validates, and deduplicates news articles from Bronze layer.
    Routes invalid records to quarantine table for visibility and compliance.

DATA FLOW:
    Bronze news → This Pipeline → news_silver_hc (valid articles)
                        ↓
               news_silver_quarantine_hc (rejected articles)
                        ↓
               Gold: fact_news_hc

TABLES CREATED:
    | Table                      | Type      | Description                             |
    |----------------------------|-----------|-----------------------------------------|
    | news_silver_hc             | Streaming | Cleaned, deduplicated news articles     |
    | news_silver_quarantine_hc  | Batch     | WAP: Rejected records with reasons      |

QUALITY CHECKS:
    - Article ID: Must be non-null (expect_or_fail)
    - Title: Must exist and be non-empty after trimming (script-neutral)
    - URL: Must exist and start with "http"
    - Timestamps: Published date <= ingestion date

WAP PATTERN (Write-Audit-Publish):
    - Invalid records go to quarantine (not silently dropped)
    - Rejection reasons tracked for debugging
    - 365-day retention for auditability
"""

import sys

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

import dlt
from news_quarantine_spark import dedupe_news_quarantine_invalid_rows
from pyspark.sql.functions import (
    array_size,
    coalesce,
    col,
    current_timestamp,
    expr,
    from_utc_timestamp,
    length,
    lit,
    to_date,
    to_timestamp,
    trim,
    when,
)
from silver_config import SilverConfig

# =============================================================================
# Configuration
# =============================================================================

_config = SilverConfig()

BRONZE_NEWS_PATH = _config.get_bronze_path("news")

# Richness threshold sourced from SilverConfig; drives the has_description
# boolean enrichment flag below. Not a validation gate — the valid_title rule
# only requires a non-empty title (see SilverConfig.get_news_expectations).
MIN_DESCRIPTION_LENGTH = _config.NEWS_MIN_DESCRIPTION_LENGTH

# News uses a longer late-arrival watermark than OHLCV (defined in BaseConfig)
LATE_ARRIVAL_WATERMARK = _config.NEWS_LATE_ARRIVAL_WATERMARK

NEWS_WAP_VALIDATION_RULES = _config.get_news_expectations()


# =============================================================================
# Bronze News Source
# =============================================================================


@dlt.table(
    name="bronze_news_source_hc",
    comment="Bronze news data from Polygon.io News API (streaming source for Silver)",
    schema="""
        article_id STRING,
        title STRING,
        description STRING,
        author STRING,
        published_utc STRING,
        article_url STRING,
        amp_url STRING,
        image_url STRING,
        tickers ARRAY<STRING>,
        keywords ARRAY<STRING>,
        insights ARRAY<STRUCT<sentiment: STRING, sentiment_reasoning: STRING, ticker: STRING>>,
        publisher_name STRING,
        publisher_homepage_url STRING,
        publisher_logo_url STRING,
        publisher_favicon_url STRING,
        published_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        processing_timestamp TIMESTAMP,
        correlation_id STRING,
        source STRING,
        date DATE,
        num_tickers INT,
        tickers_str STRING
    """,
    table_properties={"quality": "bronze"},
)
def bronze_news_source_hc():
    """Streaming read from Bronze news Delta table.

    Streaming unifies backfill (first run processes all existing data) and
    real-time ingestion in one code path; DLT manages checkpoints automatically.
    """
    return (
        spark.readStream.format("delta")
        .option("ignoreDeletes", "true")
        .option("skipChangeCommits", "true")
        .load(BRONZE_NEWS_PATH)
    )


# =============================================================================
# Silver News — intermediate enriched table (feeds apply_changes dedup)
# =============================================================================
# Mirrors the OHLCV pattern: validate + enrich in a temporary table, then use
# apply_changes to MERGE on article_id so article corrections (higher
# ingestion_timestamp) win over stale first-writes — unlike dropDuplicates
# which silently kept the first occurrence for the lifetime of the state store.


@dlt.table(
    name="news_silver_enriched_hc",
    temporary=True,
    comment="Validated + enriched news stream feeding apply_changes dedup (not persisted)",
)
@dlt.expect_or_fail("valid_article_id", "article_id IS NOT NULL AND LENGTH(article_id) > 0")
@dlt.expect_or_fail("valid_published_date", "published_utc IS NOT NULL")
def news_silver_enriched():
    """Validated + enriched intermediate table for apply_changes dedup.

    Filters to valid records only, adds enrichment columns, then hands off to
    apply_changes below.  Dedup is handled by the MERGE (sequence_by=ingestion_timestamp)
    rather than by streaming state, so article corrections (re-published with a newer
    ingestion_timestamp) correctly overwrite the stale first-write in Silver.
    """
    df = dlt.read_stream("bronze_news_source_hc")

    is_valid_title = expr(NEWS_WAP_VALIDATION_RULES["valid_title"])
    is_valid_url = expr(NEWS_WAP_VALIDATION_RULES["valid_url"])
    is_valid_timestamp = expr(NEWS_WAP_VALIDATION_RULES["valid_timestamp_order"])
    is_all_valid = is_valid_title & is_valid_url & is_valid_timestamp

    return (
        df.withWatermark("ingestion_timestamp", LATE_ARRIVAL_WATERMARK)
        .filter(is_all_valid)
        .withColumn(
            "published_date", to_date(from_utc_timestamp(to_timestamp(col("published_utc")), "America/New_York"))
        )
        .withColumn("cleaned_title", trim(col("title")))
        .withColumn("cleaned_description", trim(coalesce(col("description"), lit(""))))
        .withColumn(
            "has_description", when(length(col("cleaned_description")) >= MIN_DESCRIPTION_LENGTH, True).otherwise(False)
        )
        .withColumn("has_image", when(col("image_url").isNotNull(), True).otherwise(False))
        .withColumn("tickers_count", when(col("tickers").isNotNull(), array_size(col("tickers"))).otherwise(0))
        .withColumn("silver_processing_timestamp", current_timestamp())
    )


# =============================================================================
# Silver News Table (Main Output) — MERGE-based dedup via apply_changes
# =============================================================================

dlt.create_streaming_table(
    name="news_silver_hc",
    comment="Cleaned, deduplicated news articles — MERGE on article_id (SCD1 via apply_changes)",
    schema="""
        article_id STRING,
        title STRING,
        description STRING,
        author STRING,
        published_utc STRING,
        article_url STRING,
        amp_url STRING,
        image_url STRING,
        tickers ARRAY<STRING>,
        keywords ARRAY<STRING>,
        insights ARRAY<STRUCT<sentiment: STRING, sentiment_reasoning: STRING, ticker: STRING>>,
        publisher_name STRING,
        publisher_homepage_url STRING,
        publisher_logo_url STRING,
        publisher_favicon_url STRING,
        published_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        processing_timestamp TIMESTAMP,
        correlation_id STRING,
        source STRING,
        date DATE,
        num_tickers INT,
        tickers_str STRING,
        published_date DATE,
        cleaned_title STRING,
        cleaned_description STRING,
        has_description BOOLEAN,
        has_image BOOLEAN,
        tickers_count INT,
        silver_processing_timestamp TIMESTAMP
    """,
    partition_cols=["published_date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "article_id",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "6",
        "delta.checkpoint.writeStatsAsStruct": "true",
    },
)

dlt.apply_changes(
    target="news_silver_hc",
    source="news_silver_enriched_hc",
    keys=["article_id"],
    # Higher ingestion_timestamp wins: a Polygon article correction (re-published
    # with a newer ingestion_timestamp) correctly overwrites the stale first-write.
    # Under the old dropDuplicates pattern, a re-publish outside the 1h watermark
    # window would silently double-emit and the stale row would survive in Silver.
    sequence_by="ingestion_timestamp",
    ignore_null_updates=True,
    stored_as_scd_type=1,
)


# =============================================================================
# WAP: News Silver Quarantine Table
# =============================================================================


@dlt.table(
    name="news_silver_quarantine_hc",
    comment="WAP: Quarantined news articles that failed validation with rejection reasons",
    schema="""
        article_id STRING,
        title STRING,
        description STRING,
        author STRING,
        published_utc STRING,
        article_url STRING,
        amp_url STRING,
        image_url STRING,
        tickers ARRAY<STRING>,
        keywords ARRAY<STRING>,
        insights ARRAY<STRUCT<sentiment: STRING, sentiment_reasoning: STRING, ticker: STRING>>,
        publisher_name STRING,
        publisher_homepage_url STRING,
        publisher_logo_url STRING,
        publisher_favicon_url STRING,
        published_timestamp TIMESTAMP,
        ingestion_timestamp TIMESTAMP,
        processing_timestamp TIMESTAMP,
        correlation_id STRING,
        source STRING,
        date DATE,
        num_tickers INT,
        tickers_str STRING,
        rejection_reason STRING,
        published_date DATE,
        quarantined_at TIMESTAMP
    """,
    # Partition on published_date only. rejection_reason has just ~3 distinct
    # values, so partitioning by (published_date, rejection_reason) fanned each
    # day into up to 3 mostly-tiny files on a sparse table (only invalid articles
    # land here). It moves to zOrderCols instead — single-reason audit queries
    # still skip files, without the small-file blow-up. Mirrors the OHLCV
    # quarantine layout (see ohlcv_silver_dlt.py).
    partition_cols=["published_date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "article_id,rejection_reason",
        "delta.deletedFileRetentionDuration": "interval 365 days",  # retain quarantine records for auditability
        "delta.enableDeletionVectors": "true",
    },
)
def news_silver_quarantine():
    """WAP quarantine table for rejected news articles.

    Reads Bronze as a batch snapshot (dlt.read) so that row_number().over()
    is valid — non-time-based Window functions are not supported on streaming
    DataFrames. Mirrors ohlcv_silver_quarantine: quarantine is an audit log,
    not a latency-sensitive output, so batch mode is correct here.

    Dedup key: article_id. Tiebreaker: descending xxhash64 of payload content
    so the surviving row is a function of data, not batch order.

    Rejection reasons:
    - invalid_title: Title is null or empty after trimming
    - invalid_url: URL is null or doesn't start with 'http'
    - invalid_timestamp_order: Published timestamp > ingestion timestamp
    """
    # Batch read: window-based dedup is safe on a static DataFrame.
    df = dlt.read("bronze_news_source_hc")

    is_valid_title = expr(NEWS_WAP_VALIDATION_RULES["valid_title"])
    is_valid_url = expr(NEWS_WAP_VALIDATION_RULES["valid_url"])
    is_valid_timestamp = expr(NEWS_WAP_VALIDATION_RULES["valid_timestamp_order"])

    is_all_valid = is_valid_title & is_valid_url & is_valid_timestamp

    invalid_df = df.filter(~is_all_valid)
    deduped_invalid = dedupe_news_quarantine_invalid_rows(invalid_df)

    return (
        deduped_invalid.withColumn(
            "rejection_reason",
            when(~is_valid_title, lit("invalid_title"))
            .when(~is_valid_url, lit("invalid_url"))
            .when(~is_valid_timestamp, lit("invalid_timestamp_order"))
            .otherwise(lit("unknown")),
        )
        .withColumn(
            "published_date", to_date(from_utc_timestamp(to_timestamp(col("published_utc")), "America/New_York"))
        )
        .withColumn("quarantined_at", current_timestamp())
    )
