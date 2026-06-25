"""
Gold Layer: News Fact Table — Delta Live Tables

Grain: one row per (article_id, symbol) — article-ticker pair.
Key: (article_id, symbol) — FK to dim_ticker_hc; published_date FK to dim_date_hc.
Idempotency: full recompute from Silver news_silver_hc each run.

Explodes tickers array so each article-ticker combination is a separate row.
Cross-pipeline read via spark.read.table().
"""

import sys

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

from gold_config import GoldConfig

_config = GoldConfig()

import uuid

import dlt
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    explode,
    lit,
    substring,
)

# One UUID per DLT pipeline execution — all rows written in the same run share
# this value, making it trivial to correlate Gold rows back to a single batch.
_PIPELINE_CORRELATION_ID = str(uuid.uuid4())


@dlt.table(
    name="fact_news_hc",
    comment="News fact table with article-ticker grain. FK: symbol->dim_ticker.symbol, published_date->dim_date.date",
    schema="""
        article_id STRING,
        symbol STRING,
        published_date DATE,
        published_utc STRING,
        title STRING,
        description STRING,
        article_url STRING,
        publisher_name STRING,
        author STRING,
        processing_timestamp TIMESTAMP,
        correlation_id STRING
    """,
    cluster_by=["published_date", "symbol"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "6",
    },
)
@dlt.expect_or_fail("valid_article_id", "article_id IS NOT NULL")
# symbol comes from explode(tickers) — a null inside the array is non-auditable
# source noise, not a pipeline misconfiguration. expect_or_drop silently removes
# the null-symbol row while preserving the rest of the article's ticker rows.
@dlt.expect_or_drop("valid_symbol", "symbol IS NOT NULL")
def fact_news():
    """Gold table — news articles with one row per article-ticker pair.

    Silver WAP (news_silver_quarantine_hc) enforces title/URL/timestamp validity
    before rows reach this table. Gold uses expect_or_fail for article_id and
    expect_or_drop for non-auditable null symbols from explode(tickers).

    Cross-pipeline read: news_silver_hc is owned by the Silver DLT pipeline.
    dlt.read() cannot reference tables from a different pipeline, so
    spark.read.table() is required. Ensure the Silver pipeline task completes
    before this Gold pipeline is triggered in Databricks job orchestration.
    """
    return (
        spark.read.table(_config.get_fully_qualified_table("news_silver_hc"))
        .filter(col("tickers").isNotNull())
        .withColumn("symbol", explode(col("tickers")))
        .dropDuplicates(["article_id", "symbol"])
        .select(
            col("article_id"),
            col("symbol"),
            col("published_date"),
            col("published_utc"),
            col("cleaned_title").alias("title"),
            substring(coalesce(col("cleaned_description"), lit("")), 1, 500).alias("description"),
            col("article_url"),
            col("publisher_name"),
            col("author"),
            current_timestamp().alias("processing_timestamp"),
            lit(_PIPELINE_CORRELATION_ID).alias("correlation_id"),
        )
    )
