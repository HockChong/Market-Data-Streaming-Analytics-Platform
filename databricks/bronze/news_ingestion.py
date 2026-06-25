# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: News Article Ingestion
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Fetches news articles from Polygon.io News API and stores them in Bronze Delta Lake.
# MAGIC Captures sentiment insights from Polygon's AI analysis for downstream signal scoring.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon News API → This Notebook → Bronze Delta Lake (news)
# MAGIC                                            ↓
# MAGIC                                    Silver DLT Pipeline
# MAGIC                                            ↓
# MAGIC                              Gold fact_news_hc
# MAGIC ```
# MAGIC
# MAGIC ## Ingestion Modes
# MAGIC | Mode | Use Case | Parameters |
# MAGIC |------|----------|------------|
# MAGIC | `realtime` | Scheduled job | `LOOKBACK_MINUTES` |
# MAGIC | `backfill` | Initial load or gap filling | `BACKFILL_START_DATE`, `BACKFILL_END_DATE` |
# MAGIC
# MAGIC ## Key Features
# MAGIC - **Append-only**: Bronze is immutable — new articles and corrections land as new rows; Silver `apply_changes` owns dedup
# MAGIC - **Change Data Feed**: Enabled for optional incremental consumers
# MAGIC - **Sentiment Insights**: Captures Polygon's AI-generated sentiment per ticker
# MAGIC
# MAGIC ## Configuration (Section 3)
# MAGIC - `INGESTION_MODE`: "realtime" or "backfill"
# MAGIC - `LOOKBACK_MINUTES`: First-run fallback only — watermark takes over once table exists (default: 60)
# MAGIC - `TICKER_FILTER`: Optional list like ["AAPL", "MSFT"] or [] for all
# MAGIC - `BATCH_SIZE`: Articles per API call (default: 1000)
# MAGIC
# MAGIC ## Recommended Scheduling (2 Databricks Jobs)
# MAGIC | Job | Cron (ET) | LOOKBACK_MINUTES | Purpose |
# MAGIC |-----|-----------|------------------|---------|
# MAGIC | Market Hours | `*/15 9-15 * * MON-FRI` | 15 | Core ingestion (watermark-driven) |
# MAGIC | Daily Catch-up | `0 6 * * MON-FRI` | 60 | Off-hours + weekend gap (watermark-driven) |
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/news`
# MAGIC - **Downstream**: `news_silver_dlt.py` (sentiment insights nested in `news_silver_hc.insights` array; separate insights table deferred)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration
# MAGIC
# MAGIC Initializes Spark session, loads configuration, and validates settings.
# MAGIC Installs `polygon-api-client` package for Polygon SDK.

# COMMAND ----------

# DBTITLE 1,Cell 3
import sys

sys.path.insert(
    0,
    "/Workspace"
    + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0]
    + "/config",
)
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()
# COMMAND ----------
# MAGIC %pip install pytz
# COMMAND ----------
# DBTITLE 1,Install polygon package
# MAGIC %pip install massive
# COMMAND ----------
# MAGIC %pip install tenacity
# COMMAND ----------
# DBTITLE 1,Cell 4
from datetime import datetime, timedelta, timezone

from bronze_config import BronzeConfig
from massive import RESTClient
from massive.rest.models import TickerNews

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    array_join,
    col,
    current_timestamp,
    from_utc_timestamp,
    lit,
    size,
    to_date,
    to_timestamp,
)
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pytz import timezone as pytz_tz
from simple_logger import SimpleLogger

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
logger = SimpleLogger("news_ingestion", dbutils)
logger.log_start()

try:
    config.validate_api()
except Exception as e:
    logger.log_error("Configuration validation failed", error=e, remediation="Check BronzeConfig and secrets")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validate Polygon API Key

# COMMAND ----------

try:
    polygon_api_key = config.polygon_api_key
    if not polygon_api_key:
        raise ValueError(
            f"polygon-api-key is empty or not set. Scope: {config.scope}, value type: {type(polygon_api_key)}"
        )
    logger.log_info(f"Polygon API key loaded ({len(polygon_api_key)} chars)")
except Exception as e:
    logger.log_error(
        "Polygon API key validation failed",
        error=e,
        remediation=f"Add secret 'polygon-api-key' to Databricks scope '{config.scope}' via scripts/add_secrets_rest_api.py",
    )
    logger.exit_job("failed", "API key validation failed")
    raise  # Safety net: ensure execution halts if exit_job fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration Parameters
# MAGIC
# MAGIC Parameters are driven by **`dbutils.widgets`** so Databricks Jobs can pass values at runtime.
# MAGIC Scheduling logic lives in the Databricks Job definitions, not in this notebook.
# MAGIC
# MAGIC | Parameter | Default | Description |
# MAGIC |-----------|---------|-------------|
# MAGIC | `INGESTION_MODE` | `realtime` | `realtime` (scheduled) or `backfill` (date range) |
# MAGIC | `LOOKBACK_MINUTES` | `15` | Minutes to look back for articles (set by job schedule) |
# MAGIC | `BACKFILL_START_DATE` | *(empty)* | Start date for backfill mode (YYYY-MM-DD) |
# MAGIC | `BACKFILL_END_DATE` | *(empty)* | End date for backfill mode (YYYY-MM-DD) |
# MAGIC | `TICKER_FILTER` | *(empty)* | Comma-separated tickers e.g. `AAPL,MSFT` or empty for all |
# MAGIC | `BATCH_SIZE` | `1000` | Articles per API call |

# COMMAND ----------

dbutils.widgets.text("INGESTION_MODE", "realtime")
dbutils.widgets.text("LOOKBACK_MINUTES", "60")
dbutils.widgets.text("BACKFILL_START_DATE", "", "Backfill Start (YYYY-MM-DD, ET)")
dbutils.widgets.text("BACKFILL_END_DATE", "", "Backfill End (YYYY-MM-DD, ET)")
dbutils.widgets.text("TICKER_FILTER", "")
dbutils.widgets.text("BATCH_SIZE", "1000")

INGESTION_MODE = dbutils.widgets.get("INGESTION_MODE")
BATCH_SIZE = int(dbutils.widgets.get("BATCH_SIZE"))

if INGESTION_MODE == "backfill":
    BACKFILL_START_DATE = dbutils.widgets.get("BACKFILL_START_DATE")
    BACKFILL_END_DATE = dbutils.widgets.get("BACKFILL_END_DATE")
    if not BACKFILL_START_DATE or not BACKFILL_END_DATE:
        logger.log_error("Backfill mode requires BACKFILL_START_DATE and BACKFILL_END_DATE")
        logger.exit_job("failed", "Missing backfill date parameters")
        raise RuntimeError("Missing backfill date parameters")  # Safety net
    REALTIME_LOOKBACK_MINUTES = None
else:
    REALTIME_LOOKBACK_MINUTES = int(dbutils.widgets.get("LOOKBACK_MINUTES"))
    logger.log_info(f"Realtime mode: lookback = {REALTIME_LOOKBACK_MINUTES} minutes")
    BACKFILL_START_DATE = None
    BACKFILL_END_DATE = None

ticker_filter_param = dbutils.widgets.get("TICKER_FILTER").strip()
TICKER_FILTER = [t.strip() for t in ticker_filter_param.split(",") if t.strip()] if ticker_filter_param else []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Polygon SDK Client

# COMMAND ----------

try:
    polygon_client = RESTClient(api_key=polygon_api_key)
except Exception as e:
    logger.log_error("Failed to initialize Polygon SDK client", error=e)
    logger.exit_job("failed", "Polygon client initialization failed")
    raise  # Safety net: ensure execution halts if exit_job fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fetch News Articles
# MAGIC
# MAGIC Fetches news articles from Polygon News API based on ingestion mode.
# MAGIC
# MAGIC **Realtime Mode**:
# MAGIC - Uses watermark: reads `max(published_utc)` from Bronze table as the start of the fetch window
# MAGIC - First run fallback: if Bronze table does not exist yet, falls back to `LOOKBACK_MINUTES`
# MAGIC - Self-healing: if the job is down for hours or days, the next run automatically covers the full gap
# MAGIC
# MAGIC **Backfill Mode**:
# MAGIC - Fetches articles for specified date range
# MAGIC - Uses full-day bounds (00:00:00 to 23:59:59)
# MAGIC
# MAGIC **Ticker Filtering**:
# MAGIC - If `TICKER_FILTER` is provided: Fetches news for each ticker separately
# MAGIC - If empty: Fetches all news articles (may be slower)

# COMMAND ----------

# DBTITLE 1,Cell 14
from delta.tables import DeltaTable

bronze_path = config.get_bronze_path("news")

# Build published_utc window for Polygon API (RFC3339: YYYY-MM-DDTHH:MM:SSZ)
if INGESTION_MODE == "realtime":
    end_time = datetime.now(timezone.utc)
    if DeltaTable.isDeltaTable(spark, bronze_path):
        watermark_row = (
            spark.read.format("delta").load(bronze_path).selectExpr("max(published_utc) as watermark").collect()[0]
        )
        watermark_str = watermark_row["watermark"]
        if watermark_str:
            start_time = datetime.strptime(watermark_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            logger.log_info(f"Watermark: {watermark_str} — fetching from last ingested article")
        else:
            start_time = end_time - timedelta(minutes=REALTIME_LOOKBACK_MINUTES)
            logger.log_info(f"Watermark null — falling back to {REALTIME_LOOKBACK_MINUTES} min lookback")
    else:
        start_time = end_time - timedelta(minutes=REALTIME_LOOKBACK_MINUTES)
        logger.log_info(f"First run — using {REALTIME_LOOKBACK_MINUTES} min lookback")
    published_gte = start_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    published_lte = end_time.strftime("%Y-%m-%dT%H:%M:%SZ")
else:
    # For backfill: interpret user-provided dates as ET (trading day boundaries),
    # then convert to UTC for the Polygon API query
    et_tz = pytz_tz("America/New_York")

    # Start of day in ET → convert to UTC
    backfill_start_et = et_tz.localize(datetime.strptime(BACKFILL_START_DATE, "%Y-%m-%d"))
    published_gte = backfill_start_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # End of day in ET → convert to UTC (cap at current time if today or future)
    backfill_end_et = et_tz.localize(
        datetime.strptime(BACKFILL_END_DATE, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    )
    now = datetime.now(timezone.utc)

    if backfill_end_et.astimezone(timezone.utc) >= now:
        published_lte = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.log_info(
            f"BACKFILL_END_DATE ({BACKFILL_END_DATE}) is today or future - using current time as upper bound"
        )
    else:
        published_lte = backfill_end_et.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # Warn if backfill start date is in the future
    if backfill_start_et.astimezone(timezone.utc) > now:
        logger.log_info(
            f"WARNING: BACKFILL_START_DATE ({BACKFILL_START_DATE}) is in the future. No articles will exist for future dates."
        )

logger.log_info("Fetching news articles with parameters:")
logger.log_info(f"  Mode: {INGESTION_MODE}")
logger.log_info(f"  published_utc_gte: {published_gte}")
logger.log_info(f"  published_utc_lte: {published_lte}")
logger.log_info(f"  TICKER_FILTER: {TICKER_FILTER if TICKER_FILTER else '[] (all tickers)'}")
logger.log_info(f"  BATCH_SIZE: {BATCH_SIZE}")


from news_transforms import extract_article as _extract_article
from tenacity import retry, stop_after_attempt, wait_exponential


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def _fetch_ticker_news(ticker=None):
    """Fetch and materialise Polygon news articles with exponential backoff (handles HTTP 429)."""
    params = dict(
        published_utc_gte=published_gte,
        published_utc_lte=published_lte,
        order="desc",
        limit=BATCH_SIZE,
        sort="published_utc",
    )
    if ticker:
        params["ticker"] = ticker
    return [item for item in polygon_client.list_ticker_news(**params) if isinstance(item, TickerNews)]


all_articles = []
articles_fetched_count = 0

try:
    if TICKER_FILTER:
        logger.log_info(f"Fetching news for {len(TICKER_FILTER)} ticker(s): {TICKER_FILTER}")
        for ticker in TICKER_FILTER:
            items = _fetch_ticker_news(ticker=ticker)
            articles_fetched_count += len(items)
            all_articles.extend([_extract_article(item) for item in items])
            logger.log_info(f"Fetched {len(items)} articles for ticker {ticker}")
    else:
        logger.log_info("Fetching news for all tickers (no filter)")
        items = _fetch_ticker_news()
        articles_fetched_count += len(items)
        all_articles.extend([_extract_article(item) for item in items])

    logger.log_info(
        f"API returned {articles_fetched_count} items, {len(all_articles)} valid TickerNews objects processed"
    )

except Exception as e:
    logger.log_error("Failed to fetch news from Polygon API", error=e)
    logger.exit_job("failed", f"News fetch failed - {str(e)}")
    raise  # Safety net: ensure execution halts if exit_job fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Transform and Create DataFrame

# COMMAND ----------

if not all_articles:
    logger.log_info(f"No articles fetched for date range: {published_gte} to {published_lte}")
    logger.log_info("Possible reasons:")
    logger.log_info("  1. Date range is in the future (no articles exist yet)")
    logger.log_info("  2. Date range has no news articles (weekend/holiday)")
    logger.log_info("  3. API returned empty results for this date range")
    logger.log_info("  4. TICKER_FILTER may be too restrictive")
    logger.log_info(f"Total items from API: {articles_fetched_count}, Valid articles: {len(all_articles)}")
    logger.exit_job("success", "No news articles to process")
    dbutils.notebook.exit("No news articles to process")

transformed_articles = []
for article in all_articles:
    publisher = article.get("publisher", {})
    transformed_articles.append(
        {
            "article_id": article.get("id"),
            "title": article.get("title"),
            "description": article.get("description"),
            "author": article.get("author"),
            "published_utc": article.get("published_utc"),
            "article_url": article.get("article_url"),
            "amp_url": article.get("amp_url"),
            "image_url": article.get("image_url"),
            "tickers": article.get("tickers", []),
            "keywords": article.get("keywords", []),
            "insights": article.get("insights", []),
            "publisher_name": publisher.get("name"),
            "publisher_homepage_url": publisher.get("homepage_url"),
            "publisher_logo_url": publisher.get("logo_url"),
            "publisher_favicon_url": publisher.get("favicon_url"),
        }
    )

insight_struct = StructType(
    [
        StructField("sentiment", StringType(), True),
        StructField("sentiment_reasoning", StringType(), True),
        StructField("ticker", StringType(), True),
    ]
)

insights_array = ArrayType(insight_struct)

news_schema = StructType(
    [
        StructField("article_id", StringType(), False),
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("author", StringType(), True),
        StructField("published_utc", StringType(), True),
        StructField("article_url", StringType(), True),
        StructField("amp_url", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("tickers", ArrayType(StringType()), True),
        StructField("keywords", ArrayType(StringType()), True),
        StructField("insights", insights_array, True),  # Array of structs with sentiment analysis
        StructField("publisher_name", StringType(), True),
        StructField("publisher_homepage_url", StringType(), True),
        StructField("publisher_logo_url", StringType(), True),
        StructField("publisher_favicon_url", StringType(), True),
    ]
)

df = spark.createDataFrame(transformed_articles, schema=news_schema)
bronze_df = (
    df.withColumn("published_timestamp", to_timestamp(col("published_utc")))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("correlation_id", lit(logger.correlation_id))
    .withColumn("source", lit("polygon_news_api"))
    .withColumn("date", to_date(from_utc_timestamp(col("published_timestamp"), config.MARKET_TIMEZONE)))
    .withColumn("num_tickers", size(col("tickers")))
    .withColumn("tickers_str", array_join(col("tickers"), ","))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Philosophy
# MAGIC
# MAGIC Bronze captures ALL raw data (no filtering). Quality checks in Silver layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write to Bronze Delta Lake
# MAGIC
# MAGIC Writes articles to Bronze Delta Lake using append-only strategy.
# MAGIC
# MAGIC **Write Strategy**:
# MAGIC - **Append-only**: All runs use `mode("append")` — Bronze is an immutable audit log
# MAGIC - Corrections from Polygon land as new rows with a higher `ingestion_timestamp`; Silver `apply_changes` picks the latest version via `sequence_by=ingestion_timestamp`
# MAGIC - **Change Data Feed (CDF)**: Enabled on table creation for optional incremental consumers
# MAGIC
# MAGIC **Partitioning**: By `date` (ET trading date derived from `published_timestamp`)
# MAGIC
# MAGIC **Idempotency**: Duplicate `article_id` rows may exist in Bronze across re-runs; Silver owns dedup

# COMMAND ----------

table_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.autoOptimize.optimizeWrite": "true",
    "delta.autoOptimize.autoCompact": "true",
}

try:
    (bronze_df.write.format("delta").mode("append").partitionBy("date").options(**table_properties).save(bronze_path))
    logger.log_info("Append write completed")

except Exception as e:
    logger.log_error("Failed to write to Delta Lake", error=e, remediation=f"Check path: {bronze_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Job Summary

# COMMAND ----------

# Get metrics from Delta transaction log (instant - no data scan required)
delta_table = DeltaTable.forPath(spark, bronze_path)
latest_history = delta_table.history(1).collect()[0]
operation_metrics = latest_history["operationMetrics"] or {}

records_written = int(operation_metrics.get("numOutputRows", len(all_articles)))

logger.log_job_summary(
    status="success",
    records_read=len(all_articles),
    records_written=records_written,
    extra_metrics={
        "mode": INGESTION_MODE,
        "date_range": f"{published_gte} to {published_lte}",
        "output_path": bronze_path,
        "note": "All records written to Bronze. Silver DLT handles quality filtering.",
    },
)

logger.exit_job("success", f"{records_written} articles written to Bronze")
