# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Ticker Details Ingestion
# MAGIC
# MAGIC ## What It Does
# MAGIC Fetches company metadata (name, exchange, industry, market cap) from Polygon.io
# MAGIC Ticker Details API and stores it in Bronze Delta Lake.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon Ticker Details API → This Notebook → Bronze Delta Lake (ticker_details)
# MAGIC                                                         ↓
# MAGIC                                       Gold dim_ticker_hc (latest snapshot)
# MAGIC ```
# MAGIC
# MAGIC Reference snapshot — no Silver step. This is a clean, complete universe pulled in
# MAGIC one idempotent snapshot (no stream to dedup/quarantine), so it lands in Bronze and
# MAGIC is shaped straight into the Gold Type-1 dimension (same pattern as splits_ingestion.py).
# MAGIC
# MAGIC ## When to Use
# MAGIC - **Initial setup**: Run once to populate ticker reference data
# MAGIC - **Refresh**: Re-run when you need updated company metadata (IPOs, delistings, etc.)
# MAGIC - **Periodic refresh**: Run to capture new listings and delistings
# MAGIC
# MAGIC ## Input
# MAGIC - Calls Polygon list_tickers API to get all active US stocks
# MAGIC - Calls Polygon API for each ticker's details
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/ticker_details`
# MAGIC - **Columns**: symbol, name, type, active, primary_exchange, sic_code, sic_description, market_cap, ingestion_timestamp, source
# MAGIC - **Write Mode**: Uses MERGE for incremental updates - only updates changed records
# MAGIC
# MAGIC ## Downstream Dependencies
# MAGIC - dim_ticker_dlt.py (Gold layer — reads latest snapshot from this Bronze table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Configuration

# COMMAND ----------

import sys

sys.path.insert(
    0,
    "/Workspace"
    + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0]
    + "/config",
)
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pytz

# COMMAND ----------
# MAGIC %pip install massive
# COMMAND ----------
from bronze_config import BronzeConfig
from massive import RESTClient

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from simple_logger import SimpleLogger
from ticker_details_helpers import fetch_ticker_details_with_retry

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Initialize Spark and Configuration
# MAGIC
# MAGIC Configuration values loaded from BronzeConfig (see bronze_config.py):
# MAGIC - `TICKER_DETAILS_MAX_WORKERS`: Parallel API calls (default: 20)
# MAGIC - `TICKER_DETAILS_MAX_RETRIES`: Retry attempts (default: 3)
# MAGIC - `TICKER_DETAILS_BACKOFF_INITIAL`: Initial backoff time (default: 1.0s)
# MAGIC - `TICKER_DETAILS_BACKOFF_MULTIPLIER`: Backoff multiplier (default: 2.0x)

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
logger = SimpleLogger("ticker_details_ingestion", dbutils)
logger.log_start()

try:
    config.validate_api()
except Exception as e:
    logger.log_error("Configuration validation failed", error=e, remediation="Check BronzeConfig and secrets")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validate Polygon API Key

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
    raise SystemExit("API key validation failed")  # guard: exit_job() may not halt in all contexts

MAX_WORKERS = config.TICKER_DETAILS_MAX_WORKERS
MAX_RETRIES = config.TICKER_DETAILS_MAX_RETRIES
BACKOFF_INITIAL = config.TICKER_DETAILS_BACKOFF_INITIAL
BACKOFF_MULTIPLIER = config.TICKER_DETAILS_BACKOFF_MULTIPLIER
LOG_BATCH_SIZE = config.TICKER_DETAILS_LOG_BATCH_SIZE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Initialize Polygon REST Client

# COMMAND ----------

try:
    polygon_client = RESTClient(api_key=polygon_api_key)
    logger.log_info("Polygon REST client initialized successfully")
except Exception as e:
    logger.log_error("Failed to initialize Polygon SDK client", error=e)
    logger.exit_job("failed", "Polygon client initialization failed")
    raise SystemExit("Polygon client initialization failed")  # guard: exit_job() may not halt in all contexts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fetch Active US Stocks
# MAGIC
# MAGIC Calls list_tickers API to get all active stocks.
# MAGIC The SDK iterator handles pagination automatically via next_url.
# MAGIC
# MAGIC **Filters:**
# MAGIC - `market="stocks"`: US stock market only
# MAGIC - `active=True`: Only currently active tickers

# COMMAND ----------

try:
    ticker_list = []
    for t in polygon_client.list_tickers(
        market="stocks",
        active=True,
        order="asc",
        limit=1000,
        sort="ticker",
    ):
        ticker_list.append(t.ticker)
    logger.log_info(f"Found {len(ticker_list)} active stocks from Polygon API")
except Exception as e:
    logger.log_error(
        "Failed to fetch tickers from Polygon API",
        error=e,
        remediation="Check Polygon API key and network connectivity",
    )
    logger.exit_job("failed", "Could not fetch ticker list from API")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5b. Add Delisted Symbols Present in Our OHLCV History
# MAGIC
# MAGIC The active-only list omits delisted/renamed names. Any such symbol still in our
# MAGIC Bronze OHLCV history becomes a dimension-miss against `dim_ticker_hc` (price rows with no
# MAGIC ticker row), silently dropping from INNER joins and biasing historical sector/
# MAGIC return rollups (survivorship bias). We bound the extra fetch to symbols we
# MAGIC actually have price data for and let the existing detail loop enrich them.

# COMMAND ----------


def _distinct_ohlcv_symbols() -> set:
    """Distinct symbols across Bronze OHLCV (streaming + historical).

    Read-only scoping query — does not modify Bronze. Missing/empty paths are
    skipped so a fresh environment (no historical backfill yet) still runs.
    """
    symbols: set = set()
    for source_key in ("streaming", "historical"):
        path = config.get_bronze_path(source_key)
        try:
            rows = spark.read.format("delta").load(path).select("symbol").distinct().collect()
            symbols.update(r["symbol"] for r in rows if r["symbol"])
        except Exception as e:
            logger.log_info(f"Skipping OHLCV symbol scan for '{source_key}' ({path}): {e}")
    return symbols


_active_set = set(ticker_list)
_delisted_symbols = sorted(_distinct_ohlcv_symbols() - _active_set)
ticker_list.extend(_delisted_symbols)
logger.log_info(
    f"Universe: {len(_active_set)} active + {len(_delisted_symbols)} delisted/inactive "
    f"from OHLCV history = {len(ticker_list)} total to fetch"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fetch Ticker Details with Retry Logic
# MAGIC
# MAGIC Fetches ticker details from Polygon API for each unique ticker.
# MAGIC
# MAGIC **Processing:**
# MAGIC - Iterates through all unique tickers
# MAGIC - Calls Polygon API get_ticker_details() for each ticker
# MAGIC - Handles errors gracefully (404 for delisted tickers, API errors)
# MAGIC - Logs progress every LOG_BATCH_SIZE tickers
# MAGIC
# MAGIC **Error Handling:**
# MAGIC - 404 errors: Ticker not found (may be delisted) - logged but not fatal
# MAGIC - Other errors: Logged with error message, ticker added to failed list
# MAGIC - Continues processing remaining tickers even if some fail
# MAGIC
# MAGIC **Retry Logic:**
# MAGIC - Exponential backoff for transient errors (1s → 2s → 4s)
# MAGIC - Automatic retry for rate limits (429) and server errors (5xx)
# MAGIC - Simple, maintainable retry logic without circuit breaker complexity

# COMMAND ----------


def fetch_single_ticker(ticker: str):
    """Fetch details for a single ticker with exponential backoff retry.

    Returns (ticker_data, error) tuple.
    """
    return fetch_ticker_details_with_retry(
        ticker,
        polygon_client.get_ticker_details,
        max_retries=MAX_RETRIES,
        backoff_initial=BACKOFF_INITIAL,
        backoff_multiplier=BACKOFF_MULTIPLIER,
    )


# COMMAND ----------

all_ticker_details = []
failed_tickers = []
start_time = time.time()
completed_count = 0

logger.log_info(f"Starting to fetch details for {len(ticker_list)} tickers using {MAX_WORKERS} parallel workers")
logger.log_info(
    f"Retry configuration: max_retries={MAX_RETRIES}, backoff={BACKOFF_INITIAL}s (multiplier={BACKOFF_MULTIPLIER}x)"
)

with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    future_to_ticker = {executor.submit(fetch_single_ticker, ticker): ticker for ticker in ticker_list}

    for future in as_completed(future_to_ticker):
        ticker = future_to_ticker[future]
        completed_count += 1

        try:
            ticker_data, error = future.result()
            if ticker_data:
                all_ticker_details.append(ticker_data)
            elif error:
                error_msg = error.get("error", "")

                if "404" in error_msg or "not found" in error_msg.lower():
                    logger.log_info(f"Ticker {ticker} not found (may be delisted)")

                failed_tickers.append(error)

        except Exception as e:
            logger.log_info(f"Unexpected error for {ticker}: {str(e)}")
            failed_tickers.append({"ticker": ticker, "error": str(e)})

        if completed_count % LOG_BATCH_SIZE == 0:
            elapsed = time.time() - start_time
            rate = completed_count / elapsed if elapsed > 0 else 0
            logger.log_info(
                f"Progress: {completed_count}/{len(ticker_list)} tickers "
                f"({rate:.1f}/sec) | Success: {len(all_ticker_details)} | "
                f"Failed: {len(failed_tickers)}"
            )

elapsed_total = time.time() - start_time
success_rate = (len(all_ticker_details) / len(ticker_list) * 100) if ticker_list else 0

logger.log_info(f"Completed fetching {len(ticker_list)} tickers in {elapsed_total:.1f} seconds")
logger.log_info(f"Success: {len(all_ticker_details)} ({success_rate:.1f}%) | Failed: {len(failed_tickers)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Resolve Snapshot Date
# MAGIC
# MAGIC Content-derived business date for the Bronze partition key (`snapshot_date`).
# MAGIC Optional widget override when backfilling; defaults to today in US/Eastern.

# COMMAND ----------

dbutils.widgets.text("snapshot_date", "", "Snapshot Date (YYYY-MM-DD, ET)")
_snap_param = dbutils.widgets.get("snapshot_date").strip()
if _snap_param:
    try:
        datetime.strptime(_snap_param, "%Y-%m-%d")
    except ValueError:
        logger.log_error("Invalid snapshot_date", remediation="Use YYYY-MM-DD format")
        logger.exit_job("failed", f"Invalid snapshot_date: {_snap_param}")
    _snap_date_str = _snap_param
else:
    _snap_date_str = datetime.now(pytz.timezone(config.MARKET_TIMEZONE)).strftime("%Y-%m-%d")

logger.log_info(f"snapshot_date = {_snap_date_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Transform and Create DataFrame

# COMMAND ----------

if not all_ticker_details:
    logger.log_info("No ticker details fetched successfully")
    logger.exit_job("failed", "No ticker details to process")

# Define schema for ticker details (only columns needed for Silver/Gold)
ticker_details_schema = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("name", StringType(), True),
        StructField("type", StringType(), True),
        StructField("active", BooleanType(), True),
        StructField("primary_exchange", StringType(), True),
        StructField("sic_code", StringType(), True),
        StructField("sic_description", StringType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("list_date", StringType(), True),
    ]
)

df = spark.createDataFrame(all_ticker_details, schema=ticker_details_schema)
bronze_df = (
    df.withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("correlation_id", lit(logger.correlation_id))
    .withColumn("source", lit("polygon_ticker_details_api"))
    # snapshot_date is the Bronze partition key. One partition per daily run.
    # Allows point-in-time recovery (drop a bad date partition and re-run) and
    # full audit trail (query snapshot_date = 'YYYY-MM-DD' to replay any day).
    .withColumn("snapshot_date", lit(_snap_date_str).cast("date"))
)

logger.log_info(f"Created DataFrame with {len(all_ticker_details)} ticker records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write to Bronze Delta Lake
# MAGIC
# MAGIC **Design**: Snapshot-append — one partition per `snapshot_date`, never overwritten.
# MAGIC Bronze is immutable raw storage (snapshot partitions); Gold dim_ticker always uses the latest snapshot_date.
# MAGIC
# MAGIC **Write mode**: `replaceWhere snapshot_date = '<date>'`
# MAGIC - Overwrites today's partition only (idempotent re-runs are safe)
# MAGIC - All prior partitions are untouched (full audit trail preserved)
# MAGIC - Recovery from a bad API batch: drop the bad date partition and re-run

# COMMAND ----------

ticker_details_path = config.get_bronze_path("ticker_details")

try:
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        # replaceWhere scopes the overwrite to today's partition only.
        # All other snapshot_date partitions are left completely untouched.
        .option("replaceWhere", f"snapshot_date = '{_snap_date_str}'")
        .partitionBy("snapshot_date")
        .save(ticker_details_path)
    )
    logger.log_info(f"Snapshot for {_snap_date_str} written to {ticker_details_path} ({len(all_ticker_details)} rows)")

except Exception as e:
    logger.log_error("Failed to write to Delta Lake", error=e, remediation=f"Check path: {ticker_details_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verification and Summary

# COMMAND ----------

# Read back only the partition just written — fast (partition pruning) and confirms
# the correct row count landed without scanning the full multi-snapshot table.
verification_df = (
    spark.read.format("delta")
    .load(ticker_details_path)
    .filter(col("snapshot_date") == lit(_snap_date_str).cast("date"))
)
records_written = verification_df.count()
active_count = verification_df.filter(col("active") == True).count()

# Distinct snapshot count — useful for monitoring table growth over time.
snapshot_count = spark.read.format("delta").load(ticker_details_path).select("snapshot_date").distinct().count()

logger.log_info(
    f"Verification: snapshot_date={_snap_date_str} | "
    f"Rows written: {records_written} | Active: {active_count} | "
    f"Total snapshots in table: {snapshot_count}"
)

logger.log_job_summary(
    status="success",
    records_read=len(ticker_list),
    records_written=records_written,
    extra_metrics={
        "tickers_failed": len(failed_tickers),
        "snapshot_date": _snap_date_str,
        "active_tickers": active_count,
        "total_snapshots": snapshot_count,
        "output_path": ticker_details_path,
    },
)

logger.exit_job("success", f"Snapshot {_snap_date_str} written: {records_written} rows")
