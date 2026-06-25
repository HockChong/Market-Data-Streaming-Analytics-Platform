# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Incremental Market Data Ingestion
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Incrementally loads new OHLCV data from Polygon S3 flat files without overwriting existing data.
# MAGIC Auto-detects where the last ingestion stopped and continues from there.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon S3 (CSV.GZ) → This Notebook → Bronze Delta Lake (historical)
# MAGIC                           ↑                    ↓
# MAGIC                    MERGE (no duplicates)  Silver DLT Pipeline
# MAGIC ```
# MAGIC
# MAGIC ## When to Use This Notebook
# MAGIC | Scenario | Use This? | Alternative |
# MAGIC |----------|-----------|-------------|
# MAGIC | Daily scheduled job | ✅ Yes | - |
# MAGIC | Catch-up after gap | ✅ Yes | - |
# MAGIC | Initial setup | ❌ No | `historical_ingestion_flatfiles.py` |
# MAGIC | Real-time streaming | ❌ No | `streaming_ingestion.py` |
# MAGIC
# MAGIC ## Parameters
# MAGIC | Parameter | Required | Description |
# MAGIC |-----------|----------|-------------|
# MAGIC | `lookback_days` | Only if table missing | Days to backfill on first run |
# MAGIC | `start_date` | No | Override start (auto-detected from last run) |
# MAGIC | `end_date` | No | Override end (defaults to 3 days ago) |
# MAGIC
# MAGIC ## Key Features
# MAGIC - **MERGE Strategy**: Uses (symbol, start_timestamp) key - safe to re-run
# MAGIC - **Auto-Detection**: Finds last ingestion date and continues from there
# MAGIC - **Idempotent**: Running twice produces same result
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`
# MAGIC - **Downstream**: `ohlcv_silver_dlt.py`

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

import pytz

# COMMAND ----------
from bronze_config import BronzeConfig
from bronze_utils import (
    FLATFILES_CSV_SCHEMA,
    dedupe_incremental_bronze_keys,
    generate_date_paths,
    transform_flatfile_ohlcv,
)
from incremental_flatfiles_runtime import (
    collect_incremental_summary,
    configure_polygon_s3_access,
    parse_incremental_widget_params,
    probe_flatfiles,
    resolve_incremental_date_window,
    write_incremental_to_delta,
)
from pyspark.sql import SparkSession
from simple_logger import SimpleLogger

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
MARKET_TZ = pytz.timezone(config.MARKET_TIMEZONE)
logger = SimpleLogger("incremental_flatfiles_ingestion", dbutils)
logger.log_start()

try:
    config.validate_flatfiles()
except Exception as e:
    logger.log_error("Configuration validation failed", error=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration Parameters
# MAGIC
# MAGIC Auto-detects date range from last ingestion. Override with `start_date`/`end_date` if needed.

# COMMAND ----------

DEFAULT_END_DATE_LAG_DAYS = 1

widget_params = parse_incremental_widget_params(dbutils)
LOOKBACK_DAYS = widget_params.lookback_days
START_DATE_OVERRIDE = widget_params.start_date_override
END_DATE_OVERRIDE = widget_params.end_date_override

bronze_path = config.get_bronze_path("historical")

date_window = resolve_incremental_date_window(
    spark=spark,
    bronze_path=bronze_path,
    market_tz=MARKET_TZ,
    lookback_days=LOOKBACK_DAYS,
    start_date_override=START_DATE_OVERRIDE,
    end_date_override=END_DATE_OVERRIDE,
    default_end_date_lag_days=DEFAULT_END_DATE_LAG_DAYS,
    logger=logger,
)
START_DATE = date_window.start_date
END_DATE = date_window.end_date
table_exists = date_window.table_exists
days_to_process = date_window.days_to_process

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure S3 Access

# COMMAND ----------

configure_polygon_s3_access(spark, config, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build S3 Paths

# COMMAND ----------


s3_paths = generate_date_paths(START_DATE, END_DATE, config.polygon_flatfiles_bucket, config.polygon_flatfiles_prefix)
print(f"Generated {len(s3_paths)} S3 paths (business days only)")

if len(s3_paths) == 0:
    logger.log_info("No business days in date range")
    logger.exit_job("success", "No business days to process")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read and Transform Flat Files from S3

# COMMAND ----------

csv_schema = FLATFILES_CSV_SCHEMA

from functools import reduce

from pyspark.sql import DataFrame

# probe_flatfiles uses dbutils.fs.ls (metadata-only) — no Spark job per file.
# Returns (path, mod_time_ms) where mod_time_ms is the S3 last-modified timestamp,
# used as ingestion_timestamp so the value is file-fixed across retries.
readable_files = probe_flatfiles(dbutils, s3_paths, logger)

if not readable_files:
    logger.log_info(
        f"No trading files found for {START_DATE} to {END_DATE} "
        "(all business days in range are holidays or non-trading days)"
    )
    logger.exit_job("success", "No trading data for date range — holiday or non-trading day")

loaded_dfs = [
    transform_flatfile_ohlcv(
        spark.read.format("csv")
        .option("header", "true")
        .option("compression", "gzip")
        .option("mode", "FAILFAST")
        .schema(csv_schema)
        .load(path),
        logger.correlation_id,
        mod_time_ms,
    )
    for path, mod_time_ms in readable_files
]

bronze_df = reduce(DataFrame.unionAll, loaded_dfs)
logger.log_info(f"Loaded {len(readable_files)} files")

# COMMAND ----------


# COMMAND ----------

# Enforce one deterministic winner per Bronze key before MERGE.
bronze_df = dedupe_incremental_bronze_keys(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Philosophy
# MAGIC
# MAGIC **Bronze Layer Philosophy**:
# MAGIC - Captures **ALL** raw data as immutable audit trail
# MAGIC - No filtering or data quality checks (preserves complete history)
# MAGIC - Quality enforcement happens in Silver layer via DLT expectations
# MAGIC
# MAGIC **Performance Optimization**:
# MAGIC - Skips expensive `count()` operations (lazy evaluation)
# MAGIC - Metrics retrieved from Delta transaction logs after write (instant, no data scan)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Lake (MERGE)
# MAGIC
# MAGIC MERGE on `(symbol, start_timestamp)` key. Idempotent - safe to re-run.

# COMMAND ----------

records_inserted, records_updated = write_incremental_to_delta(
    spark=spark,
    bronze_df=bronze_df,
    bronze_path=bronze_path,
    table_exists=table_exists,
    logger=logger,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification and Summary

# COMMAND ----------

total_records_written, table_date_range = collect_incremental_summary(
    spark=spark,
    bronze_path=bronze_path,
    records_inserted=records_inserted,
    records_updated=records_updated,
)

logger.log_job_summary(
    status="success",
    records_read=total_records_written,  # For incremental, records read ≈ records written
    records_written=total_records_written,
    extra_metrics={
        "mode": "incremental",
        "records_inserted": records_inserted,
        "records_updated": records_updated,
        "date_range": f"{START_DATE} to {END_DATE}",
        "table_date_range": table_date_range,
        "output_path": bronze_path,
        "note": "All records written to Bronze. Silver DLT handles quality filtering.",
    },
)

logger.exit_job("success", f"Inserted: {records_inserted:,} | Updated: {records_updated:,}")
