# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Bronze Layer: Historical Market Data Backfill
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Loads historical 1-minute OHLCV data from Polygon S3 flat files into Bronze Delta Lake.
# MAGIC Use this for initial setup or controlled partition replacements for approved date ranges.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon S3 (CSV.GZ) → This Notebook → Bronze Delta Lake (historical)
# MAGIC                                               ↓
# MAGIC                                       Silver DLT Pipeline
# MAGIC ```
# MAGIC
# MAGIC ## When to Use This Notebook
# MAGIC | Scenario | Use This? | Alternative |
# MAGIC |----------|-----------|-------------|
# MAGIC | Initial setup | ✅ Yes | - |
# MAGIC | Large backfill (months/years) | ✅ Yes | - |
# MAGIC | Daily incremental | ❌ No | `incremental_ingestion_flatfiles.py` |
# MAGIC | Real-time streaming | ❌ No | `streaming_ingestion.py` |
# MAGIC
# MAGIC ## Parameters (Required)
# MAGIC - `start_date`: Start date (YYYY-MM-DD)
# MAGIC - `end_date`: End date (YYYY-MM-DD)
# MAGIC
# MAGIC ## Important Notes
# MAGIC - ⚠️ **OVERWRITE MODE**: Replaces only date partitions present in this run
# MAGIC - Use dates **3+ days old** (Polygon has upload lag)
# MAGIC - Processes **business days only** (skips weekends/holidays)
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`
# MAGIC - **Partitioning**: By date
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
from datetime import datetime

# COMMAND ----------
from bronze_config import BronzeConfig
from bronze_utils import FLATFILES_CSV_SCHEMA, generate_date_paths, transform_flatfile_ohlcv
from incremental_flatfiles_runtime import configure_polygon_s3_access, probe_flatfiles
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from simple_logger import SimpleLogger

spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
logger = SimpleLogger("historical_flatfiles_ingestion", dbutils)
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
# MAGIC Reads date range parameters from job widgets.
# MAGIC Both `start_date` and `end_date` are required.

# COMMAND ----------

# DBTITLE 1,Get and validate date parameters
try:
    dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD, ET trading date)")
    dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD, ET trading date)")
    START_DATE = dbutils.widgets.get("start_date")
    END_DATE = dbutils.widgets.get("end_date")
except Exception:
    START_DATE = ""
    END_DATE = ""

if not START_DATE or not END_DATE:
    logger.log_error(
        "Missing required parameters", remediation="Provide both start_date and end_date in YYYY-MM-DD format"
    )
    logger.exit_job("failed", "Missing start_date or end_date parameters")

start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

if start_dt > end_dt:
    logger.log_error("Invalid date range", remediation=f"start_date ({START_DATE}) must be <= end_date ({END_DATE})")
    logger.exit_job("failed", f"Invalid date range: {START_DATE} > {END_DATE}")

days_to_process = (end_dt - start_dt).days + 1
print(f"Date range: {START_DATE} to {END_DATE} ({days_to_process} days, ET trading dates)")

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Read and Transform Flat Files from S3
# MAGIC
# MAGIC Reads CSV.GZ files from Polygon S3 bucket and applies Bronze schema mapping per file.
# MAGIC
# MAGIC **Strategy**:
# MAGIC - `probe_flatfiles` uses `dbutils.fs.ls` (metadata-only) to check each path — no Spark job per file
# MAGIC - Silently skips missing paths (holidays, non-trading days)
# MAGIC - Each file is read and transformed independently, then unioned
# MAGIC - S3 last-modified time is used as `ingestion_timestamp` so the value is file-fixed across retries
# MAGIC
# MAGIC **Schema**: Matches Polygon flat file format (ticker, volume, open, close, high, low, window_start, transactions)

# COMMAND ----------

# DBTITLE 1,Load and Transform CSV Files
csv_schema = FLATFILES_CSV_SCHEMA

from functools import reduce

from pyspark.sql import DataFrame

# probe_flatfiles returns (path, mod_time_ms) for every path confirmed to exist.
# mod_time_ms is the S3 last-modified timestamp (epoch ms) — when Polygon uploaded the file
# (~bar date + 2–3 days). Passing it as ingestion_timestamp makes the value file-fixed:
# two re-runs of the same file produce identical timestamps, keeping the Silver dedup
# sequence deterministic across retries.
readable_files = probe_flatfiles(dbutils, s3_paths, logger)

if not readable_files:
    logger.log_error(
        "No data found for date range",
        remediation=f"Check S3 paths and date range: {START_DATE} to {END_DATE}. "
        f"Polygon has 2-3 day upload lag - use older dates.",
    )
    logger.exit_job("failed", "No data files found for date range")

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

# MAGIC %md
# MAGIC ## 6. Data Philosophy
# MAGIC
# MAGIC **Historical Notebook Contract**:
# MAGIC - Intended for one-time bootstrap and explicit replacement runs only
# MAGIC - Rewrites requested `date` partitions via controlled overwrite
# MAGIC - No filtering or data quality checks in Bronze (Silver enforces quality)
# MAGIC - Quality enforcement happens in Silver layer via DLT expectations
# MAGIC
# MAGIC **Performance Optimization**:
# MAGIC - Skips expensive `count()` operations (lazy evaluation)
# MAGIC - Metrics retrieved from Delta transaction logs after write (instant, no data scan)

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Lake
# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Lake
# MAGIC
# MAGIC ⚠️ **OVERWRITE mode** - replaces only partitions in the requested date range. Partitioned by `date`.

# COMMAND ----------

# DBTITLE 1,Write to Bronze Delta Lake
bronze_path = config.get_bronze_path("historical")

# Dynamic partition overwrite: replaces ONLY the date partitions present in this
# run's data — other partitions are left intact. This makes reruns safe: a second
# execution for the same date range produces an identical result without touching
# unrelated historical partitions. Global overwrite (the default) would wipe the
# entire table, losing all previously loaded dates on failure.
#
logger.log_info(
    f"Writing partitions for date range {START_DATE} → {END_DATE}. Only these date partitions will be replaced."
)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
try:
    (bronze_df.write.format("delta").mode("overwrite").partitionBy("date").save(bronze_path))
except Exception as e:
    logger.log_error("Failed to write to Delta Lake", error=e, remediation=f"Check path: {bronze_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification and Summary

# COMMAND ----------

from delta.tables import DeltaTable

# Get metrics from Delta transaction log (instant - no data scan required)
delta_table = DeltaTable.forPath(spark, bronze_path)
latest_history = delta_table.history(1).collect()[0]
operation_metrics = latest_history["operationMetrics"]

records_written = int(operation_metrics.get("numOutputRows", 0))
files_written = int(operation_metrics.get("numFiles", 0))
bytes_written = int(operation_metrics.get("numOutputBytes", 0))

# Get date range from Delta table (single partition scan, not full data scan)
date_stats = (
    spark.read.format("delta")
    .load(bronze_path)
    .select(spark_min("date").alias("min_date"), spark_max("date").alias("max_date"))
    .collect()[0]
)

logger.log_job_summary(
    status="success",
    records_read=records_written,  # For backfills, records read ≈ records written
    records_written=records_written,
    extra_metrics={
        "mode": "backfill",
        "date_range": f"{date_stats['min_date']} to {date_stats['max_date']}",
        "files_written": files_written,
        "bytes_written_mb": round(bytes_written / (1024 * 1024), 2),
        "output_path": bronze_path,
        "note": "All records written to Bronze. Silver DLT handles quality filtering.",
    },
)

logger.exit_job("success", f"{records_written:,} records written to Bronze")
