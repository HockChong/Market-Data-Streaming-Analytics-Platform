# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Streaming Market Data Ingestion (Kafka → Delta)
# MAGIC
# MAGIC ## Purpose
# MAGIC Consumes real-time 1-minute OHLCV bars from Kafka and writes them to Bronze Delta Lake.
# MAGIC Implements graceful shutdown with **drain mode** to minimize residual Kafka lag at market close.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon WebSocket → Kafka (Avro) → This Notebook → Bronze Delta Lake
# MAGIC                                                            ↓
# MAGIC                                                    Silver DLT Pipeline
# MAGIC ```
# MAGIC
# MAGIC ## When to Use This Notebook
# MAGIC | Scenario | Use This? | Alternative |
# MAGIC |----------|-----------|-------------|
# MAGIC | Real-time market data | ✅ Yes | - |
# MAGIC | Historical backfill | ❌ No | `historical_ingestion_flatfiles.py` |
# MAGIC | Daily incremental | ❌ No | `incremental_ingestion_flatfiles.py` |
# MAGIC
# MAGIC ## Key Features
# MAGIC | Feature | Description |
# MAGIC |---------|-------------|
# MAGIC | Market Hours Auto-Start | Runs 9:30 AM ET until session close + 20 min (e.g. 4:20 PM ET) for delayed feed. |
# MAGIC | Graceful Shutdown | **Drain mode** after effective shutdown: process all Kafka backlog before stopping. |
# MAGIC | Pre-flight Check | Exits immediately if market is closed (saves compute costs). |
# MAGIC | At-Least-Once Delivery | The checkpointed Kafka→Delta write is exactly-once per hop; Bronze is treated as at-least-once because the same bar also arrives via the historical flat-file path and replay/backfill. Silver `apply_changes` MERGE on `(symbol, start_timestamp)` is the dedup guarantee. |
# MAGIC | Dead Letter Queue | Avro deserialization failures routed to quarantine Delta table. |
# MAGIC
# MAGIC ## Performance Specifications
# MAGIC | Metric | Value | Notes |
# MAGIC |--------|-------|-------|
# MAGIC | Steady-state throughput | ~8.3 msg/sec | US tickers × 1 msg/min / 60 sec |
# MAGIC | Burst capacity | ~25 msg/sec | Handles market open rush + backlog catch-up |
# MAGIC | Processing trigger | Every 5 seconds | Micro-batch interval |
# MAGIC | Drain mode catch-up | <2 minutes | Time to clear backlog at market close |
# MAGIC | End-to-end latency (p50) | ~15 seconds | Kafka → Delta write confirmation |
# MAGIC | End-to-end latency (p95) | ~35 seconds | Includes backpressure scenarios |
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming`
# MAGIC - **Format**: Delta Lake (partitioned by `date`)
# MAGIC - **Schema**: Avro-validated OHLCV records from Kafka
# MAGIC - **Quarantine**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming_quarantine`
# MAGIC - **Downstream**: `ohlcv_silver_dlt.py` reads this table via streaming readStream
# MAGIC
# MAGIC ## Drain Mode Behavior (Section 7)
# MAGIC After effective shutdown (session close + 20 min), the notebook enters **drain mode** instead of stopping immediately:
# MAGIC
# MAGIC 1. **Catch-Up**: Calls `processAllAvailable()` in a background thread with a timeout of `DRAIN_MODE_MAX_WAIT_SECONDS` (default: 120s)
# MAGIC 2. **Final Wait**: Sleeps `DRAIN_FINAL_WAIT_SECONDS` (default: 30s) for the last micro-batch to commit
# MAGIC 3. **Check**: Inspects `numInputRows` from `lastProgress` — if 0, backlog is cleared
# MAGIC 4. **Shutdown**: Stops queries regardless of outcome; any remaining lag is replayed on the next session (at-least-once)
# MAGIC
# MAGIC **WHY Drain Mode?**
# MAGIC - Minimizes residual Kafka lag from in-flight messages at market close
# MAGIC - Ensures Silver pipeline gets most data before daily EOD processing
# MAGIC - Any records not drained are safe in Kafka and replayed next session; Silver `apply_changes` deduplicates
# MAGIC
# MAGIC **Configuration** (see BronzeConfig):
# MAGIC - `DRAIN_MODE_ENABLED`: Enable/disable drain mode
# MAGIC - `DRAIN_MODE_MAX_WAIT_SECONDS`: Timeout for `processAllAvailable()` (default: 120s)
# MAGIC - `DRAIN_MODE_FINAL_WAIT_SECONDS`: Final wait for last commit (default: 30s)

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
from datetime import datetime

from bronze_config import BaseConfig, BronzeConfig

# COMMAND ----------
from pyspark.sql import SparkSession
from simple_logger import SimpleLogger
from streaming_ingestion_runtime import (
    add_streaming_bronze_metadata,
    append_drain_metrics,
    compute_market_poll_sleep_seconds,
    ensure_streaming_volume_paths,
    get_streaming_query_fault,
    monitor_startup_batches,
    parse_kafka_avro_stream,
    run_drain_mode,
    start_streaming_writers,
    stop_queries,
)

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.set("spark.sql.shuffle.partitions", str(BronzeConfig.SHUFFLE_PARTITIONS))
# Graceful shutdown: avoid cutting mid-microbatch on job/cluster shutdown
spark.conf.set("spark.sql.streaming.stopGracefullyOnShutdown", "true")
config = BronzeConfig(dbutils)
logger = SimpleLogger("streaming_ingestion", dbutils)
logger.log_start()

try:
    config.validate_streaming()
except Exception as e:
    logger.log_error("Configuration validation failed", error=e)
    raise

ENABLE_DRAIN_MODE = config.DRAIN_MODE_ENABLED
DRAIN_MAX_WAIT_SECONDS = config.DRAIN_MODE_MAX_WAIT_SECONDS
DRAIN_FINAL_WAIT_SECONDS = config.DRAIN_MODE_FINAL_WAIT_SECONDS

logger.log_info(f"Drain mode enabled: {ENABLE_DRAIN_MODE}")
logger.log_info(f"Drain mode: max wait {DRAIN_MAX_WAIT_SECONDS}s, final wait {DRAIN_FINAL_WAIT_SECONDS}s")

# COMMAND ----------

# DBTITLE 1,Market Hours Configuration (from BaseConfig)

# Market hours and shutdown timing come from BaseConfig (shared with streaming_producer).
MARKET_TIMEZONE = BaseConfig.get_market_timezone()
MARKET_OPEN = BaseConfig.get_market_open_time()
MARKET_CLOSE = BaseConfig.get_market_close_time()

is_market_hours = BaseConfig.is_market_hours
get_market_status = BaseConfig.get_market_status


logger.log_info(f"Market Hours: {MARKET_OPEN.strftime('%H:%M')} - {MARKET_CLOSE.strftime('%H:%M')} ET (Mon-Fri)")
logger.log_info(f"Current Status: {get_market_status()}")

# COMMAND ----------

# DBTITLE 1,Pre-flight Market Hours Check
if not is_market_hours():
    logger.log_info("⚠️  Market is currently CLOSED")
    logger.log_info(f"Current time: {get_market_status()}")
    logger.log_info("Exiting without starting stream...")
    dbutils.notebook.exit("Market closed - streaming not started")

logger.log_info("✅ Market is OPEN - proceeding with stream setup...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read from Kafka Stream
# MAGIC
# MAGIC Connects to Kafka and creates a streaming DataFrame.
# MAGIC Kafka connection details are retrieved from Databricks secrets via `BronzeConfig`.

# COMMAND ----------

kafka_options = config.get_kafka_options()

try:
    kafka_raw_df = spark.readStream.format("kafka").options(**kafka_options).load()
except Exception as e:
    logger.log_error("Failed to connect to Kafka", error=e, remediation="Check Kafka credentials in secrets")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Parse Kafka Messages (Avro)

# COMMAND ----------

avro_schema_json = config.get_avro_schema(use_registry=True)
logger.log_info("Loaded Avro schema for message parsing")

try:
    parsed_df, quarantine_df = parse_kafka_avro_stream(kafka_raw_df, avro_schema_json, logger.correlation_id)
    logger.log_info("Avro parsing configured with PERMISSIVE mode (bad records → quarantine)")
except Exception as e:
    logger.log_error("Failed to parse Kafka messages", error=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Add Bronze Layer Metadata
# MAGIC
# MAGIC Adds Bronze layer standard columns:
# MAGIC - `processing_timestamp`: When this record was processed
# MAGIC - `correlation_id`: Unique ID for tracing this ingestion run
# MAGIC - `source`: Data source identifier (`polygon_kafka_delayed_streaming`)
# MAGIC - `date`: Partition column for efficient time-series queries

# COMMAND ----------

bronze_df = add_streaming_bronze_metadata(parsed_df, logger.correlation_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write to Bronze Delta Lake
# MAGIC
# MAGIC Streaming write with checkpointing, partitioned by `date`, trigger every 5 seconds.

# COMMAND ----------

bronze_path = config.get_bronze_path("streaming")
checkpoint_path = config.get_checkpoint_path("streaming_ingestion")
quarantine_path = config.get_bronze_path("streaming_quarantine")
quarantine_checkpoint_path = config.get_checkpoint_path("streaming_ingestion_quarantine")

try:
    ensure_streaming_volume_paths(dbutils, config.volume_base_path)
    logger.log_info("Bronze streaming volume paths verified")
except Exception as e:
    logger.log_error(
        "Failed to prepare volume paths",
        error=e,
        remediation="Run databricks/setup/create_volume_paths.py on a cluster with UC access",
    )
    raise

try:
    spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
    spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

    streaming_query, quarantine_query = start_streaming_writers(
        bronze_df,
        quarantine_df,
        bronze_path=bronze_path,
        checkpoint_path=checkpoint_path,
        quarantine_path=quarantine_path,
        quarantine_checkpoint_path=quarantine_checkpoint_path,
        trigger_interval=config.STREAMING_TRIGGER_INTERVAL,
        quarantine_trigger_interval=config.QUARANTINE_TRIGGER_INTERVAL,
    )

    logger.log_info(f"Streaming started: {streaming_query.id}")
    logger.log_info(f"Quarantine stream started: {quarantine_query.id} → {quarantine_path}")

except Exception as e:
    logger.log_error("Failed to start streaming write", error=e, remediation=f"Check path: {bronze_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Monitor Initial Batches
# MAGIC
# MAGIC Monitors the first 5 batches to verify streaming is working correctly.
# MAGIC This provides immediate feedback if there are connection or data issues.

# COMMAND ----------

monitor_startup_batches(streaming_query, quarantine_query, logger)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Market Hours Monitoring & Auto-Stop
# MAGIC
# MAGIC 1. Run until **effective shutdown** (session close + 20 min), so delayed-feed bars are consumed.
# MAGIC 2. Then enter **drain mode**: process all Kafka backlog before stopping.
# MAGIC Manual stop: `streaming_query.stop()`

# COMMAND ----------

logger.log_info("Monitoring market hours - stream will auto-stop at effective shutdown (session close + delay).")

# Effective shutdown = when we stop accepting "new" work (session close + SHUTDOWN_DELAY_MINUTES).
# We compute this once at job start so we don't stop at 4:00 PM; we stop at e.g. 4:20 PM.
today_et = datetime.now(MARKET_TIMEZONE).date()
market_close_datetime = BaseConfig.get_effective_shutdown_datetime(today_et)
session_close_time = BaseConfig.get_session_close_time(today_et)
logger.log_info(
    f"Today's session close: {session_close_time.strftime('%H:%M')} ET | "
    f"Effective shutdown (close + {BaseConfig.SHUTDOWN_DELAY_MINUTES} min): "
    f"{market_close_datetime.strftime('%Y-%m-%d %H:%M:%S %Z')}"
)


def is_past_market_close():
    """True if current time is past effective shutdown (session close + delay)."""
    return datetime.now(MARKET_TIMEZONE) > market_close_datetime


try:
    # --- Phase 1: Run until effective shutdown time ---
    while streaming_query.isActive and not is_past_market_close():
        # Check for stream faults each iteration.  isActive returns False when the
        # stream has faulted, so without this check a mid-session failure would
        # silently exit the loop and fall through to drain mode as if the market closed.
        label, stream_exc = get_streaming_query_fault(streaming_query, quarantine_query)
        if stream_exc:
            stop_queries(streaming_query, quarantine_query)
            logger.log_error(f"{label} stream faulted during market hours", error=stream_exc)
            raise stream_exc

        progress = streaming_query.lastProgress
        if progress:
            batch_id = progress.get("batchId", "N/A")
            input_rows = progress.get("numInputRows", 0)
            time_to_close = market_close_datetime - datetime.now(MARKET_TIMEZONE)
            minutes_to_close = max(0, int(time_to_close.total_seconds() / 60))
            logger.log_info(
                f"Stream active | Batch: {batch_id} | Rows: {input_rows} | "
                f"Time to close: {minutes_to_close} min | {get_market_status()}"
            )

        now = datetime.now(MARKET_TIMEZONE)
        seconds_to_close = (market_close_datetime - now).total_seconds()
        sleep_time = compute_market_poll_sleep_seconds(seconds_to_close)
        time.sleep(sleep_time)

    # --- Phase 2: Effective shutdown reached; drain Kafka backlog before stopping ---
    if is_past_market_close():
        logger.log_info(f"🔔 Effective shutdown at {datetime.now(MARKET_TIMEZONE).strftime('%H:%M:%S %Z')}")

        if not ENABLE_DRAIN_MODE:
            logger.log_info("Drain mode disabled. Stopping streaming queries immediately...")
            stop_queries(streaming_query, quarantine_query)
            logger.log_info("✅ Streaming queries stopped successfully")
            dbutils.notebook.exit("Market closed - streaming stopped (immediate stop)")

        logger.log_info("Entering drain mode: processing remaining Kafka messages before shutdown...")

        drain_result = {"drain_status": "error", "residual_lag_rows": 0}
        try:
            drain_result = run_drain_mode(streaming_query, logger, DRAIN_MAX_WAIT_SECONDS, DRAIN_FINAL_WAIT_SECONDS)
        except Exception as e:
            logger.log_error("Error during drain mode", error=e)

        # Stop both streaming queries only after drain thread has been joined
        if streaming_query.isActive:
            logger.log_info("Stopping main streaming query after drain mode...")
        if quarantine_query.isActive:
            logger.log_info("Stopping quarantine streaming query...")
        stop_queries(streaming_query, quarantine_query)

        logger.log_info("✅ All streaming queries stopped successfully after drain mode")

        # Write drain result to a queryable Delta table for SLA monitoring.
        # Non-critical: a write failure here must not abort the shutdown sequence.
        _metrics_path = f"{config.volume_base_path}/_metrics/streaming_ingestion"
        try:
            append_drain_metrics(
                spark,
                _metrics_path,
                "bronze_streaming_ingestion",
                drain_result["drain_status"],
                drain_result["residual_lag_rows"],
            )
            logger.log_info(
                f"Drain metrics written — status={drain_result['drain_status']}, "
                f"residual_lag={drain_result['residual_lag_rows']} rows"
            )
        except Exception as _metrics_err:
            logger.log_error("Failed to write drain metrics (non-critical)", error=_metrics_err)

        dbutils.notebook.exit("Market closed - streaming drained and stopped successfully")

except KeyboardInterrupt:
    logger.log_info("⚠️  Manual shutdown requested")
    stop_queries(streaming_query, quarantine_query)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Stream Status
# MAGIC
# MAGIC Commands: `streaming_query.stop()`, `streaming_query.status`, `streaming_query.lastProgress`

# COMMAND ----------

# DBTITLE 1,Stream Status and Market Hours Monitoring
logger.log_info(f"Query ID: {streaming_query.id}")
logger.log_info(f"Status: {streaming_query.status}")
logger.log_info(f"Output: {bronze_path}")
logger.log_info(
    f"Stream will automatically stop at {market_close_datetime.strftime('%H:%M')} ET "
    f"(session close + {BaseConfig.SHUTDOWN_DELAY_MINUTES} min for delayed feed)"
)
logger.log_info("Market hours monitoring is integrated into the dashboard loop above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Cleanup (Optional)
# MAGIC
# MAGIC Auto-cleanup at market close. Manual stop: uncomment below or use `streaming_query.stop()`

# COMMAND ----------

# DBTITLE 1,Manual Cleanup (Optional)
# Uncomment below to manually stop the stream:
# if streaming_query.isActive:
#     streaming_query.stop()
#     logger.log_info("Streaming query stopped manually")
