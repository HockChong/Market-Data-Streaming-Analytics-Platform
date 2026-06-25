# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Kafka Batch Replay — One-off Backfill
# MAGIC
# MAGIC ## Purpose
# MAGIC Reads a specific calendar date's Kafka messages in **batch mode** and appends them
# MAGIC to the Bronze streaming Delta table with correctly computed `date` partitions.
# MAGIC
# MAGIC Use this when `streaming_ingestion.py` ran successfully but records landed in a
# MAGIC wrong date partition (e.g., `1970-01-21`) due to the `timestamp-millis` cast bug.
# MAGIC
# MAGIC ## How it works
# MAGIC - Uses `spark.read` (batch, not `readStream`) — the existing streaming checkpoint
# MAGIC   is **never touched**. The live streaming job continues normally.
# MAGIC - Bounds the Kafka read to `[REPLAY_START_DATE 00:00 UTC, REPLAY_END_DATE+1 00:00 UTC)`.
# MAGIC - Reuses the same Avro parse + Bronze metadata functions as `streaming_ingestion.py`,
# MAGIC   with the fixed timestamp code already in place.
# MAGIC - Appends to `bronze/streaming` Delta path. Silver's `apply_changes` MERGE on
# MAGIC   `(symbol, start_timestamp)` deduplicates against any existing bad records.
# MAGIC
# MAGIC ## Safety
# MAGIC - `DRY_RUN = True` by default — set to `False` only after previewing the row count.
# MAGIC - Re-running is safe: Bronze gets duplicate raw rows, but Silver MERGE picks the
# MAGIC   correct winner via `source_priority` and `_dedup_sequence`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

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

from datetime import datetime, timedelta, timezone

from bronze_config import BronzeConfig
from pyspark.sql import SparkSession
from simple_logger import SimpleLogger
from streaming_ingestion_runtime import add_streaming_bronze_metadata, parse_kafka_avro_stream

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")

config = BronzeConfig(dbutils)
logger = SimpleLogger("kafka_replay_backfill", dbutils)

try:
    config.validate_streaming()
except Exception as e:
    logger.log_error("Configuration validation failed", error=e)
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parameters
# MAGIC
# MAGIC Set `REPLAY_START_DATE` and `REPLAY_END_DATE` to the inclusive date range to recover.
# MAGIC For a single day set both to the same date (e.g. `"2026-05-21"` / `"2026-05-21"`).
# MAGIC Set `DRY_RUN = False` only after previewing the row count in Section 6.

# COMMAND ----------

REPLAY_START_DATE = "2026-05-21"  # First date to replay, inclusive (YYYY-MM-DD)
REPLAY_END_DATE = "2026-05-21"  # Last date to replay, inclusive (YYYY-MM-DD)
DRY_RUN = True  # Flip to False to write to Bronze after previewing counts

logger.log_info(f"Replay range: {REPLAY_START_DATE} → {REPLAY_END_DATE} | DRY_RUN={DRY_RUN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Compute Kafka Timestamp Bounds
# MAGIC
# MAGIC Kafka timestamps on messages are UTC. To capture all ET market-hours bars for
# MAGIC the replay range, we bound the read to [REPLAY_START_DATE 00:00 UTC, REPLAY_END_DATE+1 00:00 UTC).
# MAGIC ET market hours (09:30–16:20 ET = 13:30–20:20 UTC) fall within that window for each day.

# COMMAND ----------

start_dt = datetime.strptime(REPLAY_START_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
end_dt = datetime.strptime(REPLAY_END_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc)
start_ts_ms = int(start_dt.timestamp() * 1000)
end_ts_ms = int((end_dt + timedelta(days=1)).timestamp() * 1000)  # +1 day to include full end date

logger.log_info(
    f"Kafka window: {start_dt.strftime('%Y-%m-%d %H:%M:%S UTC')} → {(end_dt + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S UTC')}"
)
logger.log_info(f"Epoch ms:     start={start_ts_ms}  end={end_ts_ms}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Read from Kafka in Batch Mode
# MAGIC
# MAGIC Build batch Kafka options from the shared config, replacing streaming-only keys:
# MAGIC - Remove `startingOffsets` and `maxOffsetsPerTrigger` (streaming-only).
# MAGIC - Add `startingTimestamp` / `endingTimestamp` to bound the read to the replay range.
# MAGIC - Set `failOnDataLoss = false`: old Kafka segments may have compacted at partition
# MAGIC   boundaries; we prefer a partial replay over a hard failure.

# COMMAND ----------

_streaming_only_keys = {"startingOffsets", "maxOffsetsPerTrigger"}

batch_kafka_options = {k: v for k, v in config.get_kafka_options().items() if k not in _streaming_only_keys}
batch_kafka_options["startingTimestamp"] = str(start_ts_ms)
batch_kafka_options["endingTimestamp"] = str(end_ts_ms)
batch_kafka_options["failOnDataLoss"] = "false"

logger.log_info(f"Kafka topic : {config.kafka_topic_streaming}")
logger.log_info(f"Kafka server: {config.kafka_bootstrap_servers}")

try:
    kafka_batch_df = spark.read.format("kafka").options(**batch_kafka_options).load()
    logger.log_info(f"Kafka batch read configured for {REPLAY_START_DATE} → {REPLAY_END_DATE}")
except Exception as e:
    logger.log_error("Failed to connect to Kafka", error=e, remediation="Check Kafka credentials in secrets")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Parse Avro and Add Bronze Metadata

# COMMAND ----------

avro_schema_json = config.get_avro_schema(use_registry=True)
logger.log_info("Loaded Avro schema")

try:
    parsed_df, quarantine_df = parse_kafka_avro_stream(kafka_batch_df, avro_schema_json, logger.correlation_id)
except Exception as e:
    logger.log_error("Failed to parse Kafka messages", error=e)
    raise

bronze_df = add_streaming_bronze_metadata(parsed_df, logger.correlation_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preview — Verify Row Counts and Date Partition Before Writing
# MAGIC
# MAGIC Check that `date` shows dates within your replay range, not `1970-01-21`.
# MAGIC If `date` still looks wrong, stop here — do not set `DRY_RUN = False`.

# COMMAND ----------

good_count = bronze_df.count()
quarantine_count = quarantine_df.count()

logger.log_info(f"Parsed records : {good_count}")
logger.log_info(f"Quarantine (bad Avro): {quarantine_count}")

print(f"\nParsed records  : {good_count}")
print(f"Quarantine rows : {quarantine_count}")
print(f"\nDistinct dates in replay batch (should show {REPLAY_START_DATE} → {REPLAY_END_DATE}):")
bronze_df.select("date").distinct().show()

print("\nSample records:")
bronze_df.select("symbol", "date", "start_timestamp", "ts_unit", "source", "open", "close", "volume").show(
    5, truncate=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Streaming Delta Path
# MAGIC
# MAGIC Append-only batch write — no checkpoint involved.
# MAGIC Only runs when `DRY_RUN = False`.

# COMMAND ----------

bronze_path = config.get_bronze_path("streaming")

if DRY_RUN:
    logger.log_info(f"DRY_RUN=True — skipping write. Set DRY_RUN=False to write {good_count} rows to {bronze_path}")
    print(f"\nDRY RUN: {good_count} rows would be written to {bronze_path}")
    print("Set DRY_RUN = False in Section 2 and re-run from Section 7 to write.")
    dbutils.notebook.exit("Dry run complete — no data written")

logger.log_info(f"Writing {good_count} rows to {bronze_path} ...")

try:
    (bronze_df.write.format("delta").mode("append").partitionBy("date").save(bronze_path))
    logger.log_info(f"Write complete: {good_count} rows appended to {bronze_path}")
    print(f"\nWrite complete: {good_count} rows written to {bronze_path}")
    print(f"Trigger the Silver DLT pipeline to process the new {REPLAY_START_DATE} → {REPLAY_END_DATE} Bronze data.")
except Exception as e:
    logger.log_error("Failed to write to Bronze", error=e, remediation=f"Check path access: {bronze_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Written Partition

# COMMAND ----------

print(f"Verifying partitions {REPLAY_START_DATE} → {REPLAY_END_DATE} in Bronze streaming table...")

written = (
    spark.read.format("delta")
    .load(bronze_path)
    .filter(f"date >= '{REPLAY_START_DATE}' AND date <= '{REPLAY_END_DATE}'")
)

written_count = written.count()
print(f"Rows in Bronze streaming for {REPLAY_START_DATE} → {REPLAY_END_DATE}: {written_count}")
written.groupBy("date").count().orderBy("date").show(truncate=False)
