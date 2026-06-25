"""Runtime helpers for Kafka -> Bronze streaming ingestion notebook."""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone

from base_config import BaseConfig
from streaming_transforms import is_stream_caught_up

STREAMING_VOLUME_REL_PATHS = (
    "bronze/streaming",
    "bronze/streaming_quarantine",
    "_checkpoints/bronze/streaming_ingestion",
    "_checkpoints/bronze/streaming_ingestion_quarantine",
    "_metrics/streaming_ingestion",
)


def ensure_streaming_volume_paths(dbutils, volume_base_path: str) -> None:
    """Create Bronze streaming, quarantine, checkpoint, and metrics dirs on the UC volume."""
    failed: list[tuple[str, str]] = []
    for relative_path in STREAMING_VOLUME_REL_PATHS:
        full_path = f"{volume_base_path.rstrip('/')}/{relative_path}"
        try:
            dbutils.fs.mkdirs(full_path)
            dbutils.fs.ls(full_path)
        except Exception as exc:
            failed.append((full_path, str(exc)))
    if failed:
        details = "; ".join(f"{path}: {err}" for path, err in failed)
        raise RuntimeError(
            f"Could not create or access required volume paths ({details}). "
            f"Run databricks/setup/create_volume_paths.py once, or ensure the base volume exists: "
            f"{volume_base_path}"
        )


def get_streaming_query_fault(main_query, quarantine_query):
    """Return (label, exception) for the first faulted query, or (None, None)."""
    for label, query in (("main", main_query), ("quarantine", quarantine_query)):
        exc = query.exception()
        if exc is not None:
            return label, exc
    return None, None


def parse_kafka_avro_stream(kafka_raw_df, avro_schema_json: str, correlation_id: str):
    """Parse Confluent-wire Kafka values into good/quarantine DataFrames."""
    from pyspark.sql.avro.functions import from_avro
    from pyspark.sql.functions import col, current_timestamp, lit

    avro_options = {"mode": "PERMISSIVE"}
    kafka_with_metadata = kafka_raw_df.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "substring(value, 6, length(value)-5) as avro_value",
        "CAST(value AS BINARY) as raw_value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
    ).select(
        col("kafka_key"),
        from_avro(col("avro_value"), avro_schema_json, avro_options).alias("data"),
        col("raw_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
    )

    parsed_df = (
        kafka_with_metadata.filter(col("data").isNotNull())
        .select(
            "data.*",
            col("kafka_timestamp").alias("kafka_ingestion_timestamp"),
            col("topic"),
            col("partition"),
            col("offset"),
        )
        .withColumn("start_timestamp", (col("start_timestamp").cast("long") * 1000).cast("long"))
        .withColumn("end_timestamp", (col("end_timestamp").cast("long") * 1000).cast("long"))
        .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("long"))
    )

    quarantine_df = (
        kafka_with_metadata.filter(col("data").isNull())
        .select(
            col("kafka_key"),
            col("raw_value"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
        )
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("correlation_id", lit(correlation_id))
        .withColumn("quarantine_reason", lit("avro_deserialization_failure"))
    )
    return parsed_df, quarantine_df


def add_streaming_bronze_metadata(parsed_df, correlation_id: str):
    """Attach standard Bronze metadata columns to parsed stream records."""
    from pyspark.sql.functions import col, current_timestamp, lit

    return (
        parsed_df.withColumn("processing_timestamp", current_timestamp())
        .withColumn("correlation_id", lit(correlation_id))
        .withColumn("source", lit("polygon_kafka_delayed_streaming"))
        .withColumn("ts_unit", lit(BaseConfig.TS_UNIT_MILLISECONDS))
        .withColumn("date", BaseConfig.timestamp_to_date(col("start_timestamp"), col("ts_unit")))
    )


def start_streaming_writers(
    bronze_df,
    quarantine_df,
    *,
    bronze_path: str,
    checkpoint_path: str,
    quarantine_path: str,
    quarantine_checkpoint_path: str,
    trigger_interval: str,
    quarantine_trigger_interval: str,
):
    """Start main and quarantine writeStream queries."""
    streaming_query = (
        bronze_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .partitionBy("date")
        .trigger(processingTime=trigger_interval)
        .start(bronze_path)
    )
    quarantine_query = (
        quarantine_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", quarantine_checkpoint_path)
        .trigger(processingTime=quarantine_trigger_interval)
        .start(quarantine_path)
    )
    return streaming_query, quarantine_query


def monitor_startup_batches(
    streaming_query, quarantine_query, logger, checks: int = 5, sleep_seconds: int = 10
) -> None:
    """Poll initial micro-batches and fail fast on startup stream exceptions."""
    for i in range(checks):
        time.sleep(sleep_seconds)
        label, stream_exc = get_streaming_query_fault(streaming_query, quarantine_query)
        if stream_exc:
            stop_queries(streaming_query, quarantine_query)
            logger.log_error(f"{label} stream faulted during startup", error=stream_exc)
            raise stream_exc
        progress = streaming_query.lastProgress
        q_progress = quarantine_query.lastProgress
        if progress:
            quarantined = (q_progress or {}).get("numInputRows", 0)
            print(
                f"Batch {progress.get('batchId', i + 1)}: {progress.get('numInputRows', 0)} rows, "
                f"{progress.get('processedRowsPerSecond', 0):.1f} rows/sec"
                f"{f' | quarantined: {quarantined}' if quarantined else ''}"
            )


def compute_market_poll_sleep_seconds(seconds_to_close: float) -> int:
    """Adaptive poll interval for market-close monitoring loop."""
    if seconds_to_close > 1800:
        return 300
    if seconds_to_close > 300:
        return 60
    return 10


def run_drain_mode(streaming_query, logger, drain_max_wait_seconds: int, drain_final_wait_seconds: int) -> dict:
    """Run processAllAvailable with timeout, finalize wait, and catch-up evaluation."""
    logger.log_info(f"Entering drain mode: max wait {drain_max_wait_seconds}s...")
    finished_in_time = False
    progress = None
    drain_completed = threading.Event()
    drain_error = []

    def _drain_worker():
        try:
            streaming_query.processAllAvailable()
            drain_completed.set()
        except Exception as exc:
            drain_error.append(exc)
            drain_completed.set()

    drain_thread = threading.Thread(target=_drain_worker, daemon=True)
    drain_thread.start()
    finished_in_time = drain_completed.wait(timeout=drain_max_wait_seconds)

    if drain_error:
        logger.log_error("Error during processAllAvailable()", error=drain_error[0])
    elif not finished_in_time:
        logger.log_info(
            f"\u26a0\ufe0f  Drain timed out after {drain_max_wait_seconds}s \u2014 "
            "stopping query with remaining lag; data will be replayed on next session."
        )
    else:
        logger.log_info("\u2705 Backlog drained within timeout window")

    logger.log_info(f"Waiting {drain_final_wait_seconds}s for final micro-batch...")
    time.sleep(drain_final_wait_seconds)
    progress = streaming_query.lastProgress
    if progress and is_stream_caught_up(progress):
        logger.log_info(f"\u2705 Stream caught up (numInputRows={progress.get('numInputRows', 0)})")
    else:
        input_rows = (progress or {}).get("numInputRows", 0)
        logger.log_info(f"\u26a0\ufe0f Some lag may remain (numInputRows={input_rows}), stopping anyway")

    drain_thread.join(timeout=drain_max_wait_seconds)
    return {
        "drain_status": "error" if drain_error else ("timeout" if not finished_in_time else "clean"),
        "residual_lag_rows": int((progress or {}).get("numInputRows", 0)),
    }


def stop_queries(streaming_query, quarantine_query) -> None:
    """Stop active streaming queries if running."""
    if streaming_query.isActive:
        streaming_query.stop()
    if quarantine_query.isActive:
        quarantine_query.stop()


def append_drain_metrics(spark, metrics_path: str, pipeline: str, drain_status: str, residual_lag_rows: int) -> None:
    """Persist drain metrics for operational visibility."""
    spark.createDataFrame(
        [
            {
                "pipeline": pipeline,
                "drain_status": drain_status,
                "residual_lag_rows": residual_lag_rows,
                "captured_at": datetime.now(timezone.utc),
            }
        ]
    ).write.format("delta").mode("append").save(metrics_path)
