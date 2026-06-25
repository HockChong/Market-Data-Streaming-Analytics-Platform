"""Runtime helpers for incremental flatfile Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from delta.tables import DeltaTable
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min


@dataclass(frozen=True)
class IncrementalWidgetParams:
    lookback_days: int | None
    start_date_override: str
    end_date_override: str


@dataclass(frozen=True)
class IncrementalDateWindow:
    start_date: str
    end_date: str
    table_exists: bool
    days_to_process: int


def parse_incremental_widget_params(dbutils) -> IncrementalWidgetParams:
    """Parse Databricks widgets with safe defaults when unavailable."""
    try:
        dbutils.widgets.text("lookback_days", "", "Lookback Days")
        dbutils.widgets.text("start_date", "", "Start Date (YYYY-MM-DD, ET trading date)")
        dbutils.widgets.text("end_date", "", "End Date (YYYY-MM-DD, ET trading date)")

        lookback_days_widget = dbutils.widgets.get("lookback_days")
        return IncrementalWidgetParams(
            lookback_days=int(lookback_days_widget) if lookback_days_widget else None,
            start_date_override=dbutils.widgets.get("start_date"),
            end_date_override=dbutils.widgets.get("end_date"),
        )
    except Exception:
        return IncrementalWidgetParams(
            lookback_days=None,
            start_date_override="",
            end_date_override="",
        )


def resolve_incremental_date_window(
    spark,
    bronze_path: str,
    market_tz,
    lookback_days: int | None,
    start_date_override: str,
    end_date_override: str,
    default_end_date_lag_days: int,
    logger,
) -> IncrementalDateWindow:
    """Resolve incremental start/end dates and table existence mode."""
    table_exists = False
    start_date_auto = None

    try:
        if DeltaTable.isDeltaTable(spark, bronze_path):
            max_date_row = spark.read.format("delta").load(bronze_path).select(spark_max("date")).collect()[0]
            last_ingestion_date = max_date_row[0]

            if last_ingestion_date:
                start_date_auto = (last_ingestion_date + timedelta(days=1)).strftime("%Y-%m-%d")
                table_exists = True
                print(f"Existing table found | Last date: {last_ingestion_date}")
            else:
                if lookback_days is None:
                    logger.log_error("Table exists but no data", remediation="Provide lookback_days parameter")
                    logger.exit_job("failed", "Table empty and no lookback_days provided")
                start_date_auto = (datetime.now(market_tz) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
                table_exists = True
        else:
            if lookback_days is None:
                logger.log_error("Table doesn't exist", remediation="Provide lookback_days parameter for initial load")
                logger.exit_job("failed", "Table missing and no lookback_days provided")
            start_date_auto = (datetime.now(market_tz) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            table_exists = False
            print(f"Table not found | Will create with {lookback_days} days lookback")
    except Exception as e:
        if lookback_days is None:
            logger.log_error("Error checking table", error=e, remediation="Provide lookback_days parameter")
            logger.exit_job("failed", "Table check error and no lookback_days provided")
        start_date_auto = (datetime.now(market_tz) - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
        table_exists = False

    if start_date_override and end_date_override:
        start_date = start_date_override
        end_date = end_date_override
        print(f"Using manual override: {start_date} to {end_date}")
    else:
        start_date = start_date_auto
        end_date = (datetime.now(market_tz) - timedelta(days=default_end_date_lag_days)).strftime("%Y-%m-%d")
        print(f"Auto-detected range: {start_date} to {end_date}")

    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")

    if start_dt > end_dt:
        logger.log_info("Already up to date - no new data")
        logger.exit_job("success", "Already up to date")

    return IncrementalDateWindow(
        start_date=start_date,
        end_date=end_date,
        table_exists=table_exists,
        days_to_process=(end_dt - start_dt).days + 1,
    )


def configure_polygon_s3_access(spark, config, logger) -> None:
    """Configure S3A credentials/settings for Polygon flatfiles and Delta writer tuning."""
    try:
        access_key = config.polygon_flatfiles_access_key
        secret_key = config.polygon_flatfiles_secret_key

        if not access_key:
            raise ValueError(
                f"polygon-flatfiles-access-key is empty or not set. Scope: {config.scope}, value type: {type(access_key)}"
            )
        if not secret_key:
            raise ValueError(
                f"polygon-flatfiles-secret-key is empty or not set. Scope: {config.scope}, value type: {type(secret_key)}"
            )

        logger.log_info(
            f"S3 credentials loaded (access_key: {len(access_key)} chars, secret_key: {len(secret_key)} chars)"
        )

        endpoint_raw = config.polygon_flatfiles_endpoint or ""
        endpoint = endpoint_raw.replace("https://", "").replace("http://", "").rstrip("/")

        hadoop_conf = spark._jsc.hadoopConfiguration()
        hadoop_conf.set("fs.s3a.access.key", access_key)
        hadoop_conf.set("fs.s3a.secret.key", secret_key)
        hadoop_conf.set("fs.s3a.endpoint", endpoint)
        hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        hadoop_conf.set("fs.s3a.path.style.access", "true")
        hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
    except Exception as e:
        logger.log_error(
            "S3 credentials validation failed",
            error=e,
            remediation="Add secrets 'polygon-flatfiles-access-key' and 'polygon-flatfiles-secret-key' "
            f"to Databricks scope '{config.scope}' via scripts/add_secrets_rest_api.py",
        )
        logger.exit_job("failed", "S3 credential validation failed")


def probe_flatfiles(dbutils, paths: list[str], logger) -> list[tuple[str, int]]:
    """Check each path via dbutils.fs.ls and return readable files with their S3 mod times.

    Uses the filesystem metadata API instead of launching a Spark job per file.
    Returns a list of (path, modification_time_ms) for every path that exists and
    is readable; silently skips missing paths (holidays, non-trading days).

    Args:
        dbutils: Databricks utilities object.
        paths:   List of s3a:// paths to probe.
        logger:  SimpleLogger instance for progress reporting.

    Returns:
        List of (path, mod_time_ms) tuples — only paths confirmed to exist.
        mod_time_ms is the S3 last-modified timestamp (epoch milliseconds), which
        represents when Polygon uploaded the file (~bar date + 2–3 days).
    """
    readable: list[tuple[str, int]] = []
    skipped: list[str] = []

    for path in paths:
        try:
            file_info = dbutils.fs.ls(path)
            readable.append((path, file_info[0].modificationTime))
        except Exception:
            skipped.append(path)

    if skipped:
        logger.log_info(f"Skipped {len(skipped)} missing files (holidays/non-trading days)")

    return readable


def write_incremental_to_delta(spark, bronze_df, bronze_path: str, table_exists: bool, logger) -> tuple[int, int]:
    """Write incremental Bronze rows using insert-only merge or initial overwrite."""
    records_inserted = 0
    records_updated = 0
    try:
        if table_exists:
            delta_table = DeltaTable.forPath(spark, bronze_path)

            # Bound the MERGE to the partitions this batch can touch. The key match
            # is (symbol, start_timestamp); adding target.date >= min(source.date)
            # lets Delta prune older date partitions explicitly instead of relying
            # only on implicit start_timestamp file-stat skipping over the full
            # multi-year target. min(source.date) is <= every source row's date, so
            # no matchable partition is excluded — purely additive pruning.
            merge_condition = "target.symbol = source.symbol AND target.start_timestamp = source.start_timestamp"
            min_source_date = bronze_df.select(spark_min("date")).collect()[0][0]
            if min_source_date is not None:
                merge_condition += f" AND target.date >= '{min_source_date}'"

            (
                delta_table.alias("target")
                .merge(
                    bronze_df.alias("source"),
                    merge_condition,
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

            history = delta_table.history(1).collect()[0]
            metrics = history["operationMetrics"] or {}
            records_inserted = int(metrics.get("numTargetRowsInserted", 0))
            records_updated = int(metrics.get("numTargetRowsUpdated", 0))
            records_skipped = int(metrics.get("numTargetRowsMatchedAndNotUpdated", 0))

            if records_skipped > 0:
                logger.log_info(f"Skipped {records_skipped:,} already-existing rows (idempotent re-run)")

            if records_updated > 0:
                logger.log_error(
                    f"INVARIANT VIOLATION: {records_updated} Bronze rows were updated — "
                    "this should never happen with insert-only MERGE. "
                    "A whenMatchedUpdate clause may have been added without updating the immutability guard.",
                    remediation="Investigate why duplicate (symbol, start_timestamp) keys exist in source.",
                )
                raise RuntimeError("Bronze immutability violated")

            print(f"MERGE complete | Inserted: {records_inserted:,} | Skipped (already exist): {records_skipped:,}")
        else:
            bronze_df.write.format("delta").mode("overwrite").partitionBy("date").save(bronze_path)
            delta_table = DeltaTable.forPath(spark, bronze_path)
            history = delta_table.history(1).collect()[0]
            if history["operationMetrics"]:
                records_inserted = int(history["operationMetrics"].get("numOutputRows", 0))
            print(f"Initial write complete | Records: {records_inserted:,}")
    except Exception as e:
        logger.log_error("Failed to write to Delta Lake", error=e, remediation=f"Check path: {bronze_path}")
        raise

    return records_inserted, records_updated


def collect_incremental_summary(
    spark, bronze_path: str, records_inserted: int, records_updated: int
) -> tuple[int, str]:
    """Return total records written and table date range string."""
    delta_table = DeltaTable.forPath(spark, bronze_path)
    latest_history = delta_table.history(1).collect()[0]
    operation_metrics = latest_history["operationMetrics"] or {}

    if latest_history["operation"] == "MERGE":
        total_records_written = records_inserted + records_updated
    else:
        total_records_written = int(operation_metrics.get("numOutputRows", records_inserted))

    date_stats = (
        spark.read.format("delta")
        .load(bronze_path)
        .select(spark_min("date").alias("min_date"), spark_max("date").alias("max_date"))
        .collect()[0]
    )
    return total_records_written, f"{date_stats['min_date']} to {date_stats['max_date']}"
