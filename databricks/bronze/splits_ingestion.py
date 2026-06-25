# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Stock Splits Ingestion
# MAGIC
# MAGIC ## What It Does
# MAGIC Fetches historical stock split events from Polygon.io `/stocks/v1/splits` and
# MAGIC stores them in Bronze Delta Lake. Each event carries Polygon's cumulative
# MAGIC `historical_adjustment_factor`, used downstream to split-adjust prices.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon Splits API → This Notebook → Bronze Delta Lake (splits)
# MAGIC                                                 ↓
# MAGIC                                    Gold dim_split_hc (latest snapshot)
# MAGIC                                                 ↓
# MAGIC                                    Gold fact_daily_market_adjusted_hc
# MAGIC ```
# MAGIC
# MAGIC ## Output
# MAGIC - **Path**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/splits`
# MAGIC - **Columns**: id, symbol, execution_date, split_from, split_to, adjustment_type,
# MAGIC   historical_adjustment_factor, ingestion_timestamp, processing_timestamp,
# MAGIC   correlation_id, source, snapshot_date
# MAGIC - **Write Mode**: snapshot-append — `replaceWhere snapshot_date = '<date>'`
# MAGIC   (immutable raw; idempotent re-runs; each snapshot is the split universe known that day)
# MAGIC
# MAGIC ## Downstream Dependencies
# MAGIC - dim_split_dlt.py (Gold — reads latest snapshot from this Bronze table)

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

import pytz

# COMMAND ----------
# MAGIC %pip install massive
# COMMAND ----------
from bronze_config import BronzeConfig
from massive import RESTClient

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from simple_logger import SimpleLogger

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Initialize Spark and Configuration

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
logger = SimpleLogger("splits_ingestion", dbutils)
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
        raise ValueError(f"polygon-api-key is empty or not set. Scope: {config.scope}")
    logger.log_info(f"Polygon API key loaded ({len(polygon_api_key)} chars)")
except Exception as e:
    logger.log_error(
        "Polygon API key validation failed",
        error=e,
        remediation=f"Add secret 'polygon-api-key' to scope '{config.scope}' via scripts/add_secrets_rest_api.py",
    )
    logger.exit_job("failed", "API key validation failed")
    raise SystemExit("API key validation failed")  # guard: exit_job() may not halt in all contexts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fetch Splits
# MAGIC
# MAGIC Single paginated call to `/stocks/v1/splits`. The SDK iterator follows
# MAGIC `next_url` automatically. We pull the full universe (all tickers) so the
# MAGIC snapshot is self-contained for the Gold dimension.

# COMMAND ----------

try:
    polygon_client = RESTClient(api_key=polygon_api_key)
    all_splits = []
    for s in polygon_client.list_stocks_splits(limit=5000, sort="execution_date.desc"):
        all_splits.append(
            {
                "id": getattr(s, "id", None),
                "symbol": s.ticker,
                "execution_date": s.execution_date,
                "split_from": float(s.split_from) if s.split_from is not None else None,
                "split_to": float(s.split_to) if s.split_to is not None else None,
                "adjustment_type": getattr(s, "adjustment_type", None),
                "historical_adjustment_factor": (
                    float(s.historical_adjustment_factor) if s.historical_adjustment_factor is not None else None
                ),
            }
        )
    logger.log_info(f"Fetched {len(all_splits)} split events from Polygon API")
except Exception as e:
    logger.log_error("Failed to fetch splits from Polygon API", error=e, remediation="Check API key and connectivity")
    logger.exit_job("failed", "Could not fetch splits from API")
    raise SystemExit("Could not fetch splits from API")  # guard: exit_job() may not halt in all contexts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resolve Snapshot Date
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
# MAGIC ## 6. Transform and Create DataFrame

# COMMAND ----------

if not all_splits:
    logger.log_info("No split events fetched")
    logger.exit_job("failed", "No splits to process")

splits_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), False),
        StructField("execution_date", StringType(), True),
        StructField("split_from", DoubleType(), True),
        StructField("split_to", DoubleType(), True),
        StructField("adjustment_type", StringType(), True),
        StructField("historical_adjustment_factor", DoubleType(), True),
    ]
)

df = spark.createDataFrame(all_splits, schema=splits_schema)
bronze_df = (
    df.withColumn("execution_date", df["execution_date"].cast("date"))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("processing_timestamp", current_timestamp())
    .withColumn("correlation_id", lit(logger.correlation_id))
    .withColumn("source", lit("polygon_stocks_splits_api"))
    # snapshot_date is the Bronze partition key. One partition per run; each snapshot
    # records the split universe known that day (lightweight point-in-time history).
    .withColumn("snapshot_date", lit(_snap_date_str).cast("date"))
)

logger.log_info(f"Created DataFrame with {len(all_splits)} split records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Write to Bronze Delta Lake
# MAGIC
# MAGIC **Design**: Snapshot-append — one partition per `snapshot_date`, never overwritten.
# MAGIC `replaceWhere` scopes the overwrite to today's partition only, so re-runs are
# MAGIC idempotent and all prior snapshots are preserved (full audit trail).

# COMMAND ----------

splits_path = config.get_bronze_path("splits")

try:
    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"snapshot_date = '{_snap_date_str}'")
        .partitionBy("snapshot_date")
        .save(splits_path)
    )
    logger.log_info(f"Snapshot for {_snap_date_str} written to {splits_path} ({len(all_splits)} rows)")
except Exception as e:
    logger.log_error("Failed to write to Delta Lake", error=e, remediation=f"Check path: {splits_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification and Summary

# COMMAND ----------

# Read back only the partition just written — fast (partition pruning) and confirms
# the correct row count landed without scanning the full multi-snapshot table.
records_written = spark.read.format("delta").load(splits_path).filter(f"snapshot_date = '{_snap_date_str}'").count()
snapshot_count = spark.read.format("delta").load(splits_path).select("snapshot_date").distinct().count()

logger.log_info(
    f"Verification: snapshot_date={_snap_date_str} | Rows written: {records_written} | "
    f"Total snapshots in table: {snapshot_count}"
)

logger.log_job_summary(
    status="success",
    records_read=len(all_splits),
    records_written=records_written,
    extra_metrics={
        "snapshot_date": _snap_date_str,
        "total_snapshots": snapshot_count,
        "output_path": splits_path,
    },
)

logger.exit_job("success", f"Snapshot {_snap_date_str} written: {records_written} rows")
