# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Polygon REST Aggregates — Gap Backfill
# MAGIC
# MAGIC ## Purpose
# MAGIC Recovers 1-minute bars for a session where the live streaming jobs failed to
# MAGIC start (e.g. cluster failure) and bars **never reached Kafka** — so there is
# MAGIC nothing for `kafka_replay_backfill.py` to replay. Fetches a time window of
# MAGIC the session per ticker from the Polygon REST aggregates API and appends to
# MAGIC the Bronze streaming Delta table.
# MAGIC
# MAGIC ## Strategy — fetch a bounded window, let Silver dedup pick winners
# MAGIC No gap detection. A time window of the session (default 09:30–10:00 ET, the
# MAGIC typical late-start gap) is fetched and appended with
# MAGIC `source = "polygon_rest_backfill"` (source_priority = 0). Silver's MERGE on
# MAGIC `(symbol, start_timestamp)` keeps live-streamed bars (priority 1) wherever
# MAGIC they exist; REST bars survive only in the holes. Next-day flat files
# MAGIC (priority 2) supersede both. Widen `start_time`/`end_time` for longer
# MAGIC outages — over-fetching is harmless, the dedup absorbs any overlap.
# MAGIC
# MAGIC ## How it works
# MAGIC - One REST call per ticker: `/v2/aggs/.../range/1/minute/{start_ms}/{end_ms}`
# MAGIC   with `adjusted=False` — Bronze holds raw unadjusted bars; split adjustment
# MAGIC   happens downstream (`split_adjust_spark.py`).
# MAGIC - Batch append to the Bronze streaming Delta path — the live stream's Kafka
# MAGIC   checkpoint is **never touched** (same isolation as `kafka_replay_backfill.py`).
# MAGIC - `backfill_date` defaults to the latest `date` partition in Bronze streaming.
# MAGIC
# MAGIC ## Safety
# MAGIC - `dry_run` defaults to `true` — preview row counts first, re-run with `false` to write.
# MAGIC - Re-running is safe: Bronze gets duplicate raw rows, Silver MERGE stays stable.
# MAGIC
# MAGIC ## Parameters
# MAGIC | Parameter | Required | Description |
# MAGIC |-----------|----------|-------------|
# MAGIC | `tickers` | No | Comma-separated override (e.g. `AAPL,MSFT`); blank = all distinct symbols already in Bronze streaming for the backfill date. With the `AM.*` subscription that can be thousands of symbols — one REST call each — so pass an explicit list to scope a faster recovery. |
# MAGIC | `backfill_date` | No | Session date YYYY-MM-DD; defaults to latest date in Bronze streaming |
# MAGIC | `start_time` | No | Window start, ET HH:MM; defaults to `09:30` (market open) |
# MAGIC | `end_time` | No | Window end (exclusive), ET HH:MM; defaults to `10:00` — widen for longer outages |
# MAGIC | `max_workers` | No | Parallel fetch threads (1–32, default 32). Fetching is network-bound; dial down if the API rate-limits (long stalls in the log). |
# MAGIC | `dry_run` | No | `true` (default) previews counts without writing |

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

# COMMAND ----------
# MAGIC %pip install massive
# COMMAND ----------
from base_config import BaseConfig
from bronze_config import BronzeConfig
from massive import RESTClient

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.functions import max as spark_max
from rest_aggs_backfill_runtime import (
    REST_BACKFILL_SOURCE,
    align_to_bronze_schema,
    market_window_to_epoch_ms,
    parse_ticker_csv,
    rest_agg_to_bronze_row,
    rest_backfill_rows_schema,
)
from simple_logger import SimpleLogger

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
config = BronzeConfig(dbutils)
logger = SimpleLogger("rest_aggs_backfill", dbutils)
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
# MAGIC ## 3. Parameters

# COMMAND ----------

dbutils.widgets.text("tickers", "", "Tickers (optional — blank = all symbols on backfill date)")
dbutils.widgets.text("backfill_date", "", "Backfill Date (YYYY-MM-DD, optional)")
dbutils.widgets.text("start_time", "09:30", "Window Start (ET HH:MM)")
dbutils.widgets.text("end_time", "10:00", "Window End (ET HH:MM, exclusive)")
dbutils.widgets.text("max_workers", "32", "Parallel Fetch Workers (1-32)")
dbutils.widgets.text("dry_run", "true", "Dry Run (true/false)")

_tickers_param = dbutils.widgets.get("tickers").strip()
START_TIME = dbutils.widgets.get("start_time").strip()
END_TIME = dbutils.widgets.get("end_time").strip()
DRY_RUN = dbutils.widgets.get("dry_run").strip().lower() != "false"

try:
    MAX_WORKERS = int(dbutils.widgets.get("max_workers").strip())
    if not 1 <= MAX_WORKERS <= 32:
        raise ValueError(f"max_workers must be between 1 and 32, got {MAX_WORKERS}")
except ValueError as e:
    logger.log_error("Invalid max_workers parameter", error=e, remediation="Use an integer between 1 and 32")
    logger.exit_job("failed", str(e))
    raise SystemExit(str(e))  # guard: exit_job() may not halt in all contexts

logger.log_info(
    f"tickers widget: {_tickers_param or '(blank — auto from Bronze)'} | "
    f"window: {START_TIME}–{END_TIME} ET | workers: {MAX_WORKERS} | DRY_RUN={DRY_RUN}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Resolve Backfill Date and Fetch Window
# MAGIC
# MAGIC Explicit `backfill_date` widget wins; otherwise use the latest `date`
# MAGIC partition already in the Bronze streaming table — the session the live
# MAGIC stream last touched (a late start leaves partial bars for that date).
# MAGIC The `start_time`/`end_time` widgets (ET) bound the fetch within that date;
# MAGIC the default 09:30–10:00 covers a 30-minute late start.

# COMMAND ----------

bronze_path = config.get_bronze_path("streaming")

_date_param = dbutils.widgets.get("backfill_date").strip()
if _date_param:
    try:
        datetime.strptime(_date_param, "%Y-%m-%d")
    except ValueError:
        logger.log_error("Invalid backfill_date", remediation="Use YYYY-MM-DD format")
        logger.exit_job("failed", f"Invalid backfill_date: {_date_param}")
        raise SystemExit(f"Invalid backfill_date: {_date_param}")  # guard: exit_job() may not halt in all contexts
    BACKFILL_DATE = _date_param
else:
    _latest = spark.read.format("delta").load(bronze_path).select(spark_max("date").alias("d")).collect()[0]["d"]
    if _latest is None:
        logger.log_error(
            "Bronze streaming table has no data to infer a date from",
            remediation="Pass backfill_date explicitly (YYYY-MM-DD)",
        )
        logger.exit_job("failed", "Cannot infer backfill_date from empty Bronze streaming table")
        raise SystemExit("Cannot infer backfill_date")  # guard: exit_job() may not halt in all contexts
    BACKFILL_DATE = _latest.strftime("%Y-%m-%d")

try:
    WINDOW_START_MS, WINDOW_END_MS = market_window_to_epoch_ms(
        BACKFILL_DATE, START_TIME, END_TIME, config.MARKET_TIMEZONE
    )
except ValueError as e:
    logger.log_error("Invalid fetch window", error=e, remediation="Use ET HH:MM with start_time before end_time")
    logger.exit_job("failed", str(e))
    raise SystemExit(str(e))  # guard: exit_job() may not halt in all contexts

logger.log_info(f"Backfill window: {BACKFILL_DATE} {START_TIME}–{END_TIME} ET ({WINDOW_START_MS}–{WINDOW_END_MS})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resolve Tickers
# MAGIC
# MAGIC Explicit `tickers` widget wins; otherwise use every distinct symbol already
# MAGIC in Bronze streaming for the backfill date — on a late-start day the stream
# MAGIC captured all actively trading symbols after it came up, so that set is the
# MAGIC universe to recover. One REST call per symbol.

# COMMAND ----------

if _tickers_param:
    try:
        TICKERS = parse_ticker_csv(_tickers_param)
    except ValueError as e:
        logger.log_error("Invalid tickers parameter", error=e, remediation="Pass tickers, e.g. 'AAPL,MSFT,NVDA'")
        logger.exit_job("failed", str(e))
        raise SystemExit(str(e))  # guard: exit_job() may not halt in all contexts
    logger.log_info(f"Tickers from widget: {len(TICKERS)}")
else:
    TICKERS = [
        row["symbol"]
        for row in (
            spark.read.format("delta")
            .load(bronze_path)
            .filter(col("date") == BACKFILL_DATE)
            .select("symbol")
            .distinct()
            .orderBy("symbol")
            .collect()
        )
    ]
    if not TICKERS:
        logger.log_error(
            f"No symbols found in Bronze streaming for {BACKFILL_DATE}",
            remediation="Pass tickers explicitly via the tickers widget",
        )
        logger.exit_job("failed", f"No symbols in Bronze streaming for {BACKFILL_DATE}")
        raise SystemExit("No symbols to backfill")  # guard: exit_job() may not halt in all contexts
    logger.log_info(f"Tickers auto-derived from Bronze streaming {BACKFILL_DATE}: {len(TICKERS)}")

_preview_tickers = ", ".join(TICKERS[:10]) + (", ..." if len(TICKERS) > 10 else "")
logger.log_info(f"Tickers ({len(TICKERS)}): {_preview_tickers}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fetch Minute Aggregates from Polygon REST (Parallel)
# MAGIC
# MAGIC One paginated call per ticker (the SDK iterator follows `next_url`),
# MAGIC bounded to the millisecond window from Section 4, fetched with a thread
# MAGIC pool — the loop is network-bound (~100ms round trip per ticker), so
# MAGIC `max_workers` concurrent calls cut wall time roughly proportionally.
# MAGIC `adjusted=False` keeps bars raw/unadjusted, matching the WebSocket and
# MAGIC flat-file sources. The API's `to` bound is inclusive, so we pass
# MAGIC `end_ms - 1` to keep the widget window end-exclusive (09:30–10:00 = bars
# MAGIC starting 09:30..09:59). Result order is irrelevant: Silver's MERGE keys
# MAGIC on (symbol, start_timestamp).

# COMMAND ----------

polygon_client = RESTClient(api_key=polygon_api_key)

# Captured once per run so every row in this backfill shares one ingestion
# timestamp (epoch seconds — matches the streaming table's unit; see runtime helper).
fetch_epoch_seconds = int(time.time())


def _fetch_symbol_bars(symbol):
    """Fetch one symbol's window as Bronze row dicts (runs on a worker thread)."""
    return [
        rest_agg_to_bronze_row(agg, symbol, fetch_epoch_seconds)
        for agg in polygon_client.list_aggs(
            symbol,
            1,
            "minute",
            WINDOW_START_MS,
            WINDOW_END_MS - 1,
            adjusted=False,
            sort="asc",
            limit=50000,
        )
    ]


rows = []
failed_tickers = []
fetch_started = time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    _futures = {executor.submit(_fetch_symbol_bars, s): s for s in TICKERS}
    for _done_count, _future in enumerate(as_completed(_futures), start=1):
        _symbol = _futures[_future]
        try:
            _symbol_rows = _future.result()
            rows.extend(_symbol_rows)
        except Exception as e:
            logger.log_error(f"Failed to fetch aggregates for {_symbol}", error=e)
            failed_tickers.append(_symbol)
            continue
        # Progress heartbeat instead of one line per symbol — at thousands of
        # tickers, per-symbol logging floods the driver log.
        if _done_count % 500 == 0 or _done_count == len(TICKERS):
            _elapsed = time.time() - fetch_started
            logger.log_info(
                f"Progress: {_done_count}/{len(TICKERS)} tickers | {len(rows)} bars | "
                f"{len(failed_tickers)} failed | {_elapsed:.0f}s elapsed"
            )

logger.log_info(
    f"Fetched {len(rows)} bars across {len(TICKERS) - len(failed_tickers)}/{len(TICKERS)} tickers "
    f"in {time.time() - fetch_started:.0f}s with {MAX_WORKERS} workers"
)

if not rows:
    logger.exit_job(
        "failed",
        f"No bars fetched for {BACKFILL_DATE} {START_TIME}–{END_TIME} ET — check tickers, window, and API key",
    )
    raise SystemExit("No bars fetched")  # guard: exit_job() may not halt in all contexts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Build DataFrame and Align to Bronze Streaming Schema
# MAGIC
# MAGIC Kafka metadata columns (topic/partition/offset/...) become typed NULLs —
# MAGIC these rows never passed through Kafka. Alignment raises if the row shape
# MAGIC would add a column the Bronze table does not have.

# COMMAND ----------

rest_df = spark.createDataFrame(rows, schema=rest_backfill_rows_schema())

bronze_df = (
    rest_df.withColumn("processing_timestamp", current_timestamp())
    .withColumn("correlation_id", lit(logger.correlation_id))
    .withColumn("source", lit(REST_BACKFILL_SOURCE))
    .withColumn("ts_unit", lit(BaseConfig.TS_UNIT_MILLISECONDS))
    .withColumn("date", BaseConfig.timestamp_to_date(col("start_timestamp"), col("ts_unit")))
)

target_schema = spark.read.format("delta").load(bronze_path).schema
bronze_df = align_to_bronze_schema(bronze_df, target_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Preview — Verify Row Counts and Date Partition Before Writing
# MAGIC
# MAGIC Check that `date` shows only the backfill date. If it looks wrong, stop
# MAGIC here — do not re-run with `dry_run = false`.

# COMMAND ----------

row_count = bronze_df.count()

print(f"Bars to write     : {row_count}")
print(f"Failed tickers    : {failed_tickers or 'none'}")
print(f"\nDistinct dates (should show only {BACKFILL_DATE}):")
bronze_df.select("date").distinct().show()

print("Per-symbol bar counts (first 50):")
bronze_df.groupBy("symbol").count().orderBy("symbol").show(min(len(TICKERS), 50), truncate=False)

print("Sample records:")
bronze_df.select("symbol", "date", "start_timestamp", "ts_unit", "source", "open", "close", "volume").show(
    5, truncate=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Write to Bronze Streaming Delta Path
# MAGIC
# MAGIC Append-only batch write — no checkpoint involved. Only runs when
# MAGIC `dry_run = false`.

# COMMAND ----------

if DRY_RUN:
    logger.log_info(f"DRY_RUN=true — skipping write. {row_count} rows would be appended to {bronze_path}")
    print(f"\nDRY RUN: {row_count} rows would be written to {bronze_path}")
    print("Re-run with widget dry_run = false to write.")
    dbutils.notebook.exit("Dry run complete — no data written")

logger.log_info(f"Writing {row_count} rows to {bronze_path} ...")

try:
    (bronze_df.write.format("delta").mode("append").partitionBy("date").save(bronze_path))
    logger.log_info(f"Write complete: {row_count} rows appended to {bronze_path}")
    print(f"\nWrite complete: {row_count} rows written to {bronze_path}")
    print(f"Trigger the Silver DLT pipeline to merge the {BACKFILL_DATE} backfill into ohlcv_silver_hc.")
except Exception as e:
    logger.log_error("Failed to write to Bronze", error=e, remediation=f"Check path access: {bronze_path}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Verification and Summary

# COMMAND ----------

written = (
    spark.read.format("delta")
    .load(bronze_path)
    .filter((col("date") == BACKFILL_DATE) & (col("source") == REST_BACKFILL_SOURCE))
)
written_count = written.count()
print(f"REST-backfill rows in Bronze streaming for {BACKFILL_DATE}: {written_count}")
written.groupBy("symbol").count().orderBy("symbol").show(min(len(TICKERS), 50), truncate=False)

logger.log_job_summary(
    status="failed" if failed_tickers else "success",
    records_read=len(rows),
    records_written=row_count,
    extra_metrics={
        "backfill_date": BACKFILL_DATE,
        "tickers_requested": len(TICKERS),
        "tickers_failed": failed_tickers,
        "output_path": bronze_path,
        "source": REST_BACKFILL_SOURCE,
    },
)

if failed_tickers:
    # Data written for the successful tickers is kept (idempotent to re-run),
    # but surface the partial failure so the run is retried for the rest.
    _failed_preview = ", ".join(failed_tickers[:20]) + (", ..." if len(failed_tickers) > 20 else "")
    logger.exit_job("failed", f"Wrote {row_count} rows but {len(failed_tickers)} tickers failed: {_failed_preview}")
    raise SystemExit("Partial fetch failure")  # guard: exit_job() may not halt in all contexts

logger.exit_job("success", f"Backfilled {row_count} rows for {BACKFILL_DATE} ({len(TICKERS)} tickers)")
