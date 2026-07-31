"""
Silver Layer: OHLCV — Delta Live Tables (DLT)

Purpose:
    Clean, validate, and deduplicate 1-minute OHLCV data from Bronze (streaming + historical),
    using a WAP (Write-Audit-Publish) pattern for visibility and governance.

Architecture Pattern:
    **Unified Streaming**: Both historical and streaming sources are read as
    continuous streams, enabling:
    - Automatic processing of incremental historical backfills
    - No pipeline reset needed when adding new historical data
    - Idempotent keyed output via `apply_changes` MERGE on (symbol, start_timestamp)

Data Flow:
    Bronze (streaming + historical)
        → bronze_unified_hc (unified streaming: both sources as continuous streams)
            → ohlcv_silver_enriched_hc (validate + enrich, temporary)
                → ohlcv_silver_hc (MERGE-based dedup via apply_changes)
            → ohlcv_silver_quarantine_hc (invalid records + rejection_reason)
            → wap_audit_log_hc (daily WAP metrics / quality gate)
        → Gold: fact_minute_market_hc (minute grain)
        → Gold: fact_daily_market_hc (via ohlcv_daily_silver_hc)

    Daily rollup: ohlcv_silver_hc → ohlcv_daily_silver_hc, a materialized view that
        aggregates 1-minute bars into daily OHLCV on (symbol, date). On this serverless
        pipeline the MV is refreshed incrementally (Enzyme) — only changed (symbol, date)
        groups are recomputed for supported aggregations, with full recompute as fallback.
    Orchestration (Layer 3): within each DLT update, the minute MERGE completes before the
        daily MV refreshes; cross-job order is Bronze streaming active → Silver trigger → Gold
        (see DEPLOYMENT_GUIDE.md Jobs 7–8).

    Ticker reference: Gold ``dim_ticker_hc`` reads latest Bronze ``ticker_details`` snapshot
    (see ``dim_ticker_dlt.py``) — no stock-metadata CDC in this Silver pipeline.

Tables Created:
    | Name                       | Mode      | Description |
    |--------------------------- |-----------|-------------|
    | bronze_streaming_source_hc    | Streaming | Read Bronze streaming Delta (Kafka ingest) |
    | bronze_historical_source_hc   | Streaming | Read Bronze historical Delta (flat files) |
    | bronze_unified_hc             | Streaming | Unified Bronze stream (both as continuous streams) |
    | ohlcv_silver_enriched_hc   | Streaming | Validated + enriched stream (temporary, for apply_changes) |
    | ohlcv_silver_hc            | Streaming | Deduplicated OHLCV via apply_changes MERGE (SCD Type 1) |
    | ohlcv_silver_quarantine_hc | Batch     | WAP: invalid rows with rejection reasons |
    | wap_audit_log_hc           | Batch     | WAP: daily counts/rejection rate + gate status (recomputed each run) |
    | ohlcv_daily_silver_hc        | MV        | Daily OHLCV (symbol, date) aggregated from minute Silver; serverless incremental refresh |

Key Validations:
    - OTC filter: Excludes over-the-counter stocks (otc IS NULL) before unification
    - Timestamps: start_timestamp < end_timestamp (fail pipeline)
    - Required fields: symbol/start_timestamp/source present (fail pipeline)
    - WAP rules (quarantine): positive prices, valid OHLC logic, non-negative volume

Deduplication Strategy:
    Uses **apply_changes (Delta MERGE)** instead of dropDuplicates() for dedup.
    MERGE uses file-level data skipping — no streaming state store overhead.
    Key: (symbol, start_timestamp) with **source priority** via sequence_by:
    1. `polygon_flatfiles_s3` (historical, priority=2) — preferred, more complete/reliable
    2. `polygon_kafka_delayed_streaming` (real-time, priority=1) — fallback when historical unavailable
    3. Unknown sources (priority=0) — lowest priority

Daily Rollup Strategy (ohlcv_daily_silver_hc):
    A materialized view that aggregates 1-minute Silver into daily OHLCV on (symbol, date)
    so Gold reads ~2.9M daily rows instead of ~420M minute rows. It is a pure groupBy
    aggregation (min/max/sum), which lets the serverless engine (Enzyme) refresh it
    incrementally: only the (symbol, date) groups whose minute rows changed are recomputed,
    with a full recompute as the cost-based fallback. The MV reads the full minute Silver
    table (no current_date() predicate) so the refresh stays deterministic and
    incrementally maintainable; the daily fact in Gold applies its own lookback window.
"""

import sys

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

import dlt
from aggregation_utils import aggregate_minute_to_daily
from base_config import BaseConfig
from ohlcv_dedup_spark import with_silver_ohlcv_dedup_sequence, with_silver_source_priority
from ohlcv_quarantine_spark import dedupe_quarantine_invalid_rows, with_quarantine_rejection_reason
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    date_format,
    expr,
    from_unixtime,
    from_utc_timestamp,
    lit,
    to_date,
    to_timestamp,
)
from silver_config import SilverConfig
from wap_audit_spark import (
    aggregate_bronze_wap_counts_by_date,
    aggregate_session_bars_by_date,
    finalize_wap_audit_metrics,
)

# =============================================================================
# Configuration
# =============================================================================

_config = SilverConfig()

BRONZE_STREAMING_PATH = _config.get_bronze_path("streaming")
BRONZE_HISTORICAL_PATH = _config.get_bronze_path("historical")

LATE_ARRIVAL_WATERMARK = _config.LATE_ARRIVAL_WATERMARK

# OTC filter — Polygon sends otc=true for OTC stocks and null otherwise;
# coalesce defends against a future change where non-OTC rows carry otc=false.
_IS_EXCHANGE_TRADED = ~coalesce(col("otc"), lit(False))


def _read_bronze_ohlcv_source(path: str):
    """Stream a Bronze OHLCV Delta table with shared column projection and watermark.

    Both streaming and historical Bronze sources have the same schema and need
    the same event_time derivation. This helper eliminates the duplication.
    """
    return (
        spark.readStream.format("delta")
        .option("ignoreDeletes", "true")
        .load(path)
        .select(
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "start_timestamp",
            "end_timestamp",
            "source",
            "otc",
            # ingestion_timestamp feeds the Silver dedup tiebreaker; ts_unit
            # makes epoch-unit conversion explicit instead of magnitude-guessed.
            # Cast to long to normalize: streaming Bronze stores it as LongType
            # (Avro timestamp-millis), historical Bronze stores it as TimestampType
            # (explicit cast in bronze_utils.py). Both become BIGINT here.
            col("ingestion_timestamp").cast("long").alias("ingestion_timestamp"),
            "ts_unit",
            # date is the Bronze partition column — projected here so that
            # bronze_unified_hc inherits it and partition pruning works on the
            # 30-day filter in wap_audit_log_hc without a full-table scan.
            "date",
        )
        .withColumn(
            "event_time",
            to_timestamp(BaseConfig.timestamp_to_seconds(col("start_timestamp"), col("ts_unit"))),
        )
        # Not what stops duplicates — apply_changes below handles that via
        # _dedup_sequence. This just lets Databricks forget rows older than
        # 10 minutes instead of tracking them forever.
        .withWatermark("event_time", LATE_ARRIVAL_WATERMARK)
    )


MARKET_TIMEZONE = _config.MARKET_TIMEZONE
MARKET_OPEN_HHMM = _config.MARKET_OPEN_HOUR * 100 + _config.MARKET_OPEN_MINUTE  # e.g. 930
MARKET_CLOSE_HHMM = _config.MARKET_CLOSE_HOUR * 100 + _config.MARKET_CLOSE_MINUTE  # e.g. 1600
WAP_VALIDATION_RULES = _config.get_wap_validation_rules()
WAP_THRESHOLDS = _config.get_wap_config()

# session_complete warn gate floor: a normal session's busiest symbol reaches ~390
# bars, an early-close half-day ~210. Warn when even the fullest symbol clears fewer
# than half of EXPECTED_BARS_PER_DAY on a trading day — a coarse market-wide-gap
# signal that tolerates early-close half-days.
_SESSION_BARS_MIN = int(BaseConfig.EXPECTED_BARS_PER_DAY * 0.5)  # 195

# =============================================================================
# Early-close session map
# On NYSE early-close days (e.g. July 3, day-before-Thanksgiving, Christmas Eve)
# the exchange closes at 1:00 PM ET, not 4:00 PM. Without this map the static
# MARKET_CLOSE_HHMM = 1600 would pass 1:00–4:00 PM bars on those days into Silver.
#
# We build a Python dict of {date_str: close_hhmm} at pipeline startup — only
# non-standard close days are stored. The dict is broadcast into Spark via
# create_map so each row can look up its own session close, then coalesce falls
# back to MARKET_CLOSE_HHMM for all normal days (no dict entry → null → coalesce).
#
# If exchange_calendars is not installed the block is skipped entirely and
# _EARLY_CLOSE_MAP_COL stays None — the filter degrades to the previous static
# 4:00 PM cutoff with no other change in behaviour.
# =============================================================================
_EARLY_CLOSE_MAP_COL = None  # default: no early-close awareness
try:
    from itertools import chain as _chain

    import pandas as _pd
    from exchange_calendars import get_calendar as _get_calendar
    from pyspark.sql.functions import create_map as _create_map

    _nyse_cal = _get_calendar("XNYS")
    _today = _pd.Timestamp.today(tz="UTC").normalize()

    _EARLY_CLOSE_HHMM: dict[str, int] = {}
    # History range tied to the Gold daily-fact retention window so any bar that
    # can land in the daily fact (including backfills near the retention edge)
    # has early-close awareness — not an independently drifting literal.
    for _session in _nyse_cal.sessions_in_range(
        _today - _pd.Timedelta(days=BaseConfig.DAILY_AGGREGATION_LOOKBACK_DAYS),
        _today + _pd.Timedelta(days=30),
    ):
        # Use timezone string, not pytz object — pytz via pandas tz_convert
        # applies the LMT offset (+3m 56s), producing 16:04 instead of 16:00.
        _close_et = _nyse_cal.session_close(_session).tz_convert(MARKET_TIMEZONE)
        _hhmm_val = _close_et.hour * 100 + _close_et.minute
        if _hhmm_val != MARKET_CLOSE_HHMM:
            _EARLY_CLOSE_HHMM[_session.strftime("%Y-%m-%d")] = _hhmm_val

    if _EARLY_CLOSE_HHMM:
        _EARLY_CLOSE_MAP_COL = _create_map(
            *_chain.from_iterable((lit(d), lit(h)) for d, h in _EARLY_CLOSE_HHMM.items())
        )
except Exception:
    pass  # exchange_calendars unavailable or calendar error — use static cutoff

# =============================================================================
# Bronze Streaming Source
# =============================================================================


@dlt.table(
    name="bronze_streaming_source_hc",
    comment="Bronze streaming data from Kafka (streaming source for Silver)",
    schema="""
        symbol STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        start_timestamp BIGINT,
        end_timestamp BIGINT,
        source STRING,
        otc BOOLEAN,
        ingestion_timestamp BIGINT,
        ts_unit STRING,
        date DATE,
        event_time TIMESTAMP
    """,
    table_properties={"quality": "bronze"},
)
def bronze_streaming_source_hc():
    return _read_bronze_ohlcv_source(BRONZE_STREAMING_PATH)


# =============================================================================
# Bronze Historical Source
# =============================================================================


@dlt.table(
    name="bronze_historical_source_hc",
    comment="Bronze historical data from Polygon flat files (streaming source for Silver)",
    schema="""
        symbol STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        start_timestamp BIGINT,
        end_timestamp BIGINT,
        source STRING,
        otc BOOLEAN,
        ingestion_timestamp BIGINT,
        ts_unit STRING,
        date DATE,
        event_time TIMESTAMP
    """,
    table_properties={"quality": "bronze"},
)
def bronze_historical_source_hc():
    """Streaming read from Bronze historical (Polygon flat files).

    Reading historical as a stream lets incremental backfills flow through
    without a pipeline reset.
    """
    return _read_bronze_ohlcv_source(BRONZE_HISTORICAL_PATH)


# =============================================================================
# Unified Bronze Source
# Both historical and streaming read as continuous streams so incremental
# backfills are picked up without a pipeline reset.
# =============================================================================

dlt.create_streaming_table(
    name="bronze_unified_hc",
    comment="Unified Bronze data (historical + streaming)",
    schema="""
        symbol STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        start_timestamp BIGINT,
        end_timestamp BIGINT,
        source STRING,
        otc BOOLEAN,
        ingestion_timestamp BIGINT,
        ts_unit STRING,
        date DATE,
        event_time TIMESTAMP
    """,
    # Partitioning on date enables partition pruning for the 30-day date predicate
    # in wap_audit_log_hc, keeping its scan cost constant regardless of total
    # Bronze history. Both append_flow sources must project the date column.
    partition_cols=["date"],
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
    },
)


@dlt.append_flow(
    target="bronze_unified_hc",
    name="historical_stream",
    comment="Streaming read of historical Bronze",
)
def stream_historical():
    return dlt.read_stream("bronze_historical_source_hc").filter(_IS_EXCHANGE_TRADED)


@dlt.append_flow(
    target="bronze_unified_hc",
    name="streaming_ingest",
    comment="Streaming ingest from Kafka via Bronze streaming table",
)
def ingest_streaming():
    return dlt.read_stream("bronze_streaming_source_hc").filter(_IS_EXCHANGE_TRADED)


# =============================================================================
# Silver OHLCV - Validated + Enriched Stream (Intermediate)
# =============================================================================


@dlt.table(
    name="ohlcv_silver_enriched_hc",
    temporary=True,  # Not persisted - intermediate for apply_changes
    comment="Validated and enriched OHLCV stream for MERGE-based dedup",
)
@dlt.expect_or_fail("valid_timestamps", "start_timestamp < end_timestamp")
@dlt.expect_or_fail("valid_start_timestamp", "start_timestamp > 0")
@dlt.expect_or_fail(
    "required_fields",
    "symbol IS NOT NULL AND LENGTH(symbol) > 0 AND start_timestamp IS NOT NULL AND source IS NOT NULL",
)
# Fail fast if a Bronze writer forgets to stamp ts_unit or emits a non-ms unit.
# Both OHLCV writers normalize start_timestamp to epoch milliseconds at Bronze
# (flat-file ns and streaming both converted), so ms is the enforced contract —
# a non-ms value would split the (symbol, start_timestamp) dedup key. Silent
# magnitude guessing previously masked this class of bug.
@dlt.expect_or_fail("known_ts_unit", "ts_unit = 'ms'")
def ohlcv_silver_enriched():
    """Validated + enriched stream feeding apply_changes for dedup.

    Dedup is handled downstream by apply_changes (MERGE); this stage only
    validates, filters to market hours, and computes the sequence key.
    """
    df = dlt.read_stream("bronze_unified_hc")
    is_valid = expr(" AND ".join(WAP_VALIDATION_RULES.values()))

    return (
        with_silver_ohlcv_dedup_sequence(
            with_silver_source_priority(df.filter(is_valid))
            # ts_unit is stamped at Bronze and hard-enforced to 'ms' above (known_ts_unit);
            # timestamp_to_seconds converts generically by unit, but only 'ms' reaches here.
            .withColumn("_ts_seconds", BaseConfig.timestamp_to_seconds(col("start_timestamp"), col("ts_unit")))
            .withColumn("_et_timestamp", from_utc_timestamp(from_unixtime(col("_ts_seconds")), MARKET_TIMEZONE))
            .withColumn("date", to_date(col("_et_timestamp")))
            .withColumn("_hhmm", date_format(col("_et_timestamp"), "HHmm").cast("int"))
            # Per-session close lookup: early-close days use the actual session
            # close time (e.g. 1300 on July 3) instead of the static 1600 cutoff.
            # coalesce returns MARKET_CLOSE_HHMM for all normal sessions (no map entry).
            .withColumn(
                "_close_hhmm",
                coalesce(
                    _EARLY_CLOSE_MAP_COL[date_format(col("_et_timestamp"), "yyyy-MM-dd")],
                    lit(MARKET_CLOSE_HHMM),
                )
                if _EARLY_CLOSE_MAP_COL is not None
                else lit(MARKET_CLOSE_HHMM),
            )
            .filter((col("_hhmm") >= MARKET_OPEN_HHMM) & (col("_hhmm") < col("_close_hhmm")))
        )
        .drop("_hhmm", "_et_timestamp", "_ts_seconds", "otc", "_close_hhmm")
        .withColumnRenamed("event_time", "source_event_time")
    )


# =============================================================================
# Silver OHLCV Table — MERGE-based dedup via apply_changes
# Key: (symbol, start_timestamp). Source priority: flatfiles_s3 > kafka > unknown.
# =============================================================================

dlt.create_streaming_table(
    name="ohlcv_silver_hc",
    comment="Cleaned, deduplicated OHLCV with source-priority MERGE",
    schema="""
        symbol STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        start_timestamp BIGINT,
        end_timestamp BIGINT,
        source STRING,
        ingestion_timestamp BIGINT,
        ts_unit STRING,
        date DATE,
        source_priority INT,
        source_event_time TIMESTAMP
    """,
    partition_cols=["date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableDeletionVectors": "true",
        # Explicit stats for MERGE key lookups
        "delta.dataSkippingStatsColumns": "symbol,start_timestamp",
    },
)

dlt.apply_changes(
    target="ohlcv_silver_hc",
    source="ohlcv_silver_enriched_hc",
    keys=["symbol", "start_timestamp"],
    sequence_by="_dedup_sequence",
    except_column_list=["_dedup_sequence"],
    # Prevents a streaming record with null optional fields from overwriting a
    # historical record's non-null values on the same key.
    ignore_null_updates=True,
    stored_as_scd_type=1,
)

# =============================================================================
# WAP Quarantine Table (Rejected Records)
# =============================================================================


@dlt.table(
    name="ohlcv_silver_quarantine_hc",
    comment="WAP: records that failed validation, with rejection reason",
    schema="""
        symbol STRING,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT,
        start_timestamp BIGINT,
        end_timestamp BIGINT,
        source STRING,
        otc BOOLEAN,
        ingestion_timestamp BIGINT,
        ts_unit STRING,
        date DATE,
        rejection_reason STRING NOT NULL,
        quarantined_at TIMESTAMP NOT NULL
    """,
    # Partition on date only. rejection_reason has just ~4 distinct values, so
    # partitioning by (date, rejection_reason) fanned each day into up to 4
    # mostly-tiny files on a sparse table (<1% of Bronze is rejected). It moves to
    # zOrderCols instead — single-reason audit queries still skip files, without the
    # small-file blow-up. (date drops out of zOrderCols: it is the partition column.)
    partition_cols=["date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "symbol,rejection_reason",
    },
)
def ohlcv_silver_quarantine():
    """Quarantine invalid records with rejection reason for audit.

    Reads Bronze as a batch snapshot (dlt.read) so that row_number().over()
    is valid — non-time-based Window functions are not supported on streaming
    DataFrames and would raise AnalysisException at pipeline startup.
    The quarantine is an audit log, not a latency-sensitive output, so batch
    mode is the correct choice here.

    Scope: a rolling 30-day window over bronze_unified_hc, partition-pruned on the
    native `date` column (same bound as wap_audit_log). As a full-refresh MV this
    keeps the per-run scan constant instead of growing with total Bronze history;
    the trade-off is that quarantine reflects only the recent window, not all time.
    """
    df = dlt.read("bronze_unified_hc").filter(col("date") >= expr("current_date() - interval 30 days"))

    is_valid_price_positive = expr(WAP_VALIDATION_RULES["valid_price_positive"])
    is_valid_ohlc_logic = expr(WAP_VALIDATION_RULES["valid_ohlc_logic"])
    is_valid_volume = expr(WAP_VALIDATION_RULES["valid_volume"])
    is_all_valid = is_valid_price_positive & is_valid_ohlc_logic & is_valid_volume

    deduped_invalid = dedupe_quarantine_invalid_rows(df.filter(~is_all_valid))

    return (
        with_quarantine_rejection_reason(deduped_invalid, WAP_VALIDATION_RULES)
        # ET date so it lines up with ohlcv_silver_hc's trading-day partition.
        .withColumn("_ts_seconds", BaseConfig.timestamp_to_seconds(col("start_timestamp"), col("ts_unit")))
        .withColumn("date", to_date(from_utc_timestamp(from_unixtime(col("_ts_seconds")), MARKET_TIMEZONE)))
        .drop("_ts_seconds")
        .withColumn("quarantined_at", current_timestamp())
        .drop("event_time")
    )


# =============================================================================
# WAP Audit Log (Quality Metrics)
# =============================================================================


@dlt.table(
    name="wap_audit_log_hc",
    comment="WAP: Audit log tracking batch quality metrics for compliance",
    schema="""
        audit_date DATE,
        total_count BIGINT,
        rejected_count BIGINT,
        rejected_price_positive BIGINT,
        rejected_ohlc_logic BIGINT,
        rejected_volume BIGINT,
        valid_count BIGINT,
        rejection_rate_pct DOUBLE,
        quality_gate_passed BOOLEAN,
        quality_gate_warning BOOLEAN,
        session_bars BIGINT,
        audit_timestamp TIMESTAMP,
        pipeline_name STRING
    """,
    # Liquid clustering, not date partitioning: one row per audit_date means a
    # date partition would be a single tiny file. cluster_by keeps date skipping
    # while OPTIMIZE compacts to right-sized files (see fact tables for the pattern).
    cluster_by=["audit_date"],
    table_properties={
        "quality": "silver",
    },
)
@dlt.expect_or_fail(
    "quality_gate_pass",
    # Halt the pipeline only when today's (or yesterday's) data breaches the
    # critical rejection-rate threshold. Rows older than 2 days are exempt —
    # they are already committed history and a retroactive halt would be both
    # un-actionable and noisy. The 2-day grace window handles late-arriving
    # Bronze records that shift a prior day's rate above the threshold.
    "quality_gate_passed OR audit_date < current_date() - interval 2 days",
)
# Completeness is a WARN-only signal (never halts): flag a trading day where even the
# fullest symbol's session fell below half of EXPECTED_BARS_PER_DAY — a coarse
# market-wide-gap signal that tolerates early-close half-days (~210 bars). NULL (no
# Silver rows yet) and the 2-day grace window are exempt, mirroring the rejection-rate
# gate above.
@dlt.expect(
    "session_complete",
    f"session_bars IS NULL OR session_bars >= {_SESSION_BARS_MIN} OR audit_date < current_date() - interval 2 days",
)
def wap_audit_log():
    """Daily quality metrics: valid vs. rejected counts, rejection rate, gate status,
    plus a coarse completeness signal (session_bars: the day's fullest-symbol session).

    Scope: rejection counts use a rolling 30-day window over bronze_unified_hc
    (partition-pruned on date) to retain history; session_bars uses a narrower 3-day
    window (the heaviest scan) since its warn gate only looks back 2 days — see below.

    Rejection rate: the rejected count and the total count both come from the same
    pre-dedup Bronze source, so a Kafka replay scales both together and the rate stays
    stable. (Counting deduped quarantine rows against a raw Bronze total would mix
    grains and understate the rate after a replay.)
    """
    bronze_df = (
        dlt.read("bronze_unified_hc")
        # bronze_unified_hc already carries the ET trading `date` (set at Bronze write
        # time), so we filter on the native partition column directly — no recompute.
        # This keeps the predicate on the physical partition so the 30-day window prunes
        # partitions instead of scanning all of Bronze.
        .filter(col("date") >= expr("current_date() - interval 30 days"))
    )

    counts = aggregate_bronze_wap_counts_by_date(bronze_df, WAP_VALIDATION_RULES)

    # session_bars reads deduped, market-hours-filtered Silver (ohlcv_silver_hc) rather
    # than pre-dedup Bronze: a Kafka replay there would inflate the bar count and mask a
    # gap, and extended-hours bars would distort the per-session count. ohlcv_silver_hc
    # already carries the ET trading `date`, so it lines up with the Bronze-derived date.
    #
    # Narrower window than the 30-day Bronze counts above: session_bars only feeds the
    # session_complete warn gate, which exempts dates older than 2 days and treats NULL as
    # pass. Recomputing the full-table (date, symbol) shuffle over 30 days every run is the
    # heaviest repeated cost here, so we scope it to the 2-day grace horizon (+1 day slack).
    # Older dates fall outside this window and get NULL session_bars via the left join below
    # — gate-safe, and an honest "not recomputed" rather than a stale value.
    silver_df = (
        dlt.read("ohlcv_silver_hc")
        .select("symbol", "date")
        .filter(col("date") >= expr("current_date() - interval 3 days"))
    )
    completeness = aggregate_session_bars_by_date(silver_df)

    return (
        finalize_wap_audit_metrics(counts, WAP_THRESHOLDS)
        .join(completeness, "date", "left")
        .withColumnRenamed("date", "audit_date")
        .withColumn("audit_timestamp", current_timestamp())
        .withColumn("pipeline_name", lit("ohlcv_silver_pipeline"))
    )


# =============================================================================
# Daily OHLCV — materialized view (serverless incremental refresh / Enzyme)
# Aggregates 1-minute bars into daily bars so Gold reads ~2.9M rows instead of
# ~420M minute rows. A pure groupBy aggregation, so the serverless engine can
# refresh only the changed (symbol, date) groups; full recompute is the fallback.
# DLT runs the minute MERGE (ohlcv_silver_hc) before this MV refreshes in the same
# pipeline update (Layer 3 orchestration).
# =============================================================================


@dlt.table(
    name="ohlcv_daily_silver_hc",
    comment="Daily OHLCV (symbol, date) aggregated from 1-minute Silver; serverless incremental MV",
    schema="""
        symbol STRING,
        date DATE,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume BIGINT
    """,
    cluster_by=["date", "symbol"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "4",
    },
)
def ohlcv_daily_silver():
    """Daily OHLCV materialized view aggregated from deduplicated minute Silver.

    Reads the full ``ohlcv_silver_hc`` table (no ``current_date()`` predicate) and
    groups to one row per (symbol, date) via ``aggregate_minute_to_daily``. Keeping the
    query a deterministic, time-independent groupBy lets the serverless engine refresh
    it incrementally — only (symbol, date) groups with changed minute rows are
    recomputed, falling back to a full recompute when that is cheaper or required.

    Grain: one row per (symbol, date). Idempotency: the output is fully determined by
    the current ``ohlcv_silver_hc`` snapshot, so any refresh (incremental or full)
    yields identical daily rows — no duplicate analytic rows. Gold applies its own
    lookback window when reading this table.
    """
    return aggregate_minute_to_daily(dlt.read("ohlcv_silver_hc"))
