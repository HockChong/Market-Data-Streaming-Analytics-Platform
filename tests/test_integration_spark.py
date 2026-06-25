"""
Integration tests that run against a real SparkSession.

Covers core capstone pipeline contracts:
  1. Minute-to-daily OHLCV aggregation (Silver→Gold transform)
  2. Dedup sequence tiebreaking (source priority beats ingestion time)
  3. WAP quarantine rejection-reason precedence
  4. WAP data-quality gate thresholds (warning / critical zones)
  5. Bronze incremental dedup idempotency

Skip with: pytest -m "not spark_integration"
"""

import sys
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

pytestmark = pytest.mark.integration

from spark_test_support import build_local_spark_session, ensure_real_pyspark, stop_local_spark_session

ensure_real_pyspark()
try:
    import pyspark  # noqa: F401
except ImportError:
    pytest.skip("pyspark required for Spark integration tests", allow_module_level=True)

from pyspark.sql.functions import col, from_unixtime, lit, row_number, to_timestamp
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _subdir in ("databricks/config", "databricks/utils", "databricks/bronze"):
    _p = str(_PROJECT_ROOT / _subdir)
    if _p not in sys.path:
        sys.path.insert(0, _p)

for _m in ("aggregation_utils", "ohlcv_dedup_spark", "ohlcv_quarantine_spark", "wap_audit_spark", "bronze_utils"):
    sys.modules.pop(_m, None)

from aggregation_utils import aggregate_minute_to_daily  # noqa: E402
from bronze_utils import dedupe_incremental_bronze_keys  # noqa: E402
from ohlcv_dedup_spark import silver_ohlcv_dedup_sequence_struct  # noqa: E402
from ohlcv_quarantine_spark import with_quarantine_rejection_reason  # noqa: E402
from silver_config import SilverConfig  # noqa: E402
from wap_audit_spark import finalize_wap_audit_metrics  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = build_local_spark_session("integration_tests")
    yield session
    stop_local_spark_session(session)


@pytest.fixture(scope="module")
def sample_minute_df(spark):
    import datetime

    data = []
    base_date = datetime.date(2024, 1, 2)
    for i in range(5):
        ts = int(datetime.datetime(2024, 1, 2, 9, 30 + i, tzinfo=datetime.timezone.utc).timestamp() * 1000)
        data.append(("AAPL", base_date, ts, 150.0 + i, 152.0 + i, 149.0 + i, 151.0 + i, 1_000_000 + i * 100))
        data.append(("MSFT", base_date, ts, 350.0 + i, 352.0 + i, 349.0 + i, 351.0 + i, 2_000_000 + i * 200))

    schema = StructType(
        [
            StructField("symbol", StringType(), False),
            StructField("date", DateType(), False),
            StructField("start_timestamp", LongType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
        ]
    )
    return spark.createDataFrame(data, schema=schema)


# =============================================================================
# 1. Minute → daily OHLCV aggregation (Silver → Gold)
# =============================================================================


class TestAggregateMinuteToDaily:
    """Validates the core Silver→Gold rollup: first open, last close, max high, min low, summed volume."""

    @pytest.mark.spark_integration
    def test_daily_rollup_aggregates_each_field(self, spark, sample_minute_df):
        daily = aggregate_minute_to_daily(sample_minute_df)
        assert daily.count() == 2  # one row per symbol

        aapl = daily.filter(col("symbol") == "AAPL").collect()[0]
        assert aapl["open"] == 150.0  # first minute open
        assert aapl["close"] == 155.0  # last minute close (151 + 4)
        assert aapl["high"] == 156.0  # max high (152..156)
        assert aapl["low"] == 149.0  # min low (149..153)
        assert aapl["volume"] == sum(1_000_000 + i * 100 for i in range(5))  # summed volume


# =============================================================================
# 2. _dedup_sequence tiebreaking (same logic as ohlcv_silver_dlt apply_changes)
#
# Simulates MAX(_dedup_sequence) semantics with a window function so these
# tests run without a live DLT pipeline.
# =============================================================================

_DEDUP_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("start_timestamp", LongType(), False),
        StructField("end_timestamp", LongType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), False),
        StructField("source_priority", LongType(), False),
        StructField("ingestion_ts_ms", LongType(), False),
        StructField("label", StringType(), False),
    ]
)

_BASE_OHLCV = dict(
    symbol="AAPL",
    start_timestamp=1_700_000_000_000,
    end_timestamp=1_700_000_060_000,
    open=150.0,
    high=151.0,
    low=149.0,
    close=150.5,
    volume=1_000_000,
)


def _dedup_row(label, source, source_priority, ingestion_ts_ms, **overrides):
    r = {
        **_BASE_OHLCV,
        "label": label,
        "source": source,
        "source_priority": source_priority,
        "ingestion_ts_ms": ingestion_ts_ms,
        **overrides,
    }
    return (
        r["symbol"],
        r["start_timestamp"],
        r["end_timestamp"],
        r["open"],
        r["high"],
        r["low"],
        r["close"],
        r["volume"],
        r["source"],
        r["source_priority"],
        r["ingestion_ts_ms"],
        r["label"],
    )


def _apply_dedup(spark, rows):
    df = spark.createDataFrame(rows, schema=_DEDUP_SCHEMA)
    df = df.withColumn(
        "ingestion_timestamp", to_timestamp(from_unixtime(col("ingestion_ts_ms") / lit(1000)))
    ).withColumn("_dedup_sequence", silver_ohlcv_dedup_sequence_struct())
    win = Window.partitionBy("symbol", "start_timestamp").orderBy(col("_dedup_sequence").desc())
    return df.withColumn("_rn", row_number().over(win)).filter(col("_rn") == 1).drop("_rn", "_dedup_sequence")


class TestDedupSequenceTiebreakSpark:
    """Validates dedup winner semantics: source priority → ingestion time → payload hash."""

    @pytest.mark.spark_integration
    def test_historical_beats_streaming_regardless_of_ingestion_time(self, spark):
        """source_priority is the leading struct field: historical (2) beats streaming (1)
        even when the streaming row arrived more recently."""
        rows = [
            _dedup_row(
                "streaming_later",
                "polygon_kafka_delayed_streaming",
                source_priority=1,
                ingestion_ts_ms=2_000_000_000_000,
            ),
            _dedup_row(
                "historical_earlier", "polygon_flatfiles_s3", source_priority=2, ingestion_ts_ms=1_700_000_000_000
            ),
        ]
        result = _apply_dedup(spark, rows).collect()
        assert len(result) == 1
        assert result[0]["label"] == "historical_earlier"


# =============================================================================
# 3. WAP quarantine (invalid-row dedup + rejection_reason precedence)
# =============================================================================

_QUARANTINE_ROW_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("start_timestamp", LongType(), False),
        StructField("end_timestamp", LongType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("source", StringType(), False),
        StructField("label", StringType(), False),
    ]
)


def _quarantine_row(label, source, **overrides):
    r = {**_BASE_OHLCV, "source": source, "label": label, **overrides}
    return (
        r["symbol"],
        r["start_timestamp"],
        r["end_timestamp"],
        r["open"],
        r["high"],
        r["low"],
        r["close"],
        r["volume"],
        r["source"],
        r["label"],
    )


class TestQuarantineRejectionReasonSpark:
    """Rejection reason precedence matches ohlcv_silver_quarantine: price → OHLC → volume."""

    def _reason(self, spark, **overrides):
        rules = SilverConfig().get_wap_validation_rules()
        row = _quarantine_row("row", "polygon_kafka_delayed_streaming", **overrides)
        df = spark.createDataFrame([row], schema=_QUARANTINE_ROW_SCHEMA)
        return with_quarantine_rejection_reason(df, rules).collect()[0]["rejection_reason"]

    @pytest.mark.spark_integration
    @pytest.mark.parametrize(
        ("overrides", "expected"),
        [
            # Non-positive price.
            ({"close": 0.0}, "invalid_price_positive"),
            # OHLC logic: close above high (prices stay positive so the price gate passes).
            ({"close": 200.0}, "invalid_ohlc_logic"),
            # Negative volume.
            ({"volume": -1}, "invalid_volume"),
            # Precedence: when several rules fail, price wins over OHLC and volume.
            ({"close": 0.0, "high": 90.0, "low": 95.0, "volume": -1}, "invalid_price_positive"),
        ],
        ids=["price", "ohlc_logic", "volume", "price_wins_precedence"],
    )
    def test_rejection_reason_precedence(self, spark, overrides, expected):
        assert self._reason(spark, **overrides) == expected


# =============================================================================
# 4. WAP data-quality gate: rejection-rate threshold zones
# =============================================================================

_GATE_COUNTS_SCHEMA = StructType(
    [
        StructField("date", DateType(), False),
        StructField("total_count", LongType(), False),
        StructField("rejected_count", LongType(), False),
    ]
)


class TestWapGateThresholds:
    """Three WAP zones: healthy (<0.5%), warning (0.5–1.0%), critical (≥1.0%).

    Uses 1 000 total rows so each rejected row = 0.1%, giving clean zone boundaries.
    """

    _TOTAL = 1_000

    def _run(self, spark, rejected: int):
        import datetime

        df = spark.createDataFrame(
            [(datetime.date(2024, 1, 2), self._TOTAL, rejected)],
            schema=_GATE_COUNTS_SCHEMA,
        )
        return finalize_wap_audit_metrics(df, SilverConfig().get_wap_config()).collect()[0]

    @pytest.mark.spark_integration
    @pytest.mark.parametrize(
        "rejected,expected_passed,expected_warning,label",
        [
            (3, True, False, "below_warning_0.3pct"),
            (6, True, True, "warning_band_0.6pct"),
            (12, False, True, "critical_1.2pct"),
        ],
    )
    def test_gate_flags_at_each_zone(self, spark, rejected, expected_passed, expected_warning, label):
        row = self._run(spark, rejected)
        rate = row["rejection_rate_pct"]
        assert row["quality_gate_passed"] is expected_passed, (
            f"{label}: rate={rate:.2f}% — expected passed={expected_passed}"
        )
        assert row["quality_gate_warning"] is expected_warning, (
            f"{label}: rate={rate:.2f}% — expected warning={expected_warning}"
        )


# =============================================================================
# 5. Bronze incremental dedup idempotency
# =============================================================================


class TestIncrementalBronzeDedupPolicy:
    """Dedup applied twice must produce the same result as applying it once."""

    @pytest.mark.spark_integration
    def test_dedup_is_idempotent(self, spark):
        import datetime

        schema = StructType(
            [
                StructField("symbol", StringType(), False),
                StructField("start_timestamp", LongType(), False),
                StructField("ingestion_timestamp", TimestampType(), True),
                StructField("processing_timestamp", TimestampType(), True),
                StructField("correlation_id", StringType(), True),
                StructField("source", StringType(), True),
                StructField("close", DoubleType(), True),
            ]
        )
        rows = [
            (
                "AAPL",
                1_704_204_600_000,
                datetime.datetime(2024, 1, 2, 9, 32, 0),
                datetime.datetime(2024, 1, 2, 9, 32, 30),
                "run-002",
                "polygon_flatfiles_s3",
                101.0,
            ),
            (
                "AAPL",
                1_704_204_600_000,
                datetime.datetime(2024, 1, 2, 9, 32, 0),
                datetime.datetime(2024, 1, 2, 9, 32, 30),
                "run-010",
                "polygon_flatfiles_s3",
                102.0,
            ),
        ]
        df = spark.createDataFrame(rows, schema=schema)
        once = dedupe_incremental_bronze_keys(df).orderBy("symbol", "start_timestamp")
        twice = dedupe_incremental_bronze_keys(once).orderBy("symbol", "start_timestamp")
        assert once.collect() == twice.collect()
