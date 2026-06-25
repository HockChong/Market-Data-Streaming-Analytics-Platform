"""Tests for rest_aggs_backfill_runtime.py.

Pure unit tests for ticker parsing and REST→Bronze row mapping, plus Spark
integration tests for the Bronze-schema alignment used by the backfill append.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from rest_aggs_backfill_runtime import (
    ONE_MINUTE_MS,
    REST_BACKFILL_SOURCE,
    align_to_bronze_schema,
    market_window_to_epoch_ms,
    parse_ticker_csv,
    rest_agg_to_bronze_row,
    rest_backfill_rows_schema,
)

_TESTS_DIR = Path(__file__).resolve().parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))


def _agg(**overrides):
    """Fake massive SDK minute Agg with the attributes the mapper reads."""
    base = {
        "open": 150.0,
        "high": 155.0,
        "low": 149.0,
        "close": 153.0,
        "volume": 1_000_000,
        "timestamp": 1_700_000_000_000,
        "transactions": 1200,
        "otc": None,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.unit
class TestParseTickerCsv:
    def test_parses_dedupes_and_uppercases(self):
        assert parse_ticker_csv(" aapl, MSFT ,aapl,nvda ") == ["AAPL", "MSFT", "NVDA"]

    def test_single_ticker(self):
        assert parse_ticker_csv("TSLA") == ["TSLA"]

    @pytest.mark.parametrize("raw", ["", "  , ,", None])
    def test_empty_raises(self, raw):
        with pytest.raises(ValueError, match="tickers parameter is empty"):
            parse_ticker_csv(raw)


@pytest.mark.unit
class TestMarketWindowToEpochMs:
    def test_winter_session_open_window_est(self):
        # 2026-01-15 is EST (UTC-5): 09:30 ET = 14:30 UTC
        from datetime import datetime, timezone

        start_ms, end_ms = market_window_to_epoch_ms("2026-01-15", "09:30", "10:00", "America/New_York")
        assert start_ms == int(datetime(2026, 1, 15, 14, 30, tzinfo=timezone.utc).timestamp() * 1000)
        assert end_ms - start_ms == 30 * ONE_MINUTE_MS

    def test_summer_session_open_window_edt(self):
        # 2026-06-15 is EDT (UTC-4): 09:30 ET = 13:30 UTC
        from datetime import datetime, timezone

        start_ms, _ = market_window_to_epoch_ms("2026-06-15", "09:30", "10:00", "America/New_York")
        assert start_ms == int(datetime(2026, 6, 15, 13, 30, tzinfo=timezone.utc).timestamp() * 1000)

    def test_bad_time_format_raises(self):
        with pytest.raises(ValueError, match="HH:MM"):
            market_window_to_epoch_ms("2026-01-15", "9.30am", "10:00", "America/New_York")

    def test_start_not_before_end_raises(self):
        with pytest.raises(ValueError, match="must be before"):
            market_window_to_epoch_ms("2026-01-15", "10:00", "10:00", "America/New_York")


@pytest.mark.unit
class TestRestAggToBronzeRow:
    def test_maps_all_fields(self):
        row = rest_agg_to_bronze_row(_agg(), "AAPL", ingestion_epoch_seconds=1_700_000_400)
        assert row["event_type"] == "AM"
        assert row["symbol"] == "AAPL"
        assert row["open"] == 150.0
        assert row["high"] == 155.0
        assert row["low"] == 149.0
        assert row["close"] == 153.0
        assert row["volume"] == 1_000_000
        assert row["start_timestamp"] == 1_700_000_000_000
        assert row["end_timestamp"] == 1_700_000_000_000 + ONE_MINUTE_MS
        assert row["transactions"] == 1200
        assert row["ingestion_timestamp"] == 1_700_000_400

    def test_websocket_only_fields_are_null(self):
        row = rest_agg_to_bronze_row(_agg(), "AAPL", 1)
        assert row["accumulated_volume"] is None
        assert row["official_open"] is None
        assert row["average_trade_size"] is None
        assert row["otc"] is None

    def test_float_volume_cast_to_int(self):
        row = rest_agg_to_bronze_row(_agg(volume=42.0), "AAPL", 1)
        assert row["volume"] == 42
        assert isinstance(row["volume"], int)

    def test_missing_optional_attrs_become_null(self):
        bare = SimpleNamespace(open=1.0, high=1.0, low=1.0, close=1.0, volume=10, timestamp=1_700_000_000_000)
        row = rest_agg_to_bronze_row(bare, "TEST", 1)
        assert row["transactions"] is None
        assert row["otc"] is None

    def test_otc_true_preserved(self):
        row = rest_agg_to_bronze_row(_agg(otc=True), "OTCX", 1)
        assert row["otc"] is True

    def test_deterministic_for_same_inputs(self):
        assert rest_agg_to_bronze_row(_agg(), "AAPL", 1_700_000_400) == rest_agg_to_bronze_row(
            _agg(), "AAPL", 1_700_000_400
        )

    def test_source_constant_value(self):
        assert REST_BACKFILL_SOURCE == "polygon_rest_backfill"


@pytest.mark.integration
class TestAlignToBronzeSchema:
    @pytest.fixture(scope="class")
    def spark(self):
        from spark_test_support import build_local_spark_session, stop_local_spark_session

        session = build_local_spark_session("rest_aggs_backfill_tests")
        yield session
        stop_local_spark_session(session)

    def _target_schema(self):
        """Bronze streaming table shape: business columns + Kafka metadata + Bronze metadata."""
        from pyspark.sql.types import (
            DateType,
            IntegerType,
            LongType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        return StructType(
            rest_backfill_rows_schema().fields
            + [
                StructField("kafka_ingestion_timestamp", TimestampType(), True),
                StructField("topic", StringType(), True),
                StructField("partition", IntegerType(), True),
                StructField("offset", LongType(), True),
                StructField("processing_timestamp", TimestampType(), True),
                StructField("correlation_id", StringType(), True),
                StructField("source", StringType(), True),
                StructField("ts_unit", StringType(), True),
                StructField("date", DateType(), True),
            ]
        )

    def _rest_df(self, spark):
        rows = [rest_agg_to_bronze_row(_agg(), "AAPL", 1_700_000_400)]
        return spark.createDataFrame(rows, schema=rest_backfill_rows_schema())

    def test_missing_columns_become_typed_nulls(self, spark):
        target = self._target_schema()
        aligned = align_to_bronze_schema(self._rest_df(spark), target)

        assert aligned.columns == [field.name for field in target.fields]
        assert [field.dataType for field in aligned.schema.fields] == [field.dataType for field in target.fields]

        out = aligned.collect()[0]
        assert out["symbol"] == "AAPL"
        assert out["start_timestamp"] == 1_700_000_000_000
        for kafka_col in ("kafka_ingestion_timestamp", "topic", "partition", "offset"):
            assert out[kafka_col] is None

    def test_extra_column_raises(self, spark):
        from pyspark.sql.functions import lit

        df = self._rest_df(spark).withColumn("not_in_bronze", lit(1))
        with pytest.raises(ValueError, match="not_in_bronze"):
            align_to_bronze_schema(df, self._target_schema())
