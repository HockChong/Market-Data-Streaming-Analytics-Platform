"""
Unit tests for streaming_transforms.py.

Pure Python — no Spark, no Databricks, no Kafka.
"""

from __future__ import annotations

import time

import pytest
from streaming_transforms import is_stream_caught_up, transform_polygon_to_avro

pytestmark = pytest.mark.unit


class TestTransformPolygonToAvro:
    def _full_msg(self):
        return {
            "ev": "AM",
            "sym": "AAPL",
            "o": 150.0,
            "h": 155.0,
            "l": 149.0,
            "c": 153.0,
            "v": 1000000,
            "s": 1700000000000,
            "e": 1700000060000,
            "av": 5000000,
            "op": 150.5,
            "z": 200,
            "otc": None,
        }

    def test_full_message_maps_all_fields(self):
        result = transform_polygon_to_avro(self._full_msg())
        assert result["event_type"] == "AM"
        assert result["symbol"] == "AAPL"
        assert result["open"] == 150.0
        assert result["high"] == 155.0
        assert result["low"] == 149.0
        assert result["close"] == 153.0
        assert result["volume"] == 1000000
        assert result["start_timestamp"] == 1700000000000
        assert result["end_timestamp"] == 1700000060000
        assert result["accumulated_volume"] == 5000000
        assert result["official_open"] == 150.5
        assert result["average_trade_size"] == 200.0
        assert result["otc"] is None

    @pytest.mark.parametrize(
        "mutate",
        [
            # Optional field absent from the message → None.
            lambda m: {k: v for k, v in m.items() if k not in ("av", "op", "z")},
            # Optional field present but zero (Polygon's "no data" sentinel) → None.
            lambda m: {**m, "av": 0, "op": 0, "z": 0},
        ],
        ids=["absent", "zero_sentinel"],
    )
    def test_optional_fields_become_none(self, mutate):
        result = transform_polygon_to_avro(mutate(self._full_msg()))
        assert result["accumulated_volume"] is None
        assert result["official_open"] is None
        assert result["average_trade_size"] is None

    def test_ingestion_timestamp_is_epoch_millis(self):
        """ingestion_timestamp is the Silver dedup tiebreaker — it must be an int in
        epoch-millis range, not seconds or a float, or the wrong row wins silently."""
        before = int(time.time() * 1000)
        result = transform_polygon_to_avro(self._full_msg())
        after = int(time.time() * 1000)
        ts = result["ingestion_timestamp"]
        assert isinstance(ts, int)
        assert before <= ts <= after


class TestIsStreamCaughtUp:
    def test_none_progress_returns_false(self):
        assert is_stream_caught_up(None) is False

    def test_zero_input_rows_returns_true(self):
        assert is_stream_caught_up({"numInputRows": 0}) is True

    def test_nonzero_input_rows_returns_false(self):
        assert is_stream_caught_up({"numInputRows": 42}) is False
