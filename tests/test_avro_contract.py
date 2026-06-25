"""
Contract tests: schemas/avro/ohlcv_aggregate.avsc ↔ streaming producer output.

Pure Python — no Spark. ``transactions`` is flatfile-only per the Avro schema doc;
the WebSocket producer does not populate it.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for subdir in ("databricks/config", "databricks/utils"):
    path = str(_PROJECT_ROOT / subdir)
    if path not in sys.path:
        sys.path.insert(0, path)

from streaming_transforms import transform_polygon_to_avro  # noqa: E402

pytestmark = [pytest.mark.unit, pytest.mark.contract]

_AVSC_PATH = _PROJECT_ROOT / "schemas" / "avro" / "ohlcv_aggregate.avsc"

# Flat-file sources may set ``transactions``; Polygon AM WebSocket events do not.
_STREAMING_OMIT_FROM_PRODUCER = frozenset({"transactions"})

_NULLABLE_OPTIONAL_FIELDS = frozenset(
    {
        "accumulated_volume",
        "official_open",
        "average_trade_size",
        "otc",
        "transactions",
    }
)


def _load_avro_fields_by_name() -> dict[str, dict]:
    with _AVSC_PATH.open(encoding="utf-8") as f:
        schema = json.load(f)
    return {field["name"]: field for field in schema["fields"]}


# Parsed once at import — the schema file does not change during a test run.
_AVRO_FIELDS_BY_NAME = _load_avro_fields_by_name()


class TestOhlcvAvroContract:
    def _full_polygon_msg(self) -> dict:
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

    def test_producer_keys_match_avro_streaming_subset(self):
        producer_keys = set(transform_polygon_to_avro(self._full_polygon_msg()).keys())
        expected = set(_AVRO_FIELDS_BY_NAME) - _STREAMING_OMIT_FROM_PRODUCER
        assert producer_keys == expected

    @pytest.mark.parametrize("field_name", sorted(_NULLABLE_OPTIONAL_FIELDS))
    def test_optional_avro_fields_are_nullable_with_default_null(self, field_name):
        field = _AVRO_FIELDS_BY_NAME[field_name]
        assert isinstance(field["type"], list)
        assert "null" in field["type"]
        # `in` + explicit None, not .get() — a missing "default" key must fail,
        # since Avro schema-evolution readers require the default to be present.
        assert "default" in field
        assert field["default"] is None
