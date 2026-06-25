"""
Pure-Python transforms for the streaming layer.

Intentionally free of Spark, Kafka, and Databricks dependencies so these
functions can be imported by Databricks notebooks AND tested directly in pytest.

Used by:
    - databricks/bronze/streaming_producer.py  (transform_polygon_to_avro)
    - databricks/bronze/streaming_ingestion.py (is_stream_caught_up)
"""

import time
from typing import Any


def transform_polygon_to_avro(polygon_data: dict[str, Any]) -> dict[str, Any]:
    """Transform a Polygon WebSocket message into an Avro-compatible dict.

    Maps Polygon's compact field names (ev, sym, o, h, l, c, …) to the full
    Avro schema field names defined in schemas/avro/ohlcv_aggregate.avsc.

    Required numeric fields (open, high, low, close, volume, start_timestamp,
    end_timestamp) default to 0 when missing; they are validated and filtered
    by the Silver DLT WAP expectations.

    Optional fields (accumulated_volume / av, official_open / op,
    average_trade_size / z) use a truthiness check, so a source value of
    exactly 0 is mapped to None.  This is intentional: Polygon omits these
    fields when unavailable, and 0 is not a meaningful value for any of them.
    The Avro schema defines them as nullable unions with default null.
    """
    return {
        "event_type": polygon_data.get("ev", "AM"),
        "symbol": polygon_data.get("sym", ""),
        "open": float(polygon_data.get("o", 0)),
        "high": float(polygon_data.get("h", 0)),
        "low": float(polygon_data.get("l", 0)),
        "close": float(polygon_data.get("c", 0)),
        "volume": int(polygon_data.get("v", 0)),
        "start_timestamp": int(polygon_data.get("s", 0)),
        "end_timestamp": int(polygon_data.get("e", 0)),
        "accumulated_volume": int(polygon_data.get("av")) if polygon_data.get("av") else None,
        "official_open": float(polygon_data.get("op")) if polygon_data.get("op") else None,
        "average_trade_size": float(polygon_data.get("z")) if polygon_data.get("z") else None,
        "otc": polygon_data.get("otc"),
        "ingestion_timestamp": int(time.time() * 1000),
    }


def is_stream_caught_up(progress) -> bool:
    """Check if a Structured Streaming query has processed all available data.

    Args:
        progress: The lastProgress dict from a StreamingQuery, or None.

    Returns:
        True if no more input rows in last batch (backlog cleared).
    """
    if not progress:
        return False
    return progress.get("numInputRows", 0) == 0
