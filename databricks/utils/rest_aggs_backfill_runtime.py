"""Runtime helpers for Polygon REST aggregates -> Bronze streaming gap backfill."""

from __future__ import annotations

REST_BACKFILL_SOURCE = "polygon_rest_backfill"

ONE_MINUTE_MS = 60_000


def parse_ticker_csv(raw: str) -> list[str]:
    """Parse a comma-separated ticker parameter into an ordered, deduped, uppercase list."""
    tickers: list[str] = []
    for token in (raw or "").split(","):
        symbol = token.strip().upper()
        if symbol and symbol not in tickers:
            tickers.append(symbol)
    if not tickers:
        raise ValueError("tickers parameter is empty — provide a comma-separated list, e.g. 'AAPL,MSFT'")
    return tickers


def market_window_to_epoch_ms(date_str: str, start_hhmm: str, end_hhmm: str, tz_name: str) -> tuple[int, int]:
    """Convert a market-time window on a session date to ``[start_ms, end_ms)`` epoch bounds.

    ``end_hhmm`` is exclusive: 09:30–10:00 covers the minute bars starting
    09:30 through 09:59. DST is handled by localizing in ``tz_name``.
    Raises ValueError on bad formats or an empty window.
    """
    from datetime import datetime

    import pytz

    day = datetime.strptime(date_str, "%Y-%m-%d")
    try:
        start_t = datetime.strptime(start_hhmm, "%H:%M").time()
        end_t = datetime.strptime(end_hhmm, "%H:%M").time()
    except ValueError as exc:
        raise ValueError(f"start/end time must be HH:MM 24-hour format, got {start_hhmm!r} / {end_hhmm!r}") from exc
    if start_t >= end_t:
        raise ValueError(f"start_time {start_hhmm} must be before end_time {end_hhmm}")

    tz = pytz.timezone(tz_name)
    start_ms = int(tz.localize(datetime.combine(day, start_t)).timestamp() * 1000)
    end_ms = int(tz.localize(datetime.combine(day, end_t)).timestamp() * 1000)
    return start_ms, end_ms


def rest_agg_to_bronze_row(agg, symbol: str, ingestion_epoch_seconds: int) -> dict:
    """Map one Polygon REST minute aggregate to the Bronze streaming business columns.

    ``ingestion_timestamp`` is epoch seconds to match the streaming Bronze table,
    where Avro timestamp-millis decodes to TIMESTAMP and ``.cast("long")`` yields
    epoch seconds (see ``parse_kafka_avro_stream`` in streaming_ingestion_runtime).
    """
    start_ms = int(agg.timestamp)
    transactions = getattr(agg, "transactions", None)
    otc = getattr(agg, "otc", None)
    return {
        # Record kind, not provenance: REST bars are the same 1-minute aggregates
        # as WebSocket AM events. Provenance is carried by the `source` column.
        "event_type": "AM",
        "symbol": symbol,
        "open": float(agg.open),
        "high": float(agg.high),
        "low": float(agg.low),
        "close": float(agg.close),
        "volume": int(agg.volume),
        "start_timestamp": start_ms,
        "end_timestamp": start_ms + ONE_MINUTE_MS,
        "accumulated_volume": None,
        "official_open": None,
        "average_trade_size": None,
        "otc": bool(otc) if otc is not None else None,
        "transactions": int(transactions) if transactions is not None else None,
        "ingestion_timestamp": int(ingestion_epoch_seconds),
    }


def rest_backfill_rows_schema():
    """Explicit schema for rows produced by ``rest_agg_to_bronze_row``."""
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("event_type", StringType(), False),
            StructField("symbol", StringType(), False),
            StructField("open", DoubleType(), True),
            StructField("high", DoubleType(), True),
            StructField("low", DoubleType(), True),
            StructField("close", DoubleType(), True),
            StructField("volume", LongType(), True),
            StructField("start_timestamp", LongType(), False),
            StructField("end_timestamp", LongType(), True),
            StructField("accumulated_volume", LongType(), True),
            StructField("official_open", DoubleType(), True),
            StructField("average_trade_size", DoubleType(), True),
            StructField("otc", BooleanType(), True),
            StructField("transactions", LongType(), True),
            StructField("ingestion_timestamp", LongType(), False),
        ]
    )


def align_to_bronze_schema(df, target_schema):
    """Project ``df`` onto the Bronze streaming table schema before an append.

    Columns missing from ``df`` (e.g. Kafka metadata like topic/partition/offset)
    become typed NULLs; column order and types follow ``target_schema``. Raises if
    ``df`` carries a column the target table does not have, so a backfill can never
    silently evolve the Bronze schema.
    """
    from pyspark.sql.functions import col, lit

    target_names = {field.name for field in target_schema.fields}
    extra = [c for c in df.columns if c not in target_names]
    if extra:
        raise ValueError(f"Columns not in Bronze streaming schema: {extra}")

    projected = [
        col(field.name).cast(field.dataType).alias(field.name)
        if field.name in df.columns
        else lit(None).cast(field.dataType).alias(field.name)
        for field in target_schema.fields
    ]
    return df.select(*projected)
