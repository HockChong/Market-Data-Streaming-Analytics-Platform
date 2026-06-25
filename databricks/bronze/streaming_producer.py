# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer: Polygon WebSocket → Kafka Producer
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Connects to Polygon.io WebSocket (delayed feed) and publishes 1-minute OHLCV bars to Kafka.
# MAGIC This is the data source for the streaming ingestion pipeline.
# MAGIC
# MAGIC ## Data Flow
# MAGIC ```
# MAGIC Polygon WebSocket → This Producer → Kafka (Avro) → streaming_ingestion.py → Bronze Delta
# MAGIC     (delayed)           ↑                                    ↓
# MAGIC                   Avro serialization              Silver DLT Pipeline
# MAGIC ```
# MAGIC
# MAGIC ## When to Run
# MAGIC - **Market Hours Only**: Auto-starts at 9:30 AM ET; auto-stops at session close + 20 min (e.g. 4:20 PM ET) so delayed-feed bars are fully produced.
# MAGIC - **Pre-flight Check**: Exits immediately if market is closed (saves compute).
# MAGIC - Run this BEFORE `streaming_ingestion.py` (or run both together).
# MAGIC
# MAGIC ## Performance
# MAGIC | Metric | Value |
# MAGIC |--------|-------|
# MAGIC | Steady-state | ~8.3 msg/sec |
# MAGIC | Burst capacity | ~25 msg/sec |
# MAGIC | Kafka batch size | 16KB |
# MAGIC | Compression | Snappy |
# MAGIC
# MAGIC ## Key Components
# MAGIC - **Schema Registry**: Avro schema validation before publishing
# MAGIC - **Kafka Topic**: Configured via `BronzeConfig.kafka_topic_streaming`
# MAGIC - **Subscription**: `AM.*` (all tickers, 1-minute bars)

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

# COMMAND ----------
# DBTITLE 1,Install dependencies
# MAGIC %pip install massive confluent-kafka[avro]==2.3.0
# COMMAND ----------
import logging
import time
from datetime import datetime, timezone
from typing import Any

from bronze_config import BaseConfig, BronzeConfig
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from massive import WebSocketClient
from massive.websocket.models import Feed, Market, WebSocketMessage
from simple_logger import SimpleLogger

# Setup logging — use SimpleLogger for consistency with other Bronze notebooks,
# plus standard logging for library-level messages (confluent_kafka, py4j)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("py4j").setLevel(logging.WARNING)
logger = SimpleLogger("streaming_producer", dbutils)
logger.log_start()

job_start_time = datetime.now(timezone.utc)

# COMMAND ----------

# DBTITLE 1,Market Hours Configuration (from BaseConfig)
# All market-hours logic lives in BaseConfig so producer and consumer stay in sync.
# - is_market_hours: used to decide "should we start?" (pre-flight).
# - should_stop_streaming: used in the loop to decide "should we stop?" (includes 20 min delay).
MARKET_OPEN = BaseConfig.get_market_open_time()
MARKET_CLOSE = BaseConfig.get_market_close_time()
MARKET_TIMEZONE = BaseConfig.get_market_timezone()

is_market_hours = BaseConfig.is_market_hours
should_stop_streaming = BaseConfig.should_stop_streaming
get_market_status = BaseConfig.get_market_status

print(f"\n{'=' * 60}")
print(f"Market Hours: {MARKET_OPEN.strftime('%H:%M')} - {MARKET_CLOSE.strftime('%H:%M')} ET (Mon-Fri)")
print(f"Shutdown: session close + {BaseConfig.SHUTDOWN_DELAY_MINUTES} min (for delayed feed)")
print(f"Current Status: {get_market_status()}")
print(f"{'=' * 60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Secrets

# COMMAND ----------

try:
    config = BronzeConfig(dbutils)
    config.validate_streaming()

    polygon_api_key = config.polygon_api_key
    kafka_bootstrap_servers = config.kafka_bootstrap_servers
    kafka_sasl_username = config.kafka_sasl_username
    kafka_sasl_password = config.kafka_sasl_password
    kafka_topic = config.kafka_topic_streaming
    schema_registry_url = config.schema_registry_url
    schema_registry_api_key = config.schema_registry_api_key
    schema_registry_api_secret = config.schema_registry_api_secret

    print(f"Secrets loaded via BronzeConfig | Topic: {kafka_topic}")
except Exception as e:
    logger.log_error(f"Failed to load secrets via BronzeConfig: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialize Schema Registry and Kafka Producer

# COMMAND ----------

avro_schema_json = config.get_avro_schema()

schema_registry_client = SchemaRegistryClient(
    {"url": schema_registry_url, "basic.auth.user.info": f"{schema_registry_api_key}:{schema_registry_api_secret}"}
)

avro_serializer = AvroSerializer(schema_registry_client, avro_schema_json, to_dict=lambda obj, ctx: obj)

producer = Producer(
    {
        "bootstrap.servers": kafka_bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": kafka_sasl_username,
        "sasl.password": kafka_sasl_password,
        "client.id": f"polygon-producer-{logger.correlation_id}",
        "acks": "all",
        "compression.type": "snappy",
        "linger.ms": 100,
        "batch.size": 16384,
        # Retry configuration for transient Kafka failures
        "retries": 5,
        "retry.backoff.ms": 500,
        "delivery.timeout.ms": 120000,
        "enable.idempotence": True,
    }
)

print("Schema Registry and Kafka Producer initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Producer Statistics and Message Handling (Simplified)

# COMMAND ----------

messages_sent = 0
messages_failed = 0
start_time = time.time()
last_log_time = time.time()


def delivery_report(err, msg):
    """Kafka delivery callback with simplified stats tracking."""
    global messages_sent, messages_failed, last_log_time

    msg_key = msg.key().decode("utf-8") if msg.key() else "unknown"

    if err:
        messages_failed += 1
        logger.log_error(f"Delivery failed for {msg_key}: {err}")
    else:
        messages_sent += 1

        now = time.time()
        if messages_sent % 100 == 0 or (now - last_log_time) > 30:
            elapsed = now - start_time
            rate = messages_sent / elapsed if elapsed > 0 else 0
            total = messages_sent + messages_failed
            success_rate = (messages_sent / total * 100) if total > 0 else 0
            print(
                f"Sent: {messages_sent} | Failed: {messages_failed} | "
                f"Rate: {rate:.1f}/sec | Success: {success_rate:.1f}%"
            )
            last_log_time = now


from streaming_transforms import transform_polygon_to_avro


def publish_message(data: dict[str, Any]):
    """Publish message to Kafka."""
    global messages_failed
    msg_key = data.get("symbol", "unknown")

    try:
        serialized_value = avro_serializer(data, SerializationContext(kafka_topic, MessageField.VALUE))
        producer.produce(
            topic=kafka_topic, key=msg_key.encode("utf-8"), value=serialized_value, on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        messages_failed += 1
        logger.log_error(f"Publish failed for {msg_key}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. WebSocket Message Handler

# COMMAND ----------


def _coalesce(d, *keys):
    """Return the first non-None value for the given keys, preserving falsy values like 0."""
    for k in keys:
        v = d.get(k)
        if v is not None:
            return v
    return None


def handle_messages(msgs: list[WebSocketMessage]):
    try:
        for msg in msgs:
            if hasattr(msg, "__dict__"):
                raw = msg.__dict__
                msg_dict = {
                    "ev": _coalesce(raw, "ev", "event_type"),
                    "sym": _coalesce(raw, "sym", "symbol"),
                    "o": _coalesce(raw, "o", "open"),
                    "h": _coalesce(raw, "h", "high"),
                    "l": _coalesce(raw, "l", "low"),
                    "c": _coalesce(raw, "c", "close"),
                    "v": _coalesce(raw, "v", "volume"),
                    "s": _coalesce(raw, "s", "start_timestamp"),
                    "e": _coalesce(raw, "e", "end_timestamp"),
                    "av": _coalesce(raw, "av", "accumulated_volume"),
                    "op": _coalesce(raw, "op", "official_open_price"),
                    "z": _coalesce(raw, "z", "average_size"),
                    "otc": raw.get("otc"),
                }
            elif isinstance(msg, dict):
                msg_dict = msg
            else:
                continue

            if msg_dict.get("ev") == "AM":
                avro_data = transform_polygon_to_avro(msg_dict)
                publish_message(avro_data)
    except Exception as e:
        logger.log_error(f"Error processing messages: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Start Streaming

# COMMAND ----------

# DBTITLE 1,Cell 14
import asyncio

print("\nStarting Polygon -> Kafka Producer")
print(f"Topic: {kafka_topic} | Feed: Delayed | Subscription: AM.*\n")

if not is_market_hours():
    print("⚠️  Market is currently CLOSED")
    print(f"Current time: {get_market_status()}")
    print("\nExiting without starting stream...")
    dbutils.notebook.exit("Market closed - streaming not started")

effective_shutdown = BaseConfig.get_effective_shutdown_datetime()
print("✅ Market is OPEN - starting stream...")
print(
    f"Stream will stop at {effective_shutdown.strftime('%H:%M')} ET (session close + {BaseConfig.SHUTDOWN_DELAY_MINUTES} min for delayed feed)\n"
)

MAX_RECONNECT_ATTEMPTS = BronzeConfig.MAX_RECONNECT_ATTEMPTS

try:

    async def run_with_market_hours_check():
        reconnect_attempt = 0

        while not should_stop_streaming() and reconnect_attempt < MAX_RECONNECT_ATTEMPTS:
            client = WebSocketClient(api_key=polygon_api_key, feed=Feed.Delayed, market=Market.Stocks)
            client.subscribe("AM.*")

            async def handle_messages_async(msgs):
                handle_messages(msgs)

            try:
                if reconnect_attempt > 0:
                    backoff = min(30 * (2 ** (reconnect_attempt - 1)), 300)
                    logger.log_info(
                        f"Reconnecting (attempt {reconnect_attempt + 1}/{MAX_RECONNECT_ATTEMPTS}) after {backoff}s backoff..."
                    )
                    await asyncio.sleep(backoff)
                else:
                    logger.log_info("Subscribed to all tickers (AM.*)")

                connection_task = asyncio.create_task(client.connect(handle_messages_async))

                # Poll every 60 seconds; stop when should_stop_streaming() or connection drops.
                while not should_stop_streaming():
                    if connection_task.done():
                        exc = connection_task.exception()
                        if exc:
                            raise ConnectionError(f"WebSocket disconnected: {exc}")
                        raise ConnectionError("WebSocket disconnected unexpectedly")
                    # Connection held for another cycle — reset the counter so transient
                    # disconnects don't consume the budget for the rest of the session.
                    reconnect_attempt = 0
                    await asyncio.sleep(60)

                # Effective shutdown reached — close WebSocket gracefully.
                logger.log_info(
                    f"🔔 Effective shutdown at {datetime.now(MARKET_TIMEZONE).strftime('%H:%M:%S %Z')} "
                    f"(session close + {BaseConfig.SHUTDOWN_DELAY_MINUTES} min)"
                )
                logger.log_info("Stopping stream...")
                await client.close()
                connection_task.cancel()
                break  # Clean exit

            except ConnectionError as e:
                reconnect_attempt += 1
                logger.log_error(
                    f"WebSocket connection lost (attempt {reconnect_attempt}/{MAX_RECONNECT_ATTEMPTS}): {e}"
                )
                try:
                    await client.close()
                except Exception:
                    pass
                if reconnect_attempt >= MAX_RECONNECT_ATTEMPTS:
                    logger.log_error(f"Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) reached. Giving up.")
                    raise
                continue

            except asyncio.CancelledError:
                logger.log_info("Stream cancelled")
                await client.close()
                break

        if reconnect_attempt >= MAX_RECONNECT_ATTEMPTS:
            logger.log_error("Exiting: exceeded max reconnect attempts")

    await run_with_market_hours_check()
    logger.log_info("Stream stopped successfully")

except KeyboardInterrupt:
    logger.log_info("⚠️  Shutdown requested by user...")
except Exception as e:
    logger.log_error(f"Connection error: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cleanup

# COMMAND ----------

# DBTITLE 1,Cleanup
print("\nShutting down producer...")

# Send any messages still buffered in the producer to Kafka before exiting.
producer.flush(timeout=10)

elapsed = time.time() - start_time
rate = messages_sent / elapsed if elapsed > 0 else 0
total = messages_sent + messages_failed
success_rate = (messages_sent / total * 100) if total > 0 else 0

logger.log_job_summary(
    status="success",
    records_read=messages_sent + messages_failed,
    records_written=messages_sent,
    extra_metrics={
        "messages_sent": messages_sent,
        "messages_failed": messages_failed,
        "success_rate": f"{success_rate:.1f}%",
        "runtime_seconds": int(elapsed),
        "average_rate": f"{rate:.2f} msg/sec",
        "market_status": get_market_status(),
    },
)

exit_message = f"Streaming completed: {messages_sent:,} sent, {messages_failed} failed, {elapsed:.0f}s"
logger.exit_job("success", exit_message)
