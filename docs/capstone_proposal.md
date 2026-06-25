# 📈 Market Data Streaming & Analytics Platform

A scalable, real-time financial intelligence platform built using a **Medallion architecture** on **Databricks** and **Delta Lake (Unity Catalog Volumes)**.
Designed to support low-latency analytics, trend detection, and investment signal discovery using **live streaming + historical stock data**.

## Table of Contents

- [Overview 🚀](#overview)
- [Architecture 🏗️](#architecture)
- [Architecture Diagram 🚀](#architecture-diagram)
- [Live Market Data Simulation 🔄](#live-market-data-simulation)
- [Investment Signal Discovery 🧠](#investment-signal-discovery)
- [Technology Justification 🧠](#technology-justification)
  - [Polygon.io (WebSocket + REST API)](#polygonio-websocket--rest-api)
  - [Kafka (Confluent Cloud)](#kafka-confluent-cloud)
  - [Databricks Structured Streaming (Bronze Streaming Ingestion)](#databricks-structured-streaming-bronze-streaming-ingestion)
  - [Databricks + Delta Live Tables](#databricks--delta-live-tables)
  - [Delta Lake (Unity Catalog Volumes)](#delta-lake-unity-catalog-volumes)
  - [Databricks Apps (Streamlit + Plotly)](#databricks-apps-streamlit--plotly)
- [Data Governance & Quality 🛡️](#data-governance--quality)
- [Data Sources 📊](#data-sources)
  - [Polygon Real-Time Minute Aggregates (WebSocket)](#polygon-real-time-minute-aggregates-websocket)
  - [Polygon News Article (REST API)](#polygon-news-article-rest-api)
  - [Polygon Flat File Minute Aggregates (REST API)](#polygon-flat-file-minute-aggregates-rest-api)
  - [Confluent Schema Registry (Data Contracts)](#confluent-schema-registry-data-contracts)
- [Key Features 📊](#key-features)

---

## Overview

This platform continuously ingests, processes, and analyzes both **streaming market data** and **historical datasets** to identify:

- New trading opportunities  
- News-correlated market movements  
- Market anomalies and trends  

Built with modern data engineering and lakehouse best practices, it combines:

- **Real-time streaming:** WebSocket → Kafka → Databricks  
- **Batch ingestion:** REST API → Databricks  
- **Multi-layer Delta Lake:** Bronze → Silver → Gold  
- **Interactive analytics:** Databricks Apps (Streamlit + Plotly)

---

## Architecture

The system follows a structured **Medallion Architecture**:

- **Bronze Layer** – Raw WebSocket/Kafka streams + REST API snapshots
- **Silver Layer** – Cleaned, validated Delta tables with schema enforcement
- **Gold Layer** – Aggregated datasets for analytics, dashboards, and ML applications

All data is stored in **Delta Lake on Unity Catalog Volumes** (`/Volumes/tabular/dataexpert/hc_market_data/`) and processed via Databricks.

---

## Architecture Diagram

### Architecture: Medallion with Databricks Structured Streaming

```
                          ┌──────────────────────────────────┐
                          │         Polygon.io API           │
                          ├──────────────────────────────────┤
                          │ WebSocket (Live, 15-min delayed) │
                          │ REST API (Historical + News)     │
                          └────────────────┬─────────────────┘
                                           │
          ┌────────────────────────────────┼────────────────────────────────┐
          │                                │                                │
          ▼                                ▼                                ▼
  ┌───────────────────┐          ┌──────────────────┐          ┌──────────────────────┐
  │ Live Streaming    │          │ Historical Data  │          │ News/Context Data    │
  │ (WebSocket Feed)  │          │ (Flat Files API) │          │ (REST API)           │
  └─────────┬─────────┘          └────────┬─────────┘          └──────────┬───────────┘
            │                             │                               │
            ▼                             │                               │
  ┌───────────────────┐                   │                               │
  │ streaming_        │                   │                               │
  │ producer.py       │                   │                               │
  │ (Avro + Schema    │                   │                               │
  │  Registry)        │                   │                               │
  └─────────┬─────────┘                   │                               │
            │                             │                               │
            ▼                             │                               │
  ┌───────────────────┐                   │                               │
  │ Kafka (Confluent) │                   │                               │
  │ + Schema Registry │                   │                               │
  │ (Avro Validation) │                   │                               │
  └─────────┬─────────┘                   │                               │
            │                             │                               │
            ▼                             │                               │
  ┌───────────────────┐                   │                               │
  │ streaming_        │                   │                               │
  │ ingestion.py      │                   │                               │
  │ (Spark Structured │                   │                               │
  │  Streaming)       │                   │                               │
  └─────────┬─────────┘                   │                               │
            │                             │                               │
            └─────────────────────────────┼───────────────────────────────┘
                                          │
                                          ▼
                      ┌────────────────────────────────────────────┐
                      │         BRONZE LAYER                       │
                      │  (Delta Lake on Unity Catalog Volumes)     │
                      ├────────────────────────────────────────────┤
                      │ • bronze/streaming  (Kafka → Delta)        │
                      │ • bronze/historical (Flat files → Delta)   │
                      │ • bronze/news       (News API → Delta)     │
                      │ • bronze/ticker_details (Ref API → Delta)  │
                      │ • bronze/splits     (Splits API → Delta)   │
                      │ • Immutable audit trail                    │
                      └──────────────────┬─────────────────────────┘
                                         │
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │      SILVER LAYER (DLT Pipelines)          │
                      ├────────────────────────────────────────────┤
                      │ • bronze_unified_hc: streaming + historical│
                      │ • ohlcv_silver_hc: WAP + apply_changes     │
                      │ • news_silver_hc: WAP + quarantine         │
                      │ • ohlcv_daily_silver_hc (daily OHLCV MV)   │
                      └──────────────────┬─────────────────────────┘
                                         │
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │      GOLD LAYER (DLT Pipelines)            │
                      ├────────────────────────────────────────────┤
                      │ Dimensions: dim_date_hc, dim_ticker_hc,    │
                      │             dim_split_hc                   │
                      │ Facts:      fact_daily_market_hc           │
                      │             fact_daily_market_adjusted_hc  │
                      │             fact_minute_market_hc          │
                      │             fact_minute_market_adjusted_hc │
                      │             fact_news_hc                   │
                      └──────────────────┬─────────────────────────┘
                                         │
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │   Databricks Apps (Streamlit + Plotly)     │
                      └────────────────────────────────────────────┘

```

**Key Architecture Notes:**
- **Streaming ingestion** uses Databricks Structured Streaming (`spark.readStream`) with 5-second micro-batches and graceful drain mode at market close
- **Bronze layer** stores data in Unity Catalog Volumes (`/Volumes/tabular/dataexpert/hc_market_data/`)
- **Silver layer** uses DLT `apply_changes` (MERGE) for OHLCV deduplication with source priority (flat files > streaming) and a daily rollup materialized view (serverless incremental refresh on `(symbol, date)`)
- **Gold layer** implements a star schema with OHLCV facts (daily and 1-minute) and news

---

## Live Market Data Simulation 

To emulate a real-time trading environment, the system integrates with the **Polygon.io (Paid Plan)** WebSocket API:

- Streams **15-minute delayed, 1-minute aggregated OHLCV bars** (Open, High, Low, Close, Volume)
- Ingested through **Kafka (Confluent Cloud)**
- Supports **low-latency processing** and near-real-time analysis

This setup simulates a full production-grade streaming workload for testing and analytics.

---

## Investment Signal Discovery 

The platform continuously analyzes market activity to identify:

- **Trending tickers and anomalies**
- **News-correlated price movements**
- **New investment opportunities** based on historical data

Historical prices and news articles are fetched through Polygon’s REST API to support correlation and signal generation.

---

## Technology Justification 

### Polygon.io (WebSocket + REST API)
- Streaming 1-minute aggregated market data (OHLCV bars)
- Access to historical prices and news for correlation analysis
- High data quality required for investment analytics

**Chosen for:**
✔ 1-minute aggregated resolution
✔ Near-real-time signal generation (15-min delayed)
✔ Historical enrichment and backtesting  

---

### Kafka (Confluent Cloud)
- Industry-standard streaming backbone for financial data
- Guarantees durability and replayability
- Handles large bursts of market volume
- **Schema Registry for data contracts and validation**
- **Avro serialization for type safety and efficiency**

**Chosen for:**
✔ Low-latency stream buffering
✔ Scalability and reliability
✔ At-least-once delivery to Bronze (checkpoint-based); idempotent Silver/Gold keyed outputs via `apply_changes` MERGE
✔ **Schema contracts at ingestion + downstream quarantine/audit in Silver for malformed records**
✔ **Type safety with Avro serialization (40-60% smaller messages)**
✔ **Schema evolution with backward compatibility (safe field additions)**

---

### Databricks Structured Streaming (Bronze Streaming Ingestion)
- `streaming_producer.py` connects to Polygon WebSocket, serializes OHLCV bars via Avro + Schema Registry, and publishes to Kafka
- `streaming_ingestion.py` reads Avro-encoded Kafka messages via `spark.readStream.format("kafka")` with 5-second micro-batch trigger
- Graceful **drain mode**: at market close (session end + 20 min), calls `processAllAvailable()` to drain remaining Kafka backlog before stopping — prevents data loss from delayed-feed messages
- At-least-once delivery via Kafka offset checkpointing; deduplication handled by Silver `apply_changes`

**Chosen for:**
✔ **Low latency** - ~15 sec end-to-end (Kafka → Delta write, p50)
✔ **Custom drain mode** - ensures all 15-min delayed Polygon bars are consumed before shutdown
✔ **At-least-once with downstream dedup** - checkpointed offsets minimize duplicates; Silver MERGE on `(symbol, start_timestamp)` guarantees uniqueness
✔ **Market-hours aware** - auto-start/stop with NYSE holiday calendar (`exchange_calendars`)

**Performance:**
- Steady-state throughput: ~8.3 msg/sec (US tickers × 1 msg/min)
- Burst capacity: ~25 msg/sec (market open rush)

---

### Databricks + Delta Live Tables
- Unified batch + streaming engine
- Auto-managed pipelines, quality enforcement, and schema handling
- Ideal for building Medallion architectures

**Chosen for:**
✔ Lakehouse-native development
✔ Automatic stateful stream management
✔ Simplifies multi-layer data orchestration

---

### Delta Lake (Unity Catalog Volumes)
- ACID transactions for financial correctness
- Time travel for debugging and auditing
- Optimized analytical performance with Z-order and compaction
- Governed via Unity Catalog (`/Volumes/tabular/dataexpert/hc_market_data/`) — no S3 credentials in code

**Chosen for:**
✔ Reliable financial data storage
✔ Large-scale historical datasets
✔ Unity Catalog governance, lineage, and fine-grained access control

---

### Databricks Apps (Streamlit + Plotly)
- Near-real-time visualization of indicators, signals, and analytics
- No external BI tools required
- Zero deployment friction

**Chosen for:**
✔ Integrated dashboards
✔ Near-real-time updates
✔ Python-native workflows

---

## Data Governance & Quality

### Schema Registry (Data Contracts)

All streaming market data passes through **Confluent Schema Registry** before entering the lakehouse, ensuring enterprise-grade data quality and governance.

#### Validation Flow

```
Polygon WebSocket → Python Producer → Schema Registry → Kafka → Databricks
                         ↓                    ↓              ↓
                   Avro Serialize      Validate Schema   Bronze Layer
                   (Type Safety)     (Reject if Invalid) (Clean Data)
```

**How It Works:**

1. **Producer Side** - WebSocket client serializes OHLCV data using Avro schema
2. **Schema Registry** - Validates data structure and types before allowing publish to Kafka
3. **Consumer Side** - Databricks deserializes using the same schema, ensuring consistency
4. **Bronze Layer** - Raw landing with schema parsing and metadata; malformed rows are retained in quarantine streams for audit

#### OHLCV Data Contract Schema

```json
{
  "type": "record",
  "name": "OHLCVAggregate",
  "namespace": "com.polygon.market_data",
  "fields": [
    {"name": "event_type", "type": "string"},
    {"name": "symbol", "type": "string"},
    {"name": "open", "type": "double"},
    {"name": "high", "type": "double"},
    {"name": "low", "type": "double"},
    {"name": "close", "type": "double"},
    {"name": "volume", "type": "long"},
    {"name": "start_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "end_timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "accumulated_volume", "type": ["null", "long"], "default": null},
    {"name": "official_open", "type": ["null", "double"], "default": null},
    {"name": "average_trade_size", "type": ["null", "double"], "default": null},
    {"name": "otc", "type": ["null", "boolean"], "default": null},
    {"name": "transactions", "type": ["null", "long"], "default": null},
    {"name": "ingestion_timestamp", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

#### Governance Benefits

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Type Safety** | Strong typing prevents "string vs number" bugs | 🛡️ Zero type errors in Bronze layer |
| **Schema Evolution** | Add optional fields without breaking consumers | 🔄 Backward compatible upgrades |
| **Audit Trail** | Track all schema changes with versioning | 📋 Full regulatory compliance |
| **Data Quality** | Schema-invalid payloads routed to quarantine for audit; Bronze stays immutable (no rows dropped) | ✅ Auditable rejects, no silent data loss |
| **Documentation** | Schema serves as self-documenting API contract | 📖 Clear data format for all teams |
| **Cost Efficiency** | Avro binary encoding reduces message size by 40-60% | 💰 Lower Kafka storage & transfer costs |

#### Schema Evolution Strategy

The platform uses **BACKWARD compatibility mode**, allowing safe schema evolution:

✅ **Allowed Changes:**
- Add optional fields with defaults
- Remove optional fields
- Update field documentation

❌ **Prohibited Changes:**
- Remove required fields
- Change field types
- Rename fields without aliases

**Example Evolution:**

```
Version 1 (Initial):
- symbol, open, high, low, close, volume

Version 2 (Add optional field):
- symbol, open, high, low, close, volume
- + trade_count (optional, default: null)  ✅ Backward compatible

Old consumers still work with new data (ignore trade_count)
New consumers work with old data (use null for trade_count)
```

#### Compliance & Lineage

**Data Lineage Tracking:**

Every message in Kafka includes metadata for full traceability:

```
Message Format:
[Magic Byte][Schema ID][Avro Binary Data]
     ↓           ↓              ↓
   0x00      0x00 0x01    <binary OHLC>
```

**Schema ID** enables:
- Identify exact schema version used to serialize data
- Query Schema Registry for full schema definition
- Audit when schema changed and by whom
- Trace data from WebSocket → Kafka → Bronze → Silver → Gold

**Regulatory Compliance:**
- ✅ Full audit trail from source to analytics
- ✅ Immutable schemas ensure data integrity
- ✅ Lineage tracking for financial data governance

---

## Data Sources 
### Polygon Real-Time Minute Aggregates (WebSocket)

| Field | Type | Description |
|-------|------|-------------|
| `ev` | string | Event type. For aggregate-per-minute messages this is `"AM"`. |
| `sym` | string | The ticker symbol for the aggregate window (e.g., `GME`). |
| `v` | number | Tick volume during this aggregate window. |
| `av` | number | Today’s accumulated volume up to this point. |
| `op` | number | Today’s official opening price. |
| `vw` | number | Today’s volume-weighted average price (VWAP). |
| `o` | number | Opening price for this aggregate window. |
| `c` | number | Closing price for this aggregate window. |
| `h` | number | Highest price within this aggregate window. |
| `l` | number | Lowest price within this aggregate window. |
| `a` | number | Today's volume-weighted average price (alternative VWAP field). |
| `z` | number | Average trade size for this window (may be missing or zero in many responses). |
| `s` | integer | Start timestamp of the aggregate window (Unix ms). |
| `e` | integer | End timestamp of the aggregate window (Unix ms). |
| `otc` | boolean | Indicates whether the ticker is OTC. This field may be missing or `false`. |

### Polygon News Article (REST API)

| Field | Type | Description |
| --- | --- | --- |
| `count` | integer | The total number of results for this request. |
| `next_url` | string | If present, this value can be used to fetch the next page of data. |
| `request_id` | string | A request id assigned by the server. |
| `results` | array[object] | An array of results containing the requested data. |
| `results[].amp_url` | string | The mobile friendly Accelerated Mobile Page (AMP) URL. |
| `results[].article_url` | string | A link to the news article. |
| `results[].author` | string | The article's author. |
| `results[].description` | string | A description of the article. |
| `results[].id` | string | Unique identifier for the article. |
| `results[].image_url` | string | The article's image URL. |
| `results[].insights` | array[object] | The insights related to the article. |
| `results[].keywords` | array[string] | The keywords associated with the article (which will vary depending on the publishing source). |
| `results[].published_utc` | string | The UTC date and time when the article was published, formatted in RFC3339 standard (e.g. YYYY-MM-DDTHH:MM:SSZ). |
| `results[].publisher` | object | Details the source of the news article, including the publisher's name, logo, and homepage URLs. This information helps users identify and access the original source of news content. |
| `results[].tickers` | array[string] | The ticker symbols associated with the article. |
| `results[].title` | string | The title of the news article. |
| `status` | string | The status of this request's response. |

### Polygon Flat File Minute Aggregates (REST API) 

| Field          | Type                | Description                                                              |
| -------------- | ------------------- | ------------------------------------------------------------------------ |
| `close`        | number              | The close price for the symbol in the given time period.                 |
| `high`         | number              | The highest price for the symbol in the given time period.               |
| `low`          | number              | The lowest price for the symbol in the given time period.                |
| `open`         | number              | The open price for the symbol in the given time period.                  |
| `ticker`       | string              | The exchange symbol that this item is traded under.                      |
| `transactions` | integer             | The number of transactions in the aggregate window.                      |
| `volume`       | number              | The trading volume of the symbol in the given time period.               |
| `window_start` | timestamp (integer) | The Unix nanosecond timestamp marking the start of the aggregate window. |

### Confluent Schema Registry (Data Contracts)

Enforces data quality and schema validation for all streaming Kafka topics.

#### Schema Configuration

| Component | Purpose | Schema Format |
|-----------|---------|---------------|
| **OHLCV Aggregates Schema** | Validates 1-minute OHLCV bars from WebSocket | Avro |
| **Compatibility Mode** | Backward compatibility for safe schema evolution | N/A |
| **Schema Storage** | Centralized schema repository with versioning | Confluent Cloud |

#### Schema Fields (OHLCV Aggregates)

| Field Name | Avro Type | Must Have? | Description |
|------------|-----------|------------|-------------|
| `event_type` | `string` | **Yes** | Event type (always "AM" for aggregate-per-minute) |
| `symbol` | `string` | **Yes** | Ticker symbol (e.g., AAPL, TSLA) |
| `open` | `double` | **Yes** | Opening price for 1-minute window |
| `high` | `double` | **Yes** | Highest price in window |
| `low` | `double` | **Yes** | Lowest price in window |
| `close` | `double` | **Yes** | Closing price for window |
| `volume` | `long` | **Yes** | Trading volume in window |
| `start_timestamp` | `long` (timestamp-millis) | **Yes** | Window start time (Unix ms) |
| `end_timestamp` | `long` (timestamp-millis) | **Yes** | Window end time (Unix ms) |
| `accumulated_volume` | `["null", "long"]` | Optional | Today's accumulated volume up to this point (can be null) |
| `official_open` | `["null", "double"]` | Optional | Today's official opening price (can be null) |
| `average_trade_size` | `["null", "double"]` | Optional | Average trade size for this window (can be null) |
| `otc` | `["null", "boolean"]` | Optional | OTC ticker indicator (can be null) |
| `transactions` | `["null", "long"]` | Optional | Number of transactions in window; null for streaming (not in WebSocket AM events), populated from flat-file sources |
| `ingestion_timestamp` | `long` (timestamp-millis) | **Yes** | Timestamp when message was ingested by producer (Unix ms) |

**Field Requirements:**
- **"Yes"** = Field **must be present** in every message. Messages missing these fields will **fail schema validation**.
- **"Optional"** = Field **can be missing or null**. Schema validation will still pass if absent.

#### Schema Registry Benefits

| Benefit | Description | Impact |
|---------|-------------|--------|
| **Validation at Source** | Schema contracts catch malformed payloads early; malformed rows are routed to quarantine paths for audit | 🛡️ Faster issue detection with auditable rejects |
| **Compact Encoding** | Avro binary encoding vs JSON text | 💾 40-60% smaller message sizes |
| **Type Safety** | Strong typing prevents type coercion bugs | ✅ Guaranteed type correctness |
| **Evolution Support** | Add optional fields without breaking consumers | 🔄 Safe backward-compatible upgrades |
| **Self-Documentation** | Schema serves as API contract | 📖 Clear data format for all teams |
| **Cost Efficiency** | Smaller messages reduce Kafka storage & network costs | 💰 Lower operational expenses |

#### Message Wire Format

Each message in Kafka includes schema metadata for deserialization:

```
┌────────────┬────────────────┬──────────────────────────┐
│ Magic Byte │   Schema ID    │   Avro Binary Payload    │
│   (1 byte) │   (4 bytes)    │   (variable length)      │
├────────────┼────────────────┼──────────────────────────┤
│    0x00    │  0x00 0x00 0x01│  <OHLCV data encoded>    │
└────────────┴────────────────┴──────────────────────────┘
         ↑            ↑                    ↑
     Fixed        Registry         Compressed data
    Protocol    Schema Lookup    (40-60% smaller)
```

**Consumer Deserialization:**
1. Read Schema ID from message header
2. Fetch schema from Schema Registry (cached after first fetch)
3. Deserialize Avro payload using schema
4. Validate types and constraints
5. Write to Bronze Delta table

---

## Key Features

| Feature Category | Description |
|-----------------|-------------|
| **Streaming Ingestion** | WebSocket → Kafka (with Schema Registry validation) → Databricks pipeline for 1-minute OHLCV bars |
| **Batch Ingestion** | REST API retrieval for historical prices and news articles |
| **Data Architecture** | Bronze/Silver/Gold medallion layers with Delta Lake |
| **Data Quality Enforcement** | Schema Registry validates all streaming data against Avro contracts before Bronze layer |
| **Signal Detection** | Trend identification and market anomaly detection |
| **Correlation Analysis** | News sentiment correlated with price movements |
| **Data Governance** | Full lineage tracking and schema versioning across Bronze → Silver → Gold |
| **Visualization** | Interactive Streamlit + Plotly dashboards for indicators and insights |  

