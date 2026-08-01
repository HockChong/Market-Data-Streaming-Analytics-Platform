# Architecture Overview

A streaming lakehouse on **Databricks** and **Delta Lake (Unity Catalog Volumes)** built on a **Medallion architecture**, combining a live 15-minute-delayed market feed with historical flat files into one analytics-ready star schema.

> This document covers the *why* behind the design — layer rationale, technology choices, and the schema-contract model. The canonical end-to-end **logical DAG** lives in the [README architecture diagram](../README.md#architecture); the component view below adds script-level detail.

## Overview

The platform ingests, processes, and serves both **streaming market data** and **historical datasets**:

- **Real-time streaming:** Polygon WebSocket → Kafka → Databricks Structured Streaming → Bronze
- **Batch ingestion:** Polygon REST / flat-file API → Bronze
- **Multi-layer Delta Lake:** Bronze → Silver → Gold (Delta Live Tables)
- **Interactive analytics:** Databricks Apps (Streamlit + Plotly)

All data is stored in Delta Lake on Unity Catalog Volumes (`/Volumes/tabular/dataexpert/hc_market_data/`) — no cloud credentials in code.

## Medallion layers

- **Bronze** – Raw WebSocket/Kafka streams + flat-file and REST snapshots. Immutable, append-only audit trail; no business filtering.
- **Silver** – Cleaned, validated, deduplicated Delta tables. DLT `apply_changes` (MERGE) for OHLCV dedup with source priority, WAP quarantine for invalid rows, and a daily rollup materialized view.
- **Gold** – Analytics-ready star schema: OHLCV facts (daily and 1-minute, raw and split-adjusted), news facts, and conformed dimensions.

## Component data flow

```
                          ┌──────────────────────────────────┐
                          │         Polygon.io API           │
                          ├──────────────────────────────────┤
                          │ WebSocket (Live, 15-min delayed) │
                          │ REST API (Historical + News)     │
                          └────────────────┬─────────────────┘
                                           │
          ┌────────────────────────────────┼────────────────────────────────┐
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
            ▼                             │                               │
  ┌───────────────────┐                   │                               │
  │ Kafka (Confluent) │                   │                               │
  │ + Schema Registry │                   │                               │
  └─────────┬─────────┘                   │                               │
            ▼                             │                               │
  ┌───────────────────┐                   │                               │
  │ streaming_        │                   │                               │
  │ ingestion.py      │                   │                               │
  │ (Spark Structured │                   │                               │
  │  Streaming)       │                   │                               │
  └─────────┬─────────┘                   │                               │
            └─────────────────────────────┼───────────────────────────────┘
                                          ▼
                      ┌────────────────────────────────────────────┐
                      │  BRONZE — Delta on Unity Catalog Volumes   │
                      │  streaming · historical · news · splits    │
                      │  ticker_details · immutable audit trail    │
                      └──────────────────┬─────────────────────────┘
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │  SILVER — DLT pipelines                    │
                      │  bronze_unified_hc (stream + historical)   │
                      │  ohlcv_silver_hc (WAP + apply_changes)     │
                      │  news_silver_hc (WAP + quarantine)         │
                      │  ohlcv_daily_silver_hc (daily MV)          │
                      └──────────────────┬─────────────────────────┘
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │  GOLD — DLT pipelines (star schema)        │
                      │  dims: dim_date · dim_ticker · dim_split   │
                      │  facts: fact_daily/minute_market(_adjusted)│
                      │         fact_news                          │
                      └──────────────────┬─────────────────────────┘
                                         ▼
                      ┌────────────────────────────────────────────┐
                      │  Databricks Apps (Streamlit + Plotly)      │
                      └────────────────────────────────────────────┘
```

**Notes:**
- **Streaming ingestion** uses Structured Streaming (`spark.readStream`) with 5-second micro-batches and a graceful drain mode at market close.
- **Silver** uses DLT `apply_changes` (MERGE) for OHLCV dedup with source priority (flat files > streaming) and a daily rollup MV (serverless incremental refresh on `(symbol, date)`).
- **Gold** implements a star schema with OHLCV facts (daily and 1-minute, raw and split-adjusted) and news.

## Technology choices & justification

### Polygon.io (WebSocket + REST API)
Streaming 1-minute aggregated OHLCV bars plus historical prices and news for correlation.

**Chosen for:** 1-minute resolution; near-real-time signal generation (15-minute delayed feed); historical enrichment and backtesting.

### Kafka (Confluent Cloud)
Streaming backbone for the market feed, with Schema Registry for data contracts and Avro serialization.

**Chosen for:**
- Low-latency stream buffering, scalability, and durable replayability
- At-least-once delivery to Bronze (checkpoint-based); idempotent Silver/Gold via `apply_changes` MERGE on keyed outputs
- Schema contracts at ingestion + downstream quarantine/audit in Silver for malformed records
- Compact binary Avro encoding (smaller than JSON) and backward-compatible schema evolution (safe optional-field additions)

### Databricks Structured Streaming (Bronze ingestion)
- `streaming_producer.py` connects to the Polygon WebSocket, serializes OHLCV bars via Avro + Schema Registry, and publishes to Kafka.
- `streaming_ingestion.py` reads Avro-encoded Kafka messages via `spark.readStream.format("kafka")` on a 5-second micro-batch trigger.
- Graceful **drain mode**: at market close (session end + 20 min) it calls `processAllAvailable()` to drain the remaining Kafka backlog before stopping, so delayed-feed bars aren't lost.

**Chosen for:**
- Low end-to-end latency (~15 s Kafka → Delta write, p50)
- Drain mode ensures all 15-minute-delayed bars are consumed before shutdown
- At-least-once with downstream dedup — checkpointed offsets minimize duplicates; Silver MERGE on `(symbol, start_timestamp)` guarantees uniqueness
- Market-hours aware — auto start/stop with the NYSE holiday calendar (`exchange_calendars`)

**Capacity:** steady-state ~8.3 msg/sec (US tickers × 1 msg/min); burst ~25 msg/sec at the open.

### Databricks Delta Live Tables
Unified batch + streaming engine with managed pipelines, declarative quality expectations, and stateful stream management — a natural fit for the medallion layers.

**Chosen for:**
- Declarative `expect_or_fail`/`expect`/WAP quarantine primitives instead of hand-rolled validation filters and a manually-wired dead-letter table
- Automatic dependency ordering across `bronze_unified_hc` → `ohlcv_silver_hc` → `ohlcv_daily_silver_hc` in one pipeline update, instead of sequencing separate Structured Streaming jobs via Databricks Jobs/Airflow
- Managed checkpointing, retry, and auto-optimize (ZORDER, liquid clustering) — less operational surface than a hand-rolled Structured Streaming + orchestrator setup
- Trade-off: less control over exact micro-batch scheduling, and lock-in to Databricks' pipeline runtime versus a portable Structured Streaming job any Spark cluster could run

### Delta Lake (Unity Catalog Volumes)
ACID transactions for financial correctness, time travel for debugging/auditing, and OPTIMIZE/Z-ORDER + compaction where read patterns justify it. Governed via Unity Catalog — no S3 credentials in code.

**Chosen for:**
- MERGE semantics the `apply_changes` dedup design depends on (`ohlcv_silver_hc`, `news_silver_hc`) — a plain Parquet table on UC Volumes has no atomic keyed upsert, so this isn't a generic ACID preference, it's a hard dependency
- Time travel for auditing a bad Silver/Gold run without re-deriving state from Bronze
- Trade-off vs. open alternatives (Iceberg/Hudi): narrower ecosystem outside Databricks, but tighter Unity Catalog governance and DLT's Enzyme incremental refresh engine (recomputes only changed groups instead of the whole table; used by `ohlcv_daily_silver_hc`), which doesn't exist for Iceberg/Hudi here

### Databricks Apps (Streamlit + Plotly)
Python-native dashboards (screener, ticker deep dive, watchlist) with no external BI tool and zero deployment friction.

## Schema contracts & data quality

All streaming market data passes through **Confluent Schema Registry** before entering the lakehouse:

```
Polygon WebSocket → Python Producer → Schema Registry → Kafka → Databricks
                         ↓                    ↓              ↓
                   Avro Serialize      Validate Schema   Bronze Layer
                   (Type Safety)     (Reject if Invalid) (Raw landing)
```

The OHLCV Avro contract is the single source of truth: [schemas/avro/ohlcv_aggregate.avsc](../schemas/avro/ohlcv_aggregate.avsc). The platform uses **BACKWARD** compatibility mode — new fields must be optional with defaults, so old consumers keep working and new consumers tolerate old data. Each Kafka message carries a `[magic byte][schema ID][Avro payload]` header, so the exact schema version used to serialize any record is recoverable.

Schema-invalid payloads are routed to quarantine paths for audit rather than dropped, keeping Bronze immutable. The full row-level → aggregate enforcement model (schema `expect_or_fail`, WAP quarantine, and the `wap_audit_log_hc` quality gate) is documented in [DATA_QUALITY_ENFORCEMENT.md](DATA_QUALITY_ENFORCEMENT.md).

## Data sources

| Source | Transport | Grain |
|---|---|---|
| Polygon real-time minute aggregates | WebSocket (15-min delayed) → Kafka (Avro) | symbol × 1-minute bar |
| Polygon flat-file minute aggregates | REST / flat-file API | symbol × 1-minute bar |
| Polygon news articles | REST API | article × ticker |

Full column-level field definitions for every source and table are in [DATA_DICTIONARY.md](DATA_DICTIONARY.md); end-to-end table lineage is in [DATA_LINEAGE.md](DATA_LINEAGE.md).
