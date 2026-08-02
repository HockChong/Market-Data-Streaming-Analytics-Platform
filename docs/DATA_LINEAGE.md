# Data Lineage Documentation

This document provides a comprehensive view of data flow through the Market Data Streaming & Analytics Platform.

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 EXTERNAL DATA SOURCES                                         │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                               │
│  Polygon.io WebSocket ──┐                                                                    │
│  (Real-time OHLCV)      │  Polygon.io REST API ──┐  Polygon S3 Flat Files ──┐               │
│                         │  (Ticker Details,       │  (Historical OHLCV)      │               │
│                         ▼   News, Splits)         │                          │               │
│              ┌──────────────────┐                  │                          │               │
│              │  Kafka (Avro)    │                  │                          │               │
│              │  Schema Registry │                  │                          │               │
│              └────────┬─────────┘                  │                          │               │
│                       │                            │                          │               │
└───────────────────────┼────────────────────────────┼──────────────────────────┼───────────────┘
                        │                            │                          │
  streaming_producer.py │                            │                          │
  (WebSocket → Kafka)   │                            │                          │
                        ▼                            ▼                          ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                       BRONZE LAYER                                            │
│                                  (Raw, Immutable Data)                                        │
├──────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                               │
│  streaming_ingestion.py  ticker_details_     historical_ingestion_flatfiles.py               │
│  kafka_replay_backfill.py  ingestion.py      incremental_ingestion_flatfiles.py              │
│  rest_aggs_backfill.py         │              news_ingestion.py  splits_ingestion.py           │
│         │                      │                  │        │          │                        │
│         ▼                      ▼                  │        │          │                        │
│  ┌────────────────┐  ┌──────────────────┐       ▼        │          ▼                        │
│  │ bronze/streaming│  │ bronze/ticker_   │ ┌──────────┐  │  ┌──────────────┐                 │
│  │ (Delta Lake)   │  │ details          │ │ bronze/  │  │  │ bronze/splits │                 │
│  └───────┬────────┘  │ (Delta Lake)     │ │historical│  │  │ (Delta Lake)  │                 │
│          │           └────────┬─────────┘ └────┬─────┘  │  └──────┬───────┘                  │
│          │                    │                 │        │         │                           │
│          │                    │                 │        ▼         │                           │
│          │                    │                 │  ┌──────────┐   │                           │
│          │                    │                 │  │ bronze/  │   │                           │
│          │                    │                 │  │ news     │   │                           │
│          │                    │                 │  └────┬─────┘   │                           │
│          │                    │                 │       │         │                           │
└──────────┼────────────────────┼─────────────────┼───────┼─────────┼───────────────────────────┘
           │                    │                 │       │         │
           ▼                    │                 ▼       ▼         │
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                    SILVER LAYER                                          │
│                    (Cleaned, Validated, Deduplicated Data - DLT)                        │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│                              ohlcv_silver_dlt.py                                        │
│  ┌────────────────────────────────────────────────────────────────────────────────┐    │
│  │                                                                                 │    │
│  │  bronze_streaming ──┐                                                          │    │
│  │                     ├──► bronze_unified_hc ──► ohlcv_silver_hc                 │    │
│  │  bronze_historical ─┘   (unified)       │      (deduplicated)                  │    │
│  │                                         │            │                          │    │
│  │                                         │            └──► ohlcv_daily_silver_hc│    │
│  │                                         │                  (materialized view)   │    │
│  │                                         │                                       │    │
│  │                                         ├──► ohlcv_silver_quarantine_hc (WAP)  │    │
│  │                                         └──► wap_audit_log_hc                  │    │
│  │                                                                                 │    │
│  └────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                          │
│                              news_silver_dlt.py                                         │
│  ┌────────────────────────────────────────────────────────────────────────────────┐    │
│  │  bronze/news ──► news_silver_hc                                                 │    │
│  │                  └──► news_silver_quarantine_hc (WAP)                          │    │
│  └────────────────────────────────────────────────────────────────────────────────┘    │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                     GOLD LAYER                                           │
│                           (Aggregated Analytics - Star Schema)                           │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                              DIMENSION TABLES                                    │   │
│  ├─────────────────────────────────────────────────────────────────────────────────┤   │
│  │                                                                                  │   │
│  │  dim_date_dlt.py         dim_ticker_dlt.py            dim_split_dlt.py          │   │
│  │  ┌─────────────┐        ┌────────────────────────┐   ┌──────────────┐           │   │
│  │  │ dim_date_hc │        │ dim_ticker_hc          │   │ dim_split_hc │           │   │
│  │  │ (generated) │        │ (latest bronze/        │   │ (latest      │           │   │
│  │  └─────────────┘        │  ticker_details snap)  │   │  bronze/     │           │   │
│  │                         └────────────────────────┘   │  splits snap)│           │   │
│  │                                                      └──────────────┘           │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────┐   │
│  │                                FACT TABLES                                       │   │
│  ├─────────────────────────────────────────────────────────────────────────────────┤   │
│  │                                                                                  │   │
│  │  fact_daily_market_dlt.py            fact_minute_market_dlt.py                  │   │
│  │  ┌──────────────────────────┐        ┌───────────────────────────┐              │   │
│  │  │ fact_daily_market_hc     │        │ fact_minute_market_hc     │              │   │
│  │  │ Source: ohlcv_daily_     │        │ Source: ohlcv_silver_hc   │              │   │
│  │  │   silver_hc (pre-agg'd) │        │ (1-min, rolling window)   │              │   │
│  │  └──────────────────────────┘        └───────────────────────────┘              │   │
│  │                                                                                  │   │
│  │  dim_split_dlt.py (adjusted fact tables)                                        │   │
│  │  ┌─────────────────────────────────────────────────────────────────────────┐   │   │
│  │  │ fact_daily_market_adjusted_hc   fact_minute_market_adjusted_hc         │   │   │
│  │  │ (raw daily + adj_* columns)     (raw minute + adj_* columns)           │   │   │
│  │  │ Source: fact_daily + dim_split   Source: fact_minute + dim_split        │   │   │
│  │  └─────────────────────────────────────────────────────────────────────────┘   │   │
│  │                                                                                  │   │
│  │  fact_news_dlt.py                                                               │   │
│  │  ┌─────────────────────────────────────┐                                        │   │
│  │  │ fact_news_hc                        │                                        │   │
│  │  │ Source: news_silver_hc              │                                        │   │
│  │  │ (explode tickers → one row/ticker)  │                                        │   │
│  │  └─────────────────────────────────────┘                                        │   │
│  │                                                                                  │   │
│  └─────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                   CONSUMERS                                              │
├─────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                          │
│  Streamlit Dashboard (Signal Screener + Stock Deep Dive + Watchlist)  │  Databricks SQL  │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Table Dependencies

### Bronze Layer

| Table | Source | Script(s) | Description |
|-------|--------|-----------|-------------|
| `bronze/streaming` | Kafka (Polygon WebSocket) | `streaming_ingestion.py`, `kafka_replay_backfill.py`, `rest_aggs_backfill.py` | Streaming 1-min OHLCV bars (Polygon **delayed feed, ~15 min** — not true real-time; replay script re-reads Kafka; REST script backfills sessions that never reached Kafka — both write bounded batch appends to same table) |
| `bronze/streaming_quarantine` | Kafka (Avro deserialization failures) | `streaming_ingestion.py` | Dead-letter **Delta table** (on the Bronze volume, not a Kafka topic) for messages that fail Avro deserialization; written by a separate `writeStream` query off the same Kafka source via `from_avro(mode="PERMISSIVE")` |
| `bronze/historical` | Polygon S3 Flat Files | `historical_ingestion_flatfiles.py`, `incremental_ingestion_flatfiles.py` | Historical OHLCV data (historical = bootstrap overwrite, incremental = daily MERGE) |
| `bronze/ticker_details` | Polygon REST API | `ticker_details_ingestion.py` | Company metadata (partitioned snapshots; Gold `dim_ticker_hc` reads latest `snapshot_date`) |
| `bronze/news` | Polygon News API | `news_ingestion.py` | News articles |
| `bronze/splits` | Polygon `/stocks/v1/splits` | `splits_ingestion.py` | Stock split events + cumulative `historical_adjustment_factor` (partitioned snapshots; Gold `dim_split_hc` reads latest `snapshot_date`) |

**Pre-Bronze dependency:** `streaming_producer.py` connects to Polygon WebSocket and publishes Avro-serialized messages to the Kafka topic. It must be running during market hours for `streaming_ingestion.py` to receive data.

### Silver Layer

| Table | Source(s) | Script | Pattern |
|-------|-----------|--------|---------|
| `bronze_streaming_source_hc` | `bronze/streaming` | `ohlcv_silver_dlt.py` | Streaming read |
| `bronze_historical_source_hc` | `bronze/historical` | `ohlcv_silver_dlt.py` | Streaming read |
| `bronze_unified_hc` | Both sources above (OTC-filtered) | `ohlcv_silver_dlt.py` | Unified Streaming via `append_flow` |
| `ohlcv_silver_hc` | `bronze_unified_hc` (via `ohlcv_silver_enriched_hc` temp) | `ohlcv_silver_dlt.py` | Deduplicated via apply_changes MERGE on (symbol, start_timestamp) |
| `ohlcv_daily_silver_hc` | `ohlcv_silver_hc` | `ohlcv_silver_dlt.py` | Daily OHLCV materialized view via `aggregate_minute_to_daily` groupBy on `(symbol, date)`; serverless incremental refresh; source for Gold `fact_daily_market_hc` |
| `ohlcv_silver_quarantine_hc` | `bronze_unified_hc` | `ohlcv_silver_dlt.py` | WAP: Invalid records with rejection_reason |
| `wap_audit_log_hc` | `bronze_unified_hc` (rejection counts) + `ohlcv_silver_hc` (`session_bars`) | `ohlcv_silver_dlt.py` | Daily quality metrics: rejection rate from Bronze counts, `session_bars` from deduped Silver |
| `bronze_news_source_hc` | `bronze/news` | `news_silver_dlt.py` | Streaming read |
| `news_silver_hc` | `bronze_news_source_hc` (via `news_silver_enriched_hc` temp) | `news_silver_dlt.py` | Cleaned, deduplicated articles via apply_changes MERGE on article_id |
| `news_silver_quarantine_hc` | `bronze_news_source_hc` | `news_silver_dlt.py` | WAP: Invalid news records with rejection_reason |

### Gold Layer

| Table | Source(s) | Script | Description |
|-------|-----------|--------|-------------|
| `dim_date_hc` | Generated (2020-01-01 to current year + 5) | `dim_date_dlt.py` | Date dimension with NYSE calendar (requires `exchange_calendars` cluster library) |
| `dim_ticker_hc` | `bronze/ticker_details` (latest `snapshot_date`) | `dim_ticker_dlt.py` | Type 1 ticker dimension — active **plus** delisted names referenced in OHLCV history (`is_active` flag; survivorship-free); SIC→sector mapping + cap tier in `ticker_details_dim_spark.py` |
| `dim_split_hc` | `bronze/splits` (latest `snapshot_date`) | `dim_split_dlt.py` | Stock split events with cumulative `historical_adjustment_factor` (projection in `split_adjust_spark.py`) |
| `fact_daily_market_hc` | `ohlcv_daily_silver_hc` | `fact_daily_market_dlt.py` | Daily OHLCV (reads pre-aggregated Silver, not raw minute Silver) |
| `fact_daily_market_adjusted_hc` | `fact_daily_market_hc` + `dim_split_hc` | `dim_split_dlt.py` | Daily OHLCV with split-adjusted columns beside raw (factor join in `split_adjust_spark.py`) plus rolling serving metrics — lag closes and `rvol_20d` (`daily_metrics_spark.py`) |
| `fact_minute_market_hc` | `ohlcv_silver_hc` | `fact_minute_market_dlt.py` | 1-min OHLCV (market hours only, rolling window) |
| `fact_minute_market_adjusted_hc` | `fact_minute_market_hc` + `dim_split_hc` | `dim_split_dlt.py` | 1-min OHLCV with split-adjusted columns beside raw; powers the dashboard intraday chart so the 2-day horizon stays continuous across splits |
| `fact_news_hc` | `news_silver_hc` | `fact_news_dlt.py` | News articles by ticker (one row per article-ticker pair via explode) |

### Unity Catalog Constraints (Informational)

Added by `setup/add_table_constraints.py` after DLT pipelines create the tables. Unity Catalog constraints are informational (not enforced at write time) — they help the Catalyst optimizer and document star schema relationships.

| Table | Constraint | Type | Columns |
|-------|------------|------|---------|
| `dim_ticker_hc` | `pk_dim_ticker_hc` | PRIMARY KEY | `symbol` |
| `dim_date_hc` | `pk_dim_date_hc` | PRIMARY KEY | `date` |
| `fact_daily_market_hc` | `pk_fact_daily_market_hc` | PRIMARY KEY | `(symbol, date)` |
| `fact_daily_market_hc` | `fk_..._ticker` | FOREIGN KEY | `symbol` → `dim_ticker_hc` |
| `fact_daily_market_hc` | `fk_..._date` | FOREIGN KEY | `date` → `dim_date_hc` |
| `fact_news_hc` | `pk_fact_news_hc` | PRIMARY KEY | `(article_id, symbol)` |
| `fact_news_hc` | `fk_..._ticker` | FOREIGN KEY | `symbol` → `dim_ticker_hc` |
| `fact_news_hc` | `fk_..._date` | FOREIGN KEY | `published_date` → `dim_date_hc` |

## Architecture Patterns

### Layer routing: when data skips Silver

Not every Bronze table flows through Silver. The routing depends on what the data **is**:

- **Event/time-series data → Bronze → Silver → Gold.** OHLCV (`streaming`, `historical`) and `news` arrive at-least-once, out of order, and with quality defects. Silver earns its place here: keyed dedup (`apply_changes` MERGE), DLT expectations, and WAP quarantine. Without Silver these would land dirty in Gold.
- **Reference / slowly-changing snapshots → Bronze → Gold (skip Silver).** `ticker_details` and `splits` are pulled as a complete, already-clean universe in one idempotent REST snapshot (`replaceWhere snapshot_date = ...`). There are no duplicates, no late events, and no per-record quality triage for Silver to do. The only shaping needed — pick the latest `snapshot_date`, drop unusable rows, conform to the dimension grain — is small and is done at the Gold boundary via `expect_*` on the dimension (`dim_ticker_hc`, `dim_split_hc`). A pass-through Silver table would be a no-op copy, so it is omitted.

In short: Silver exists to *clean a messy stream*. Reference snapshots are not a messy stream, so they go straight to a Gold Type-1 dimension. See the two subsections below for the specific tables.

### Unified Streaming

Used for OHLCV data ingestion where both historical and real-time data flow through the same pipeline.

```
Historical Files ──┐
                   ├──► bronze_unified ──► ohlcv_silver_hc
Real-time Kafka ───┘
```

**Benefits:**
- Single codebase for backfill and streaming
- No pipeline reset needed when adding historical data
- Effectively-once end-to-end — **at-least-once on the wire, exactly-once in effect**: Bronze is at-least-once, and Silver `apply_changes` MERGE deduplicates on the natural key so retries/overlap collapse (not Kafka transactional exactly-once)
- Automatic deduplication across sources

### Ticker dimension: Bronze snapshot → Gold (Type 1)

`dim_ticker_hc` is rebuilt each DLT run from the **latest** `snapshot_date` partition in `bronze/ticker_details`. Ticker metadata is not maintained through a separate Silver slowly-changing-dimension table; historical snapshots remain queryable in Bronze by `snapshot_date`.

### Split adjustment: raw stays immutable, adjustment is derived in Gold

Stock splits and reverse splits are handled as a **derived, recomputable** transform — never by mutating raw prices. `bronze/splits` lands the Polygon split feed (snapshot-append, like `ticker_details`); `dim_split_hc` projects the latest snapshot; `fact_daily_market_adjusted_hc` left-joins each daily bar to its split factor and emits `adj_*` columns beside the raw OHLCV. The same logic applies to `fact_minute_market_adjusted_hc` at 1-minute grain.

```
bronze/splits ──► dim_split_hc ──► factor segments ─┐
                                                    ├──► fact_daily_market_adjusted_hc
fact_daily_market_hc (raw, untouched) ──────────────┘
                                                    ┌──► fact_minute_market_adjusted_hc
fact_minute_market_hc (raw, untouched) ─────────────┘
```

**Factor rule** (Polygon `historical_adjustment_factor` is cumulative): for a price on date D, multiply by the factor of the first split whose `execution_date` is after D; rows on/after the latest split keep factor 1.0. Volume is rescaled by the inverse. A new split rewrites a symbol's entire history, so the adjusted facts are full-recompute materialized views (not append-only) — at ~2.9M daily rows the recompute is seconds. Raw Bronze/Silver/`fact_daily_market_hc` prices are never modified.

### WAP Pattern (Write-Audit-Publish)

Used in Silver layer to capture invalid records for audit.

```
bronze_unified_hc ──► Filter (valid) ──► ohlcv_silver_enriched_hc ──► ohlcv_silver_hc (apply_changes)
       │
       └──► Filter (invalid) ──► ohlcv_silver_quarantine_hc
       │
       └──► Aggregate by date ──► wap_audit_log_hc
```

**Quarantine columns:**
- `rejection_reason`: Why the record was rejected
- `quarantined_at`: Timestamp of rejection

## Data Quality Checkpoints

### Bronze Layer
- Schema validation via Avro (Kafka + Schema Registry)
- No data quality filtering (immutable audit trail — all raw data preserved)
- Quality enforcement deferred to Silver layer via DLT expectations
- OTC (over-the-counter) stocks are filtered at the Bronze→Silver boundary (`otc IS NULL` via `coalesce(otc, false)` in `ohlcv_silver_dlt.py`)

### Silver Layer (DLT Expectations)

**expect_or_fail (pipeline halts):**
- `valid_timestamps`: `start_timestamp < end_timestamp`
- `valid_start_timestamp`: `start_timestamp > 0`
- `required_fields`: `symbol IS NOT NULL AND start_timestamp IS NOT NULL AND source IS NOT NULL`
- `known_ts_unit`: `ts_unit = 'ms'` (all OHLCV sources are normalized to epoch ms at Bronze)

**WAP Validation (routes to quarantine):**
- `valid_price_positive`: `close > 0 AND open > 0 AND high > 0 AND low > 0`
- `valid_ohlc_logic`: `high >= low AND high >= open AND high >= close AND low <= open AND low <= close`
- `valid_volume`: `volume >= 0`

**Market hours filter (applied after validation):**
- Regular sessions: `[9:30 AM, 4:00 PM) ET` — pre-market and after-hours bars excluded
- Early-close sessions: uses `exchange_calendars` NYSE calendar to detect days closing at 1:00 PM ET (July 3, day-before-Thanksgiving, etc.) — bars between the early close and 4:00 PM are excluded on those days
- Falls back to static 4:00 PM cutoff if `exchange_calendars` is unavailable

**Completeness check (warn-only, never halts):**
- `wap_audit_log_hc`: `session_bars` is the most bars any single symbol reached that day; the `session_complete` warn expectation fires when it falls below 195 — half of `EXPECTED_BARS_PER_DAY` (390) — on a trading day (tolerates early-close ~210)
- Reads from deduped Silver (`ohlcv_silver_hc`) so Kafka replays don't inflate bar counts

**News Silver (expect_or_fail):**
- `valid_article_id`: `article_id IS NOT NULL AND LENGTH(article_id) > 0`
- `valid_published_date`: `published_utc IS NOT NULL`

**News WAP Validation (routes to quarantine):**
- `valid_title`: Title non-empty after trimming (script-neutral)
- `valid_url`: URL starts with 'http'
- `valid_timestamp_order`: `published_timestamp <= ingestion_timestamp`

### Gold Layer (DLT Expectations)

**expect_or_fail (pipeline halts):**
- `fact_daily_market_hc`: symbol IS NOT NULL, date IS NOT NULL
- `fact_minute_market_hc`: symbol IS NOT NULL, start_timestamp IS NOT NULL
- `fact_news_hc`: article_id IS NOT NULL
- `dim_ticker_hc`: symbol IS NOT NULL, 1-8 chars
- `dim_split_hc`: symbol IS NOT NULL, execution_date IS NOT NULL, historical_adjustment_factor > 0

**expect_or_drop (silent removal of non-auditable noise):**
- `fact_news_hc`: symbol IS NOT NULL — null symbols from `explode(tickers)` are dropped (not halted)

**expect (warn-only, logged to DLT event log):**
- `fact_daily_market_adjusted_hc`: `no_unexplained_gap` — a large day-over-day move on `adj_close` with no split usually means a missing split event
- `dim_ticker_hc`: exchange IS NOT NULL, company_name IS NOT NULL, is_active IS NOT NULL

## Storage Locations

**Bronze data** is stored in Unity Catalog Managed Volumes:

```
/Volumes/tabular/dataexpert/hc_market_data/
├── bronze/
│   ├── streaming/
│   ├── streaming_quarantine/        ← Avro deserialization dead letter queue
│   ├── historical/
│   ├── news/
│   ├── ticker_details/
│   └── splits/
├── _checkpoints/
│   └── bronze/
│       ├── streaming_ingestion/
│       └── streaming_ingestion_quarantine/
└── _metrics/
    └── streaming_ingestion/         ← Drain mode metrics (append-only Delta)
```

**Silver and Gold tables** are managed by DLT and stored in Unity Catalog managed storage (not in the user's volume). They are accessible via fully qualified names:

```
tabular.dataexpert.<table_name>
```

**Unity Catalog references:**
- **Catalog**: `tabular`
- **Schema**: `dataexpert`
- **Silver tables**: `ohlcv_silver_hc`, `ohlcv_daily_silver_hc` (materialized view), `ohlcv_silver_quarantine_hc`, `wap_audit_log_hc`, `news_silver_hc`, `news_silver_quarantine_hc`
- **Gold tables**: `dim_date_hc`, `dim_ticker_hc`, `dim_split_hc`, `fact_daily_market_hc`, `fact_daily_market_adjusted_hc`, `fact_minute_market_hc`, `fact_minute_market_adjusted_hc`, `fact_news_hc`
- **DLT-internal tables** (not directly queried): `bronze_streaming_source_hc`, `bronze_historical_source_hc`, `bronze_unified_hc`, `bronze_news_source_hc`

## Pipeline Orchestration (Layer 3)

| When | Job | Depends on | Notes |
|------|-----|------------|-------|
| Market hours | Bronze streaming producer + ingestion (Jobs 1–2) | — | Kafka → Bronze Delta; Silver does not read Kafka directly |
| 9:30 AM–5:00 PM ET (continuous) | Silver OHLCV DLT | Bronze streaming **running** | Continuous pipeline, started/stopped by Job 7 (not a fixed trigger cron); each update: minute MERGE → daily MV refresh (serverless incremental) |
| 9:30 AM–5:00 PM ET (continuous, parallel with Silver) | Gold DLT | Silver OHLCV **running** | Continuous pipeline, started/stopped by Job 7; reads `ohlcv_daily_silver_hc` via `spark.read.table` on each cycle, not gated on a Silver completion signal |
| Daily off-hours | Incremental flat files (Job 4) → Silver | Bronze job success | Flat-file backfill flows into minute Silver; daily MV picks up the changed dates once Silver's continuous pipeline (or a manual start) processes them |

See `databricks/DEPLOYMENT_GUIDE.md` Step 6, Job 7 for the continuous pipeline start/stop schedule and DLT configuration keys.

## Monitoring Points

| Check | Table | Threshold | Alert |
|-------|-------|-----------|-------|
| Quality Gate | `wap_audit_log_hc` | `quality_gate_passed = false` | CRITICAL |
| Rejection Rate | `wap_audit_log_hc` | > 1% | CRITICAL |
| Rejection Rate | `wap_audit_log_hc` | > 0.5% | WARNING |
| Session Bars | `wap_audit_log_hc` | `session_bars < 195` on a trading day | WARNING (possible market-wide outage) |
| Gold Partial Write | `fact_daily_market_hc` | Latest `correlation_id` has < expected symbols | WARNING |
| Empty Dimension | `dim_ticker_hc` | count = 0 | CRITICAL |
| Undersized Dimension | `dim_ticker_hc` | count < 100 | WARNING |
| Data Freshness | All layers | > 24 hours | WARNING |
| Kafka Lag | Streaming | > 10 seconds | WARNING |
| Drain Residual | `_metrics/streaming_ingestion` | `residual_lag_rows > 0` | INFO |

## Key Transformations

### OTC Filter

Applied at the Bronze→Silver boundary in `ohlcv_silver_dlt.py`. Records where `otc = true` are excluded before entering `bronze_unified_hc`. Polygon sends `otc=true` for OTC stocks and null otherwise; `coalesce(otc, false)` defends against future changes where non-OTC rows carry `otc=false`.

### Market Hours Filter

Applied in `ohlcv_silver_enriched_hc` (after validation, before dedup). Only bars within `[MARKET_OPEN, session_close)` ET pass through to `ohlcv_silver_hc`. Session close is looked up per date from `exchange_calendars` NYSE calendar to handle early-close days correctly.

### Minute to Daily Aggregation

Applied in Silver layer (`ohlcv_daily_silver_hc` — `ohlcv_silver_dlt.py`) as a **materialized view**: `aggregate_minute_to_daily` groups the full minute Silver table to one row per `(symbol, date)`, and on serverless the engine refreshes it incrementally (recomputing only changed `(symbol, date)` groups). Gold `fact_daily_market_hc` reads these pre-aggregated daily rows directly as raw OHLCV, eliminating a full minute-level scan at Gold compute time. Daily returns are not derived on the raw fact (a split-day move on raw close is mechanical, not economic); the split-safe `prev_adj_close` lives in `fact_daily_market_adjusted_hc`.

| Metric | Aggregation |
|--------|-------------|
| `open` | First minute's open (min `start_timestamp` struct) |
| `high` | MAX(high) |
| `low` | MIN(low) |
| `close` | Last minute's close (max `start_timestamp` struct) |
| `volume` | SUM(volume) |

### Derived metrics (Silver / Gold)

| Metric | Table | Implementation |
|--------|-------|----------------|
| Price Change % | `fact_daily_market_adjusted_hc` | `((adj_close - prev_adj_close) / prev_adj_close) * 100` — split-safe; computed by consumers from the adjusted columns (dashboard / `ANALYTICS_QUESTIONS.md`), not materialized on the raw fact |
| Split-adjusted OHLCV | `fact_daily_market_adjusted_hc` | `adj_* = raw * price_factor` (price), `adj_volume = volume / price_factor`; `price_factor` from the first split after each date (`split_adjust_spark.py`) |
| Split-adjusted minute OHLCV | `fact_minute_market_adjusted_hc` | Same factor logic at minute grain; read by the dashboard intraday chart (keeps the 2-day horizon continuous across splits) |
| `prev_adj_close` | `fact_daily_market_adjusted_hc` | `lag(adj_close)` over symbol/date window — powers the `no_unexplained_gap` DQ check |
| `close_5d` / `close_21d` / `close_63d` / `close_126d` / `close_252d` | `fact_daily_market_adjusted_hc` | `lag(adj_close, N)` over symbol/date window (`daily_metrics_spark.py`) — stored period-return anchors; the dashboard screener/watchlist derive 1W–1Y gains from them as point reads instead of query-time window scans |
| `rvol_20d` | `fact_daily_market_adjusted_hc` | `adj_volume ÷ avg(adj_volume)` over the 20 prior trading days (`daily_metrics_spark.py`); NULL unless a full 20-day base exists, so young tickers read "—" in both dashboard surfaces |

> **Consumer note:** for any cross-time price/return (charts, period gains), read the `adj_*` columns from `fact_daily_market_adjusted_hc`. No raw `price_change_pct` is materialized — a raw split-day move is a mechanical drop, not an economic return; derive the daily change from `adj_close`/`prev_adj_close` instead. All adjusted values are **price-return only** — split-adjusted but not dividend-adjusted (no cash-dividend feed).
