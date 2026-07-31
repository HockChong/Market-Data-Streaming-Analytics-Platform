# Data Dictionary - Market Data Platform
## Bronze, Silver, and Gold Layer Schemas

---

## BRONZE LAYER (Raw Data)

### Table: `bronze/streaming` (Streaming OHLCV from Kafka)
**Source**: Polygon.io WebSocket (**delayed feed, ~15 min** — not true real-time) → Kafka → Databricks Streaming (plus batch gap backfills from the Polygon REST aggregates API via `rest_aggs_backfill.py`)
**Format**: Delta Lake
**Partition**: `date`
**Location**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol (e.g., AAPL, TSLA) |
| `event_type` | STRING | NO | Event type identifier (always "AM" for aggregate-per-minute) |
| `open` | DOUBLE | NO | Opening price for 1-minute window |
| `high` | DOUBLE | NO | Highest price within 1-minute window |
| `low` | DOUBLE | NO | Lowest price within 1-minute window |
| `close` | DOUBLE | NO | Closing price for 1-minute window |
| `volume` | BIGINT | NO | Trading volume within 1-minute window |
| `start_timestamp` | BIGINT | NO | Window start time (Unix timestamp milliseconds) |
| `end_timestamp` | BIGINT | NO | Window end time (Unix timestamp milliseconds) |
| `accumulated_volume` | BIGINT | YES | Today's total accumulated volume (optional) |
| `otc` | BOOLEAN | YES | Over-the-counter indicator (optional) |
| `official_open` | DOUBLE | YES | Today's official opening price (optional) |
| `average_trade_size` | DOUBLE | YES | Average trade size for this window (optional) |
| `transactions` | BIGINT | YES | Number of transactions in the window (NULL for WebSocket rows — not provided by Polygon AM events; populated for REST-backfill rows) |
| `ingestion_timestamp` | BIGINT | NO | Producer ingestion timestamp — from_avro decodes Avro `timestamp-millis` logicalType to Spark TIMESTAMP, then `.cast("long")` normalizes to BIGINT (epoch seconds); REST-backfill rows write epoch seconds directly; Silver dedup casts it back to TIMESTAMP for sequence ordering |
| `kafka_ingestion_timestamp` | TIMESTAMP | YES | Kafka message timestamp (when message was ingested by Kafka) |
| `topic` | STRING | YES | Kafka topic name |
| `partition` | INT | YES | Kafka partition number |
| `offset` | BIGINT | YES | Kafka message offset |
| `processing_timestamp` | TIMESTAMP | NO | Bronze layer processing timestamp |
| `correlation_id` | STRING | NO | Correlation ID for tracking |
| `source` | STRING | NO | Source identifier ("polygon_kafka_delayed_streaming"; "polygon_rest_backfill" for REST gap backfills) |
| `ts_unit` | STRING | NO | Epoch unit stamped by producer (always "ms" for streaming) |
| `date` | DATE | NO | Date extracted from start_timestamp (Eastern Time) |

---

### Table: `bronze/historical` (Historical OHLCV from Polygon Flat Files)
**Source**: Polygon.io S3 Flat Files → Databricks Batch
**Format**: Delta Lake
**Partition**: `date`
**Location**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol |
| `event_type` | STRING | NO | Event type (always "AM") |
| `open` | DOUBLE | NO | Opening price |
| `high` | DOUBLE | NO | Highest price |
| `low` | DOUBLE | NO | Lowest price |
| `close` | DOUBLE | NO | Closing price |
| `volume` | BIGINT | NO | Trading volume |
| `start_timestamp` | BIGINT | NO | Window start (Unix timestamp milliseconds) |
| `end_timestamp` | BIGINT | NO | Window end (start + 60000ms) |
| `transactions` | INT | YES | Number of transactions (from CSV) |
| `otc` | BOOLEAN | YES | Over-the-counter indicator (null for historical) |
| `ingestion_timestamp` | TIMESTAMP | NO | Bronze ingestion timestamp |
| `processing_timestamp` | TIMESTAMP | NO | Bronze processing timestamp |
| `correlation_id` | STRING | NO | Correlation ID for tracking |
| `source` | STRING | NO | Source identifier ("polygon_flatfiles_s3") |
| `ts_unit` | STRING | NO | Epoch unit stamped at ingest (always "ms" for flat files) |
| `date` | DATE | NO | Date extracted from start_timestamp (Eastern Time) |

---

### Table: `bronze/news` (News Articles from Polygon API)
**Source**: Polygon.io News API → Databricks Batch
**Format**: Delta Lake
**Partition**: `date`
**Location**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/news`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `article_id` | STRING | NO | Unique article identifier |
| `title` | STRING | YES | Article title |
| `description` | STRING | YES | Article description/summary |
| `author` | STRING | YES | Article author |
| `published_utc` | STRING | YES | Published timestamp (UTC, ISO format string) |
| `article_url` | STRING | YES | Full article URL |
| `amp_url` | STRING | YES | AMP (Accelerated Mobile Pages) URL |
| `image_url` | STRING | YES | Article image URL |
| `tickers` | ARRAY<STRING> | YES | Array of associated ticker symbols |
| `keywords` | ARRAY<STRING> | YES | Array of article keywords |
| `insights` | ARRAY<STRUCT> | YES | Array of sentiment insights (see below) |
| `publisher_name` | STRING | YES | Publisher name |
| `publisher_homepage_url` | STRING | YES | Publisher homepage URL |
| `publisher_logo_url` | STRING | YES | Publisher logo URL |
| `publisher_favicon_url` | STRING | YES | Publisher favicon URL |
| `published_timestamp` | TIMESTAMP | YES | Parsed published timestamp |
| `ingestion_timestamp` | TIMESTAMP | NO | Bronze ingestion timestamp |
| `processing_timestamp` | TIMESTAMP | NO | Bronze processing timestamp |
| `correlation_id` | STRING | NO | Correlation ID for tracking |
| `source` | STRING | NO | Source identifier ("polygon_news_api") |
| `date` | DATE | NO | Date extracted from published_timestamp (Eastern Time) |
| `num_tickers` | INT | NO | Count of associated tickers |
| `tickers_str` | STRING | NO | Comma-separated ticker string |

**Nested Structure: `insights` Array**
- `sentiment` (STRING): Sentiment classification (bullish/bearish/neutral)
- `sentiment_reasoning` (STRING): Reasoning for sentiment
- `ticker` (STRING): Associated ticker symbol

---

### Table: `bronze/ticker_details` (Ticker Reference Data from Polygon API)
**Source**: Polygon.io Ticker Details API → Databricks Batch
**Format**: Delta Lake
**Partition**: `snapshot_date` (one partition per daily run; Gold `dim_ticker_hc` reads `max(snapshot_date)`)
**Location**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/ticker_details`
**Universe**: active US stocks **plus** any delisted/renamed symbols that still appear in our Bronze OHLCV history (those get `active=false`). Bounding the delisted pull to symbols we actually have price data for keeps the silent-drop ("survivorship") of delisted names out of `dim_ticker_hc` without fetching the full Polygon delisted universe.

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol (e.g., AAPL, TSLA) |
| `name` | STRING | YES | Company name |
| `type` | STRING | YES | Security type (CS=Common Stock, ETF, etc.) |
| `active` | BOOLEAN | YES | Whether ticker is currently trading |
| `primary_exchange` | STRING | YES | Primary exchange code (XNYS, XNAS, etc.) |
| `sic_code` | STRING | YES | SIC industry code |
| `sic_description` | STRING | YES | SIC industry description |
| `market_cap` | DOUBLE | YES | Market capitalization |
| `list_date` | STRING | YES | IPO / listing date (nullable; cast to DATE in Gold `dim_ticker_hc`) |
| `ingestion_timestamp` | TIMESTAMP | NO | Bronze ingestion timestamp |
| `processing_timestamp` | TIMESTAMP | NO | Bronze processing timestamp |
| `correlation_id` | STRING | NO | Correlation ID for tracking |
| `source` | STRING | NO | Source identifier ("polygon_ticker_details_api") |
| `snapshot_date` | DATE | NO | Partition key — one partition per daily run; enables point-in-time recovery |

---

### Table: `bronze/splits` (Stock Split Events from Polygon API)
**Source**: Polygon.io `/stocks/v1/splits` → Databricks Batch
**Format**: Delta Lake
**Partition**: `snapshot_date` (one partition per run; Gold `dim_split_hc` reads `max(snapshot_date)`)
**Location**: `/Volumes/tabular/dataexpert/hc_market_data/bronze/splits`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `id` | STRING | YES | Unique split-event identifier from Polygon |
| `symbol` | STRING | NO | Ticker symbol that executed the split |
| `execution_date` | DATE | YES | Date the split was applied and shares adjusted |
| `split_from` | DOUBLE | YES | Denominator of the split ratio (old shares) |
| `split_to` | DOUBLE | YES | Numerator of the split ratio (new shares) |
| `adjustment_type` | STRING | YES | `forward_split`, `reverse_split`, or `stock_dividend` |
| `historical_adjustment_factor` | DOUBLE | YES | Cumulative factor to offset split effects on historical prices |
| `ingestion_timestamp` | TIMESTAMP | NO | Bronze ingestion timestamp |
| `processing_timestamp` | TIMESTAMP | NO | Bronze processing timestamp |
| `correlation_id` | STRING | NO | Correlation ID for tracking |
| `source` | STRING | NO | Source identifier ("polygon_stocks_splits_api") |
| `snapshot_date` | DATE | NO | Partition key — one partition per run; records the split universe known that day |

---

## SILVER LAYER (Cleaned & Validated Data)

### Table: `ohlcv_silver_hc` (Main OHLCV Silver Table)
**Source**: Bronze streaming + historical (unified streaming)
**Format**: Delta Lake
**Partition**: `date`
**Location**: `tabular.dataexpert.ohlcv_silver_hc` (Unity Catalog)
**DLT Pipeline**: `databricks/silver/ohlcv_silver_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol (1-8 characters, validated) |
| `start_timestamp` | BIGINT | NO | Window start (Unix timestamp, < end_timestamp) |
| `end_timestamp` | BIGINT | NO | Window end (Unix timestamp, > start_timestamp) |
| `open` | DOUBLE | NO | Opening price (> 0, validated) |
| `high` | DOUBLE | NO | Highest price (validated OHLC logic) |
| `low` | DOUBLE | NO | Lowest price (validated OHLC logic) |
| `close` | DOUBLE | NO | Closing price (> 0, validated) |
| `volume` | BIGINT | NO | Trading volume (>= 0, validated) |
| `source` | STRING | NO | Source identifier (streaming/historical) |
| `ingestion_timestamp` | BIGINT | NO | Bronze ingest time (cast to long at Bronze read — streaming is already BIGINT, historical TIMESTAMP is cast); used as dedup tiebreaker in apply_changes |
| `ts_unit` | STRING | NO | Epoch unit; OHLCV is normalized to `ms` at Bronze and enforced via expect_or_fail |
| `source_priority` | INT | NO | Dedup priority: 2=historical, 1=streaming, 0=unknown |
| `date` | DATE | NO | ET trading date (partition key) |
| `source_event_time` | TIMESTAMP | YES | Original event time (renamed from watermark column) |

**Quality Checks Applied**:
- `expect_or_fail`: `valid_timestamps` (start_timestamp < end_timestamp), `valid_start_timestamp` (start_timestamp > 0), `required_fields` (symbol/start_timestamp/source IS NOT NULL), `known_ts_unit` (ts_unit = 'ms')
- WAP filter (quarantine): positive prices, valid OHLC logic, non-negative volume
- Deduplication: `apply_changes` MERGE on (symbol, start_timestamp) with source_priority sequence (flat-file wins)
- Market hours filter: only regular trading hours (9:30 AM - 4:00 PM ET)

---

### Table: `ohlcv_silver_quarantine_hc` (WAP: Rejected OHLCV Records)
**Source**: `bronze_unified` (records failing WAP validation)
**Format**: Delta Lake
**Partition**: `date` (ZORDER: `symbol`, `rejection_reason`)
**Location**: `tabular.dataexpert.ohlcv_silver_quarantine_hc`
**DLT Pipeline**: `databricks/silver/ohlcv_silver_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | YES | Ticker symbol |
| `open` | DOUBLE | YES | Opening price |
| `high` | DOUBLE | YES | Highest price |
| `low` | DOUBLE | YES | Lowest price |
| `close` | DOUBLE | YES | Closing price |
| `volume` | BIGINT | YES | Trading volume |
| `start_timestamp` | BIGINT | YES | Window start timestamp |
| `end_timestamp` | BIGINT | YES | Window end timestamp |
| `source` | STRING | YES | Data source identifier |
| `otc` | BOOLEAN | YES | Over-the-counter indicator |
| `ingestion_timestamp` | BIGINT | YES | Bronze ingest time (epoch ms, cast to long at Bronze read) |
| `ts_unit` | STRING | YES | Epoch unit: s, ms, or ns |
| `rejection_reason` | STRING | NO | Why the record was rejected (ZORDER column) |
| `date` | DATE | NO | Date (Eastern Time, partition key) |
| `quarantined_at` | TIMESTAMP | NO | When the record was quarantined |

**Rejection Reasons**: `invalid_price_positive`, `invalid_ohlc_logic`, `invalid_volume`

---

### Table: `wap_audit_log_hc` (WAP: OHLCV Quality Audit)
**Source**: `bronze_unified_hc` (pre-dedup Bronze) for validity/rejection metrics; `ohlcv_silver_hc` (deduped, market-hours-filtered) for `session_bars` — both numerator and denominator of the rejection rate read from the same Bronze source so Kafka replay does not skew it, while `session_bars` reads deduped Silver so replay duplicates cannot mask a gap
**Format**: Delta Lake
**Clustering**: liquid clustering (`cluster_by`) on `audit_date` — one row per date, so date partitioning would create one tiny file per partition
**Location**: `tabular.dataexpert.wap_audit_log_hc`
**DLT Pipeline**: `databricks/silver/ohlcv_silver_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `audit_date` | DATE | NO | Date being audited (cluster key) |
| `valid_count` | BIGINT | NO | `total_count` − `rejected_count`. Counts pre-dedup Bronze landing events that passed validation, **not** distinct bars — a Kafka replay or historical/streaming overlap inflates it, so it will not reconcile to `COUNT(*)` of deduped `ohlcv_silver_hc` |
| `rejected_count` | BIGINT | NO | Pre-dedup Bronze rows that failed validation. **Raw** count (not deduped), so it does **not** equal `COUNT(*)` of `ohlcv_silver_quarantine_hc` (which is deduped by symbol + start_timestamp + source) |
| `total_count` | BIGINT | NO | All pre-dedup Bronze landing events processed that day, duplicates included — a landing-event count, not a distinct-bar count |
| `rejection_rate_pct` | DOUBLE | NO | Percentage of records rejected |
| `rejected_price_positive` | BIGINT | NO | Count rejected for invalid prices |
| `rejected_ohlc_logic` | BIGINT | NO | Count rejected for OHLC logic |
| `rejected_volume` | BIGINT | NO | Count rejected for invalid volume |
| `quality_gate_passed` | BOOLEAN | NO | True if rejection_rate < 1% |
| `quality_gate_warning` | BOOLEAN | NO | True if rejection_rate >= 0.5% |
| `session_bars` | BIGINT | YES | Bars in that day's fullest session (~390 normal, ~210 early-close); drives the warn-only `session_complete` expectation — far lower on a trading day flags a market-wide outage |
| `audit_timestamp` | TIMESTAMP | NO | When audit was computed |
| `pipeline_name` | STRING | NO | Pipeline identifier ("ohlcv_silver_pipeline") |

**Completeness check** (`session_complete`, warn-only, never halts): `session_bars` is the most bars any single symbol reached that day, read from deduped Silver. The gate warns when it falls below 195 — half of `EXPECTED_BARS_PER_DAY` (390) — on a trading day, a coarse market-wide-gap signal that tolerates early-close half-days (~210). NULL (no Silver rows yet) and a 2-day grace window are exempt. Blind spot: a fully-absent symbol-day (zero bars) is not separately counted.

**Validation scope & known limitations** (what the rejection metrics do *not* catch):
- **Structural checks only.** The three rules (`invalid_price_positive`, `invalid_ohlc_logic`, `invalid_volume`) validate a bar's internal shape — positive prices and OHLC ordering. They do **not** detect inter-bar problems: price spikes / fat-fingers, stale or frozen prints, trading halts, or unadjusted splits. `valid_volume` is a floor check (`volume >= 0`) only, so it effectively never rejects and zero-volume bars pass.
- **NULL OHLCV cannot occur here.** `open`/`high`/`low`/`close`/`volume` are non-nullable in the Avro contract (`schemas/avro/ohlcv_aggregate.avsc`); a record missing them fails deserialization and lands in the Bronze dead-letter quarantine, never reaching this audit.
- **A fully-empty or all-rejected trading day is invisible.** Rows exist only for dates with ≥1 Bronze row, so a zero-data day produces no row to fail and its `session_bars` would be NULL (exempt). Catching "a trading day with no usable data" would require a trading calendar (intentionally out of scope) — watch for a missing `audit_date` on a known trading day.

---

### Table: `news_silver_hc` (Main News Silver Table)
**Source**: Bronze news
**Format**: Delta Lake
**Partition**: `published_date`
**Location**: `tabular.dataexpert.news_silver_hc`
**DLT Pipeline**: `databricks/silver/news_silver_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `article_id` | STRING | NO | Unique article identifier (validated, non-null) |
| `title` | STRING | YES | Article title |
| `description` | STRING | YES | Article description |
| `author` | STRING | YES | Article author |
| `published_utc` | STRING | NO | Published timestamp (UTC, validated non-null) |
| `article_url` | STRING | YES | Article URL (validated, must start with 'http') |
| `amp_url` | STRING | YES | AMP URL |
| `image_url` | STRING | YES | Image URL |
| `tickers` | ARRAY<STRING> | YES | Array of associated tickers |
| `keywords` | ARRAY<STRING> | YES | Array of keywords |
| `insights` | ARRAY<STRUCT> | YES | Array of sentiment insights |
| `publisher_name` | STRING | YES | Publisher name |
| `publisher_homepage_url` | STRING | YES | Publisher homepage |
| `publisher_logo_url` | STRING | YES | Publisher logo |
| `publisher_favicon_url` | STRING | YES | Publisher favicon |
| `published_timestamp` | TIMESTAMP | YES | Parsed published timestamp |
| `ingestion_timestamp` | TIMESTAMP | YES | Bronze ingestion timestamp |
| `processing_timestamp` | TIMESTAMP | YES | Bronze processing timestamp |
| `correlation_id` | STRING | YES | Correlation ID |
| `source` | STRING | YES | Source identifier |
| `date` | DATE | YES | Bronze date |
| `num_tickers` | INT | YES | Count of tickers (from Bronze) |
| `tickers_str` | STRING | YES | Comma-separated tickers (from Bronze) |
| `published_date` | DATE | NO | Date from published_utc (Eastern Time, partition key) |
| `cleaned_title` | STRING | YES | Trimmed and normalized title |
| `cleaned_description` | STRING | YES | Trimmed and normalized description |
| `has_description` | BOOLEAN | NO | True if description >= 20 chars |
| `has_image` | BOOLEAN | NO | True if image_url exists |
| `tickers_count` | INT | NO | Number of associated tickers |
| `silver_processing_timestamp` | TIMESTAMP | NO | Silver processing timestamp |

**Quality Checks Applied**:
- `expect_or_fail`: article_id IS NOT NULL, published_utc IS NOT NULL
- WAP filter (quarantine): title non-empty (after trim), URL starts with 'http', published <= ingestion
- Deduplication: `apply_changes` MERGE on `article_id` (sequence_by=ingestion_timestamp; later ingestion wins for article corrections)

---

### Table: `news_silver_quarantine_hc` (WAP: Rejected News Records)
**Source**: `bronze_news_source` (records failing WAP validation)
**Format**: Delta Lake
**Partition**: `published_date` (ZORDER: `article_id`, `rejection_reason`)
**Location**: `tabular.dataexpert.news_silver_quarantine_hc`
**DLT Pipeline**: `databricks/silver/news_silver_dlt.py`

All Bronze news columns are preserved, plus:

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `rejection_reason` | STRING | NO | Why the record was rejected (ZORDER column) |
| `published_date` | DATE | YES | Date from published_utc (partition key) |
| `quarantined_at` | TIMESTAMP | NO | When the record was quarantined |

**Rejection Reasons**: `invalid_title`, `invalid_url`, `invalid_timestamp_order`

---

### Table: `ohlcv_daily_silver_hc` (Pre-Aggregated Daily OHLCV)
**Source**: `ohlcv_silver_hc` (full minute Silver table)
**Format**: Delta Lake
**Write pattern**: Materialized view (`@dlt.table` batch aggregation); serverless incremental refresh
**Clustering**: liquid clustering (`cluster_by`) on `date, symbol`
**Location**: `tabular.dataexpert.ohlcv_daily_silver_hc`
**DLT Pipeline**: `databricks/silver/ohlcv_silver_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol |
| `date` | DATE | NO | ET trading date, cluster key |
| `open` | DOUBLE | YES | First minute's open price for the day |
| `high` | DOUBLE | YES | Day's highest 1-min high |
| `low` | DOUBLE | YES | Day's lowest 1-min low |
| `close` | DOUBLE | YES | Last minute's close price for the day |
| `volume` | BIGINT | YES | Sum of all 1-min volumes |

**Grain**: One row per (symbol, date)

**Purpose**: Pure grain transformation (Silver concern). Gold `fact_daily_market_hc` reads this table instead of raw 1-min `ohlcv_silver_hc`, reducing the Gold scan from ~420 M rows to ~2.9 M rows per lookback window.

**Incremental refresh (serverless):** the MV is a pure `groupBy(symbol, date)` aggregation
(`min`/`max`/`sum`), so on serverless the engine (Enzyme) refreshes it incrementally —
recomputing only the `(symbol, date)` groups whose minute rows changed, with full recompute
as the cost-based fallback. There is no dirty-date logic, staging table, or `apply_changes`;
the MV reads the full minute Silver table (no `current_date()` predicate) so the query stays
deterministic and incrementally maintainable. See `[DAILY_ROLLUP_DESIGN.md](DAILY_ROLLUP_DESIGN.md)`.

**Orchestration (Layer 3):** DLT completes the `ohlcv_silver_hc` minute MERGE before the daily MV refreshes in the same update. Gold runs in a separate pipeline after Silver succeeds.

**Intraday behavior:** when Silver triggers every 5 minutes during market hours, today's row is a *rolling snapshot* (open fixed at session start, close/volume grow until session end).

**Deploy note:** a streaming table cannot become a materialized view in place — drop the existing `ohlcv_daily_silver_hc` once before the first run materializes it as an MV. That first run is a full recompute; subsequent runs are incremental.

---

## GOLD LAYER (Star Schema)

### Table: `dim_date_hc` (Date Dimension)
**Source**: Generated date sequence (2020-01-01 to current year + 5) + NYSE trading calendar
**Format**: Delta Lake
**Partition**: None (small dimension)
**Location**: `tabular.dataexpert.dim_date_hc`
**DLT Pipeline**: `databricks/gold/dim_date_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `date` | DATE | NO | PK, calendar date |
| `year` | INT | NO | Year (e.g., 2024) |
| `quarter` | INT | NO | Quarter 1-4 |
| `month` | INT | NO | Month 1-12 |
| `day_of_week` | INT | NO | 1=Sunday, 2=Monday, ..., 7=Saturday |
| `day_name` | STRING | NO | Day name (Monday, Tuesday, etc.) |
| `week_of_year` | INT | NO | ISO week number 1-53 |
| `month_name` | STRING | NO | Month name (January, February, etc.) |
| `is_weekend` | BOOLEAN | NO | Saturday or Sunday |
| `is_month_end` | BOOLEAN | NO | Last calendar day of month |
| `is_quarter_end` | BOOLEAN | NO | Last calendar day of quarter |
| `is_options_expiry` | BOOLEAN | NO | Third Friday of month (monthly OpEx), holiday-adjusted — if the third Friday is an NYSE holiday (e.g. Good Friday), rolls back to the prior trading day |
| `is_trading_day` | BOOLEAN | NO | NYSE trading day (from exchange_calendars library) |

**Quality Checks**: `expect_or_fail("pk_date_not_null", "date IS NOT NULL")`
**Grain**: One row per calendar date

---

### Table: `dim_ticker_hc` (Ticker Dimension)
**Source**: `bronze/ticker_details` — latest `snapshot_date` partition only (column mapping in `databricks/utils/ticker_details_dim_spark.py`). Includes active **and** delisted/inactive names referenced by our OHLCV history (`is_active` distinguishes them).
**Format**: Delta Lake
**Partition**: None (small dimension — ~20K rows: active US stocks plus delisted/renamed names still referenced by OHLCV history; see [screenshots/README.md](screenshots/README.md) for a live pipeline run)
**Location**: `tabular.dataexpert.dim_ticker_hc`
**DLT Pipeline**: `databricks/gold/dim_ticker_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Primary key, ticker symbol (e.g., AAPL) |
| `company_name` | STRING | YES | Full company name |
| `type` | STRING | YES | Security type (CS=Common Stock, ETF, ADRC, etc.) |
| `sector` | STRING | YES | Business sector (Technology, Healthcare, etc.) |
| `industry` | STRING | YES | SIC industry description |
| `exchange` | STRING | YES | Primary exchange (NYSE, NASDAQ, ARCX, etc.); may be null for delisted names (kept, not dropped — see Quality Checks) |
| `market_cap_category` | STRING | YES | Size classification (Mega/Large/Mid/Small/Micro/Nano Cap; Unknown if market cap is null) |
| `is_active` | BOOLEAN | YES | Currently trading status (`false` = delisted/inactive, retained for survivorship-free history) |
| `list_date` | DATE | YES | IPO / listing date (nullable) |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, 1-8 chars
- `expect` (warn): exchange IS NOT NULL — logged, **not dropped**, so delisted names with a null exchange survive into the dimension
- `expect` (warn): company_name IS NOT NULL
- `expect` (warn): is_active IS NOT NULL — logged, not dropped

**Grain**: One row per ticker symbol. Universe is active names plus delisted/inactive symbols still referenced by our OHLCV history; attributes are current-state (Type 1, no point-in-time history).

---

### Table: `dim_split_hc` (Stock Split Dimension)
**Source**: `bronze/splits` — latest `snapshot_date` partition only (projection in `databricks/utils/split_adjust_spark.py`)
**Format**: Delta Lake
**Partition**: None (small dimension)
**Location**: `tabular.dataexpert.dim_split_hc`
**DLT Pipeline**: `databricks/gold/dim_split_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | Ticker symbol that executed the split (FK to dim_ticker_hc) |
| `execution_date` | DATE | NO | Date the split was applied and shares adjusted |
| `split_from` | DOUBLE | NO | Denominator of the split ratio (old shares) |
| `split_to` | DOUBLE | NO | Numerator of the split ratio (new shares) |
| `adjustment_type` | STRING | YES | `forward_split`, `reverse_split`, or `stock_dividend` |
| `historical_adjustment_factor` | DOUBLE | NO | Cumulative factor applied to prices before the next split |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, execution_date IS NOT NULL, historical_adjustment_factor > 0
- Pre-filter: `latest_splits_snapshot_to_dim_df` drops rows with null/non-positive factor before they reach the DLT expectation — the `expect_or_fail` is a safety net

**Grain**: One row per split event (symbol, execution_date)

---

### Table: `fact_daily_market_hc` (Daily Market Fact)
**Source**: `ohlcv_daily_silver_hc` (pre-aggregated daily Silver table — see Silver layer)
**Format**: Delta Lake
**Cluster By**: `date`, `symbol` (liquid clustering; no Hive partitioning)
**Location**: `tabular.dataexpert.fact_daily_market_hc`
**DLT Pipeline**: `databricks/gold/fact_daily_market_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | PK (part 1), FK to dim_ticker_hc |
| `date` | DATE | NO | PK (part 2), FK to dim_date_hc, clustering key |
| `open` | DOUBLE | YES | First minute's open price |
| `high` | DOUBLE | YES | Day's highest price |
| `low` | DOUBLE | YES | Day's lowest price |
| `close` | DOUBLE | YES | Last minute's close price |
| `volume` | BIGINT | YES | Total daily volume |
| `processing_timestamp` | TIMESTAMP | NO | When this Gold row was written |
| `correlation_id` | STRING | NO | One UUID per DLT pipeline execution |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, date IS NOT NULL
- OHLC validity enforced at Silver via `ohlcv_silver_quarantine_hc`; Gold enforces PK columns only

**Grain**: One row per (symbol, date)

---

### Table: `fact_daily_market_adjusted_hc` (Split-Adjusted Daily Market Fact)
**Source**: `fact_daily_market_hc` (raw daily bars) + `dim_split_hc` (split factors) — join in `databricks/utils/split_adjust_spark.py`
**Format**: Delta Lake
**Cluster By**: `date`, `symbol`
**Location**: `tabular.dataexpert.fact_daily_market_adjusted_hc`
**DLT Pipeline**: `databricks/gold/dim_split_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | PK (part 1), FK to dim_ticker_hc |
| `date` | DATE | NO | PK (part 2), FK to dim_date_hc |
| `open` | DOUBLE | YES | Raw opening price (unadjusted, from fact_daily_market_hc) |
| `high` | DOUBLE | YES | Raw high price |
| `low` | DOUBLE | YES | Raw low price |
| `close` | DOUBLE | YES | Raw closing price |
| `volume` | BIGINT | YES | Raw daily volume |
| `processing_timestamp` | TIMESTAMP | NO | When this Gold row was written |
| `correlation_id` | STRING | NO | One UUID per DLT pipeline execution (inherited from raw fact) |
| `adj_open` | DOUBLE | YES | Split-adjusted opening price (`open * price_factor`) |
| `adj_high` | DOUBLE | YES | Split-adjusted high price |
| `adj_low` | DOUBLE | YES | Split-adjusted low price |
| `adj_close` | DOUBLE | YES | Split-adjusted closing price |
| `adj_volume` | BIGINT | YES | Split-adjusted volume (`volume / price_factor`) |
| `price_factor` | DOUBLE | YES | Cumulative adjustment factor applied to this row (1.0 if on/after the latest split) |
| `prev_adj_close` | DOUBLE | YES | Previous-day `adj_close` (powers the unexplained-gap DQ check) |
| `close_5d` | DOUBLE | YES | `adj_close` 5 trading days earlier (1W-gain anchor); NULL until 5 prior rows exist |
| `close_21d` | DOUBLE | YES | `adj_close` 21 trading days earlier (1M-gain anchor) |
| `close_63d` | DOUBLE | YES | `adj_close` 63 trading days earlier (3M-gain anchor) |
| `close_126d` | DOUBLE | YES | `adj_close` 126 trading days earlier (6M-gain anchor) |
| `close_252d` | DOUBLE | YES | `adj_close` 252 trading days earlier (1Y-gain anchor) |
| `rvol_20d` | DOUBLE | YES | `adj_volume` ÷ 20-day prior average `adj_volume`; NULL unless a full 20-day base exists (`daily_metrics_spark.py`) |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, date IS NOT NULL
- `expect` (warn): `no_unexplained_gap` — a large day-over-day move on `adj_close` with no split usually means a missing split; surfaces in the DLT event log

**Grain**: One row per (symbol, date) — same row set as `fact_daily_market_hc`, with adjusted columns and rolling serving metrics added
**Recomputed**: Full materialized-view recompute each run (a split rewrites a symbol's entire history; append-only cannot express it) — this also keeps the stored lag/RVOL metrics consistent after a split
**Serving metrics**: `close_5d`..`close_252d` and `rvol_20d` are materialized here (`databricks/utils/daily_metrics_spark.py`) so the dashboard screener/watchlist read single-date point queries instead of re-deriving window functions over a 400-day scan; both surfaces share one definition of each metric
**Consumer note**: For charts and cross-time returns, use `adj_close`/`prev_adj_close` (the daily % change = `(adj_close - prev_adj_close) / prev_adj_close * 100`). No raw `price_change_pct` column is materialized — a raw split-day move is a mechanical drop, not an economic return. **All adjusted values are price-return only** — split-adjusted but **not** dividend-adjusted (no cash-dividend feed exists), so multi-period returns exclude dividends and understate dividend-paying names.

---

### Table: `fact_minute_market_hc` (1-Minute Market Fact)
**Source**: `ohlcv_silver_hc` (market-hours-only 1-minute bars)
**Format**: Delta Lake
**Cluster By**: `date`, `symbol`, `start_timestamp` (liquid clustering; no Hive partitioning)
**Location**: `tabular.dataexpert.fact_minute_market_hc`
**DLT Pipeline**: `databricks/gold/fact_minute_market_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | PK (part 1), FK to dim_ticker_hc |
| `date` | DATE | NO | PK (part 2) / clustering key, trading date (ET) |
| `start_timestamp` | BIGINT | NO | PK (part 3), Unix ms — start of 1-minute bar |
| `open` | DOUBLE | YES | Bar opening price |
| `high` | DOUBLE | YES | Bar highest price |
| `low` | DOUBLE | YES | Bar lowest price |
| `close` | DOUBLE | YES | Bar closing price |
| `volume` | BIGINT | YES | Volume traded in this bar |
| `processing_timestamp` | TIMESTAMP | NO | When this Gold row was written |
| `correlation_id` | STRING | NO | One UUID per DLT pipeline execution |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, start_timestamp IS NOT NULL
- OHLC validity enforced at Silver via `ohlcv_silver_quarantine_hc`

**Grain**: One row per (symbol, start_timestamp) — 390 bars/day during market hours (9:30–16:00 ET)
**Filter applied**: Silver pipeline filters to `[9:30 AM, 4:00 PM)` ET before MERGE — pre-market and after-hours bars excluded

---

### Table: `fact_minute_market_adjusted_hc` (Split-Adjusted 1-Minute Fact)
**Source**: `fact_minute_market_hc` (raw minute bars) + `dim_split_hc` (split factors)
**Format**: Delta Lake
**Cluster By**: `date`, `symbol`, `start_timestamp` (liquid clustering; no Hive partitioning)
**Location**: `tabular.dataexpert.fact_minute_market_adjusted_hc`
**DLT Pipeline**: `databricks/gold/dim_split_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `symbol` | STRING | NO | PK (part 1), FK to dim_ticker_hc |
| `date` | DATE | NO | PK (part 2) / clustering key, trading date (ET) |
| `start_timestamp` | BIGINT | NO | PK (part 3), Unix ms — start of 1-minute bar |
| `open` | DOUBLE | YES | Raw bar opening price (unadjusted) |
| `high` | DOUBLE | YES | Raw bar high price |
| `low` | DOUBLE | YES | Raw bar low price |
| `close` | DOUBLE | YES | Raw bar closing price |
| `volume` | BIGINT | YES | Raw volume traded in this bar |
| `processing_timestamp` | TIMESTAMP | NO | When this Gold row was written (inherited from raw fact) |
| `correlation_id` | STRING | NO | One UUID per DLT pipeline execution (inherited from raw fact) |
| `adj_open` | DOUBLE | YES | Split-adjusted opening price (`open * price_factor`) |
| `adj_high` | DOUBLE | YES | Split-adjusted high price |
| `adj_low` | DOUBLE | YES | Split-adjusted low price |
| `adj_close` | DOUBLE | YES | Split-adjusted closing price |
| `adj_volume` | BIGINT | YES | Split-adjusted volume (`volume / price_factor`) |
| `price_factor` | DOUBLE | YES | Cumulative adjustment factor applied to this row (1.0 if on/after the latest split) |

**Quality Checks**:
- `expect_or_fail`: symbol IS NOT NULL, start_timestamp IS NOT NULL
- OHLC validity enforced upstream at Silver via `ohlcv_silver_quarantine_hc`

**Grain**: One row per (symbol, date, start_timestamp) — same row set as `fact_minute_market_hc`, with adjusted columns added
**Recomputed**: Full materialized-view recompute each run (a split rewrites a symbol's history; append-only cannot express it)
**Consumer note**: The dashboard's intraday chart reads this table's `adj_*` columns (aliased to the raw names). A single session never spans a split, so 1-day mode is visually identical to raw; the reason for adjusted is the **2-day** intraday horizon, which concatenates two sessions and can straddle a split — raw bars would show a mechanical price cliff at the session boundary (e.g. a multi-session VWAP has the same issue). No `prev_adj_close`/gap check — those are daily, cross-session concepts. Price-return only (split-adjusted, not dividend-adjusted).

---

### Table: `fact_news_hc` (News Fact)
**Source**: `news_silver_hc` — one row per article-ticker pair via `explode(tickers)`
**Format**: Delta Lake
**Cluster By**: `published_date`, `symbol` (liquid clustering; no Hive partitioning)
**Location**: `tabular.dataexpert.fact_news_hc`
**DLT Pipeline**: `databricks/gold/fact_news_dlt.py`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| `article_id` | STRING | NO | PK (part 1), unique article identifier |
| `symbol` | STRING | NO | PK (part 2), FK to dim_ticker_hc |
| `published_date` | DATE | NO | FK to dim_date_hc, clustering key |
| `published_utc` | STRING | YES | Exact publication time (UTC) |
| `title` | STRING | NO | Article headline (cleaned) |
| `description` | STRING | YES | Article body (max 500 chars, cleaned) |
| `article_url` | STRING | YES | Link to full article |
| `publisher_name` | STRING | YES | News source |
| `author` | STRING | YES | Article author |
| `processing_timestamp` | TIMESTAMP | NO | When this Gold row was written |
| `correlation_id` | STRING | NO | One UUID per DLT pipeline execution |

**Quality Checks**:
- `expect_or_fail`: article_id IS NOT NULL
- `expect_or_drop`: symbol IS NOT NULL (null symbol from explode is non-auditable source noise — row is dropped, not halted)
- Title/URL/timestamp validity enforced at Silver via `news_silver_quarantine_hc`

**Grain**: One row per (article_id, symbol) — articles with multiple tickers have multiple rows

---

## Notes

### Data Quality Strategy

**Bronze Layer**: No quality checks (immutable audit trail). All raw data preserved.

**Silver Layer** (WAP Pattern):
- `expect_or_fail`: Pipeline fails if violated (critical validations — nulls in PKs, timestamp ordering)
- WAP filter: Invalid records routed to quarantine tables with rejection reasons (replaces silent `expect_or_drop`)

**Gold Layer**:
- `expect_or_fail`: Pipeline fails if PKs are null
- OHLC and news validity enforced upstream at Silver; Gold does not maintain its own quarantine tables

### Partitioning Strategy

- **Bronze**: Partitioned by `date`
- **Silver**: Partitioned by `date` (ohlcv_silver_hc), `published_date` (news_silver_hc)
- **Gold**: Fact tables use liquid clustering (`fact_daily_market_hc`, `fact_daily_market_adjusted_hc`, `fact_minute_market_hc`, `fact_minute_market_adjusted_hc`, `fact_news_hc`); dimension tables unpartitioned

### Z-Order / Clustering Optimization

- **Liquid clustering** (`cluster_by` in the DLT `@dlt.table` decorator, instead of Z-ORDER) is used by all Gold fact tables, plus the Silver `ohlcv_daily_silver_hc` (on `date, symbol`) and `wap_audit_log_hc` (on `audit_date`) — small daily-grain tables where date partitioning would create one tiny file per day.
- **DLT auto-managed Z-ORDER** applies to tables with `pipelines.autoOptimize.zOrderCols` in their table properties: `ohlcv_silver_hc` on `symbol`, `dim_ticker_hc` on `symbol`, `news_silver_hc` on `article_id`, `ohlcv_silver_quarantine_hc` on `symbol, rejection_reason`, `news_silver_quarantine_hc` on `article_id, rejection_reason`, `dim_split_hc` on `symbol`. DLT manages the `OPTIMIZE ... ZORDER BY` runs automatically.

### Unity Catalog Location

All tables are stored in Unity Catalog:
- **Catalog**: `tabular`
- **Schema**: `dataexpert`
- **Full Path**: `tabular.dataexpert.<table_name>`

### Volume Storage

Raw data stored in Unity Catalog Managed Volumes:
- **Base Path**: `/Volumes/tabular/dataexpert/hc_market_data`
- **Bronze**: `/bronze/{streaming|historical|news|ticker_details|splits}`
- **Silver**: Managed by Unity Catalog DLT tables
- **Gold**: Managed by Unity Catalog DLT tables
