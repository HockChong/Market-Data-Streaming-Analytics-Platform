# Silver Layer Entity Relationship Diagram (ERD)

This document contains the conceptual Entity Relationship Diagram for the Silver Layer. The Silver Layer contains cleaned, conformed, and enriched data, along with various derived quality metrics and aggregations.

**Daily OHLCV (`ohlcv_daily_silver_hc`):** materialized view aggregating minute Silver to one row per `(symbol, date)` via a pure `groupBy` (`min`/`max`/`sum`). On serverless the engine refreshes it incrementally — recomputing only changed `(symbol, date)` groups, with full recompute as fallback. DLT runs the minute MERGE before the daily MV refreshes in the same pipeline update (Layer 3 in-pipeline ordering).

```mermaid
erDiagram
    %% =========================================================
    %% OHLCV Pipeline (ohlcv_silver_dlt.py)
    %% =========================================================

    ohlcv_silver_hc {
        string symbol "PK part 1"
        bigint start_timestamp "PK part 2 - Unix ms"
        bigint end_timestamp
        double open
        double high
        double low
        double close
        bigint volume
        string source "polygon_flatfiles_s3, polygon_kafka_delayed_streaming, or polygon_rest_backfill"
        timestamp ingestion_timestamp "Bronze ingest time; used as dedup tiebreaker"
        string ts_unit "Epoch unit: s, ms, or ns"
        int source_priority "2=historical (preferred), 1=streaming, 0=other (e.g. REST backfill)"
        date date "ET trading date, partition key"
        timestamp source_event_time "Original event_time used for watermarking"
    }

    ohlcv_silver_quarantine_hc {
        string symbol "PK part 1"
        bigint start_timestamp "PK part 2"
        bigint end_timestamp
        double open
        double high
        double low
        double close
        bigint volume
        string source
        boolean otc "Over-the-counter flag (preserved from Bronze)"
        timestamp ingestion_timestamp
        string ts_unit "Epoch unit: s, ms, or ns"
        string rejection_reason "ZORDER column: invalid_price_positive, invalid_ohlc_logic, invalid_volume"
        date date "Partition key"
        timestamp quarantined_at
    }

    wap_audit_log_hc {
        date audit_date "PK, cluster key"
        bigint total_count
        bigint rejected_count
        bigint rejected_price_positive
        bigint rejected_ohlc_logic
        bigint rejected_volume
        bigint valid_count
        double rejection_rate_pct
        boolean quality_gate_passed
        boolean quality_gate_warning
        bigint session_bars "Bars in that day's fullest session (~390 normal, ~210 early-close); drives the session_complete warn gate"
        timestamp audit_timestamp
        string pipeline_name
    }

    %% =========================================================
    %% Daily Pre-Aggregation (ohlcv_silver_dlt.py)
    %% Materialized view (serverless incremental refresh); source for Gold fact_daily_market_hc
    %% =========================================================

    ohlcv_daily_silver_hc {
        string symbol "PK part 1 (materialized view)"
        date date "PK part 2, cluster key, ET trading date"
        double open "First minute's open"
        double high "Day's highest 1-min high"
        double low "Day's lowest 1-min low"
        double close "Last minute's close"
        bigint volume "Sum of all 1-min volumes"
    }

    %% =========================================================
    %% News Pipeline (news_silver_dlt.py)
    %% =========================================================

    news_silver_hc {
        string article_id "PK"
        string title
        string description
        string author
        string published_utc
        string article_url
        string amp_url "Nullable"
        string image_url
        array_string tickers
        array_string keywords
        array_struct insights "sentiment, sentiment_reasoning, ticker"
        string publisher_name
        string publisher_homepage_url
        string publisher_logo_url
        string publisher_favicon_url
        timestamp published_timestamp
        timestamp ingestion_timestamp
        timestamp processing_timestamp
        string correlation_id
        string source
        date date
        int num_tickers
        string tickers_str
        date published_date "Partition key"
        string cleaned_title
        string cleaned_description
        boolean has_description
        boolean has_image
        int tickers_count
        timestamp silver_processing_timestamp
    }

    news_silver_quarantine_hc {
        string article_id "PK"
        string title
        string description
        string author
        string published_utc
        string article_url
        string amp_url "Nullable"
        string image_url
        array_string tickers
        array_string keywords
        array_struct insights
        string publisher_name
        string publisher_homepage_url
        string publisher_logo_url
        string publisher_favicon_url
        timestamp published_timestamp
        timestamp ingestion_timestamp
        timestamp processing_timestamp
        string correlation_id
        string source
        date date
        int num_tickers
        string tickers_str
        string rejection_reason "ZORDER column"
        date published_date "Partition key"
        timestamp quarantined_at
    }

    %% =========================================================
    %% Relationships
    %% =========================================================

    %% OHLCV relationships
    ohlcv_silver_hc }o--|| ohlcv_daily_silver_hc : "daily MV: groupBy (symbol, date)"

    %% News relationships
    news_silver_hc ||--o| news_silver_quarantine_hc : "rejected records"
```
