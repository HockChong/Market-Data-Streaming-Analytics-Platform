# Quick Start Guide

Get the full pipeline running — Bronze ingestion through the Streamlit dashboard.

## Prerequisites

- Databricks workspace with Unity Catalog enabled
- Polygon.io API key (paid plan for WebSocket + flat files)
- Confluent Cloud Kafka cluster with Schema Registry
- Python 3.11+ installed locally

---

## Step 1: Configure Environment

```bash
cd "Capstone Project"

# Copy environment template and fill in credentials
cp .env.example .env
```

Edit `.env` with your credentials (see `.env.example` for all required keys):
- Polygon API key
- Kafka bootstrap servers + SASL credentials
- Schema Registry URL + API key/secret
- Polygon S3 flat file credentials
- Databricks host + personal access token

### Step 2: Push Secrets to Databricks

```bash
pip install requests python-dotenv
python scripts/add_secrets_rest_api.py
```

**Expected Output:**
```
✓ Successfully added: 13/13 secrets
🎉 All secrets added successfully!
```

### Step 3: Upload to Databricks

**Using Repos (recommended):** Push to GitHub → In Databricks: **Repos** → **Add Repo** → enter GitHub URL.

**Using CLI:**
```bash
databricks workspace import_dir databricks /Users/your-email@domain.com/Capstone-Project/databricks
```

### Step 4: Create Volume Paths

Run `databricks/setup/create_volume_paths.py` once in a Databricks notebook. This creates the directory structure under `/Volumes/tabular/dataexpert/hc_market_data/`.

### Step 5: Test Historical Ingestion

1. Open `bronze/historical_ingestion_flatfiles.py` in Databricks
2. Set widgets: `start_date` = `2024-01-02`, `end_date` = `2024-01-05`
3. Click **Run All**
4. Verify completion in notebook output (~2–3 minutes)

### Step 6: Run Silver DLT Pipeline

1. Go to **Delta Live Tables** → **Create Pipeline**
2. Source: `databricks/silver/ohlcv_silver_dlt.py`
3. Target schema: `tabular.dataexpert`
4. Cluster libraries: add `exchange_calendars`
5. Click **Start** — Silver cleans, deduplicates, and enforces quality on Bronze data

### Step 7: Run Gold DLT Pipeline

1. Create pipeline with all files in `databricks/gold/`
2. Target schema: `tabular.dataexpert`
3. Cluster libraries: add `exchange_calendars`
4. Click **Start** — Gold builds fact and dimension tables from Silver

### Step 8: Add Table Constraints (One-Time)

Run `databricks/setup/add_table_constraints.py` to add PRIMARY KEY and FOREIGN KEY constraints on Gold tables.

---

## Verify Everything Works

### Check Secrets
```python
dbutils.secrets.list(scope="ganhockchong-market-data")
```

### Check Unity Catalog Access
```sql
SHOW VOLUMES IN tabular.dataexpert;
```

### Check Bronze Data
```sql
SELECT * FROM delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/historical` LIMIT 10;

SELECT * FROM delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/news` LIMIT 10;
```

### Check Silver Data
```sql
SELECT COUNT(*) AS total, MIN(date) AS earliest, MAX(date) AS latest
FROM tabular.dataexpert.ohlcv_silver_hc;
```

### Check Gold Pipeline Health
```sql
SELECT correlation_id,
       COUNT(*) AS rows_written,
       COUNT(DISTINCT symbol) AS symbols_covered,
       MIN(date) AS earliest_date,
       MAX(date) AS latest_date,
       MIN(processing_timestamp) AS run_started
FROM tabular.dataexpert.fact_daily_market_hc
GROUP BY correlation_id
ORDER BY run_started DESC
LIMIT 5;
```

A healthy run shows ~500 symbols and consistent row counts. A partial run (pipeline killed mid-write) shows a lower symbol count — re-trigger the Gold pipeline to fix it.

### Check WAP Quality Gate
```sql
SELECT audit_date, total_count, rejection_rate_pct, quality_gate_passed
FROM tabular.dataexpert.wap_audit_log_hc
ORDER BY audit_date DESC
LIMIT 7;
```

---

## Run the Streamlit Dashboard

Once Gold tables are populated:

```bash
pip install -r databricks/dashboard/requirements.txt

streamlit run databricks/dashboard/app.py
```

**Page 1 — Signal Screener** (`localhost:8501`): cross-layer screener joining `fact_daily_market_hc` and `dim_ticker_hc`. Filter by sector, gain range, volume, and date.

**Page 2 — Stock Deep Dive**: single-ticker terminal with OHLCV chart, intraday 1-minute bars (from `fact_minute_market_adjusted_hc`, so the 2-day horizon stays continuous across splits), technical indicators, and latest news from `fact_news_hc`.

**Page 3 — Watchlist**: save favourite tickers and view their latest price, daily change, and screener-style metrics at a glance. The list persists to `watchlist.json` in the dashboard directory so it survives page reloads within the same Databricks App instance.

> The dashboard reads from Databricks SQL via the connection configured in `databricks/dashboard/utils/connection.py`. Ensure your Databricks workspace credentials are set before launching.

---

## Common Commands

### View Bronze Statistics
```sql
SELECT date, COUNT(*) AS rows, COUNT(DISTINCT symbol) AS symbols
FROM delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`
GROUP BY date
ORDER BY date DESC
LIMIT 10;
```

### View Delta Lake History
```sql
DESCRIBE HISTORY delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/historical` LIMIT 5;
```

### Run Local Tests
```bash
pip install -r requirements.txt
pytest
ruff check .
```

---

## What to Set Up Next

After verifying all layers work:

1. Deploy **streaming ingestion** for real-time market data (requires `streaming_producer.py` + `streaming_ingestion.py` during market hours)
2. Schedule **Silver OHLCV DLT** every 5 minutes during market hours; schedule **Gold DLT** after session (depends on Silver success)
3. Set up **news ingestion** on a 15-minute schedule during market hours
4. Schedule weekly **ticker details + splits** refresh
5. **First Silver run:** if migrating from the old streaming `ohlcv_daily_silver_hc`, drop it once (`DROP TABLE tabular.dataexpert.ohlcv_daily_silver_hc`) so it can be recreated as a materialized view; the first run is a full recompute, then serverless refreshes it incrementally

See [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md) for full job configuration details.

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Secret not found | Re-run `python scripts/add_secrets_rest_api.py` |
| Permission denied (Unity Catalog) | Request access from workspace admin |
| No data returned (Polygon API) | Check API key, verify dates are 3+ days old (flat file lag) |
| Kafka connection failed | Verify credentials in Confluent Cloud, check topic exists |
| DLT quality gate failed | Check `wap_audit_log_hc` for rejection reasons |
| Silver pipeline shows 0 rows | Ensure Bronze data exists and pipeline source paths are correct |

---

## File Reference

```
databricks/
├── QUICK_START.md                              ← You are here
├── DEPLOYMENT_GUIDE.md                         ← Full deployment + job scheduling
├── config/
│   ├── __init__.py                             ← Package init
│   ├── base_config.py                          ← Shared config (paths, secrets, market hours)
│   ├── bronze_config.py                        ← Bronze layer config (Kafka, S3, schema registry)
│   ├── silver_config.py                        ← Silver layer config (DLT expectations, WAP)
│   ├── gold_config.py                          ← Gold layer config (rolling windows, partitioning)
│   ├── path_bootstrap.py                       ← Adds config/ to sys.path in notebooks
│   └── simple_logger.py                        ← Structured logger for notebooks
├── bronze/
│   ├── streaming_producer.py                   ← Polygon WebSocket → Kafka producer
│   ├── streaming_ingestion.py                  ← Kafka → Bronze Delta (real-time)
│   ├── historical_ingestion_flatfiles.py       ← S3 flat files → Bronze (bootstrap backfill)
│   ├── incremental_ingestion_flatfiles.py      ← S3 flat files → Bronze (daily fill)
│   ├── news_ingestion.py                       ← Polygon REST → Bronze (news articles)
│   ├── ticker_details_ingestion.py             ← Polygon REST → Bronze (ticker metadata)
│   ├── splits_ingestion.py                     ← Polygon REST → Bronze (stock splits)
│   ├── kafka_replay_backfill.py                ← Kafka batch replay for specific dates
│   └── bronze_utils.py                         ← Shared Bronze helpers
├── silver/
│   ├── ohlcv_silver_dlt.py                     ← DLT: clean, deduplicate, WAP quarantine OHLCV
│   └── news_silver_dlt.py                      ← DLT: clean, deduplicate news articles
├── gold/
│   ├── fact_daily_market_dlt.py                ← Daily OHLCV fact table
│   ├── fact_minute_market_dlt.py               ← 1-minute OHLCV fact table (rolling window)
│   ├── fact_news_dlt.py                        ← News fact table (article × ticker grain)
│   ├── dim_date_dlt.py                         ← Date dimension (calendar + NYSE trading days)
│   ├── dim_ticker_dlt.py                       ← Ticker dimension (sectors, market cap tiers)
│   └── dim_split_dlt.py                        ← Split dimension + adjusted fact tables
├── utils/
│   ├── ohlcv_dedup_spark.py                    ← Deterministic dedup for OHLCV records
│   ├── ohlcv_quarantine_spark.py               ← WAP quarantine logic for OHLCV
│   ├── aggregation_utils.py                    ← Daily bar aggregation from minute data (daily MV)
│   ├── wap_audit_spark.py                      ← WAP audit log writer
│   ├── streaming_transforms.py                 ← Kafka message parsing transforms
│   ├── streaming_ingestion_runtime.py          ← Streaming lifecycle management
│   ├── news_transforms.py                      ← News article cleaning transforms
│   ├── news_quarantine_spark.py                ← WAP quarantine logic for news
│   ├── ticker_details_dim_spark.py             ← Ticker dimension transforms (SIC → sector)
│   ├── ticker_details_helpers.py               ← Polygon API helpers for ticker details
│   ├── split_adjust_spark.py                   ← Split-adjusted price calculations
│   ├── market_cap_classification.py            ← Market cap tier classification
│   └── incremental_flatfiles_runtime.py        ← Date range detection for incremental loads
├── setup/
│   ├── create_volume_paths.py                  ← One-time: create UC volume directories
│   └── add_table_constraints.py                ← One-time: add PK/FK to Gold tables
└── dashboard/
    ├── app.py                                  ← Streamlit entry point
    ├── app.yaml                                ← Deployment config
    ├── requirements.txt                        ← Dashboard-specific dependencies
    ├── pages/
    │   ├── 1_Signal_Screener.py                ← Market screener page
    │   ├── 2_Stock_Deep_Dive.py                ← Single-ticker deep dive
    │   └── 3_Watchlist.py                      ← Saved-ticker watchlist page
    └── utils/
        ├── connection.py                       ← Databricks SQL connection
        ├── theme.py                            ← UI theme constants
        ├── screener_data.py                    ← Screener query logic
        ├── screener_filters.py                 ← Screener filter components
        ├── stock_terminal_data.py              ← Stock terminal query logic
        ├── stock_terminal_charts.py            ← Plotly chart builders
        ├── stock_terminal_indicators.py        ← Technical indicator calculations
        ├── stock_terminal_render.py            ← Terminal layout rendering
        ├── watchlist_data.py                   ← Watchlist query logic
        └── watchlist_store.py                  ← Watchlist persistence (watchlist.json)
```

---

## Support

- **Full Deployment Guide**: [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md)
- **Repository Guide**: [CLAUDE.md](../CLAUDE.md) for development patterns
- **Architecture Docs**: [capstone_proposal.md](../capstone_proposal.md)

**Need Help?** Check the troubleshooting section in [DEPLOYMENT_GUIDE.md](DEPLOYMENT_GUIDE.md).
