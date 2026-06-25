# Databricks Deployment Guide

Full deployment guide for the Market Data Streaming & Analytics Platform — Bronze through Gold layers and the Streamlit dashboard.

## Architecture Overview

```
Polygon WebSocket → Kafka → Bronze (streaming)  ─┐
                                                   ├─→ Silver DLT → Gold DLT → Streamlit Dashboard
Polygon S3 Flat Files → Bronze (historical)      ─┘
Polygon REST API → Bronze (news, ticker_details, splits)
```

---

## Step 1: Setup Databricks Secrets

From your local machine (repository root):

```bash
# Copy environment template and fill in your credentials
cp .env.example .env

# Install dependencies (if not already)
pip install requests python-dotenv

# Push secrets to Databricks
python scripts/add_secrets_rest_api.py
```

This will:
- Load credentials from `.env` file (or `DATABRICKS_HOST` / `DATABRICKS_TOKEN` env vars)
- Create secret scope `ganhockchong-market-data`
- Add all required secrets (Polygon API, Kafka, Schema Registry, S3 flat files)

**Required `.env` variables** (see `.env.example`):
- `POLYGON_API_KEY` — Polygon.io API key
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD` — Confluent Cloud Kafka
- `SCHEMA_REGISTRY_URL`, `SCHEMA_REGISTRY_API_KEY`, `SCHEMA_REGISTRY_API_SECRET` — Confluent Schema Registry
- `POLYGON_AWS_ACCESS_KEY_ID`, `POLYGON_AWS_SECRET_ACCESS_KEY` — Polygon S3 flat file access
- `DATABRICKS_HOST`, `DATABRICKS_TOKEN` — Databricks workspace

---

## Step 2: Upload Code to Databricks

### Automated: GitHub Actions CD (pull-based) — primary path

On every push to `main`, the **CI** workflow runs (lint, tests, schema validation). If CI
passes, the **CD** workflow (`.github/workflows/cd.yml`) syncs the workspace to the latest
commit with a single command:

```bash
databricks repos update "$DATABRICKS_REPO_PATH" --branch main
```

This is **pull-based**: the Databricks Git folder fetches `main` from GitHub, so mirroring —
including deletions and renames — comes free from `git pull` and there are no orphaned
workspace files to prune. (This replaced an earlier `workspace import-dir --overwrite` push,
which only added/replaced files and left stale ones behind.)

One-time setup so CD can run:

1. Create the Git folder in Databricks (see **Option A** below) linked to this repo on `main`.
   This requires linking a GitHub PAT under your Databricks Git integration user settings so
   the workspace is allowed to pull.
2. Add a GitHub Actions **repo variable** `DATABRICKS_REPO_PATH` pointing at that Git folder's
   workspace path (e.g. `/Workspace/Repos/your-email@domain.com/Capstone-Project`), plus the
   `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secrets the workflow already uses.
3. Point your Databricks Jobs / DLT pipelines at notebook paths inside that Git folder.

### Option A: Databricks Repos (Git folder) — required for CD, and the manual fallback

1. Push code to GitHub
2. In Databricks: **Repos** → **Add Repo** → enter your GitHub URL → select branch
3. Access notebooks directly from the Repos folder. (To deploy manually without CD, use
   **Repos** → the folder's **⋮** → **Pull**, or `databricks repos update` as shown above.)

### Option B: Databricks CLI

```bash
pip install databricks-cli
databricks configure --token

databricks workspace import_dir databricks /Users/your-email@domain.com/Capstone-Project/databricks --overwrite
```

### Option C: Web UI

Upload the entire `databricks/` directory preserving this structure:
- `config/` — all 7 config files (`base_config.py`, `bronze_config.py`, `silver_config.py`, `gold_config.py`, `path_bootstrap.py`, `simple_logger.py`, `__init__.py`)
- `bronze/` — all ingestion notebooks
- `silver/` — DLT pipeline files
- `gold/` — DLT pipeline files
- `utils/` — shared Spark transforms
- `setup/` — one-time setup scripts

---

## Step 3: Create Volume Paths

Run `databricks/setup/create_volume_paths.py` once on a cluster with Unity Catalog access. This creates the required directory structure under `/Volumes/tabular/dataexpert/hc_market_data/`.

Verify access:

```sql
SHOW VOLUMES IN tabular.dataexpert;

SELECT * FROM delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming` LIMIT 1;
```

---

## Step 4: Test Individual Scripts

### Test Configuration

1. Open `config/bronze_config.py` — run all cells
2. Verify output shows Kafka servers, paths, and secret scope

### Test Historical Ingestion (Small Dataset)

1. Open `bronze/historical_ingestion_flatfiles.py`
2. Set widget parameters:
   - `start_date` = `2024-01-02`
   - `end_date` = `2024-01-05` (a few business days)
3. Run all cells
4. Verify records written to `/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`

### Test News Ingestion

1. Open `bronze/news_ingestion.py`
2. Set widget: `INGESTION_MODE` = `realtime`, `LOOKBACK_MINUTES` = `60`
3. Run all cells
4. Verify articles written to `/Volumes/tabular/dataexpert/hc_market_data/bronze/news`

### Test Ticker Details Ingestion

1. Open `bronze/ticker_details_ingestion.py`
2. Run all cells (no parameters needed — fetches all active US tickers)
3. Verify data at `/Volumes/tabular/dataexpert/hc_market_data/bronze/ticker_details`

### Test Splits Ingestion

1. Open `bronze/splits_ingestion.py`
2. Run all cells (no parameters needed — fetches full split history)
3. Verify data at `/Volumes/tabular/dataexpert/hc_market_data/bronze/splits`

---

## Step 5: Deploy DLT Pipelines

### Silver OHLCV Pipeline

> **Note:** `ohlcv_silver_dlt.py` is a Delta Live Tables *pipeline definition*, not a
> standalone script. The `import dlt` decorators and `/Workspace/...` path bootstrap only
> resolve inside a DLT pipeline runtime — running it with `python` or `pytest` will fail.
> Once the pipeline is created (below), run it manually with **Start**, or on a schedule
> via Job 7 (Step 6).

1. Go to **Delta Live Tables** → **Create Pipeline**
2. Name: `Silver OHLCV Pipeline`
3. Source code: `databricks/silver/ohlcv_silver_dlt.py`
4. Target schema: `tabular.dataexpert`
5. Cluster: **Enhanced autoscaling**, min 1 / max 2 workers
6. Cluster libraries: add `exchange_calendars` (required for early-close awareness)
7. Pipeline mode: **Triggered** (see Step 6 for schedule — every 5 minutes during market hours recommended)
8. **Configuration** (Pipeline settings → Configuration): no daily-rollup configuration keys are required. `ohlcv_daily_silver_hc` is a materialized view that re-derives the daily grain from the full minute Silver snapshot each refresh; on serverless the engine refreshes it incrementally.

   **One-time migration:** if a previous deploy created `ohlcv_daily_silver_hc` as a streaming table, drop it once (`DROP TABLE tabular.dataexpert.ohlcv_daily_silver_hc`) before the first run so it can be recreated as a materialized view (a streaming table cannot be converted in place). That first run is a full recompute.

### Silver News Pipeline

1. Create pipeline: `Silver News Pipeline`
2. Source code: `databricks/silver/news_silver_dlt.py`
3. Target schema: `tabular.dataexpert`
4. Pipeline mode: **Triggered**

### Gold Pipeline

1. Create pipeline: `Gold Pipeline`
2. Source code: add all Gold files:
   - `databricks/gold/fact_daily_market_dlt.py`
   - `databricks/gold/fact_minute_market_dlt.py`
   - `databricks/gold/fact_news_dlt.py`
   - `databricks/gold/dim_date_dlt.py`
   - `databricks/gold/dim_ticker_dlt.py`
   - `databricks/gold/dim_split_dlt.py`
3. Target schema: `tabular.dataexpert`
4. Cluster libraries: add `exchange_calendars`
5. Pipeline mode: **Triggered** (run after Silver pipelines complete)

### Add Table Constraints (One-Time)

After DLT pipelines have created all tables, run `databricks/setup/add_table_constraints.py` to add PRIMARY KEY and FOREIGN KEY constraints to Gold tables.

---

## Step 6: Deploy as Databricks Jobs

### Job 1: Streaming Producer (Market Hours)

- Notebook: `bronze/streaming_producer.py`
- Cluster: **Single node**, 4 cores, 14 GB RAM
- Schedule: `25 13 * * 1-5` (9:25 AM ET = 1:25 PM UTC, Mon–Fri)
- The notebook auto-stops at session close + 20 minutes (no stop schedule needed)

### Job 2: Streaming Ingestion (Market Hours)

- Notebook: `bronze/streaming_ingestion.py`
- Cluster: **Single node**, 4 cores, 14 GB RAM
- Schedule: `25 13 * * 1-5` (9:25 AM ET, Mon–Fri)
- Auto-stops after market close + drain mode (handles shutdown internally)

### Job 3: News Ingestion (Two Schedules)

| Schedule | Cron (ET) | LOOKBACK_MINUTES | Purpose |
|----------|-----------|------------------|---------|
| Market Hours | `*/15 9-15 * * 1-5` | 15 | Core ingestion |
| Daily Catch-up | `0 6 * * 1-5` | 60 | Off-hours + weekend gap |

- Notebook: `bronze/news_ingestion.py`
- Cluster: **Single node**, 4 cores, 14 GB RAM

### Job 4: Incremental Flat Files (Daily)

- Notebook: `bronze/incremental_ingestion_flatfiles.py`
- Cluster: **Single node**, 4 cores, 14 GB RAM
- Schedule: `0 7 * * 1-5` (7:00 AM ET, Mon–Fri)
- Auto-detects where last ingestion stopped

### Job 5: Ticker Details + Splits (Weekly)

- Run `bronze/ticker_details_ingestion.py` and `bronze/splits_ingestion.py` as tasks in one job
- Cluster: **Single node**, 4 cores, 14 GB RAM
- Schedule: `0 6 * * 1` (6:00 AM ET, Monday)

### Job 6: Historical Backfill (Manual)

- Notebook: `bronze/historical_ingestion_flatfiles.py`
- Cluster: **Standard**, 2 nodes, 8 cores each
- No schedule — run manually with `start_date` and `end_date` widgets
- Timeout: 2–4 hours depending on date range

### Job 7: Silver OHLCV DLT (Market Hours — every 5 minutes)

- **Type**: Delta Live Tables pipeline trigger (not a notebook)
- **Pipeline**: `Silver OHLCV Pipeline` (`ohlcv_silver_dlt.py`)
- **Schedule**: `*/5 9-16 * * 1-5` (every 5 minutes, 9 AM–4 PM ET, Mon–Fri) — adjust for your timezone in the job UI
- **Prerequisite**: Jobs 1 + 2 (Bronze streaming producer + ingestion) must be **running** during this window so Bronze Delta receives Kafka bars. Silver does not read Kafka directly.
- **Layer 3 (in-pipeline)**: Each DLT update finishes `ohlcv_silver_hc` MERGE **before** `ohlcv_daily_silver_hc` incremental rollup in the same run — no separate daily job needed.

### Job 8: Pipeline Orchestration (Gold + off-hours Silver)

**Intraday:** Job 7 runs Silver every 5 minutes while Bronze streaming is active. Today's daily bar in `ohlcv_daily_silver_hc` is a rolling snapshot until the session ends.

**End-of-day chain** (run after Bronze streaming stops ~4:20 PM ET):

1. Task 1: Trigger Silver OHLCV DLT pipeline (final catch-up for today's session)
2. Task 2: Trigger Silver News DLT pipeline (parallel with Task 1)
3. Task 3: Trigger Gold DLT pipeline (**depends on Tasks 1 + 2** — Gold reads Silver via `spark.read.table`)

Optional morning chain (after flat-file incremental):

1. Task A: `incremental_ingestion_flatfiles.py` (Job 4)
2. Task B: Silver OHLCV DLT (**depends on Task A**) — picks up the new Bronze rows through the minute stream; the daily MV refreshes the affected dates

**Manual historical backfill:**

1. Run Job 6 with `start_date` / `end_date`
2. Trigger Silver OHLCV DLT once (metadata + optional `silver.backfill_start_date` / `silver.backfill_end_date` config)
3. Trigger Gold DLT

**First-time migration:** if an old streaming `ohlcv_daily_silver_hc` exists, drop it once before the first run so it is recreated as a materialized view; that run is a full recompute, then serverless refreshes it incrementally.

---

## Verification Checklist

- [ ] Secrets loaded correctly (`dbutils.secrets.list(scope="ganhockchong-market-data")`)
- [ ] Unity Catalog volume accessible (`/Volumes/tabular/dataexpert/hc_market_data/`)
- [ ] Historical ingestion test successful
- [ ] News ingestion test successful
- [ ] Ticker details ingestion successful
- [ ] Splits ingestion successful
- [ ] Silver OHLCV DLT pipeline runs without errors
- [ ] Silver News DLT pipeline runs without errors
- [ ] Gold DLT pipeline runs without errors
- [ ] Table constraints added (PKs and FKs on Gold tables)
- [ ] Bronze Delta tables exist:
  - `/Volumes/.../bronze/streaming`
  - `/Volumes/.../bronze/historical`
  - `/Volumes/.../bronze/news`
  - `/Volumes/.../bronze/ticker_details`
  - `/Volumes/.../bronze/splits`
- [ ] Silver tables populated: `ohlcv_silver_hc`, `news_silver_hc`, `ohlcv_daily_silver_hc`
- [ ] Gold tables populated: `fact_daily_market_hc`, `fact_minute_market_hc`, `fact_news_hc`, `dim_ticker_hc`, `dim_date_hc`, `dim_split_hc`
- [ ] WAP audit log healthy: `wap_audit_log_hc` shows quality_gate_passed = true
- [ ] Streaming ingestion receiving data (if producer running)
- [ ] Databricks Jobs created and scheduled

---

## Monitoring

### Check Delta Lake Statistics

```sql
DESCRIBE DETAIL delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming`;

DESCRIBE HISTORY delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming` LIMIT 10;

SELECT date, COUNT(*) AS record_count
FROM delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming`
GROUP BY date
ORDER BY date DESC
LIMIT 10;
```

### Check WAP Quality Gate

```sql
SELECT audit_date, total_count, rejected_count, rejection_rate_pct,
       quality_gate_passed, session_bars
FROM tabular.dataexpert.wap_audit_log_hc
ORDER BY audit_date DESC
LIMIT 7;
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

### Monitor Kafka Consumer Lag

1. Open **Confluent Cloud** dashboard
2. Go to **Topics** → `polygon-streaming-ohlcv`
3. Check **Consumer Lag** for consumer group `market-data-consumer-group`

---

## How to Replay Data

### Scenario 1 — Reprocess a specific date range (most common)

**When to use:** Bad source data was corrected in Polygon S3, or a Bronze partition is corrupt.

**How it works:** Re-runs `historical_ingestion_flatfiles.py` for the target date range. Dynamic partition overwrite replaces only those `date` partitions in Bronze. Silver picks up the changes on its next DLT run: minute rows via `apply_changes` MERGE on `(symbol, start_timestamp)`, and the daily materialized view `ohlcv_daily_silver_hc` refreshes the affected `(symbol, date)` groups.

**Steps:**
1. Open `databricks/bronze/historical_ingestion_flatfiles.py`
2. Set widgets: `start_date` and `end_date` to the target range
3. Run all cells
4. Trigger the Silver OHLCV DLT pipeline (optional: set `silver.backfill_start_date` / `silver.backfill_end_date` to the same range if metadata path is unavailable)
5. After Silver succeeds, trigger Gold DLT if daily analytics must refresh immediately

**Verification:**
```sql
DESCRIBE HISTORY delta.`/Volumes/tabular/dataexpert/hc_market_data/bronze/historical`
LIMIT 5;

SELECT date, COUNT(*) AS rows, MAX(ingestion_timestamp) AS latest_ingest
FROM tabular.dataexpert.ohlcv_silver_hc
WHERE date BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'
GROUP BY date ORDER BY date;
```

### Scenario 2 — Replay Kafka from the beginning

**When to use:** The Bronze streaming checkpoint was deleted or corrupted.

**How it works:** The streaming job uses `startingOffsets: "latest"` in `BronzeConfig`. When the checkpoint is deleted, the stream has no memory and starts from `latest`. To replay historical Kafka data, use `kafka_replay_backfill.py` which does a bounded batch read for specific dates.

**Steps:**
1. Stop the streaming ingestion job
2. Delete the checkpoint directories:
   ```
   /Volumes/tabular/dataexpert/hc_market_data/_checkpoints/bronze/streaming_ingestion
   /Volumes/tabular/dataexpert/hc_market_data/_checkpoints/bronze/streaming_ingestion_quarantine
   ```
3. To replay specific dates from Kafka, use `bronze/kafka_replay_backfill.py` with `REPLAY_START_DATE` and `REPLAY_END_DATE` set (preview with `DRY_RUN=True` first)
4. Restart the streaming ingestion job — it resumes from the latest Kafka offset
5. Silver deduplicates minute rows via `apply_changes` MERGE; the daily materialized view refreshes the affected dates on the next Silver trigger

### Scenario 3 — Full DLT pipeline reset (Silver / Gold)

**When to use:** Breaking schema change, or Silver state needs a clean rebuild.

**How it works:** `bronze_unified_hc` has `"pipelines.reset.allowed": "true"`. A full refresh drops streaming state and reprocesses all Bronze data from scratch.

**Steps:**
1. In Databricks: **Delta Live Tables** → open the Silver pipeline
2. Click **⋮ (more options)** → **Full Refresh**
3. After Silver completes, trigger the Gold pipeline normally

> During a full refresh, downstream Gold queries see partial data until the pipeline completes. Schedule during off-hours if the dashboard is active.

### Quick Reference

| Scenario | Mechanism | Bronze touched | Silver action | Scope |
|---|---|---|---|---|
| Fix specific dates | Historical backfill re-run | Partition overwrite (target dates) | Auto-merges on next DLT run | Targeted |
| Replay Kafka dates | `kafka_replay_backfill.py` | Append-only | Minute MERGE + daily MV refresh via Silver DLT | Specific dates |
| Checkpoint lost | Delete checkpoint + restart | Resumes from latest offset | No action needed | Forward only |
| Rebuild Silver | DLT Full Refresh | Not touched | Full reprocess from all Bronze | All Silver + Gold |

---

## Troubleshooting

### "Secret not found in scope"

```bash
python scripts/add_secrets_rest_api.py

# Verify in Databricks:
dbutils.secrets.list(scope="ganhockchong-market-data")
```

### "Path does not exist" (Unity Catalog)

Run `databricks/setup/create_volume_paths.py` on a cluster with UC access. Or manually:

```sql
CREATE VOLUME IF NOT EXISTS tabular.dataexpert.hc_market_data;
```

### Streaming job not receiving data

1. Is `streaming_producer.py` running? (Check Confluent Cloud topic activity)
2. Is topic name correct? (`kafka-topic` secret → `polygon-streaming-ohlcv`)
3. Are Kafka credentials valid? (`kafka-sasl-username`, `kafka-sasl-password`)
4. Is market currently open? (Both producer and consumer exit if market is closed)

### Historical ingestion returns no data

1. Are dates 3+ days old? (Polygon has upload lag for flat files)
2. Are S3 credentials set? (`polygon-flatfiles-access-key`, `polygon-flatfiles-secret-key`)
3. Is the date range business days? (Weekends are automatically skipped)

### DLT pipeline fails with quality gate error

Check the WAP audit log for the failing date:

```sql
SELECT * FROM tabular.dataexpert.wap_audit_log_hc
WHERE NOT quality_gate_passed
ORDER BY audit_date DESC;
```

If `rejection_rate_pct` exceeds the critical threshold (1%), investigate the quarantine table:

```sql
SELECT rejection_reason, COUNT(*) AS cnt
FROM tabular.dataexpert.ohlcv_silver_quarantine_hc
WHERE date = 'YYYY-MM-DD'
GROUP BY rejection_reason;
```

---

## Cost Optimization

**Estimated Monthly Costs (market hours only):**

| Component | Cluster | Hours/Month | Est. Cost |
|-----------|---------|-------------|-----------|
| Streaming Producer | Single node, 4 cores | ~140 hrs | ~$56 |
| Streaming Ingestion | Single node, 4 cores | ~140 hrs | ~$56 |
| News Ingestion | Single node, 4 cores | ~21 hrs | ~$8 |
| Incremental Flat Files | Single node, 4 cores | ~5 hrs | ~$2 |
| Silver DLT (triggered) | Enhanced, 1–2 workers | ~10 hrs | ~$15 |
| Gold DLT (triggered) | Enhanced, 1–2 workers | ~5 hrs | ~$8 |
| Storage (Bronze + Silver + Gold) | ~20 GB/month | N/A | <$2 |
| **Total** | | | **~$147/month** |

**Savings Tips:**
- Use spot instances (30–50% savings)
- Auto-terminate idle clusters
- Run backfills during off-peak hours
- Combine producer + consumer on one cluster if latency allows

---

## Support

- **Databricks Documentation**: https://docs.databricks.com/
- **Polygon.io API Docs**: https://polygon.io/docs
- **Confluent Kafka Docs**: https://docs.confluent.io/
- **Repository Guide**: See [CLAUDE.md](../CLAUDE.md) for development patterns
