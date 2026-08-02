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

### Test REST Aggregates Gap Backfill

1. Open `bronze/rest_aggs_backfill.py` — manual/on-demand, no schedule (see Scenario 4 in Step "How to Replay Data")
2. Leave `dry_run` = `true` and run all cells to preview row counts for the default 09:30–10:00 ET window
3. Re-run with `dry_run` = `false` to append the fetched bars to `/Volumes/tabular/dataexpert/hc_market_data/bronze/streaming` (source = `polygon_rest_backfill`, source_priority 0 — Silver's dedup keeps live-streamed bars over these wherever both exist)

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
7. Pipeline mode: **Continuous** — started and stopped by Jobs, not run on a fixed trigger cron (see Step 6)
8. **Configuration** (Pipeline settings → Configuration): no daily-rollup configuration keys are required. `ohlcv_daily_silver_hc` is a materialized view that re-derives the daily grain from the full minute Silver snapshot each refresh; on serverless the engine refreshes it incrementally.

   **One-time migration:** if a previous deploy created `ohlcv_daily_silver_hc` as a streaming table, drop it once (`DROP TABLE tabular.dataexpert.ohlcv_daily_silver_hc`) before the first run so it can be recreated as a materialized view (a streaming table cannot be converted in place). That first run is a full recompute.

### Silver News Pipeline

1. Create pipeline: `Silver News Pipeline`
2. Source code: `databricks/silver/news_silver_dlt.py`
3. Target schema: `tabular.dataexpert`
4. Pipeline mode: **Continuous** — started and stopped by Jobs (see Step 6)

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
5. Pipeline mode: **Continuous** — runs in parallel with the Silver pipelines during market hours, not triggered after them (see Step 6). Gold reads Silver via `spark.read.table`, so it naturally lags Silver's MERGE by however long its own continuous cycle takes to pick up the change.

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

### Job 7: Continuous Pipeline & Dashboard Lifecycle (Market Hours)

The three DLT pipelines (Silver OHLCV, Silver News, Gold) run in **Continuous** mode, not on a
trigger cron. A continuous pipeline keeps a long-running update open and reacts to new data as it
arrives instead of being invoked per run, so "scheduling" it means starting and stopping that
update — not firing a triggered run every few minutes. Four Databricks Jobs do this, each running
a small notebook from `databricks/silver/jobs/` that calls the Databricks SDK
(`WorkspaceClient.pipelines.start_update` / `.stop`, or `.apps.start` / `.stop` for the dashboard):

| Job | Start notebook | Stop notebook | Start schedule | Stop schedule |
|---|---|---|---|---|
| Silver OHLCV DLT | `ohlcv_silver_pipeline_start.py` | `ohlcv_silver_pipeline_stop.py` | 9:30 AM ET Mon–Fri | 5:00 PM ET Mon–Fri |
| Silver News DLT | `news_silver_pipeline_start.py` | `news_silver_pipeline_stop.py` | 9:30 AM ET Mon–Fri | 5:00 PM ET Mon–Fri |
| Gold DLT | `gold_pipeline_start.py` | `gold_pipeline_stop.py` | 9:30 AM ET Mon–Fri | 5:00 PM ET Mon–Fri |
| Dashboard App (`market-analytics-dashboard`) | `market_analytics_app_start.py` | `market_analytics_app_stop.py` | 9:30 AM ET Mon–Fri | 5:00 PM ET Mon–Fri |

Each start notebook is idempotent — it checks pipeline/app state first and only starts if
`IDLE`/`FAILED` (or `STOPPED` for the app), so it's safe to re-run or overlap with an
already-running instance. Each stop notebook only stops if currently `RUNNING`. Update the
hardcoded `PIPELINE_ID` / `APP_NAME` constants at the top of each notebook to match your workspace
before scheduling these as Jobs.

- **Prerequisite**: Jobs 1 + 2 (Bronze streaming producer + ingestion) must be **running** during
  this window so Bronze Delta receives Kafka bars. Silver does not read Kafka directly.
- **Gold ordering**: Gold starts at the same time as Silver, not after it — it reads Silver via
  `spark.read.table`, so a given Gold cycle picks up whatever Silver has committed by then rather
  than waiting on a completion signal.
- **Layer 3 (in-pipeline)**: Each Silver update finishes `ohlcv_silver_hc` MERGE **before**
  `ohlcv_daily_silver_hc` incremental rollup in the same cycle — no separate daily job needed.
- Today's daily bar in `ohlcv_daily_silver_hc` is a rolling snapshot that keeps refreshing as the
  continuous pipeline reacts to new Bronze rows until the 5:00 PM ET stop.

**Manual/off-hours flows** (flat-file incremental, historical backfill) still use the Trigger
button or `databricks pipelines start-update <id> --full-refresh=false` on demand, since the
pipeline may be idle outside 9:30 AM–5:00 PM ET:

1. `incremental_ingestion_flatfiles.py` (Job 4) → start the Silver OHLCV pipeline update if it
   isn't already running — it picks up the new Bronze rows through the minute stream, and the
   daily MV refreshes the affected dates
2. Historical backfill: run Job 6 with `start_date`/`end_date` → start Silver OHLCV once
   (metadata path, or `silver.backfill_start_date`/`silver.backfill_end_date` config) → start Gold

**First-time migration:** if an old streaming `ohlcv_daily_silver_hc` exists, drop it once before
the first run so it is recreated as a materialized view; that run is a full recompute, then
serverless refreshes it incrementally.

---

## Step 7: Deploy the Streamlit Dashboard

### Production: Databricks App

Once Gold tables are populated, the dashboard runs as a **Databricks App** named
`market-analytics-dashboard`, configured by `databricks/dashboard/app.yaml`
(`streamlit run app.py`, with `DATABRICKS_WAREHOUSE_ID` set as an app env var). It is started and
stopped on the same 9:30 AM / 5:00 PM ET Mon–Fri schedule as the DLT pipelines via
`market_analytics_app_start.py` / `market_analytics_app_stop.py` (see Step 6, Job 7).

1. In Databricks: **Compute** → **Apps** → **Create app**, point it at `databricks/dashboard/`
2. Deploy — it installs `databricks/dashboard/requirements.txt` and runs the `app.yaml` command
3. Confirm the app is reachable and reads Gold tables via the connection configured in
   `databricks/dashboard/utils/connection.py`

### Local development

To run the dashboard locally against the same Databricks SQL warehouse:

```bash
pip install -r databricks/dashboard/requirements.txt

streamlit run databricks/dashboard/app.py
```

**Page 1 — Signal Screener** (`localhost:8501`): cross-layer screener joining `fact_daily_market_hc` and `dim_ticker_hc`. Filter by sector, gain range, volume, and date.

**Page 2 — Stock Deep Dive**: single-ticker terminal with OHLCV chart, intraday 1-minute bars (from `fact_minute_market_adjusted_hc`, so the 2-day horizon stays continuous across splits), technical indicators, and latest news from `fact_news_hc`.

**Page 3 — Watchlist**: save favourite tickers and view their latest price, daily change, and screener-style metrics at a glance. The list persists to `watchlist.json` in the dashboard directory so it survives page reloads within the same Databricks App instance.

> The dashboard reads from Databricks SQL via the connection configured in `databricks/dashboard/utils/connection.py`. Ensure your Databricks workspace credentials are set before launching.

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

**How it works:** Re-runs `historical_ingestion_flatfiles.py` for the target date range. Dynamic partition overwrite replaces only those `date` partitions in Bronze. Silver picks up the changes automatically if its continuous pipeline is already running (9:30 AM–5:00 PM ET); otherwise start it manually. Minute rows merge via `apply_changes` MERGE on `(symbol, start_timestamp)`, and the daily materialized view `ohlcv_daily_silver_hc` refreshes the affected `(symbol, date)` groups.

**Steps:**
1. Open `databricks/bronze/historical_ingestion_flatfiles.py`
2. Set widgets: `start_date` and `end_date` to the target range
3. Run all cells
4. If outside market hours, start the Silver OHLCV DLT pipeline update (optional: set `silver.backfill_start_date` / `silver.backfill_end_date` to the same range if metadata path is unavailable) — during market hours it's already running and will pick this up on its own
5. After Silver processes the change, start/confirm the Gold pipeline if daily analytics must refresh immediately

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
5. Silver deduplicates minute rows via `apply_changes` MERGE; the daily materialized view refreshes the affected dates on the next cycle of the (already-running) Silver pipeline

### Scenario 3 — Full DLT pipeline reset (Silver / Gold)

**When to use:** Breaking schema change, or Silver state needs a clean rebuild.

**How it works:** `bronze_unified_hc` has `"pipelines.reset.allowed": "true"`. A full refresh drops streaming state and reprocesses all Bronze data from scratch.

**Steps:**
1. In Databricks: **Delta Live Tables** → open the Silver pipeline
2. Click **⋮ (more options)** → **Full Refresh** (this stops any active continuous update first)
3. After Silver completes, start the Gold pipeline (or wait for its next scheduled 9:30 AM ET start)

> During a full refresh, downstream Gold queries see partial data until the pipeline completes. Schedule during off-hours if the dashboard is active.

### Scenario 4 — Session gap where streaming never reached Kafka

**When to use:** The producer or streaming ingestion job failed to start (e.g. cluster failure) during market hours, so a window of bars for that session was never published to Kafka — `kafka_replay_backfill.py` has nothing to replay because the data never arrived there.

**How it works:** `bronze/rest_aggs_backfill.py` fetches the gap window per ticker directly from the Polygon REST aggregates API (`adjusted=False`, matching Bronze's raw convention) and appends it to Bronze streaming with `source = "polygon_rest_backfill"` (source_priority 0). No gap detection — it fetches a bounded window (default 09:30–10:00 ET) and lets Silver's dedup pick winners: live-streamed bars (priority 1) win wherever they exist, REST bars survive only in true holes, and next-day flat files (priority 2) supersede both. Over-fetching is harmless.

**Steps:**
1. Open `databricks/bronze/rest_aggs_backfill.py`
2. Leave `dry_run = true` and run to preview row counts; widen `start_time`/`end_time` for longer outages
3. Set `dry_run = false` to append
4. No Silver action needed if its continuous pipeline is already running — it merges the new Bronze rows on its next cycle

### Quick Reference

| Scenario | Mechanism | Bronze touched | Silver action | Scope |
|---|---|---|---|---|
| Fix specific dates | Historical backfill re-run | Partition overwrite (target dates) | Auto-merges on next continuous cycle | Targeted |
| Replay Kafka dates | `kafka_replay_backfill.py` | Append-only | Minute MERGE + daily MV refresh via Silver DLT | Specific dates |
| Checkpoint lost | Delete checkpoint + restart | Resumes from latest offset | No action needed | Forward only |
| Rebuild Silver | DLT Full Refresh | Not touched | Full reprocess from all Bronze | All Silver + Gold |
| Streaming never started (session gap) | `rest_aggs_backfill.py` | Append-only | Dedup picks winner on next continuous cycle | Bounded time window |

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

> **Note:** Silver OHLCV, Silver News, and Gold DLT now run in **Continuous** mode, started/stopped
> by Jobs at 9:30 AM / 5:00 PM ET (Step 6, Job 7) instead of firing as short triggered runs — each
> holds compute for the full ~7.5-hour window, roughly matching the streaming jobs' footprint below
> rather than the few-triggered-runs figure this table previously assumed. The Dashboard App
> (`market-analytics-dashboard`) adds its own compute on the same schedule and isn't itemized here.
> Treat the figures below as rough, pre-continuous-mode estimates — pull actual DBU usage from the
> workspace's usage/billing dashboard for current numbers.

| Component | Cluster | Hours/Month | Est. Cost |
|-----------|---------|-------------|-----------|
| Streaming Producer | Single node, 4 cores | ~140 hrs | ~$56 |
| Streaming Ingestion | Single node, 4 cores | ~140 hrs | ~$56 |
| News Ingestion | Single node, 4 cores | ~21 hrs | ~$8 |
| Incremental Flat Files | Single node, 4 cores | ~5 hrs | ~$2 |
| Silver OHLCV DLT (continuous, ~140 hrs) | Enhanced, 1–2 workers | ~140 hrs | ~$60 |
| Silver News DLT (continuous, ~140 hrs) | Enhanced, 1–2 workers | ~140 hrs | ~$60 |
| Gold DLT (continuous, ~140 hrs) | Enhanced, 1–2 workers | ~140 hrs | ~$60 |
| Dashboard App (~140 hrs) | Databricks App compute | ~140 hrs | not itemized |
| Storage (Bronze + Silver + Gold) | ~20 GB/month | N/A | <$2 |
| **Total** | | | **~$302/month + App compute** |

**Savings Tips:**
- Use spot instances (30–50% savings)
- Auto-terminate idle clusters
- Run backfills during off-peak hours
- Combine producer + consumer on one cluster if latency allows

---

## File Reference

```
databricks/
├── DEPLOYMENT_GUIDE.md                         ← You are here
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
│   ├── rest_aggs_backfill.py                   ← REST gap-fill for sessions streaming never reached
│   └── bronze_utils.py                         ← Shared Bronze helpers
├── silver/
│   ├── ohlcv_silver_dlt.py                     ← DLT: clean, deduplicate, WAP quarantine OHLCV
│   ├── news_silver_dlt.py                      ← DLT: clean, deduplicate news articles
│   └── jobs/                                   ← Continuous pipeline/app start-stop notebooks (Step 6, Job 7)
│       ├── ohlcv_silver_pipeline_start.py
│       ├── ohlcv_silver_pipeline_stop.py
│       ├── news_silver_pipeline_start.py
│       ├── news_silver_pipeline_stop.py
│       ├── gold_pipeline_start.py
│       ├── gold_pipeline_stop.py
│       ├── market_analytics_app_start.py
│       └── market_analytics_app_stop.py
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
│   ├── daily_metrics_spark.py                  ← Rolling screener/watchlist metrics on Gold adjusted daily fact
│   ├── rest_aggs_backfill_runtime.py           ← Runtime helpers for rest_aggs_backfill.py
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
    ├── app.yaml                                ← Databricks App deployment config (Step 7)
    ├── requirements.txt                        ← Dashboard-specific dependencies
    ├── DASHBOARD_QUERIES.md                    ← Reference: SQL queries backing each dashboard view
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

- **Databricks Documentation**: https://docs.databricks.com/
- **Polygon.io API Docs**: https://polygon.io/docs
- **Confluent Kafka Docs**: https://docs.confluent.io/
- **Repository Guide**: See [CLAUDE.md](../CLAUDE.md) for development patterns
