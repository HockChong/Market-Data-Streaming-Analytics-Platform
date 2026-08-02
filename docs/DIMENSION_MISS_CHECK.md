# Dimension-Miss Symbol Check (Gold)

How to measure **dimension-miss symbols** — a `symbol` present in a Gold fact but absent
from `dim_ticker_hc`. Unity Catalog does not enforce foreign keys, so a dimension-miss is
never a hard error; it just disappears from any `INNER JOIN` to the ticker dimension,
silently under-counting movers, sector rollups, and liquidity tiers.

These are **read-only** checks (`SELECT` only). Nothing in the DLT pipelines is modified.

## Mental model

This is the standard fact/dimension grain mismatch: `dim_ticker_hc` and the fact tables
are populated from different Bronze sources with different symbol universes, so some
fact symbols will never resolve against the dimension. That's expected, not a bug — the
job here is to measure it and confirm the rate stays within a known, bounded set of
causes (below), so a real regression (e.g. a broken join key or a stalled dimension
refresh) stands out against the baseline.

## Why dimension-misses happen

| Source | Symbol universe | Resulting dim-miss class |
|---|---|---|
| `dim_ticker_hc` | Built from the **latest** Bronze `ticker_details` snapshot: active US stocks ([ticker_details_ingestion.py:158](../databricks/bronze/ticker_details_ingestion.py#L158)) **plus** any delisted/renamed symbol still present in Bronze OHLCV history, enriched with `active=false` (the "5b" step in `ticker_details_ingestion.py`). Null-exchange rows are also kept, not dropped ([dim_ticker_dlt.py:58](../databricks/gold/dim_ticker_dlt.py#L58), `expect_all`). | Delisted names referenced by OHLCV now resolve — this class is largely closed. |
| `fact_daily_market_hc` / `fact_minute_market_hc` | 400-day rolling window ([gold_config.py:50](../databricks/config/gold_config.py#L50)). | Any symbol outside the bounded delisted pull. |
| `fact_news_hc` | `explode(tickers)` ([fact_news_dlt.py:84](../databricks/gold/fact_news_dlt.py#L84)) — routinely includes ETFs, foreign/ADR names, and other tickers that never appear in OHLCV. | **Dominant class**: news-only symbols with no OHLCV history. |
| `fact_daily_market_adjusted_hc` / `fact_minute_market_adjusted_hc` (what the dashboard actually reads — [screener_data.py](../databricks/dashboard/utils/screener_data.py), [watchlist_data.py](../databricks/dashboard/utils/watchlist_data.py)) | `LEFT JOIN` of the raw fact to `dim_split_hc` on `(symbol, date)` ([split_adjust_spark.py:71-74](../databricks/utils/split_adjust_spark.py#L71-L74)). | Inherits the identical universe — and therefore the identical misses — as the raw fact. Not a new class; included below so the check covers what the dashboard actually serves. |

**Bottom line:** now that delisted names are folded into the dimension, the dominant
remaining dim-miss class is **news-only symbols** (tickers in `fact_news_hc` with no
OHLCV history, e.g. ETFs and foreign/ADR names), plus any symbol outside the bounded
delisted pull. The earlier "delisted names" and "null-exchange tickers" classes are
largely resolved.

---

## Query 1 — dim-miss rate summary (one row per fact)

Pin this in a Databricks SQL dashboard (or attach a SQL alert on `dim_miss_symbol_pct`)
to track the rate against a baseline.

```sql
-- dim-miss = a symbol present in a fact but absent from dim_ticker_hc.
-- dim_ticker_hc = latest active-US-stocks snapshot, exchange NOT NULL.
-- Read-only: SELECT only, no table is modified.
WITH daily AS (
    SELECT
        'fact_daily_market_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS dim_miss_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS dim_miss_symbols
    FROM tabular.dataexpert.fact_daily_market_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
daily_adj AS (
    SELECT
        'fact_daily_market_adjusted_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS dim_miss_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS dim_miss_symbols
    FROM tabular.dataexpert.fact_daily_market_adjusted_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
minute AS (
    SELECT
        'fact_minute_market_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS dim_miss_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS dim_miss_symbols
    FROM tabular.dataexpert.fact_minute_market_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
minute_adj AS (
    SELECT
        'fact_minute_market_adjusted_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS dim_miss_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS dim_miss_symbols
    FROM tabular.dataexpert.fact_minute_market_adjusted_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
news AS (
    SELECT
        'fact_news_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS dim_miss_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS dim_miss_symbols
    FROM tabular.dataexpert.fact_news_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
)
SELECT
    fact_table,
    total_rows,
    dim_miss_rows,
    ROUND(100.0 * dim_miss_rows / NULLIF(total_rows, 0), 3)       AS dim_miss_row_pct,
    total_symbols,
    dim_miss_symbols,
    ROUND(100.0 * dim_miss_symbols / NULLIF(total_symbols, 0), 3) AS dim_miss_symbol_pct
FROM (
    SELECT * FROM daily
    UNION ALL SELECT * FROM daily_adj
    UNION ALL SELECT * FROM minute
    UNION ALL SELECT * FROM minute_adj
    UNION ALL SELECT * FROM news
)
ORDER BY dim_miss_symbol_pct DESC;
```

- **`dim_miss_row_pct`** — how much *data* an inner join to `dim_ticker_hc` would drop.
- **`dim_miss_symbol_pct`** — how many distinct *names* are unmatched (a few delisted
  tickers can be a low row % but still worth knowing).

---

## Query 2 — drill-down: which symbols are missing

Run once to classify the offenders (delisted vs. ETF vs. foreign vs. news-only). For
`fact_news_hc` replace `f.date` with `f.published_date`; for `fact_minute_market_hc`
keep `f.date`.

```sql
-- Top dim-miss symbols in a chosen fact, with how many rows hang off each.
-- LEFT ANTI JOIN keeps only fact symbols with NO dim_ticker_hc match.
SELECT
    f.symbol,
    COUNT(*)    AS fact_rows,
    MIN(f.date) AS first_seen,
    MAX(f.date) AS last_seen
FROM tabular.dataexpert.fact_daily_market_hc f
LEFT ANTI JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
GROUP BY f.symbol
ORDER BY fact_rows DESC
LIMIT 100;
```

---

## How to use it

1. Run **Query 1** once to establish a baseline dim-miss rate per fact.
2. Run **Query 2** to confirm the misses are the expected kind, then document the
   scope (e.g. "analytics are scoped to currently-active, exchange-listed tickers;
   dim-misses are predominantly delisted names and news-only ETFs").
3. Optionally alert on `dim_miss_symbol_pct` exceeding the baseline so a future
   ingestion or join regression surfaces instead of silently dropping rows.
