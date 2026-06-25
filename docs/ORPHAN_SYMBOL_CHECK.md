# Orphan-Symbol Data-Quality Check (Gold)

How to measure **orphan symbols** — a `symbol` present in a Gold fact but absent from
`dim_ticker_hc`. Unity Catalog does not enforce foreign keys, so an orphan is never an
error; it just disappears from any `INNER JOIN` to the ticker dimension, silently
under-counting movers, sector rollups, and liquidity tiers.

These are **read-only** checks (`SELECT` only). Nothing in the DLT pipelines is modified.

## Why orphans happen

The fact and dimension symbol universes come from different sources:

- `dim_ticker_hc` is built from the **latest** Bronze `ticker_details` snapshot. Its
  universe is active US stocks ([ticker_details_ingestion.py:158](../databricks/bronze/ticker_details_ingestion.py#L158))
  **plus** any delisted/renamed symbol still present in our Bronze OHLCV history, which the
  ingestion enriches with `active=false` (the "5b" cell in `ticker_details_ingestion.py`).
  The dimension also **keeps** null-exchange rows rather than dropping them
  ([dim_ticker_dlt.py:58](../databricks/gold/dim_ticker_dlt.py#L58), `expect_all`), so a
  delisted name with no exchange still lands in the dim.
- Facts are broader/historical: `fact_daily_market_hc` covers a 400-day window
  ([gold_config.py:51](../databricks/config/gold_config.py#L51)), and `fact_news_hc` symbols
  come from `explode(tickers)` ([fact_news_dlt.py:84](../databricks/gold/fact_news_dlt.py#L84)),
  which routinely includes ETFs, foreign/ADR, and other tickers that never appear in OHLCV.

Because delisted names referenced by OHLCV are now in the dimension, the dominant remaining
orphan class is **news-only symbols** (tickers in `fact_news_hc` with no OHLCV history, e.g.
ETFs and foreign/ADR names), plus any symbol outside the bounded delisted pull. The earlier
"delisted names" and "null-exchange tickers" classes are largely resolved.

---

## Query 1 — orphan-rate summary (one row per fact)

Pin this in a Databricks SQL dashboard (or attach a SQL alert on `orphan_symbol_pct`)
to track the rate against a baseline.

```sql
-- Orphan = a symbol present in a fact but absent from dim_ticker_hc.
-- dim_ticker_hc = latest active-US-stocks snapshot, exchange NOT NULL.
-- Read-only: SELECT only, no table is modified.
WITH daily AS (
    SELECT
        'fact_daily_market_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS orphan_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS orphan_symbols
    FROM tabular.dataexpert.fact_daily_market_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
minute AS (
    SELECT
        'fact_minute_market_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS orphan_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS orphan_symbols
    FROM tabular.dataexpert.fact_minute_market_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
),
news AS (
    SELECT
        'fact_news_hc' AS fact_table,
        COUNT(*)                                                     AS total_rows,
        COUNT_IF(d.symbol IS NULL)                                   AS orphan_rows,
        COUNT(DISTINCT f.symbol)                                     AS total_symbols,
        COUNT(DISTINCT CASE WHEN d.symbol IS NULL THEN f.symbol END) AS orphan_symbols
    FROM tabular.dataexpert.fact_news_hc f
    LEFT JOIN tabular.dataexpert.dim_ticker_hc d ON f.symbol = d.symbol
)
SELECT
    fact_table,
    total_rows,
    orphan_rows,
    ROUND(100.0 * orphan_rows / NULLIF(total_rows, 0), 3)       AS orphan_row_pct,
    total_symbols,
    orphan_symbols,
    ROUND(100.0 * orphan_symbols / NULLIF(total_symbols, 0), 3) AS orphan_symbol_pct
FROM (
    SELECT * FROM daily
    UNION ALL SELECT * FROM minute
    UNION ALL SELECT * FROM news
)
ORDER BY orphan_symbol_pct DESC;
```

- **`orphan_row_pct`** — how much *data* an inner join to `dim_ticker_hc` would drop.
- **`orphan_symbol_pct`** — how many distinct *names* are unmatched (a few delisted
  tickers can be a low row % but still worth knowing).

---

## Query 2 — drill-down: which symbols are orphaned

Run once to classify the offenders (delisted vs. ETF vs. foreign vs. news-only). For
`fact_news_hc` replace `f.date` with `f.published_date`; for `fact_minute_market_hc`
keep `f.date`.

```sql
-- Top orphan symbols in a chosen fact, with how many rows hang off each.
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

1. Run **Query 1** once to establish a baseline orphan rate per fact.
2. Run **Query 2** to confirm the orphans are the expected kind, then document the
   scope (e.g. "analytics are scoped to currently-active, exchange-listed tickers;
   orphans are predominantly delisted names and news-only ETFs").
3. Optionally alert on `orphan_symbol_pct` exceeding the baseline so a future ingestion
   or join regression surfaces instead of silently dropping rows.
