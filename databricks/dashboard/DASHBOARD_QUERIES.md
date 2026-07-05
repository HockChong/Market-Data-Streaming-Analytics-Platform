# Dashboard SQL Queries

All SQL executed by the Streamlit dashboard, grouped by source module. Table names
are fully qualified at runtime via `table()` → `tabular.dataexpert.<name>`.

Every read goes through one of three execution helpers in
[utils/connection.py](utils/connection.py):

- **`run_query`** — cached on `(query, gold_version, params)`; busts when Gold's `MAX(date)` advances.
- **`run_query_versioned`** — same cache, fetches the Gold version at call time (`_run_query` wrapper).
- **`run_query_uncached`** — bypasses the Gold sentinel; for intraday minute-grain tables that update within the day. Freshness governed by the caller's own `@st.cache_data` TTL.

---

## 1. Connection / sentinel queries
Source: [utils/connection.py](utils/connection.py)

### `get_gold_version` — Gold freshness sentinel (cache key driver)
```sql
SELECT MAX(date) AS max_date FROM tabular.dataexpert.fact_daily_market_adjusted_hc
```

### `get_minute_version` — minute freshness sentinel (intraday cache key driver, TTL 60s)
Cheap probe polled every 60s; advances when the streaming pipeline lands a new 1-min
bar. Passed as `minute_version` into the Stock Terminal intraday fetchers so they bust
as soon as new data arrives, while the page re-runs on the header refresh timer.
```sql
SELECT MAX(start_timestamp) AS max_ts FROM tabular.dataexpert.fact_minute_market_adjusted_hc
```

### `fetch_latest_minute_timestamp` — latest 1-min bar in Gold
```sql
SELECT MAX(start_timestamp) AS max_ts FROM tabular.dataexpert.fact_minute_market_hc
```

---

## 2. Signal Screener page
Source: [utils/screener_data.py](utils/screener_data.py)

### `fetch_available_dates` — recent selectable dates (last 90 days)
```sql
SELECT DISTINCT date FROM fact_daily_market_adjusted_hc
WHERE date >= DATE_SUB((SELECT MAX(date) FROM fact_daily_market_adjusted_hc), 90)
ORDER BY date DESC
```

### `fetch_sectors` — distinct sectors for the filter dropdown
```sql
SELECT DISTINCT sector FROM dim_ticker_hc
WHERE sector IS NOT NULL
ORDER BY sector
```

### `fetch_screener_base` — main screener grid for a selected date
Single-date point read: the rolling context (`close_5d`..`close_252d`, `rvol_20d`,
`prev_adj_close`) is materialized on `fact_daily_market_adjusted_hc` by the Gold
pipeline (`databricks/utils/daily_metrics_spark.py`), so the query prunes to one date
via the table's `(date, symbol)` clustering instead of window-scanning 400 days per
cache miss. Stored metrics come from the split-**adjusted** series, so period returns
stay continuous across splits. Bound param `?` is the selected date (supplied once).
```sql
SELECT
    f.symbol                                                                       AS Symbol,
    t.company_name                                                                 AS Company,
    t.sector                                                                       AS Sector,
    f.adj_close                                                                    AS Close,
    ROUND((f.adj_close - f.prev_adj_close) / NULLIF(f.prev_adj_close, 0) * 100, 2) AS `Chg %`,
    ROUND((f.adj_close - f.adj_open)       / NULLIF(f.adj_open, 0)       * 100, 2) AS `Intraday %`,
    ROUND((f.adj_open  - f.prev_adj_close) / NULLIF(f.prev_adj_close, 0) * 100, 2) AS `Gap %`,
    ROUND(f.rvol_20d, 2)                                                           AS RVOL,
    f.adj_volume                                                                   AS Volume,
    ROUND(f.adj_close * f.adj_volume, 2)                                           AS `Dollar Volume`,
    ROUND((f.adj_close - f.close_5d)   / NULLIF(f.close_5d,   0) * 100, 2)         AS `1W Gain %`,
    ROUND((f.adj_close - f.close_21d)  / NULLIF(f.close_21d,  0) * 100, 2)         AS `1M Gain %`,
    ROUND((f.adj_close - f.close_63d)  / NULLIF(f.close_63d,  0) * 100, 2)         AS `3M Gain %`,
    ROUND((f.adj_close - f.close_126d) / NULLIF(f.close_126d, 0) * 100, 2)         AS `6M Gain %`,
    ROUND((f.adj_close - f.close_252d) / NULLIF(f.close_252d, 0) * 100, 2)         AS `1Y Gain %`
FROM fact_daily_market_adjusted_hc f
JOIN dim_ticker_hc t ON f.symbol = t.symbol
WHERE f.date = ?
  AND f.adj_close >= 5.0
```

---

## 3. Stock Deep Dive / Terminal page
Source: [utils/stock_terminal_data.py](utils/stock_terminal_data.py)

### `fetch_active_tickers` — symbol picker
```sql
SELECT symbol, company_name FROM dim_ticker_hc
WHERE is_active = true
ORDER BY symbol
```

### `fetch_daily_market_data` — daily adjusted OHLCV series for the chart
`{fetch_days}` is inlined (int); `?` is the symbol. Returns computed from
`adj_close`/`prev_adj_close`, not the raw `price_change_pct` (which reflects the
mechanical split-day drop).
```sql
SELECT
    date, adj_open AS open, adj_high AS high, adj_low AS low,
    adj_close AS close, adj_volume AS volume,
    prev_adj_close AS prev_close,
    CASE
        WHEN prev_adj_close > 0 THEN (adj_close - prev_adj_close) / prev_adj_close * 100
    END AS price_change_pct
FROM fact_daily_market_adjusted_hc
WHERE symbol = ?
  AND date >= DATE_SUB(CURRENT_DATE(), {fetch_days})
ORDER BY date
```

### `fetch_latest_minute_date` — latest minute date for a symbol (keyed on `minute_version`, TTL 300s safety net)
```sql
SELECT MAX(date) AS max_date FROM fact_minute_market_adjusted_hc WHERE symbol = ?
```

### `fetch_prev_session_close` — prior-session close anchoring the intraday "Previous close" line
`?` params: symbol, before_date (the latest minute date). Returns the split-adjusted
close of the most recent session strictly before the intraday session shown, so the
reference line stays correct under a custom/historical daily window (where the daily
picker's last bar need not be the session before the live minute session).
```sql
SELECT adj_close
FROM fact_daily_market_adjusted_hc
WHERE symbol = ?
  AND date < ?
ORDER BY date DESC
LIMIT 1
```

### `fetch_intraday` — 1-minute intraday split-ADJUSTED bars (keyed on `minute_version`, TTL 300s safety net)
`?` params: symbol, date. Reads `adj_*` aliased to raw names so the 2-day intraday
horizon stays continuous when the two sessions straddle a split (a raw read would show
a mechanical price cliff at the session boundary). Within a single session the
adjustment factor is constant, so 1-day mode is visually unchanged.
```sql
SELECT
    start_timestamp,
    adj_open AS open, adj_high AS high, adj_low AS low,
    adj_close AS close, adj_volume AS volume
FROM fact_minute_market_adjusted_hc
WHERE symbol = ?
  AND date = ?
ORDER BY start_timestamp
```

### `fetch_daily_date_range` — available daily range for a symbol
```sql
SELECT MIN(date) AS min_date, MAX(date) AS max_date
FROM fact_daily_market_adjusted_hc
WHERE symbol = ?
```

### `fetch_recent_news` — news panel
`{limit}` is inlined (int, default 50); `?` params: symbol, since_date. `since_date`
is anchored to the data window, not `CURRENT_DATE()`, so news shows even when ingestion lags.
```sql
SELECT
    published_date,
    title,
    publisher_name,
    article_url
FROM fact_news_hc
WHERE symbol = ?
  AND published_date >= ?
ORDER BY published_date DESC, published_utc DESC
LIMIT {limit}
```

---

## 4. Watchlist page
Source: [utils/watchlist_data.py](utils/watchlist_data.py)

### `fetch_all_tickers` — add-stock dropdown
```sql
SELECT symbol, company_name FROM dim_ticker_hc ORDER BY symbol
```

### `fetch_watchlist_quotes` — latest screener-style metrics per watchlisted ticker
Reads the same stored metrics as the screener (`prev_adj_close`, `close_5d/21d/63d`,
`rvol_20d`), so both surfaces share one definition of each metric. `ROW_NUMBER` picks
each symbol's own latest session (a halted ticker still shows its last trade); the
150-day bound keeps the scan short while preserving the inclusion window for stale
symbols. `?` placeholders: one per watchlisted symbol.
```sql
WITH latest AS (
    SELECT
        symbol,
        adj_open   AS open,
        adj_close  AS close,
        adj_volume AS volume,
        prev_adj_close AS prev_close,
        rvol_20d,
        close_5d,
        close_21d,
        close_63d,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
    FROM fact_daily_market_adjusted_hc
    WHERE symbol IN (?, ...)
      AND date >= DATE_SUB((SELECT MAX(date) FROM fact_daily_market_adjusted_hc), 150)
)
SELECT
    c.symbol,
    t.sector,
    c.close                                                             AS price,
    ROUND((c.close - c.prev_close) / NULLIF(c.prev_close, 0) * 100, 2)  AS chg_pct,
    ROUND((c.close - c.open)       / NULLIF(c.open, 0)       * 100, 2)  AS intraday_pct,
    ROUND((c.open  - c.prev_close) / NULLIF(c.prev_close, 0) * 100, 2)  AS gap_pct,
    ROUND(c.rvol_20d, 2)                                                AS rvol,
    c.volume                                                            AS volume,
    ROUND(c.close * c.volume, 2)                                        AS dollar_volume,
    ROUND((c.close - c.close_5d)  / NULLIF(c.close_5d,  0) * 100, 2)    AS gain_1w_pct,
    ROUND((c.close - c.close_21d) / NULLIF(c.close_21d, 0) * 100, 2)    AS gain_1m_pct,
    ROUND((c.close - c.close_63d) / NULLIF(c.close_63d, 0) * 100, 2)    AS gain_3m_pct
FROM latest c
JOIN dim_ticker_hc t ON c.symbol = t.symbol
WHERE c.rn = 1
```

The Watchlist's inline intraday chart reuses the Stock Terminal fetchers
(`fetch_latest_minute_date`, `fetch_prev_session_close`, `fetch_intraday`) for the
single expanded ticker — see section 3.

---

## Gold tables referenced
| Table | Used by |
|-------|---------|
| `fact_daily_market_adjusted_hc`  | gold sentinel, screener dates/grid, daily chart, date range, intraday previous-close, watchlist quotes |
| `fact_minute_market_adjusted_hc` | intraday bars, latest minute date, minute sentinel (`get_minute_version`) |
| `fact_minute_market_hc`          | minute freshness sentinel (`fetch_latest_minute_timestamp`) |
| `fact_news_hc`                   | news panel |
| `dim_ticker_hc`                  | sectors filter, ticker picker, screener/watchlist joins, add-stock dropdown |
