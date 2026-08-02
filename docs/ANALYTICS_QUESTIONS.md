# Gold Layer Analytics — Top 10 Business Questions

This document lists ten business questions that the **Gold layer** can answer, framed
from an **investment data analyst** perspective. Every query runs against the Star Schema
tables and uses only columns that exist in the code (verified against
[`databricks/gold/`](../databricks/gold/)).

## Tables Used

All tables live in `tabular.dataexpert` (Unity Catalog).

| Table | Grain | Key analytic columns | Lookback |
|---|---|---|---|
| `fact_daily_market_adjusted_hc` | symbol × date | Raw OHLCV + `adj_open, adj_high, adj_low, adj_close, adj_volume, prev_adj_close, price_factor` + serving metrics (`close_5d`..`close_252d`, `rvol_20d`) | 400 days |
| `fact_minute_market_hc` | symbol × minute | `start_timestamp` (BIGINT epoch), `open, high, low, close, volume` | **5 days** |
| `fact_news_hc` | article × symbol | `published_date, title, publisher_name, author` | — |
| `dim_ticker_hc` | symbol | `company_name, sector, industry, exchange, market_cap_category, is_active` | — |
| `dim_date_hc` | date | `is_trading_day, is_options_expiry, is_month_end, quarter, ...` | — |

## Assumptions & Limitations

- **Ticker universe (active + delisted-in-history):** every query below joins `dim_ticker_hc`,
  the **latest** snapshot built from Bronze `ticker_details`. It covers active US stocks **and**
  delisted/renamed names still referenced by our OHLCV history (`is_active=false`, and
  null-exchange rows are kept), so delisted names within the fact window are no longer dropped
  by inner joins — this removes the survivorship bias that an active-only universe would cause.
  Residual dimension-misses are mostly **news-only** symbols (ETFs, foreign/ADR tickers in `fact_news_hc`
  that never appear in OHLCV); measure the rate with [DIMENSION_MISS_CHECK.md](DIMENSION_MISS_CHECK.md).
  Attributes remain current-state (Type 1) — point-in-time classification is not yet modelled.
- **Returns are split-adjusted, price-return only:** Q1–Q8 compute returns from `adj_close` /
  `prev_adj_close` on `fact_daily_market_adjusted_hc`, so stock splits no longer produce
  mechanical artifacts (false movers, inflated volatility). Returns still exclude cash dividends
  (no dividend feed exists), so dividend-paying names are understated over multi-period horizons,
  biasing return/momentum rankings toward low-yield stocks. Total-return is out of scope.
- **Idempotency / duplicates:** facts originate from at-least-once Kafka, but Silver dedups
  (`databricks/utils/ohlcv_dedup_spark.py`) and Gold enforces keys, so a plain
  `GROUP BY symbol, date` is safe.
- **No sentiment / NLP:** `fact_news_hc` has no sentiment score, so Q6 measures news
  *coverage volume*, not tone. Sentiment would be a new Silver-layer feature (out of scope).
- **Minute window is 5 days deep** (`GoldConfig.minute_lookback_days = 5`), so intraday
  questions (Q9) are short-horizon by design; longer-horizon questions use
  `fact_daily_market_adjusted_hc` (400 days).
- **⚠ window functions** are used in Q4 and Q5.
- **🕐 5-day minute window** applies to Q9.

---

## 1. Top daily movers — who gained / lost the most?

**Value:** The #1 dashboard question for any trader/PM — surfaces actionable names instantly.
**Grain:** one row per symbol for a given day.

```sql
SELECT f.symbol, t.company_name, t.sector,
       f.close,
       ROUND((f.adj_close - f.prev_adj_close)
             / NULLIF(f.prev_adj_close, 0) * 100, 2) AS price_change_pct,
       f.volume
FROM tabular.dataexpert.fact_daily_market_adjusted_hc f
JOIN tabular.dataexpert.dim_ticker_hc t USING (symbol)
WHERE f.date = (SELECT MAX(date) FROM tabular.dataexpert.fact_daily_market_adjusted_hc)
  AND f.prev_adj_close IS NOT NULL AND f.prev_adj_close > 0
ORDER BY price_change_pct DESC
LIMIT 20;   -- tail (ASC) gives the biggest losers
```

## 2. Sector rotation — which sectors are leading or lagging?

**Value:** Tells a PM where money is flowing; the backbone of any allocation story.
**Grain:** sector × month.

```sql
SELECT d.year, d.month, t.sector,
       ROUND(AVG((f.adj_close - f.prev_adj_close)
                 / NULLIF(f.prev_adj_close, 0) * 100), 3) AS avg_daily_return_pct,
       COUNT(DISTINCT f.symbol)                            AS names
FROM tabular.dataexpert.fact_daily_market_adjusted_hc f
JOIN tabular.dataexpert.dim_ticker_hc t USING (symbol)
JOIN tabular.dataexpert.dim_date_hc   d ON d.date = f.date
WHERE t.sector IS NOT NULL
  AND f.prev_adj_close IS NOT NULL AND f.prev_adj_close > 0
GROUP BY d.year, d.month, t.sector
ORDER BY d.year, d.month, avg_daily_return_pct DESC;
```

## 3. Volatility ranking — riskiest names by intraday range

**Value:** Risk teams and options/swing traders size positions off this.
**Grain:** symbol over a 90-day window.

```sql
SELECT f.symbol, t.sector,
       ROUND(AVG((f.high - f.low) / NULLIF(f.low, 0)) * 100, 2) AS avg_daily_range_pct,
       ROUND(STDDEV((f.adj_close - f.prev_adj_close)
                    / NULLIF(f.prev_adj_close, 0) * 100), 2)     AS return_volatility
FROM tabular.dataexpert.fact_daily_market_adjusted_hc f
JOIN tabular.dataexpert.dim_ticker_hc t USING (symbol)
WHERE f.date >= current_date() - INTERVAL 90 days
  AND f.prev_adj_close IS NOT NULL AND f.prev_adj_close > 0
GROUP BY f.symbol, t.sector
HAVING COUNT(*) >= 30
ORDER BY return_volatility DESC
LIMIT 25;
```

## 4. ⚠ Unusual volume — today's volume vs its 20-day average

**Value:** Volume spikes precede or confirm big moves; classic "something is happening here" screen.
**Grain:** symbol × day with trailing window.

```sql
WITH v AS (
  SELECT symbol, date, adj_volume AS volume,
         ROUND((adj_close - prev_adj_close)
               / NULLIF(prev_adj_close, 0) * 100, 2)       AS price_change_pct,
         AVG(adj_volume) OVER (PARTITION BY symbol
                               ORDER BY date ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) AS avg20
  FROM tabular.dataexpert.fact_daily_market_adjusted_hc
  WHERE prev_adj_close IS NOT NULL AND prev_adj_close > 0
)
SELECT symbol, date, volume, ROUND(volume/NULLIF(avg20,0), 1) AS volume_x_avg, price_change_pct
FROM v
WHERE avg20 IS NOT NULL AND volume > 2 * avg20
ORDER BY volume_x_avg DESC
LIMIT 30;
```

## 5. ⚠ Momentum — best / worst trailing 20-day performers

**Value:** Momentum is a core factor strategy; ranks names by trend, not just one day.
**Grain:** symbol, latest day vs 20 sessions prior.

```sql
WITH r AS (
  SELECT symbol, date, adj_close,
         FIRST_VALUE(adj_close) OVER (PARTITION BY symbol
            ORDER BY date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW) AS close_20d_ago,
         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC)   AS rn
  FROM tabular.dataexpert.fact_daily_market_adjusted_hc
)
SELECT symbol, ROUND((adj_close/NULLIF(close_20d_ago,0) - 1) * 100, 2) AS return_20d_pct
FROM r
WHERE rn = 1 AND close_20d_ago IS NOT NULL
ORDER BY return_20d_pct DESC
LIMIT 25;
```

## 6. News coverage vs price moves — do big moves draw press?

**Value:** Links the news feed to price action (coverage *volume*, since there is no sentiment) —
an attention/narrative signal.
**Grain:** symbol × day.

```sql
WITH moves AS (
  SELECT symbol, date,
         ROUND((adj_close - prev_adj_close)
               / NULLIF(prev_adj_close, 0) * 100, 2) AS price_change_pct
  FROM tabular.dataexpert.fact_daily_market_adjusted_hc
  WHERE prev_adj_close IS NOT NULL AND prev_adj_close > 0
)
SELECT m.symbol, m.date, m.price_change_pct,
       COUNT(n.article_id) AS article_count
FROM moves m
LEFT JOIN tabular.dataexpert.fact_news_hc n
       ON n.symbol = m.symbol AND n.published_date = m.date
GROUP BY m.symbol, m.date, m.price_change_pct
HAVING ABS(m.price_change_pct) > 5
ORDER BY article_count DESC, ABS(m.price_change_pct) DESC
LIMIT 30;
```

## 7. Market breadth — advancers vs decliners per day

**Value:** A one-number "is the market healthy?" gauge that institutions watch daily.
**Grain:** one row per trading day.

```sql
WITH daily_returns AS (
  SELECT date,
         (adj_close - prev_adj_close) / NULLIF(prev_adj_close, 0) AS adj_return
  FROM tabular.dataexpert.fact_daily_market_adjusted_hc
  WHERE prev_adj_close IS NOT NULL AND prev_adj_close > 0
)
SELECT date,
       SUM(CASE WHEN adj_return > 0 THEN 1 ELSE 0 END) AS advancers,
       SUM(CASE WHEN adj_return < 0 THEN 1 ELSE 0 END) AS decliners,
       ROUND(SUM(CASE WHEN adj_return > 0 THEN 1 ELSE 0 END)
             / NULLIF(SUM(CASE WHEN adj_return < 0 THEN 1 ELSE 0 END), 0), 2) AS adv_dec_ratio
FROM daily_returns
GROUP BY date
ORDER BY date DESC
LIMIT 30;
```

## 8. Overnight gaps — open vs prior close

**Value:** Gap-up/gap-down is a high-conviction day-trading setup and an earnings-reaction proxy.
**Grain:** symbol × day.

```sql
SELECT symbol, date,
       prev_adj_close, adj_open,
       ROUND((adj_open - prev_adj_close) / NULLIF(prev_adj_close, 0) * 100, 2) AS gap_pct,
       ROUND((adj_close - prev_adj_close) / NULLIF(prev_adj_close, 0) * 100, 2) AS price_change_pct
FROM tabular.dataexpert.fact_daily_market_adjusted_hc
WHERE prev_adj_close IS NOT NULL AND prev_adj_close > 0
  AND ABS((adj_open - prev_adj_close) / NULLIF(prev_adj_close, 0)) > 0.03
ORDER BY date DESC, ABS(gap_pct) DESC
LIMIT 30;
```

## 9. 🕐 Intraday liquidity profile — when does volume concentrate?

**Value:** Execution/trading desks time orders to the open/close liquidity humps; this proves the U-shape.
**Grain:** hour-of-day across the 5-day minute window.
**Note:** assumes `start_timestamp` is epoch **milliseconds** (Polygon convention).

```sql
SELECT HOUR(from_utc_timestamp(timestamp_millis(start_timestamp), 'America/New_York')) AS et_hour,
       ROUND(AVG(volume), 0) AS avg_minute_volume,
       COUNT(*)              AS minute_bars
FROM tabular.dataexpert.fact_minute_market_hc
GROUP BY et_hour
ORDER BY et_hour;
```

## 10. Liquidity by market-cap tier — average dollar volume

**Value:** Confirms tradability/capacity per tier; large funds can only act in liquid names.
**Grain:** market-cap category over a 30-day window.

```sql
SELECT t.market_cap_category,
       COUNT(DISTINCT f.symbol)                  AS names,
       ROUND(AVG(f.close * f.volume), 0)         AS avg_daily_dollar_volume
FROM tabular.dataexpert.fact_daily_market_adjusted_hc f
JOIN tabular.dataexpert.dim_ticker_hc t USING (symbol)
WHERE f.date >= current_date() - INTERVAL 30 days
  AND t.market_cap_category IS NOT NULL
GROUP BY t.market_cap_category
ORDER BY avg_daily_dollar_volume DESC;
```
