# Daily OHLCV Rollup — Design Rationale

This document explains **why** the daily OHLCV rollup is built the way it is. For the
table schema see `[DATA_DICTIONARY.md](DATA_DICTIONARY.md)`; for end-to-end lineage and
scheduling see `[DATA_LINEAGE.md](DATA_LINEAGE.md)`. All references below are grounded in
`[databricks/silver/ohlcv_silver_dlt.py](../databricks/silver/ohlcv_silver_dlt.py)`.

## The problem it solves

Gold analytics and charts need **daily** OHLCV (open / high / low / close / volume per
symbol per trading day), not raw 1-minute bars. Letting every Gold query scan the full
minute history would be slow and expensive. The rollup pre-computes the daily grain once
in Silver so Gold reads **~2.9M daily rows instead of ~420M minute rows**. This is the
Silver→Gold read-optimization the medallion contract asks for: cleaned data reshaped into
the grain analytics actually consumes.

## The object

| Object                  | Type                                                              | Role                                                                                          |
| ----------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `ohlcv_daily_silver_hc` | Materialized view (`@dlt.table`, batch query over minute Silver)  | Daily OHLCV at grain `(symbol, date)`, aggregated from deduplicated minute Silver.            |

`ohlcv_daily_silver_hc` is a **materialized view**: a deterministic batch aggregation whose
content is fully defined by the current `ohlcv_silver_hc` snapshot. There is no staging
table, no `apply_changes` MERGE, and no streaming state.

## Why a materialized view, not a streaming aggregation

Daily OHLC is **first / last / min / max over a whole trading day**:

- `open` = first minute of the day, `close` = last minute — both require knowing the full
day, and the day is not "done" until the session closes.
- Kafka is at-least-once with late and out-of-order events, so a streaming aggregation over
an open day would emit partial or wrong bars.

A batch aggregation over the **complete set of minute bars for a date** (`dlt.read("ohlcv_silver_hc")`)
recomputes a correct daily bar no matter how late the data arrived. The MV re-derives the
daily grain from the current minute snapshot each refresh — correctness over cleverness.

## How it stays cheap on serverless (incremental refresh)

Re-aggregating all ~420M minute rows on every trigger would be wasteful. On this
**serverless** pipeline, the engine (Enzyme) refreshes the MV **incrementally**: because the
query is a pure `groupBy(symbol, date)` of associative aggregates (`min` / `max` / `sum`,
see `[aggregation_utils.py](../databricks/utils/aggregation_utils.py)`), it can recompute
only the `(symbol, date)` groups whose underlying minute rows changed, rather than scanning
the whole table.

Two things keep this incremental path available:

- **No `current_date()` predicate.** The MV reads the full minute Silver table. A
time-dependent filter would make the query non-deterministic across runs and force a full
recompute; the daily fact in Gold applies its own lookback window instead.
- **Aggregation shape.** `sum(volume)` is cleanly incrementally maintainable. The
`min`/`max` aggregates (high, low, and the first/last-bar structs for open/close) are
**best-effort**: when a row that held a group's min/max is updated or deleted by the
upstream minute MERGE, that group is recomputed, and the engine may fall back to a full
recompute when that is cheaper or required. Expect "recompute the changed days," not a
guaranteed minimal touch.

## Idempotency

The grain is **one row per `(symbol, date)`**. The MV output is a pure function of the
current `ohlcv_silver_hc` contents, so:

- Any refresh — incremental or full — yields **identical** daily rows for a given date. There
is no path that can duplicate analytic rows.
- Reruns, retries, and overlapping backfills converge to the same result because there is no
accumulated streaming state to drift; the MV is simply re-derived.

`open`/`close` are selected via `min`/`max` of a `(start_timestamp, price)` struct
(`aggregation_utils.py`), and `(symbol, start_timestamp)` is unique per group after Silver
dedup — so the first/last-bar choice is deterministic with no timestamp ties.

## Refresh behavior

- **Normal runs** refresh the MV incrementally (changed `(symbol, date)` groups), as above.
- **A full refresh** drops and fully recomputes the daily grain from the entire current
minute Silver snapshot — no special flag or bootstrap window is needed, because the MV
reads the full table by definition.

## Deploy note (one-time)

`ohlcv_daily_silver_hc` was previously a streaming table fed by `apply_changes`. A streaming
table cannot be converted to a materialized view in place, so the existing table must be
**dropped once** (`DROP TABLE tabular.dataexpert.ohlcv_daily_silver_hc`) before the first
pipeline run materializes it as an MV. That first run is a full recompute; subsequent runs
are incremental.

## In one sentence

A **correct, idempotent daily rollup** as a single materialized view: re-derive daily bars
from the current minute snapshot so late data and reruns never corrupt or duplicate the
result, and let serverless incremental refresh recompute only the days that moved.
