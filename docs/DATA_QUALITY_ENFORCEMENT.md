# Data Quality Enforcement — Three Layers of Defense (OHLCV Silver)

How the OHLCV Silver pipeline ([`ohlcv_silver_dlt.py`](../databricks/silver/ohlcv_silver_dlt.py))
decides whether a record is allowed through, set aside, or whether the whole pipeline
should stop. There are **three distinct mechanisms**. Two of them use `expect_or_fail`
and look alike, but they judge different units and guard against different failure modes.

| Mechanism | Unit of judgment | What it catches | On violation | Threshold configured in |
|-----------|------------------|-----------------|--------------|--------------------------|
| Row-level `expect_or_fail` | a single record | structurally impossible rows | **halt** immediately | n/a — boolean predicates |
| WAP (Write-Audit-Publish) quarantine | a single record | bad-but-well-formed values | **excluded** from Silver, **copied** to quarantine with a reason | [`silver_config.py:103-107`](../databricks/config/silver_config.py#L103-L107) |
| Aggregate gate (`wap_audit_log_hc`) | a whole day's rejection *rate* | quality *drift* in bulk | **halt** if the rate reaches **1.0%** | [`base_config.py:100-101`](../databricks/config/base_config.py#L100-L101) |

In one line: **`expect_or_fail` enforces the schema contract (fail-fast on impossible
rows); the WAP rules exclude bad-but-valid rows from Silver while preserving them in an
auditable quarantine copy; `wap_audit_log_hc` enforces the data-quality SLA (fail-slow on
a bad trend).**

---

## 1. Row-level `expect_or_fail` — the schema contract

On the enriched intermediate table at
[`ohlcv_silver_dlt.py:352-363`](../databricks/silver/ohlcv_silver_dlt.py#L352-L363):

```python
@dlt.expect_or_fail("valid_timestamps", "start_timestamp < end_timestamp")
@dlt.expect_or_fail("valid_start_timestamp", "start_timestamp > 0")
@dlt.expect_or_fail("required_fields", "symbol IS NOT NULL AND LENGTH(symbol) > 0 AND start_timestamp IS NOT NULL AND source IS NOT NULL")
@dlt.expect_or_fail("known_ts_unit", "ts_unit = 'ms'")
```

- **Unit of judgment:** one row at a time.
- **Trips on:** a *single* violating row — the pipeline halts.
- **Catches:** structural / contract breakage — a malformed timestamp range, a missing
  key, a non-`ms` epoch unit.
- **Why fail and not quarantine:** these rows are *impossible to process correctly*. A
  non-`ms` `ts_unit`, for example, would silently split the `(symbol, start_timestamp)`
  dedup key — better to stop than to corrupt the key space. The contract is "Bronze
  normalizes timestamps to epoch milliseconds," and this check makes a violation loud
  instead of silent.

Think of this as **a bouncer checking IDs at the door** — binary, per-person, immediate.

---

## 2. WAP quarantine — exclude from Silver, preserve for audit

The WAP validation rules (positive price, valid OHLC logic, non-negative volume)
deliberately **do not** use `expect_or_fail`. A price of `-5.00` with a perfectly valid
timestamp, symbol, and `ts_unit` passes every row-level check above — none of them look at
price sign — but it must not reach Silver, and it must not vanish either.

### It is two reads, not a router

This is the part worth being precise about, because "routing" is the obvious mental model
and it is not what the code does. The valid and invalid paths are **two independent reads
of the same Bronze source sharing one predicate**:

| Path | Read | Scope | Where |
|---|---|---|---|
| Silver production | `dlt.read_stream("bronze_unified_hc")`, keeps rows where all rules pass | streaming, unbounded | [`ohlcv_silver_dlt.py:370-375`](../databricks/silver/ohlcv_silver_dlt.py#L370-L375) |
| Quarantine | `dlt.read("bronze_unified_hc")`, keeps rows where any rule fails | **batch, rolling 30 days** | [`ohlcv_silver_dlt.py:500-507`](../databricks/silver/ohlcv_silver_dlt.py#L500-L507) |

Both derive their predicate from the same `WAP_VALIDATION_RULES` dict, so the two paths
cannot drift apart on *what* counts as invalid.

**Why quarantine is batch, not streaming:** it dedupes with `row_number().over()`, and
non-time-based window functions are not supported on streaming DataFrames — the pipeline
would raise `AnalysisException` at startup. Quarantine is an audit log, not a
latency-sensitive output, so batch is the right call
([`ohlcv_silver_dlt.py:483-496`](../databricks/silver/ohlcv_silver_dlt.py#L483-L496)).

**The trade-off this creates:** because quarantine is bounded to 30 days and the Silver
filter is not, an invalid row older than 30 days is excluded from Silver **with no
quarantine record**. The win is a per-run scan cost that stays constant instead of growing
with total Bronze history. Bronze still holds the raw row either way — nothing is
destroyed, it just isn't surfaced in the audit table.

### Two details that matter when reading the table

- **Quarantine is deduped**, to one row per `(symbol, start_timestamp, source)`, with the
  winner chosen by descending `xxhash64` over the payload columns — so the surviving row
  is a function of content, not batch arrival order
  ([`ohlcv_quarantine_spark.py:15-34`](../databricks/utils/ohlcv_quarantine_spark.py#L15-L34)).
  This is why `COUNT(*)` here does **not** match `rejected_count` in `wap_audit_log_hc`,
  which is a raw pre-dedup count.
- **`rejection_reason` is first-match precedence, not a set**: price → OHLC logic → volume
  ([`ohlcv_quarantine_spark.py:37-48`](../databricks/utils/ohlcv_quarantine_spark.py#L37-L48)).
  A row that breaks all three is labelled `invalid_price_positive` only. Counting by
  reason therefore counts *primary* causes, not total rule violations.

Quarantine is the pile of rejected rows. The next layer turns that pile into a *number*.

---

## 3. The aggregate gate in `wap_audit_log_hc` — the data-quality SLA

`wap_audit_log_hc` produces **one row per trading day** summarizing valid vs. rejected
counts and a `rejection_rate_pct`, then carries its own `expect_or_fail` at
[`ohlcv_silver_dlt.py:551-559`](../databricks/silver/ohlcv_silver_dlt.py#L551-L559):

```python
@dlt.expect_or_fail(
    "quality_gate_pass",
    "quality_gate_passed OR audit_date < current_date() - interval 2 days",
)
```

- **Unit of judgment:** a *daily aggregate* row. `quality_gate_passed` is
  `rejection_rate_pct < 1.0` — a property of the day, not of any single record
  ([`wap_audit_spark.py:64-65`](../databricks/utils/wap_audit_spark.py#L64-L65)).
- **Trips on:** a **statistical pattern**. A handful of bad ticks on a busy day keeps the
  rate near zero and the pipeline runs — correct, a few bad ticks are normal. At **0.5%**
  the day is flagged `quality_gate_warning` but still publishes; at **1.0%** the gate
  fails and the run **halts** before that data reaches Gold.

### The rate is computed at a single grain — deliberately

Both the numerator and the denominator come from the *same pre-dedup Bronze scan*
([`ohlcv_silver_dlt.py:582-591`](../databricks/silver/ohlcv_silver_dlt.py#L582-L591)):

```python
counts = aggregate_bronze_wap_counts_by_date(bronze_df, WAP_VALIDATION_RULES)
```

`aggregate_bronze_wap_counts_by_date` derives `total_count` and `rejected_count` in one
`groupBy` over the same rows ([`wap_audit_spark.py:22-30`](../databricks/utils/wap_audit_spark.py#L22-L30)).
That is what makes the rate replay-stable: a Kafka replay duplicates good and bad rows
alike, so both sides scale together and the ratio holds.

The tempting alternative — count deduped quarantine rows over a raw Bronze total — mixes
grains. After a replay the denominator inflates while the deduped numerator does not, the
rate collapses toward zero, and a genuine quality breach reads as healthy. Same three
rules, same data, wrong answer, and the gate silently stops protecting anything.

### The 2-day grace window is a trade-off, not just a feature

Rows older than 2 days are exempt from the gate. The upside: already-committed history
can't retroactively halt a run, and a late-arriving Bronze record that nudges last month's
rate over the line doesn't produce an un-actionable failure.

The cost, stated plainly: **a breach discovered more than 2 days late can never halt the
pipeline.** `quality_gate_passed` is still written to the row, so the breach is visible in
the audit table forever — it just becomes a dashboard finding rather than a gate. That is
the intended behaviour for a platform whose Bronze is immutable and replayable, but it
does mean the gate protects *freshness of detection*, not history.

### Companion: the warn-only completeness signal

The same table carries a non-halting `@dlt.expect` at
[`ohlcv_silver_dlt.py:565-568`](../databricks/silver/ohlcv_silver_dlt.py#L565-L568):

```python
@dlt.expect(
    "session_complete",
    f"session_bars IS NULL OR session_bars >= {_SESSION_BARS_MIN} OR audit_date < current_date() - interval 2 days",
)
```

`session_bars` is the most bars any single symbol reached that day
([`wap_audit_spark.py:50-51`](../databricks/utils/wap_audit_spark.py#L50-L51)) — ~390 on a
normal session, ~210 on an early-close half-day. If even the busiest symbol barely traded,
that is a market-wide ingestion gap rather than a per-symbol problem. The floor is derived,
not hardcoded: `EXPECTED_BARS_PER_DAY * 0.5` = **195**
([`ohlcv_silver_dlt.py:173`](../databricks/silver/ohlcv_silver_dlt.py#L173)), which sits
below the ~210 of a half-day so early closes don't false-trip it.

It reads deduped Silver rather than pre-dedup Bronze on purpose: a replay against Bronze
would inflate the bar count and mask the very gap this is looking for.

**The honest limit:** `session_bars` is computed over a **3-day** Silver window
([`ohlcv_silver_dlt.py:601-605`](../databricks/silver/ohlcv_silver_dlt.py#L601-L605)).
Every older `audit_date` gets `NULL` from the left join, and `NULL` passes. So for most
rows in the table this check is inert *by construction*. That is the deliberate trade:
the full `(date, symbol)` shuffle is the heaviest repeated cost in the pipeline, and the
gate only ever looks back 2 days, so recomputing it across 30 days every run would buy
nothing. `NULL` is an honest "not recomputed," not a stale pass.

---

## How the three fit together

```
Structural junk   → expect_or_fail (enriched)   → HALT immediately, per row
Bad-but-formed    → WAP rules                    → excluded from Silver
   ↓ same rules   → quarantine (batch, 30d)      → preserved + reason-tagged
   ↓ counted by   → wap_audit_log_hc             → day's RATE ≥ 1.0% → HALT
                                                  → session_bars < 195 → WARN
```

They are complementary, not redundant:

- **Row-level `expect_or_fail`** answers *"is this row structurally valid?"* — one
  violation is fatal, because a malformed key can't be averaged away.
- **WAP rules + quarantine** answer *"is this value plausible?"* — implausible rows are
  kept out of Silver and preserved for audit rather than silently dropped.
- **The `wap_audit_log_hc` gate** answers *"is today's data, in bulk, healthy enough to
  trust?"* — only a bad *trend* halts.

A record can pass every row-level check and still be wrong (negative price); the WAP rules
and aggregate gate are what catch that class. You need all three.

---

## Note: what the streaming watermark actually does

The `.withWatermark(...)` call on the Bronze streams
([`ohlcv_silver_dlt.py:159`](../databricks/silver/ohlcv_silver_dlt.py#L159)) looks like it
prevents duplicates — it doesn't. That's handled by `apply_changes`, which always keeps the
right row via `sequence_by="_dedup_sequence"`
([`ohlcv_silver_dlt.py:442`](../databricks/silver/ohlcv_silver_dlt.py#L442)), no matter how
late it arrives.

The watermark only tells Databricks it can stop tracking a row once nothing has updated it
in 10 minutes (1 hour for news) — memory cleanup, not correctness. Without it, tracking
would grow unbounded over the pipeline's life.

---

## When a gate trips

- **What fails:** the DLT update fails. Nothing downstream of the failing flow publishes,
  so Gold never sees the bad data.
- **What survives:** everything. Bronze is append-only and immutable, so the raw records
  are all still there — the halt costs you a pipeline run, not data.
- **How to recover:** fix the upstream cause, then rerun. The rerun is idempotent by
  design: Silver upserts on `(symbol, start_timestamp)` via `apply_changes`
  ([`ohlcv_silver_dlt.py:438-448`](../databricks/silver/ohlcv_silver_dlt.py#L438-L448)),
  so replayed rows converge onto the same keys instead of duplicating. This is the same
  property that lets Kafka be treated as at-least-once.
- **How to triage:** start with the daily rate in `wap_audit_log_hc` to see *when* it
  turned, then read example rows from `ohlcv_silver_quarantine_hc` to see *what* broke.
  Both queries, plus the DLT event-log query for expectation pass/fail counts, are in
  [QUARANTINE_QUERIES.md](QUARANTINE_QUERIES.md).
- **Known gap:** a breach surfaces as a pipeline failure, not as an alert. Wiring the audit
  gate to a notification channel is listed as a next step in the
  [project README](../README.md#limitations--what-id-do-next).

---

## What this does not catch

Worth being explicit, because "data quality enforcement" can imply more than three
structural predicates:

- **Only structural validity is checked.** Positive prices, OHLC ordering, and a
  `volume >= 0` floor. There is no detection of price spikes, stale prints, trading halts,
  or suspicious zero-volume bars — a bar can be internally consistent and still wrong.
- **No cross-source reconciliation.** Flat-file and Kafka bars for the same minute are
  resolved by source priority in the dedup, not compared for agreement, so a systematic
  disagreement between the two feeds would not be flagged.
- **No freshness or latency SLA.** `session_complete` is a coarse volume signal; nothing
  asserts that today's data arrived on time.
- **Rolling windows, not full history.** Quarantine and the audit counts cover 30 days;
  `session_bars` covers 3. Older rejections age out of the audit surface. That trade bounds
  per-run scan cost but ties retention length to scan cost — decoupling them (an
  incremental/append `wap_audit_log_hc`, upserting on `audit_date` instead of recomputing
  the full window each run) is listed as a next step in the
  [project README](../README.md#limitations--what-id-do-next).
- **Three-valued logic at the boundary.** The WAP predicates are SQL comparisons, so a
  `NULL` price would make both `is_valid` and `~is_all_valid` evaluate to `NULL` and the
  row would fall out of *both* paths. The Avro contract closes this for the streaming feed
  — `open`/`high`/`low`/`close`/`volume` are non-nullable
  ([`ohlcv_aggregate.avsc:17-41`](../schemas/avro/ohlcv_aggregate.avsc#L17-L41)), so a null
  price fails deserialization into the Bronze dead-letter table instead of reaching Silver.

---

## How this is verified

The enforcement logic lives in plain Spark helpers in `databricks/utils/` rather than
inline in the DLT notebook, specifically so it can be exercised without a DLT runtime:

| Behaviour | Helper | Test |
|---|---|---|
| Gate flags across all three zones (0.3% pass, 0.6% warn, 1.2% fail) | `finalize_wap_audit_metrics` | [`test_integration_spark.py:312`](../tests/test_integration_spark.py#L312) |
| `rejection_reason` precedence, including price-wins-over-all | `with_quarantine_rejection_reason` | [`test_integration_spark.py:269`](../tests/test_integration_spark.py#L269) |

Both are real-`SparkSession` integration tests, and both read their thresholds and rules
from `SilverConfig` rather than restating literals — so changing a threshold in config
moves the test with it instead of leaving a stale assertion behind.

---

## Other enforcement stages in the platform

This document covers OHLCV Silver only. The same WAP shape recurs elsewhere: the news
pipeline pairs `expect_or_fail` on article identity with its own
`news_silver_quarantine_hc` ([`news_silver_dlt.py:141-142`](../databricks/silver/news_silver_dlt.py#L141-L142)),
Bronze routes Avro deserialization failures to a path-based dead-letter table
([`streaming_ingestion.py:184`](../databricks/bronze/streaming_ingestion.py#L184)), and Gold
carries warn-only expectations that flag rows in place rather than quarantining them.
[QUARANTINE_QUERIES.md](QUARANTINE_QUERIES.md) maps all of them with runnable queries.
