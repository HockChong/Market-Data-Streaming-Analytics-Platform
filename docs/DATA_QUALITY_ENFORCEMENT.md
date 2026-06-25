# Data Quality Enforcement — Three Layers of Defense (OHLCV Silver)

How the OHLCV Silver pipeline (`databricks/silver/ohlcv_silver_dlt.py`) decides
whether a record is allowed through, set aside, or whether the whole pipeline
should stop. There are **three distinct mechanisms**, and they look similar
(two of them use `expect_or_fail`) but guard against completely different
failure modes.

| Mechanism | Unit of judgment | What it catches | On violation |
|-----------|------------------|-----------------|--------------|
| Row-level `expect_or_fail` | a single record | structurally impossible rows | **halt** immediately |
| WAP quarantine | a single record | bad-but-well-formed values | **set aside**, keep the row, count it |
| Aggregate gate (`wap_audit_log_hc`) | a whole day's rejection *rate* | quality *drift* in bulk | **halt** if the rate crosses a threshold |

The one-line summary: **`expect_or_fail` enforces the schema contract
(fail-fast on impossible rows); quarantine preserves and measures bad-but-valid
rows; `wap_audit_log_hc` enforces the data-quality SLA (fail-slow on a
bad-quality trend).**

---

## 1. Row-level `expect_or_fail` — the schema contract

On the enriched intermediate table at
[`ohlcv_silver_dlt.py:346-357`](../databricks/silver/ohlcv_silver_dlt.py#L346-L357):

```python
@dlt.expect_or_fail("valid_timestamps", "start_timestamp < end_timestamp")
@dlt.expect_or_fail("valid_start_timestamp", "start_timestamp > 0")
@dlt.expect_or_fail("required_fields", "symbol IS NOT NULL AND LENGTH(symbol) > 0 AND start_timestamp IS NOT NULL AND source IS NOT NULL")
@dlt.expect_or_fail("known_ts_unit", "ts_unit = 'ms'")
```

- **Unit of judgment:** one row at a time.
- **Trips on:** a *single* violating row — the pipeline halts.
- **Catches:** structural / contract breakage — a malformed timestamp range, a
  missing key, a non-`ms` epoch unit.
- **Why fail and not quarantine:** these rows are *impossible to process
  correctly*. A non-`ms` `ts_unit`, for example, would silently split the
  `(symbol, start_timestamp)` dedup key — better to stop than to corrupt the
  key space. The contract is "Bronze normalizes timestamps to epoch
  milliseconds," and this check makes a violation loud instead of silent.

Think of this as **a bouncer checking IDs at the door** — binary, per-person,
immediate.

---

## 2. WAP quarantine — preserve and measure bad-but-valid rows

The WAP validation rules (positive price, valid OHLC logic, non-negative
volume) deliberately **do not** use `expect_or_fail`. Invalid rows are routed to
`ohlcv_silver_quarantine_hc` at
[`ohlcv_silver_dlt.py:449`](../databricks/silver/ohlcv_silver_dlt.py#L449),
each tagged with a `rejection_reason`.

- **Unit of judgment:** one row at a time.
- **Catches:** values that are *well-formed but wrong* — e.g. a price of
  `-5.00` with a perfectly valid timestamp, symbol, and `ts_unit`. It passes
  every row-level `expect_or_fail` (none of them check price sign) but fails the
  WAP rules.
- **On violation:** the row is **not** dropped and does **not** halt the
  pipeline — it is set aside in quarantine for audit, and counted.

Why a third mode at all? A bad price shouldn't kill the pipeline *or* silently
vanish. WAP (Write-Audit-Publish) keeps it visible and auditable. Quarantine is
the pile of rejected rows; the next layer turns that pile into a *number*.

---

## 3. The aggregate gate in `wap_audit_log_hc` — the data-quality SLA

`wap_audit_log_hc` produces **one row per trading day** summarizing valid vs.
rejected counts and a `rejection_rate_pct`, then carries its own
`expect_or_fail` at
[`ohlcv_silver_dlt.py:545-553`](../databricks/silver/ohlcv_silver_dlt.py#L545-L553):

```python
@dlt.expect_or_fail(
    "quality_gate_pass",
    "quality_gate_passed OR audit_date < current_date() - interval 2 days",
)
```

- **Unit of judgment:** a *daily aggregate* row. `quality_gate_passed` is
  computed by comparing the day's rejection **rate** to a threshold — it is not
  a property of any single record.
- **Trips on:** a **statistical pattern**, not one bad row. A handful of bad
  ticks on a busy day keeps the rate low and the pipeline runs (correct — a few
  bad ticks are normal). But if an upstream feed degrades and, say, 15% of the
  day goes negative, the rate crosses the threshold and the gate **halts the
  pipeline** before that garbage publishes.
- **2-day grace window:** rows older than 2 days are exempt — they are already
  committed history, and a late-arriving Bronze record shouldn't retroactively
  halt the pipeline.

Think of this as **a fire marshal watching the crowd** — any one person is
fine; the alarm trips when the room is dangerously full in aggregate.

### Companion: the warn-only completeness signal

The same table carries a non-halting `@dlt.expect` at
[`ohlcv_silver_dlt.py:559-562`](../databricks/silver/ohlcv_silver_dlt.py#L559-L562)
(`session_complete`). It flags a trading day where even the fullest symbol's
session fell below half of `EXPECTED_BARS_PER_DAY` — a coarse market-wide-gap
signal that **warns** (never halts) and tolerates early-close half-days.

---

## How the three fit together

```
Structural junk   → expect_or_fail (enriched)   → HALT immediately, per row
Bad-but-formed    → WAP quarantine rules         → set aside + keep the row
   ↓ counted by   → wap_audit_log_hc             → if the day's RATE is too high → HALT
                                                  → (session_bars too low → WARN)
```

They are complementary, not redundant:

- **Row-level `expect_or_fail`** answers *"is this row structurally valid?"* —
  one violation is fatal.
- **Quarantine** answers *"is this value plausible?"* — implausible rows are
  preserved and measured, never silently dropped.
- **`wap_audit_log_hc` gate** answers *"is today's data, in bulk, healthy enough
  to trust?"* — only a bad *trend* halts.

A record can pass every row-level check and still be wrong (negative price); the
quarantine + aggregate gate are what catch that class of problem. Conversely, a
single malformed key can't be averaged away — the row-level gate stops it at the
door. You need all three.

---

## Capstone framing

- **Medallion clarity:** all three live in Silver — the layer whose contract is
  "cleaned, deduped, quality-enforced." This is what makes that claim concrete.
- **Interview one-liner:** *row-level `expect_or_fail` enforces the schema
  contract (fail-fast); quarantine preserves bad-but-valid rows for audit; the
  `wap_audit_log_hc` gate enforces a data-quality SLA (fail-slow on a trend).*
- **Why it matters:** dropping `wap_audit_log_hc` would remove the *aggregate*
  guard (and the warn-only completeness signal) while leaving the structural
  row-level checks intact — quality drift would no longer halt the pipeline,
  though individual malformed rows still would.
