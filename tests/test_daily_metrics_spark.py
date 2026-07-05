"""Integration tests for daily_metrics_spark.py against a real SparkSession.

Covers the rolling-metric contract:
  1. Lag closes anchor N trading days back per symbol; NULL until N prior rows exist
  2. rvol_20d gates on a full 20-day prior-volume base (COUNT = 20), else NULL
  3. Windows are partitioned per symbol — no cross-symbol leakage
  4. Re-running on the same input is byte-identical (idempotency)

Skip with: pytest -m "not integration"
"""

import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

_TESTS_DIR = Path(__file__).resolve().parent
if str(_TESTS_DIR) not in sys.path:
    sys.path.insert(0, str(_TESTS_DIR))

pytestmark = pytest.mark.integration

from spark_test_support import build_local_spark_session, ensure_real_pyspark, stop_local_spark_session

ensure_real_pyspark()
try:
    import pyspark  # noqa: F401
except ImportError:
    pytest.skip("pyspark required for Spark integration tests", allow_module_level=True)

from pyspark.sql.types import (  # noqa: E402
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for _subdir in ("databricks/config", "databricks/utils"):
    _p = str(_PROJECT_ROOT / _subdir)
    if _p not in sys.path:
        sys.path.insert(0, _p)

sys.modules.pop("daily_metrics_spark", None)

from daily_metrics_spark import (  # noqa: E402
    LAG_CLOSE_OFFSETS,
    RVOL_BASELINE_DAYS,
    add_daily_metric_columns,
)

_ADJUSTED_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("adj_close", DoubleType(), True),
        StructField("adj_volume", LongType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = build_local_spark_session("daily_metrics_tests")
    yield session
    stop_local_spark_session(session)


def _series(symbol: str, n_days: int, close_start: float = 100.0, volume: int = 1000):
    """Fixed daily series: close increases by 1.0/day, constant volume."""
    start = date(2024, 1, 1)
    return [(symbol, start + timedelta(days=i), close_start + i, volume) for i in range(n_days)]


def _run(spark, rows):
    df = spark.createDataFrame(rows, schema=_ADJUSTED_SCHEMA)
    return {(r.symbol, r.date): r for r in add_daily_metric_columns(df).collect()}


def test_lag_closes_anchor_prior_trading_days(spark):
    n = 30
    out = _run(spark, _series("AAPL", n))
    last = out[("AAPL", date(2024, 1, 1) + timedelta(days=n - 1))]

    # close on row i is 100 + i, so a 5-row lag from the last row (i=29) is 100 + 24.
    assert last.close_5d == 100.0 + (n - 1 - 5)
    assert last.close_21d == 100.0 + (n - 1 - 21)
    # Not enough history for the longer anchors on a 30-row series.
    assert last.close_63d is None
    assert last.close_126d is None
    assert last.close_252d is None

    # Row with exactly 5 prior rows gets a value; row with 4 prior rows does not.
    assert out[("AAPL", date(2024, 1, 6))].close_5d == 100.0
    assert out[("AAPL", date(2024, 1, 5))].close_5d is None


def test_rvol_requires_full_20_day_base(spark):
    n = RVOL_BASELINE_DAYS + 2  # rows 0..21: row 20 is the first with a full base
    out = _run(spark, _series("AAPL", n))
    dates = sorted(d for (_, d) in out)

    # 19 prior rows -> NULL; 20 prior rows -> constant volume gives RVOL exactly 1.0.
    assert out[("AAPL", dates[RVOL_BASELINE_DAYS - 1])].rvol_20d is None
    assert out[("AAPL", dates[RVOL_BASELINE_DAYS])].rvol_20d == pytest.approx(1.0)
    assert out[("AAPL", dates[RVOL_BASELINE_DAYS + 1])].rvol_20d == pytest.approx(1.0)


def test_rvol_scales_against_prior_volume_average(spark):
    rows = _series("AAPL", RVOL_BASELINE_DAYS, volume=1000)
    spike_date = date(2024, 1, 1) + timedelta(days=RVOL_BASELINE_DAYS)
    rows.append(("AAPL", spike_date, 200.0, 3000))
    out = _run(spark, rows)

    # 3000 vs a 20-day base averaging 1000 -> RVOL 3.0.
    assert out[("AAPL", spike_date)].rvol_20d == pytest.approx(3.0)


def test_windows_do_not_cross_symbols(spark):
    rows = _series("AAPL", 10, close_start=100.0) + _series("TSLA", 3, close_start=900.0)
    out = _run(spark, rows)

    # TSLA has only 2 prior rows — AAPL's history must not leak into its lag.
    assert out[("TSLA", date(2024, 1, 3))].close_5d is None
    # AAPL's lag still resolves within its own partition.
    assert out[("AAPL", date(2024, 1, 10))].close_5d == 104.0


def test_all_metric_columns_present(spark):
    df = spark.createDataFrame(_series("AAPL", 3), schema=_ADJUSTED_SCHEMA)
    result = add_daily_metric_columns(df)
    for name in [*LAG_CLOSE_OFFSETS, "rvol_20d"]:
        assert name in result.columns


def test_rerun_is_idempotent(spark):
    rows = _series("AAPL", 25) + _series("TSLA", 25, close_start=900.0, volume=500)
    first = _run(spark, rows)
    second = _run(spark, rows)

    assert {k: v.asDict() for k, v in first.items()} == {k: v.asDict() for k, v in second.items()}
