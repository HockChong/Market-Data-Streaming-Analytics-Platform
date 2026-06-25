"""Integration tests for split_adjust_spark.py against a real SparkSession.

Covers the split-adjustment contract:
  1. Forward and reverse splits rescale pre-split prices; volume scales inversely
  2. Re-running on the same input is byte-identical (idempotency)

Skip with: pytest -m "not integration"
"""

import sys
from datetime import date
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

sys.modules.pop("split_adjust_spark", None)

from split_adjust_spark import (  # noqa: E402
    adjustment_factor_segments,
    apply_split_adjustment,
    latest_splits_snapshot_to_dim_df,
)

_SPLITS_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("execution_date", DateType(), False),
        StructField("split_from", DoubleType(), True),
        StructField("split_to", DoubleType(), True),
        StructField("adjustment_type", StringType(), True),
        StructField("historical_adjustment_factor", DoubleType(), True),
    ]
)

_PRICES_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), False),
        StructField("date", DateType(), False),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = build_local_spark_session("split_adjust_tests")
    yield session
    stop_local_spark_session(session)


def _adjust(spark, splits_rows, price_rows):
    """Run the full util chain (dim -> segments -> apply) and return {(symbol, date): Row}."""
    splits = latest_splits_snapshot_to_dim_df(spark.createDataFrame(splits_rows, schema=_SPLITS_SCHEMA))
    segments = adjustment_factor_segments(splits)
    prices = spark.createDataFrame(price_rows, schema=_PRICES_SCHEMA)
    return {(r.symbol, r.date): r for r in apply_split_adjustment(prices, segments).collect()}


@pytest.mark.parametrize(
    (
        "symbol",
        "exec_date",
        "split_from",
        "split_to",
        "adj_type",
        "factor",
        "price_date",
        "raw_price",
        "raw_vol",
        "exp_adj_price",
        "exp_adj_vol",
    ),
    [
        # Forward 2-for-1: pre-split prices halved, volume doubled (factor 0.5).
        ("AAPL", date(2020, 8, 31), 1.0, 2.0, "forward_split", 0.5, date(2020, 8, 28), 500.0, 1000, 250.0, 2000),
        # Reverse 1-for-10: pre-split prices scaled up 10x, volume cut to a tenth (factor 10.0).
        ("TSLA", date(2020, 1, 10), 10.0, 1.0, "reverse_split", 10.0, date(2020, 1, 5), 1.0, 5000, 10.0, 500),
    ],
    ids=["forward_split", "reverse_split"],
)
def test_split_rescales_prices_and_inverts_volume(
    spark,
    symbol,
    exec_date,
    split_from,
    split_to,
    adj_type,
    factor,
    price_date,
    raw_price,
    raw_vol,
    exp_adj_price,
    exp_adj_vol,
):
    splits = [(symbol, exec_date, split_from, split_to, adj_type, factor)]
    prices = [(symbol, price_date, raw_price, raw_price, raw_price, raw_price, raw_vol)]
    out = _adjust(spark, splits, prices)

    row = out[(symbol, price_date)]
    assert row.adj_close == exp_adj_price
    assert row.adj_open == exp_adj_price
    assert row.adj_volume == exp_adj_vol
    assert row.price_factor == factor
    assert row.close == raw_price  # raw column preserved


def test_rerun_is_idempotent(spark):
    splits = [("AAPL", date(2020, 8, 31), 1.0, 2.0, "forward_split", 0.5)]
    prices = [
        ("AAPL", date(2020, 8, 28), 500.0, 500.0, 500.0, 500.0, 1000),
        ("AAPL", date(2020, 9, 1), 250.0, 250.0, 250.0, 250.0, 2000),
    ]
    first = _adjust(spark, splits, prices)
    second = _adjust(spark, splits, prices)

    assert {k: v.asDict() for k, v in first.items()} == {k: v.asDict() for k, v in second.items()}
