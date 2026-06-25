"""Shared setup for tests that need a real PySpark (not conftest mocks)."""

from __future__ import annotations

import shutil
import sys
import tempfile
from unittest.mock import MagicMock

_SPARK_LOCAL_DIRS: dict[int, list[str]] = {}


def ensure_real_pyspark():
    """Remove MagicMock pyspark entries so ``import pyspark`` loads the real package."""
    mocked = [k for k in list(sys.modules) if k.startswith("pyspark") and isinstance(sys.modules[k], MagicMock)]
    for mod in mocked:
        del sys.modules[mod]


def build_local_spark_session(app_name: str):
    """Local SparkSession tuned for pytest on Windows (single core, stable Python workers)."""
    ensure_real_pyspark()
    from pyspark.sql import SparkSession

    local_dir = tempfile.mkdtemp(prefix="pytest-spark-")

    session = (
        SparkSession.builder.master("local[1]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.local.dir", local_dir)
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.timeout", "600")
        .getOrCreate()
    )
    session.conf.set("spark.sql.session.timeZone", "UTC")
    _SPARK_LOCAL_DIRS.setdefault(id(session), []).append(local_dir)
    return session


def stop_local_spark_session(session) -> None:
    """Stop Spark and remove only the temp dirs created for this session."""
    dirs = _SPARK_LOCAL_DIRS.pop(id(session), [])
    session.stop()
    for path in dirs:
        shutil.rmtree(path, ignore_errors=True)
