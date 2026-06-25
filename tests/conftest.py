"""
Shared test infrastructure.

Sets up sys.path so bare module imports (e.g. ``from base_config import BaseConfig``)
work locally the same way they do inside Databricks notebooks, and mocks PySpark
so unit tests import production code without needing a running Spark cluster.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

# Ensure PySpark worker processes use the same Python interpreter that is
# running the tests. Without this, Windows App Execution Aliases intercepts
# the bare "python" command and points to the Microsoft Store instead.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# ---------------------------------------------------------------------------
# Path setup: production code under databricks/config/ and databricks/utils/
# uses bare imports that only resolve inside Databricks.  Adding these
# directories to sys.path lets local tests import the same modules.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
for subdir in ("databricks/config", "databricks/utils"):
    _path = str(_PROJECT_ROOT / subdir)
    if _path not in sys.path:
        sys.path.insert(0, _path)

# ---------------------------------------------------------------------------
# Mock PySpark modules before any production code imports them.
# Integration tests that need real Spark call ensure_real_pyspark() from
# spark_test_support to remove these stubs before importing pyspark.
# ---------------------------------------------------------------------------
_PYSPARK_MODULES = [
    "pyspark",
    "pyspark.sql",
    "pyspark.sql.functions",
    "pyspark.sql.types",
    "pyspark.sql.window",
    "pyspark.sql.column",
    "pyspark.sql.dataframe",
]
for mod in _PYSPARK_MODULES:
    sys.modules.setdefault(mod, MagicMock())
