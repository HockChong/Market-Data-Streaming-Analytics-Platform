# Databricks notebook source
# MAGIC %md
# MAGIC # Volume Path Setup Script
# MAGIC
# MAGIC Creates all required Unity Catalog Volume paths for the Market Data Platform.
# MAGIC Run this notebook once during initial deployment to set up the directory structure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

import sys

sys.path.insert(
    0,
    "/Workspace"
    + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().rsplit("/", 2)[0]
    + "/config",
)
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()
from base_config import BaseConfig

# COMMAND ----------

VOLUME_BASE_PATH = BaseConfig.VOLUME_BASE_PATH
VOLUME_NAME = VOLUME_BASE_PATH.rstrip("/").split("/")[-1]
VOLUME_FQN = f"{BaseConfig.CATALOG}.{BaseConfig.SCHEMA}.{VOLUME_NAME}"

# All paths that need to be created
# Note: Silver/Gold tables are DLT managed tables, not volume paths
VOLUME_PATHS = {
    # Bronze Layer - Raw data ingestion
    "bronze": [
        "bronze/streaming",
        "bronze/streaming_quarantine",  # Dead letter queue for Avro deserialization failures
        "bronze/historical",
        "bronze/news",
        "bronze/ticker_details",
    ],
    # Checkpoints - Bronze streaming only; DLT manages silver/gold checkpoints internally
    "checkpoints": [
        "_checkpoints/bronze/streaming_ingestion",
        "_checkpoints/bronze/streaming_ingestion_quarantine",
    ],
    # Metrics - Drain metrics written by streaming_ingestion.py on shutdown
    "metrics": [
        "_metrics/streaming_ingestion",
    ],
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Functions

# COMMAND ----------


def _get_spark_session():
    """Return the active Spark session (notebook cluster or SparkSession builder)."""
    spark_session = globals().get("spark")
    if spark_session is not None:
        return spark_session
    from pyspark.sql import SparkSession

    spark_session = SparkSession.getActiveSession()
    if spark_session is None:
        raise RuntimeError("Spark session not available. Attach a cluster to this notebook and re-run.")
    return spark_session


def ensure_base_volume(spark_session, volume_fqn, base_path, dbutils):
    """Create the Unity Catalog volume if missing and verify it is reachable."""
    spark_session.sql(f"CREATE VOLUME IF NOT EXISTS {volume_fqn}")
    print(f"Ensured volume exists: {volume_fqn}")

    try:
        dbutils.fs.ls(base_path)
        print(f"Base volume reachable: {base_path}")
    except Exception as e:
        raise RuntimeError(
            f"Volume {volume_fqn} was created or already exists, but path is not reachable: "
            f"{base_path}. Confirm catalog '{BaseConfig.CATALOG}' and schema "
            f"'{BaseConfig.SCHEMA}' exist and you have USE CATALOG / USE SCHEMA / "
            f"CREATE VOLUME privileges. Manual fix: CREATE VOLUME IF NOT EXISTS {volume_fqn};"
        ) from e


def setup_all_paths(dbutils, base_path, paths_config):
    """Create all volume paths from configuration."""

    ok, failed = 0, []
    for layer, paths in paths_config.items():
        print(f"\n{layer.upper()}:")
        for relative_path in paths:
            full_path = f"{base_path}/{relative_path}"
            try:
                dbutils.fs.mkdirs(full_path)
                dbutils.fs.ls(full_path)  # verify the directory is reachable after creation
                print(f"  OK: {full_path}")
                ok += 1
            except Exception as e:
                print(f"  FAILED: {full_path} - {e}")
                failed.append(full_path)

    print(f"\nDone: {ok} OK, {len(failed)} failed")
    if failed:
        raise RuntimeError(f"Failed to create paths: {failed}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Setup

# COMMAND ----------

_dbutils = globals().get("dbutils")
if _dbutils is not None:
    ensure_base_volume(_get_spark_session(), VOLUME_FQN, VOLUME_BASE_PATH, _dbutils)
    setup_all_paths(_dbutils, VOLUME_BASE_PATH, VOLUME_PATHS)
elif __name__ == "__main__":
    print(
        "This script must run on Databricks (notebook or job) where dbutils is available.\n"
        "It creates directories under the Unity Catalog volume via dbutils.fs.\n"
        "Open this file as a notebook in your workspace and run all cells, "
        "or paste the setup cells into a notebook on a cluster.",
        file=sys.stderr,
    )
    sys.exit(1)
