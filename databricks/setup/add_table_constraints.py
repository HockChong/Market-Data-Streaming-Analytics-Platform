# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Catalog Table Constraints Setup
# MAGIC
# MAGIC ## What This Notebook Does
# MAGIC Adds PRIMARY KEY and FOREIGN KEY constraints to Gold layer tables in Unity Catalog.
# MAGIC These constraints provide:
# MAGIC - **Documentation**: Clearly defines table relationships for data consumers
# MAGIC - **Query Optimization**: Catalyst optimizer uses constraints for better join planning
# MAGIC - **Data Governance**: Enforces referential integrity awareness (informational only)
# MAGIC
# MAGIC ## Important Notes
# MAGIC - Unity Catalog constraints are **informational only** (not enforced at write time)
# MAGIC - They help the query optimizer and provide documentation
# MAGIC - Run this notebook AFTER DLT pipelines have created the tables
# MAGIC
# MAGIC ## Tables Covered
# MAGIC | Table | Constraint Type | Description |
# MAGIC |-------|-----------------|-------------|
# MAGIC | dim_ticker_hc | PRIMARY KEY | symbol |
# MAGIC | dim_date_hc | PRIMARY KEY | date |
# MAGIC | fact_daily_market_hc | PRIMARY KEY + 2 FK | (symbol, date) -> dim_ticker, dim_date |
# MAGIC | fact_news_hc | PRIMARY KEY + 2 FK | (article_id, symbol) -> dim_ticker; published_date -> dim_date |

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

# Unity Catalog settings
CATALOG = BaseConfig.CATALOG
SCHEMA = BaseConfig.SCHEMA

# Table names (as created by DLT pipelines)
TABLES = {
    "dim_ticker_hc": f"{CATALOG}.{SCHEMA}.dim_ticker_hc",
    "dim_date_hc": f"{CATALOG}.{SCHEMA}.dim_date_hc",
    "fact_daily_market_hc": f"{CATALOG}.{SCHEMA}.fact_daily_market_hc",
    "fact_news_hc": f"{CATALOG}.{SCHEMA}.fact_news_hc",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------


def table_exists(table_name: str) -> bool:
    """Check if a table exists in Unity Catalog."""
    try:
        spark.sql(f"DESCRIBE TABLE {table_name}")
        return True
    except Exception:
        return False


def constraint_exists(table_name: str, constraint_name: str) -> bool:
    """Check if a constraint already exists on the table."""
    try:
        constraints = spark.sql(f"SHOW TBLPROPERTIES {table_name}").collect()
        for row in constraints:
            if constraint_name.lower() in str(row).lower():
                return True
        return False
    except Exception:
        return False


def add_constraint_safe(sql: str, constraint_name: str, table_name: str) -> None:
    """Add a constraint with error handling."""
    try:
        if not table_exists(table_name):
            print(f"⚠️  Table {table_name} does not exist - skipping {constraint_name}")
            return

        spark.sql(sql)
        print(f"✅ Added {constraint_name} to {table_name}")
    except Exception as e:
        error_msg = str(e).lower()
        if "already exists" in error_msg or "duplicate" in error_msg:
            print(f"ℹ️  {constraint_name} already exists on {table_name}")
        else:
            print(f"❌ Failed to add {constraint_name} to {table_name}: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dimension Table Primary Keys

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["dim_ticker_hc"]}
    ADD CONSTRAINT pk_dim_ticker_hc PRIMARY KEY (symbol)
    """,
    "pk_dim_ticker_hc",
    TABLES["dim_ticker_hc"],
)

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["dim_date_hc"]}
    ADD CONSTRAINT pk_dim_date_hc PRIMARY KEY (date)
    """,
    "pk_dim_date_hc",
    TABLES["dim_date_hc"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fact Table Primary Keys

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_daily_market_hc"]}
    ADD CONSTRAINT pk_fact_daily_market_hc PRIMARY KEY (symbol, date)
    """,
    "pk_fact_daily_market_hc",
    TABLES["fact_daily_market_hc"],
)

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_news_hc"]}
    ADD CONSTRAINT pk_fact_news_hc PRIMARY KEY (article_id, symbol)
    """,
    "pk_fact_news_hc",
    TABLES["fact_news_hc"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Foreign Key Constraints

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_daily_market_hc"]}
    ADD CONSTRAINT fk_fact_daily_market_hc_ticker
    FOREIGN KEY (symbol) REFERENCES {TABLES["dim_ticker_hc"]}(symbol)
    """,
    "fk_fact_daily_market_hc_ticker",
    TABLES["fact_daily_market_hc"],
)

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_daily_market_hc"]}
    ADD CONSTRAINT fk_fact_daily_market_hc_date
    FOREIGN KEY (date) REFERENCES {TABLES["dim_date_hc"]}(date)
    """,
    "fk_fact_daily_market_hc_date",
    TABLES["fact_daily_market_hc"],
)

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_news_hc"]}
    ADD CONSTRAINT fk_fact_news_hc_ticker
    FOREIGN KEY (symbol) REFERENCES {TABLES["dim_ticker_hc"]}(symbol)
    """,
    "fk_fact_news_hc_ticker",
    TABLES["fact_news_hc"],
)

# COMMAND ----------

add_constraint_safe(
    f"""
    ALTER TABLE {TABLES["fact_news_hc"]}
    ADD CONSTRAINT fk_fact_news_hc_date
    FOREIGN KEY (published_date) REFERENCES {TABLES["dim_date_hc"]}(date)
    """,
    "fk_fact_news_hc_date",
    TABLES["fact_news_hc"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Constraints

# COMMAND ----------


def show_table_constraints(table_name: str) -> None:
    """Display constraints for a table."""
    if not table_exists(table_name):
        print(f"⚠️  Table {table_name} does not exist")
        return

    print(f"\n{'=' * 60}")
    print(f"Constraints for: {table_name}")
    print(f"{'=' * 60}")
    try:
        # Show table properties that contain constraint info
        props = spark.sql(f"DESCRIBE TABLE EXTENDED {table_name}").collect()
        for row in props:
            row_str = str(row)
            if "constraint" in row_str.lower() or "primary" in row_str.lower() or "foreign" in row_str.lower():
                print(row)
    except Exception as e:
        print(f"Could not retrieve constraints: {e}")


# Show constraints for all tables
for table_name in TABLES.values():
    show_table_constraints(table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ### Constraints Added
# MAGIC
# MAGIC | Table | Constraint | Type | Columns |
# MAGIC |-------|------------|------|---------|
# MAGIC | dim_ticker_hc | pk_dim_ticker_hc | PRIMARY KEY | symbol |
# MAGIC | dim_date_hc | pk_dim_date_hc | PRIMARY KEY | date |
# MAGIC | fact_daily_market_hc | pk_fact_daily_market_hc | PRIMARY KEY | (symbol, date) |
# MAGIC | fact_daily_market_hc | fk_fact_daily_market_hc_ticker | FOREIGN KEY | symbol -> dim_ticker_hc |
# MAGIC | fact_daily_market_hc | fk_fact_daily_market_hc_date | FOREIGN KEY | date -> dim_date_hc |
# MAGIC | fact_news_hc | pk_fact_news_hc | PRIMARY KEY | (article_id, symbol) |
# MAGIC | fact_news_hc | fk_fact_news_hc_ticker | FOREIGN KEY | symbol -> dim_ticker_hc |
# MAGIC | fact_news_hc | fk_fact_news_hc_date | FOREIGN KEY | published_date -> dim_date_hc |
# MAGIC
# MAGIC ### Benefits
# MAGIC 1. **Query Optimization**: Catalyst uses constraints for join elimination and predicate pushdown
# MAGIC 2. **Documentation**: Star schema relationships are explicitly documented
# MAGIC 3. **Data Governance**: Lineage and impact analysis in Unity Catalog
# MAGIC
# MAGIC ### Next Steps
# MAGIC - Run this notebook after DLT pipelines complete
# MAGIC - Verify constraints in Unity Catalog UI (Catalog Explorer > Table > Details)
# MAGIC - Consider adding NOT NULL constraints for critical columns
