"""SQL Warehouse Connection Helper

Provides a cached connection to Databricks SQL Warehouse for the Streamlit dashboard.
- Deployed as Databricks App: uses service principal OAuth (auto-injected credentials)
- Local development: set DATABRICKS_HOST, DATABRICKS_CLIENT_ID,
  DATABRICKS_CLIENT_SECRET, and DATABRICKS_WAREHOUSE_ID env vars

Cache strategy:
  - get_gold_version(): sentinel query (TTL via SENTINEL_TTL) — checks MAX(date) in Gold layer
  - run_query(): keyed to the Gold version string — only re-fetches when Gold actually
    has new data, regardless of time elapsed
"""

import os
from datetime import datetime, timezone
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

import streamlit as st

_ET = ZoneInfo("America/New_York")

if TYPE_CHECKING:
    import pandas as pd

# Unity Catalog coordinates
CATALOG = "tabular"
SCHEMA = "dataexpert"

# How often to poll Gold for a new version (seconds).
# 5 min matches the intraday fetch TTL — one consistent refresh cadence across the dashboard.
SENTINEL_TTL = 300

# Safety-net TTL for run_query. Real invalidation is driven by the gold_version cache key change;
# this only fires if the sentinel itself fails (e.g. warehouse briefly down).
_SAFETY_NET_TTL = 86_400


def _credential_provider():
    """Create an OAuth credential provider using the service principal."""
    try:
        from databricks.sdk.core import Config, oauth_service_principal
    except ModuleNotFoundError as e:
        st.error(
            "Missing dependency `databricks-sdk`. Install dashboard dependencies with "
            "`pip install -r databricks/dashboard/requirements.txt`."
        )
        raise e

    host = os.getenv("DATABRICKS_HOST", "")
    config = Config(
        host=host,
        client_id=os.getenv("DATABRICKS_CLIENT_ID", ""),
        client_secret=os.getenv("DATABRICKS_CLIENT_SECRET", ""),
    )
    return oauth_service_principal(config)


@st.cache_resource
def _get_connection():
    """Create and cache a Databricks SQL connection using OAuth.

    Cached as a shared resource so the TCP handshake and OAuth round-trip
    happen once per process, not once per query.
    """
    try:
        # Provided by `databricks-sql-connector`
        from databricks import sql as databricks_sql
    except ModuleNotFoundError as e:
        st.error(
            "Missing dependency `databricks-sql-connector` (module `databricks.sql`). "
            "Install dashboard dependencies with "
            "`pip install -r databricks/dashboard/requirements.txt`."
        )
        raise e

    host = os.getenv("DATABRICKS_HOST", "")
    warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    if not all([host, warehouse_id]):
        st.error("Missing connection config. Set DATABRICKS_HOST and DATABRICKS_WAREHOUSE_ID as environment variables.")
        st.stop()

    server_hostname = host.replace("https://", "").replace("http://", "").rstrip("/")

    return databricks_sql.connect(
        server_hostname=server_hostname,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=_credential_provider,
    )


def _execute(query: str, params: tuple = ()):
    """Run a SQL query and return a DataFrame. Not cached — use the public helpers."""
    import pandas as pd

    conn = _get_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, list(params))
        else:
            cursor.execute(query)
        if cursor.description is None:
            return pd.DataFrame()
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        cursor.close()


@st.cache_data(ttl=SENTINEL_TTL, show_spinner=False)
def get_gold_version() -> str:
    """Return a version string representing the current state of the Gold layer.

    Runs a cheap single-aggregate query every SENTINEL_TTL seconds. When the
    Gold DLT pipeline writes new rows, MAX(date) advances and the returned
    string changes — busting the cache of any downstream run_query call that
    uses this version as a key argument.
    """
    df = _execute(f"SELECT MAX(date) AS max_date FROM {CATALOG}.{SCHEMA}.fact_daily_market_adjusted_hc")
    if df.empty or df["max_date"].iloc[0] is None:
        return "unknown"
    return str(df["max_date"].iloc[0])


# How often to poll the minute fact for a new bar (seconds). Short, because the
# streaming pipeline writes a new 1-min bar roughly every minute during market hours.
MINUTE_SENTINEL_TTL = 60


@st.cache_data(ttl=MINUTE_SENTINEL_TTL, show_spinner=False)
def get_minute_version() -> str:
    """Return a version string for the current state of the minute Gold layer.

    Cheap MAX(start_timestamp) probe over fact_minute_market_adjusted_hc, refreshed
    every MINUTE_SENTINEL_TTL seconds. When the streaming pipeline lands a new 1-min
    bar the value advances — busting any cached intraday query that uses this version
    as a key argument. Global across symbols (one shared cache entry), mirroring
    get_gold_version().
    """
    df = _execute(f"SELECT MAX(start_timestamp) AS max_ts FROM {CATALOG}.{SCHEMA}.fact_minute_market_adjusted_hc")
    if df.empty or df["max_ts"].iloc[0] is None:
        return "unknown"
    return str(df["max_ts"].iloc[0])


@st.cache_data(ttl=_SAFETY_NET_TTL, show_spinner="Querying data...")
def run_query(query: str, gold_version: str = "", params: tuple = ()):
    """Execute a SQL query and return a DataFrame.

    Cache is keyed to (query, gold_version, params). When get_gold_version() returns a
    new date string the key changes and the cache busts automatically — no data
    is re-fetched until Gold actually updates. The _SAFETY_NET_TTL acts only as a
    safety net in case the sentinel fails (e.g. warehouse is down briefly).

    Usage:
        run_query(sql)                              # tables with no user parameters
        run_query(sql, params=(symbol,))            # parameterized query (? placeholders, qmark)
        run_query(sql, gold_version=v, params=(...))  # explicit override if needed
    """
    return _execute(query, params)


def run_query_versioned(query: str, params: tuple = ()):
    """Execute SQL with a call-time Gold version cache key."""
    return run_query(query, gold_version=get_gold_version(), params=params)


def run_query_uncached(query: str, params: tuple = ()) -> "pd.DataFrame":
    """Execute SQL bypassing the versioned cache.

    Use only for minute-grain tables (fact_minute_market_hc) that update
    intraday and are not tracked by the gold sentinel. Freshness is governed
    entirely by the caller's own @st.cache_data TTL.
    """
    return _execute(query, params)


@st.cache_data(ttl=300, show_spinner=False)
def fetch_latest_minute_timestamp() -> str | None:
    """Return the latest 1-min bar datetime available in Gold as an ET string.

    start_timestamp is stored as BIGINT epoch-milliseconds. Returns e.g.
    "20 May 2026, 3:45 PM ET", or None if the table is empty.
    """
    df = _execute(f"SELECT MAX(start_timestamp) AS max_ts FROM {CATALOG}.{SCHEMA}.fact_minute_market_hc")
    if df.empty or df["max_ts"].iloc[0] is None:
        return None
    max_ts = int(df["max_ts"].iloc[0])
    dt = datetime.fromtimestamp(max_ts / 1000, tz=timezone.utc).astimezone(_ET)
    date_str = f"{dt.day} {dt.strftime('%b %Y')}"
    time_str = dt.strftime("%I:%M %p ET").lstrip("0") or dt.strftime("%I:%M %p ET")
    return f"{date_str}, {time_str}"


def table(name: str) -> str:
    """Return fully qualified table name."""
    return f"{CATALOG}.{SCHEMA}.{name}"
