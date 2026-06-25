"""
Base Configuration Module

Shared constants and base configuration for all medallion layers (Bronze, Silver, Gold).
This module provides a single source of truth for common configuration values.

Market hours (streaming): Open 9:30 AM ET, close per session (4:00 PM or early close).
Streaming jobs stop at effective shutdown = session close + SHUTDOWN_DELAY_MINUTES (e.g. 4:20 PM)
so delayed-feed data is fully produced and consumed.
"""

from datetime import datetime, timedelta
from datetime import time as dt_time

import pytz

# ---------------------------------------------------------------------------
# Optional: exchange_calendars for NYSE holiday and early-close awareness.
# If not installed, market hours logic falls back to weekday-only (no holidays).
# ---------------------------------------------------------------------------
try:
    from exchange_calendars import get_calendar

    _NYSE_CALENDAR = get_calendar("XNYS")
    _HAS_EXCHANGE_CALENDARS = True
except ImportError:
    _NYSE_CALENDAR = None
    _HAS_EXCHANGE_CALENDARS = False


class BaseConfig:
    """
    Base configuration class with shared constants for all medallion layers.

    Provides:
    - Unity Catalog paths and identifiers
    - Market hours configuration
    - Data validation constants
    - Common thresholds and parameters

    Usage:
        class BronzeConfig(BaseConfig):
            def __init__(self, dbutils):
                super().__init__()
                self.dbutils = dbutils
                ...
    """

    # =========================================================================
    # Unity Catalog Configuration
    # =========================================================================
    CATALOG = "tabular"
    SCHEMA = "dataexpert"
    VOLUME_BASE_PATH = "/Volumes/tabular/dataexpert/hc_market_data"

    # Secret scope for Databricks secrets
    SECRET_SCOPE = "ganhockchong-market-data"

    # =========================================================================
    # Data Validation Constants
    # =========================================================================
    # Symbol validation - supports international tickers (e.g., BRK.B, TSM)
    SYMBOL_MIN_LENGTH = 1
    SYMBOL_MAX_LENGTH = 8

    # =========================================================================
    # Market Hours Configuration (US Eastern Time)
    # =========================================================================
    MARKET_TIMEZONE = "America/New_York"
    MARKET_OPEN_HOUR = 9
    MARKET_OPEN_MINUTE = 30
    MARKET_CLOSE_HOUR = 16
    MARKET_CLOSE_MINUTE = 0
    MARKET_DAYS = [0, 1, 2, 3, 4]  # Monday=0 to Friday=4
    EXPECTED_BARS_PER_DAY = 390  # 6.5 hours * 60 minutes

    # Daily-aggregation lookback (calendar days). Gold's daily-fact window
    # (daily_fact_lookback_days) references this single value. (Silver's daily
    # rollup is now a materialized view over the full minute table and applies
    # no lookback window of its own.)
    DAILY_AGGREGATION_LOOKBACK_DAYS = 400

    # Shutdown delay: minutes to keep producer/consumer running after session close.
    # Polygon delayed feed sends last bars ~15 min after close; this buffer ensures
    # we receive, produce, and consume them before stopping (e.g. stop at 4:20 PM ET).
    SHUTDOWN_DELAY_MINUTES = 20

    # =========================================================================
    # Quality Thresholds
    # =========================================================================
    QUALITY_THRESHOLD_WARNING = 0.95  # 95% pass rate triggers warning
    QUALITY_THRESHOLD_CRITICAL = 0.80  # 80% triggers failure

    # WAP (Write-Audit-Publish) thresholds
    WAP_REJECTION_RATE_WARNING = 0.5  # Warn if >0.5% rejected
    WAP_REJECTION_RATE_CRITICAL = 1.0  # Block if >1% rejected
    WAP_AUDIT_RETENTION_DAYS = 365

    # =========================================================================
    # Streaming Configuration
    # =========================================================================
    LATE_ARRIVAL_WATERMARK = "10 minutes"
    NEWS_LATE_ARRIVAL_WATERMARK = "1 hour"

    # =========================================================================
    # Timestamp Conversion Helpers
    # =========================================================================

    # Bronze writers stamp TS_UNIT_MS / _NS / _S into the `ts_unit` column so
    # Silver doesn't have to guess the unit from magnitude. Magnitude-based
    # detection collapsed distinct events into ties at boundary values
    # (e.g. any seconds value >1e12 was silently treated as ms).
    TS_UNIT_SECONDS = "s"
    TS_UNIT_MILLISECONDS = "ms"
    TS_UNIT_NANOSECONDS = "ns"

    @staticmethod
    def timestamp_to_seconds(ts_col, unit_col):
        """
        Convert an epoch timestamp column to fractional seconds using an explicit
        unit column ('s' | 'ms' | 'ns'). Unknown units produce NULL and are caught
        by the `known_ts_unit` expectation in Silver.
        """
        from pyspark.sql.functions import lit, when

        return (
            when(unit_col == lit(BaseConfig.TS_UNIT_NANOSECONDS), ts_col / 1_000_000_000)
            .when(unit_col == lit(BaseConfig.TS_UNIT_MILLISECONDS), ts_col / 1_000)
            .when(unit_col == lit(BaseConfig.TS_UNIT_SECONDS), ts_col)
            .otherwise(lit(None))
        )

    @staticmethod
    def timestamp_to_date(ts_col, unit_col):
        """
        Convert an epoch timestamp column to an ET date using an explicit unit
        column ('s' | 'ms' | 'ns').

        Unknown units produce NULL and are caught by the Silver `known_ts_unit`
        expectation, consistent with how timestamp_to_seconds handles them.
        """
        from pyspark.sql.functions import from_unixtime, from_utc_timestamp, to_date

        ts_seconds = BaseConfig.timestamp_to_seconds(ts_col, unit_col)
        return to_date(from_utc_timestamp(from_unixtime(ts_seconds), "America/New_York"))

    def __init__(self):
        self.catalog = self.CATALOG
        self.schema = self.SCHEMA
        self.volume_base_path = self.VOLUME_BASE_PATH

    # =========================================================================
    # Path Generation Methods
    # =========================================================================

    def get_bronze_path(self, data_type: str) -> str:
        """
        Get Bronze layer path for specific data type.

        Args:
            data_type: One of 'streaming', 'streaming_quarantine', 'historical', 'news', 'ticker_details', 'splits'

        Returns:
            Full Unity Catalog Volume path to Bronze data
        """
        return f"{self.volume_base_path}/bronze/{data_type}"

    def get_silver_path(self, table_name: str) -> str:
        """
        Get Silver layer path for specific table.

        Args:
            table_name: Name of Silver table (e.g., 'ohlcv_silver_hc')

        Returns:
            Full Unity Catalog Volume path to Silver table
        """
        return f"{self.volume_base_path}/silver/{table_name}"

    def get_gold_path(self, table_name: str) -> str:
        """
        Get Gold layer path for specific table.

        Args:
            table_name: Name of Gold table (e.g., 'fact_daily_market')

        Returns:
            Full Unity Catalog Volume path to Gold table
        """
        return f"{self.volume_base_path}/gold/{table_name}"

    def get_checkpoint_path(self, layer: str, pipeline_name: str) -> str:
        """
        Get checkpoint path for streaming pipeline.

        Args:
            layer: Layer name ('bronze', 'silver', 'gold')
            pipeline_name: Name of the pipeline

        Returns:
            Checkpoint path in Unity Catalog Volume
        """
        return f"{self.volume_base_path}/_checkpoints/{layer}/{pipeline_name}"

    # =========================================================================
    # Validation Methods
    # =========================================================================

    def get_fully_qualified_table(self, table_name: str) -> str:
        """
        Get fully qualified Unity Catalog table name: catalog.schema.table.

        Args:
            table_name: Table name (e.g., 'ohlcv_silver_hc')

        Returns:
            Fully qualified table name (e.g., 'tabular.dataexpert.ohlcv_silver_hc')
        """
        return f"{self.catalog}.{self.schema}.{table_name}"

    # =========================================================================
    # Market Hours Methods (Consolidated)
    # Single place for when market is open, when to stop streaming, and holidays.
    # =========================================================================

    @classmethod
    def get_market_timezone(cls):
        """Return the market timezone (America/New_York) for ET-based checks."""
        return pytz.timezone(cls.MARKET_TIMEZONE)

    @classmethod
    def is_trading_day(cls, check_date=None) -> bool:
        """
        True if the given date is a trading day (weekday and not an NYSE holiday).

        Uses exchange_calendars for holidays when available; otherwise weekdays only.

        Args:
            check_date: Date to check (default: current date in ET)

        Returns:
            True if trading day, False otherwise

        Note:
            NYSE holidays include: New Year's Day, MLK Day, Presidents Day,
            Good Friday, Memorial Day, Juneteenth, Independence Day,
            Labor Day, Thanksgiving, Christmas
        """
        tz = cls.get_market_timezone()

        if check_date is None:
            check_date = datetime.now(tz).date()
        elif isinstance(check_date, datetime):
            if check_date.tzinfo is None:
                check_date = tz.localize(check_date)
            check_date = check_date.astimezone(tz).date()

        # Prefer exchange_calendars so we exclude NYSE holidays, not just weekends.
        if _HAS_EXCHANGE_CALENDARS and _NYSE_CALENDAR is not None:
            try:
                import pandas as pd

                pd_date = pd.Timestamp(check_date)
                return _NYSE_CALENDAR.is_session(pd_date)
            except Exception:
                pass

        # No exchange_calendars or error: treat any weekday as a trading day (no holiday list).
        return check_date.weekday() in cls.MARKET_DAYS

    @classmethod
    def get_session_close_time(cls, check_date=None) -> dt_time:
        """
        Return the clock time when the market closes on the given date (e.g. 16:00 or 13:00).

        On early-close days (e.g. day before Thanksgiving, July 3), returns 1:00 PM ET
        instead of 4:00 PM. Uses exchange_calendars when available; otherwise 4:00 PM.

        Args:
            check_date: Date to check (default: current date in ET)

        Returns:
            Close time for the session (dt_time object)
        """
        tz = cls.get_market_timezone()

        if check_date is None:
            check_date = datetime.now(tz).date()
        elif isinstance(check_date, datetime):
            if check_date.tzinfo is None:
                check_date = tz.localize(check_date)
            check_date = check_date.astimezone(tz).date()

        # Prefer exchange_calendars so early-close days (e.g. 1:00 PM) are correct.
        if _HAS_EXCHANGE_CALENDARS and _NYSE_CALENDAR is not None:
            try:
                import pandas as pd

                pd_date = pd.Timestamp(check_date)
                if _NYSE_CALENDAR.is_session(pd_date):
                    close_dt = _NYSE_CALENDAR.session_close(pd_date)
                    # Use the timezone string, not the pytz object. Passing a pytz
                    # timezone object to pandas tz_convert can yield the LMT offset
                    # (+3m 56s for New York), producing 16:04 instead of 16:00.
                    close_et = close_dt.tz_convert(cls.MARKET_TIMEZONE)
                    return close_et.time()
            except Exception:
                pass

        return dt_time(cls.MARKET_CLOSE_HOUR, cls.MARKET_CLOSE_MINUTE)

    @classmethod
    def get_effective_shutdown_datetime(cls, check_date=None) -> datetime:
        """
        Datetime when we should stop the stream: session close + SHUTDOWN_DELAY_MINUTES.

        Example: session close 4:00 PM, delay 20 min → effective shutdown 4:20 PM ET.
        Lets delayed-feed data (e.g. last bars arriving ~15 min after close) be
        produced to Kafka and consumed before shutdown.

        Args:
            check_date: Date to check (default: current date in ET)

        Returns:
            Timezone-aware datetime (ET) when streaming should stop
        """
        tz = cls.get_market_timezone()

        if check_date is None:
            check_date = datetime.now(tz).date()
        elif isinstance(check_date, datetime):
            if check_date.tzinfo is None:
                check_date = tz.localize(check_date)
            check_date = check_date.astimezone(tz).date()

        session_close_time = cls.get_session_close_time(check_date)
        close_datetime = tz.localize(datetime.combine(check_date, session_close_time))
        return close_datetime + timedelta(minutes=cls.SHUTDOWN_DELAY_MINUTES)

    @classmethod
    def is_market_hours(cls, check_time: datetime = None) -> bool:
        """
        True if the given time is within regular market hours [9:30 AM, close ET), Mon–Fri.

        Uses a HALF-OPEN interval: the open boundary is inclusive and the close
        boundary is EXCLUSIVE. This matches Polygon's OHLCV bar convention, where
        a 1-minute bar labeled ``t`` covers ``[t, t+1min)``. A bar labeled 16:00
        therefore represents ``[16:00, 16:01)``, which is AFTER the NYSE close
        and is not a market-hours bar. The Silver enriched filter
        (``_hhmm < MARKET_CLOSE_HHMM``) and ``EXPECTED_BARS_PER_DAY = 390``
        assume the same half-open semantics.

        Excludes weekends and NYSE holidays. On early-close days, "close" is 1:00 PM ET.
        Use this for "should we start?" (pre-flight). For "should we stop?" use
        should_stop_streaming() which includes the shutdown delay.

        Args:
            check_time: Datetime to check (default: current time)

        Returns:
            True if within market hours, False otherwise
        """
        tz = cls.get_market_timezone()
        now_et = check_time or datetime.now(tz)

        if now_et.tzinfo is None or now_et.tzinfo != tz:
            now_et = now_et.astimezone(tz) if now_et.tzinfo else tz.localize(now_et)

        if not cls.is_trading_day(now_et.date()):
            return False

        current_time = now_et.time()
        market_open = dt_time(cls.MARKET_OPEN_HOUR, cls.MARKET_OPEN_MINUTE)
        market_close = cls.get_session_close_time(now_et.date())

        is_trading_hours = market_open <= current_time < market_close

        return is_trading_hours

    @classmethod
    def should_stop_streaming(cls) -> bool:
        """
        True when the producer/consumer should stop: past session close + shutdown delay.

        On a trading day we stop at effective shutdown (e.g. 4:20 PM ET), not at
        session close (4:00 PM), so delayed-feed bars are fully produced and consumed.
        On a non-trading day (weekend/holiday) we always return True (do not run).
        """
        tz = cls.get_market_timezone()
        now = datetime.now(tz)
        if not cls.is_trading_day(now.date()):
            return True
        return now >= cls.get_effective_shutdown_datetime(now.date())

    @classmethod
    def get_market_status(cls) -> str:
        """Get detailed market status string for logging."""
        tz = cls.get_market_timezone()
        now_et = datetime.now(tz)
        status = "OPEN" if cls.is_market_hours() else "CLOSED"
        return f"{now_et.strftime('%Y-%m-%d %H:%M:%S %Z')} | Market: {status}"

    @classmethod
    def get_market_open_time(cls) -> dt_time:
        return dt_time(cls.MARKET_OPEN_HOUR, cls.MARKET_OPEN_MINUTE)

    @classmethod
    def get_market_close_time(cls) -> dt_time:
        """Get today's market close time (respects early-close days via get_session_close_time)."""
        return cls.get_session_close_time()

    # =========================================================================
    # SIC Code to Sector Mappings (ticker_details → Gold dim_ticker)
    # Based on official SIC Manual: https://www.osha.gov/data/sic-manual
    # =========================================================================
    SIC_SECTOR_MAPPINGS = {
        # Agriculture, Forestry, Fishing (01-09)
        "01": "Agriculture",  # Crops
        "02": "Agriculture",  # Livestock
        "07": "Agriculture",  # Agricultural services
        "08": "Agriculture",  # Forestry
        "09": "Agriculture",  # Fishing, hunting, trapping
        # Mining (10-14)
        "10": "Mining",  # Metal mining
        "11": "Mining",  # Anthracite mining
        "12": "Energy",  # Coal mining
        "13": "Energy",  # Oil and gas extraction
        "14": "Mining",  # Non-metallic minerals (except fuels)
        # Construction (15-17)
        "15": "Construction",  # General building contractors
        "16": "Construction",  # Heavy construction (except building)
        "17": "Construction",  # Special trade contractors
        # Manufacturing (20-39)
        "20": "Food & Beverage",  # Food and kindred products
        "21": "Food & Beverage",  # Tobacco products
        "22": "Manufacturing",  # Textile mill products
        "23": "Manufacturing",  # Apparel and other textile products
        "24": "Manufacturing",  # Lumber and wood products
        "25": "Manufacturing",  # Furniture and fixtures
        "26": "Manufacturing",  # Paper and allied products
        "27": "Media & Publishing",  # Printing, publishing, and allied industries
        "28": "Chemicals & Pharmaceuticals",  # Chemicals and allied products
        "29": "Energy",  # Petroleum refining and related industries
        "30": "Manufacturing",  # Rubber and miscellaneous plastics products
        "31": "Manufacturing",  # Leather and leather products
        "32": "Manufacturing",  # Stone, clay, glass, and concrete products
        "33": "Manufacturing",  # Primary metal industries
        "34": "Manufacturing",  # Fabricated metal products
        "35": "Industrial Machinery",  # Industrial and commercial machinery, computer equipment
        "36": "Electronics & Technology",  # Electronic and electrical equipment
        "37": "Transportation Equipment",  # Motor vehicles, aircraft, ships, spacecraft
        "38": "Industrial Machinery",  # Measuring, analyzing, and controlling instruments
        "39": "Manufacturing",  # Miscellaneous manufacturing industries
        # Transportation & Public Utilities (40-49)
        "40": "Transportation",  # Railroad transportation
        "41": "Transportation",  # Local and suburban transit
        "42": "Transportation",  # Motor freight transportation and warehousing
        "43": "Transportation",  # United States Postal Service
        "44": "Transportation",  # Water transportation
        "45": "Transportation",  # Transportation by air
        "46": "Transportation",  # Pipelines (except natural gas)
        "47": "Transportation",  # Transportation services
        "48": "Communications",  # Communications services
        "49": "Utilities",  # Electric, gas, and sanitary services
        # Wholesale Trade (50-51)
        "50": "Wholesale Trade",  # Wholesale trade - durable goods
        "51": "Wholesale Trade",  # Wholesale trade - non-durable goods
        # Retail Trade (52-59)
        "52": "Retail",  # Building materials, hardware, garden supply
        "53": "Retail",  # General merchandise stores
        "54": "Retail",  # Food stores
        "55": "Retail",  # Automotive dealers and gasoline service stations
        "56": "Retail",  # Apparel and accessory stores
        "57": "Retail",  # Home furniture, furnishings, and equipment stores
        "58": "Retail",  # Eating and drinking places
        "59": "Retail",  # Miscellaneous retail
        # Finance, Insurance, Real Estate (60-67)
        "60": "Financial Services",  # Depository institutions (banks)
        "61": "Financial Services",  # Non-depository credit institutions
        "62": "Financial Services",  # Security & commodity brokers, dealers, exchanges
        "63": "Financial Services",  # Insurance carriers
        "64": "Financial Services",  # Insurance agents, brokers & service
        "65": "Real Estate",  # Real estate
        "67": "Financial Services",  # Holding and other investment offices
        # Services (70-89)
        "70": "Hospitality & Leisure",  # Hotels, rooming houses, camps
        "72": "Services",  # Personal services
        "73": "Business Services",  # Business services (includes tech/software)
        "75": "Services",  # Automotive repair, services, and parking
        "76": "Services",  # Miscellaneous repair services
        "78": "Media & Entertainment",  # Motion pictures
        "79": "Hospitality & Leisure",  # Amusement and recreation services
        "80": "Healthcare",  # Health services
        "81": "Services",  # Legal services
        "82": "Education",  # Educational services
        "83": "Services",  # Social services
        "84": "Services",  # Museums, art galleries, and botanical/zoological gardens
        "86": "Services",  # Membership organizations
        "87": "Business Services",  # Engineering, accounting, research, management services
        "88": "Services",  # Private households
        "89": "Services",  # Services, not elsewhere classified
        # Public Administration (91-97)
        "91": "Government",  # Executive, legislative, general government
        "92": "Government",  # Justice, public order, and safety
        "93": "Government",  # Public finance, taxation, monetary policy
        "94": "Government",  # Administration of human resource programs
        "95": "Government",  # Administration of environmental and housing programs
        "96": "Government",  # Administration of economic programs
        "97": "Government",  # National security and international affairs
        # Non-classifiable (99)
        "99": "Conglomerates",  # Non-classifiable establishments
    }

    # =========================================================================
    # Display Methods
    # =========================================================================

    def print_base_config(self):
        print("=" * 60)
        print("Base Configuration")
        print("=" * 60)
        print(f"Catalog: {self.catalog}")
        print(f"Schema: {self.schema}")
        print(f"Volume Base Path: {self.volume_base_path}")
        print()
        print("Data Validation:")
        print(f"  Symbol Length: {self.SYMBOL_MIN_LENGTH}-{self.SYMBOL_MAX_LENGTH} chars")
        print()
        print("Market Hours (US Eastern):")
        print(f"  Open: {self.MARKET_OPEN_HOUR}:{self.MARKET_OPEN_MINUTE:02d}")
        print(f"  Close: {self.MARKET_CLOSE_HOUR}:{self.MARKET_CLOSE_MINUTE:02d}")
        print(f"  Expected Bars/Day: {self.EXPECTED_BARS_PER_DAY}")
        print()
        print("Quality Thresholds:")
        print(f"  Warning: {self.QUALITY_THRESHOLD_WARNING * 100}%")
        print(f"  Critical: {self.QUALITY_THRESHOLD_CRITICAL * 100}%")
        print(f"  WAP Rejection Rate Critical: {self.WAP_REJECTION_RATE_CRITICAL}%")
        print("=" * 60)


if __name__ == "__main__":
    config = BaseConfig()
    config.print_base_config()
