"""
Gold Layer: Date Dimension — Delta Live Tables

Grain: one row per calendar date from 2020 to current year + 5.
Key: date (PK).
Idempotency: deterministic static generation — same output every run.

Requires exchange_calendars as a cluster library in the DLT pipeline
configuration for NYSE trading day and early-close awareness.
"""

import datetime
import sys

sys.path.insert(0, "/Workspace/Users/ganhockchong@gmail.com/Capstone-Project/databricks/config")
from path_bootstrap import bootstrap_project_paths

bootstrap_project_paths()

import dlt
import exchange_calendars as xcals
from gold_config import GoldConfig
from pyspark.sql.functions import col, date_format, dayofweek, last_day, month, quarter, weekofyear, when, year

_config = GoldConfig()
START_DATE = _config.dim_date_start
END_DATE = _config.dim_date_end

_nyse = xcals.get_calendar("XNYS")
# Cap END_DATE at the last available session in the XNYS calendar
# to avoid exchange_calendars raising an error for dates beyond its range
_calendar_end = _nyse.last_session.strftime("%Y-%m-%d")
_trading_end = min(END_DATE, _calendar_end)
_sessions = _nyse.sessions_in_range(START_DATE, _trading_end)
NYSE_TRADING_DATES = set(d.strftime("%Y-%m-%d") for d in _sessions)


def _third_friday(year_, month_):
    """Return the third Friday of the given year/month as a datetime.date."""
    first = datetime.date(year_, month_, 1)
    # weekday(): Monday=0..Sunday=6; Friday=4. Offset to the first Friday, then +2 weeks.
    first_friday = first + datetime.timedelta(days=(4 - first.weekday()) % 7)
    return first_friday + datetime.timedelta(days=14)


def _build_options_expiry_dates(start_date, end_date, trading_dates):
    """
    Monthly options expiry dates, holiday-adjusted to the NYSE calendar.

    US monthly options expire the third Friday; if that Friday is an NYSE
    holiday (e.g. Good Friday) expiration rolls back to the prior trading day.
    Months whose third Friday falls beyond the loaded XNYS calendar cannot be
    holiday-validated and are omitted (consistent with is_trading_day).
    """
    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)
    expiry = set()
    year_, month_ = start.year, start.month
    while (year_, month_) <= (end.year, end.month):
        candidate = _third_friday(year_, month_)
        # Roll back to the prior trading day if the third Friday is a holiday.
        day = candidate
        while day.strftime("%Y-%m-%d") not in trading_dates and day > candidate - datetime.timedelta(days=7):
            day -= datetime.timedelta(days=1)
        if day.strftime("%Y-%m-%d") in trading_dates and start <= day <= end:
            expiry.add(day.strftime("%Y-%m-%d"))
        year_, month_ = (year_ + 1, 1) if month_ == 12 else (year_, month_ + 1)
    return expiry


OPTIONS_EXPIRY_DATES = _build_options_expiry_dates(START_DATE, _trading_end, NYSE_TRADING_DATES)


@dlt.table(
    name="dim_date_hc",
    comment="Date dimension table for time-based analytics",
    schema="""
        date DATE,
        year INT,
        quarter INT,
        month INT,
        day_of_week INT,
        day_name STRING,
        week_of_year INT,
        month_name STRING,
        is_weekend BOOLEAN,
        is_month_end BOOLEAN,
        is_quarter_end BOOLEAN,
        is_options_expiry BOOLEAN,
        is_trading_day BOOLEAN
    """,
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
        # Small dimension table (~4000 rows) - optimize for broadcast joins
        "delta.enableDeletionVectors": "true",
        "delta.dataSkippingNumIndexedCols": "6",
    },
)
@dlt.expect_or_fail("pk_date_not_null", "date IS NOT NULL")
def dim_date():
    """
    Generate static date dimension with calendar attributes and NYSE trading flags.

    Columns:
    - date: DATE (PK, calendar date — fact tables join on this directly)
    - year, quarter, month: INT (calendar hierarchy)
    - day_of_week: INT (1=Sunday..7=Saturday), day_name: STRING
    - week_of_year: INT (ISO week), month_name: STRING
    - is_weekend, is_month_end, is_quarter_end, is_options_expiry: BOOLEAN
    - is_trading_day: BOOLEAN (NYSE open, from exchange_calendars)
    """
    date_df = (
        spark.sql(f"""
            SELECT explode(sequence(
                to_date('{START_DATE}'),
                to_date('{END_DATE}'),
                interval 1 day
            )) as date
        """)
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("day_of_week", dayofweek("date"))
        .withColumn("day_name", date_format("date", "EEEE"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("is_weekend", when(dayofweek("date").isin(1, 7), True).otherwise(False))
        .withColumn("is_month_end", col("date") == last_day("date"))
        .withColumn(
            "is_quarter_end",
            (month("date").isin(3, 6, 9, 12)) & (col("date") == last_day("date")),
        )
        .withColumn(
            "is_options_expiry",
            date_format("date", "yyyy-MM-dd").isin(OPTIONS_EXPIRY_DATES),
        )
        .withColumn("is_trading_day", date_format("date", "yyyy-MM-dd").isin(NYSE_TRADING_DATES))
        .select(
            "date",
            "year",
            "quarter",
            "month",
            "day_of_week",
            "day_name",
            "week_of_year",
            "month_name",
            "is_weekend",
            "is_month_end",
            "is_quarter_end",
            "is_options_expiry",
            "is_trading_day",
        )
    )

    return date_df
