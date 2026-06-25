"""Shared color palette, chart styling constants, and helpers for the dashboard."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

_ET = ZoneInfo("America/New_York")

# Core market colors
BULLISH = "#34d399"  # Emerald-400
BEARISH = "#F23645"  # Financial Red
NEUTRAL = "#787B86"
ACCENT = "#2962FF"

# Categorical palette for multi-line charts
CAT_1 = "#00E5FF"  # Cyan
CAT_2 = "#E040FB"  # Purple
CAT_3 = "#FFD740"  # Gold
CAT_4 = "#FF6E40"  # Orange
CAT_5 = "#1DE9B6"  # Teal
CAT_6 = "#F50057"  # Deep Pink


def get_bullish_color() -> str:
    return BULLISH


def get_bearish_color() -> str:
    return BEARISH


def get_bullish_rgba(alpha: float = 0.1) -> str:
    return f"rgba(52, 211, 153, {alpha})"


def get_bearish_rgba(alpha: float = 0.1) -> str:
    return f"rgba(242, 54, 69, {alpha})"


def get_accent_color() -> str:
    return ACCENT


def get_warning_color() -> str:
    return "#FF9800"


def get_cat_1() -> str:
    return CAT_1


CHART_HEIGHT_LG = 550


def apply_page_theme():
    """Inject shared Streamlit CSS so all dashboard pages look consistent."""
    st.markdown(
        """
        <style>
        /* Compress overall padding */
        .block-container {
            padding-top: 1.5rem;
            padding-bottom: 1rem;
            max-width: 1400px;
        }
        /* Style Metric Cards */
        [data-testid="stMetric"] {
            background-color: #131722;
            border: 1px solid #2B2B43;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.3);
            height: 108px;
            width: 100%;
            box-sizing: border-box;
            display: flex;
            flex-direction: column;
            justify-content: space-between;
        }
        [data-testid="stMetricLabel"] {
            font-size: 0.82rem;
            letter-spacing: 0.04em;
            text-transform: uppercase;
            color: #8B909D;
        }
        /* Monospace metric values for aligned decimals */
        [data-testid="stMetricValue"] {
            font-family: 'Courier New', Courier, monospace;
            font-size: 1.8rem;
        }
        [data-testid="stPlotlyChart"] {
            border: 1px solid #2B2B43;
            border-radius: 8px;
            padding: 4px 6px;
            background: #131722;
        }
        div[data-baseweb="select"] > div,
        div[data-baseweb="input"] > div {
            border-radius: 8px;
        }
        /* Terminal-style tabs */
        .stTabs [data-baseweb="tab-list"] {
            gap: 20px;
            background-color: transparent;
            border-bottom: 1px solid #2B2B43;
        }
        .stTabs [data-baseweb="tab"] {
            height: 40px;
            white-space: pre-wrap;
            border-radius: 4px 4px 0 0;
            padding: 10px 16px;
            font-size: 0.88rem;
            letter-spacing: 0.03em;
        }
        /* News card styling */
        .news-card {
            border: 1px solid transparent;
            border-left: 4px solid #2B2B43;
            padding: 12px 16px;
            margin: 8px 0;
            background: #131722;
            border-radius: 6px;
            transition: all 0.2s ease;
        }
        .news-card:hover {
            background: #1A1F2E;
            border: 1px solid #2B2B43;
            border-left: 4px solid #2962FF;
        }
        .news-card a {
            text-decoration: none;
            color: #D1D4DC;
        }
        .news-card a:hover {
            color: #2962FF;
        }
        /* Sidebar section divider */
        [data-testid="stSidebar"] hr {
            border-color: #2B2B43;
        }
        /* Popovers (e.g., Dashboard Parameters) — keep above charts/iframes */
        [data-baseweb="popover"] {
            z-index: 10000 !important;
        }
        [data-testid="stPopoverBody"] {
            z-index: 10000 !important;
        }
        /* Avoid popover being clipped by column containers */
        [data-testid="stHorizontalBlock"] {
            overflow: visible;
        }
        /* Ticker Identity Bar (Page 2) */
        .ticker-identity {
            display: flex;
            align-items: center;
            background-color: #131722;
            border: 1px solid #2B2B43;
            border-radius: 8px;
            padding: 12px 20px;
            font-family: 'Courier New', Courier, monospace;
            margin-bottom: 20px;
        }
        .ticker-identity-left {
            display: flex;
            align-items: center;
            padding-right: 20px;
            border-right: 1px solid #2B2B43;
            flex-shrink: 0;
            position: relative;
        }
        .ticker-identity-symbol {
            font-weight: bold;
            font-size: 1.1rem;
            color: #FFFFFF;
            background: #2962FF;
            padding: 3px 10px;
            border-radius: 6px;
            display: inline-block;
            white-space: nowrap;
            cursor: default;
        }
        .ticker-identity-name {
            color: #787B86;
            font-size: 0.9rem;
            font-family: Inter, sans-serif;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            max-width: 200px;
            margin-left: 12px;
        }
        .ticker-identity-metrics {
            display: flex;
            align-items: center;
            flex: 1;
            flex-wrap: wrap;
        }
        .ticker-identity-metric {
            display: flex;
            align-items: center;
            gap: 6px;
            color: #787B86;
            padding: 0 20px;
            border-right: 1px solid #2B2B43;
            white-space: nowrap;
        }
        .ticker-identity-metric:last-child {
            border-right: none;
        }
        .ticker-identity-value {
            color: #D1D4DC;
            font-weight: bold;
            font-size: 1.05rem;
        }
        /* Filter panel grouping (screener page) */
        .filter-panel {
            border: 1px solid #2B2B43;
            border-left: 4px solid #2B2B43;
            padding: 12px 16px;
            margin: 8px 0;
            background: #131722;
            border-radius: 6px;
        }
        [data-testid="stBorderContainer"] {
            border: 1px solid #2B2B43 !important;
            border-left: 4px solid #2B2B43 !important;
            background: #131722 !important;
            border-radius: 6px !important;
        }
        /* Google Finance Style Header */
        .gf-header {
            font-family: Arial, sans-serif;
            margin-bottom: 20px;
            padding: 10px 0;
        }
        .gf-title {
            font-size: 1.2rem;
            color: #D1D4DC;
            margin-bottom: 8px;
        }
        .gf-price-row {
            display: flex;
            align-items: baseline;
            gap: 8px;
        }
        .gf-price {
            font-size: 3rem;
            font-weight: 500;
            color: #FFFFFF;
            line-height: 1;
        }
        .gf-currency {
            font-size: 1.2rem;
            color: #8B909D;
        }
        .gf-change-row {
            font-size: 1.1rem;
            margin-top: 6px;
        }
        .gf-green {
            color: #34d399;
        }
        .gf-red {
            color: #F23645;
        }
        .gf-timestamp {
            font-size: 0.85rem;
            color: #8B909D;
            margin-top: 6px;
        }

        </style>
        """,
        unsafe_allow_html=True,
    )


def fmt_volume(val: float) -> str:
    """Format a volume number into a human-readable string (K / M / B)."""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)

    if pd.isna(val):
        return "0"

    if val >= 1_000_000_000:
        return f"{val / 1_000_000_000:.2f}B"
    if val >= 1_000_000:
        return f"{val / 1_000_000:.2f}M"
    if val >= 1_000:
        return f"{val / 1_000:.0f}K"
    return f"{val:.0f}"


def fmt_dollar_compact(val: float) -> str:
    """Format a dollar value into a compact human-readable string ($K / $M / $B)."""
    try:
        val = float(val)
    except (ValueError, TypeError):
        return str(val)

    if pd.isna(val):
        return "$0"

    sign = "-" if val < 0 else ""
    abs_val = abs(val)

    if abs_val >= 1_000_000_000:
        return f"{sign}${abs_val / 1_000_000_000:.2f}B"
    if abs_val >= 1_000_000:
        return f"{sign}${abs_val / 1_000_000:.2f}M"
    if abs_val >= 1_000:
        return f"{sign}${abs_val / 1_000:.0f}K"
    return f"{sign}${abs_val:.0f}"


def apply_rangebreaks(fig, dates):
    """Apply non-trading-day rangebreaks to all x-axes of a Plotly figure."""
    all_dates = pd.to_datetime(dates)
    if len(all_dates) < 2:
        return fig
    sorted_dates = all_dates.sort_values().reset_index(drop=True)
    date_breaks = [
        sorted_dates.iloc[i - 1] + pd.Timedelta(days=d)
        for i in range(1, len(sorted_dates))
        for d in range(1, (sorted_dates.iloc[i] - sorted_dates.iloc[i - 1]).days)
    ]
    if date_breaks:
        fig.update_xaxes(rangebreaks=[dict(values=[d.strftime("%Y-%m-%d") for d in date_breaks])])
    return fig


def section_header(title: str):
    """Render a Bloomberg-style uppercase section divider."""
    st.markdown(
        f"""<div style="border-bottom:1px solid #2B2B43;color:#787B86;font-size:0.72rem;
            letter-spacing:0.08em;text-transform:uppercase;padding-bottom:5px;
            margin-bottom:10px;margin-top:4px">{title}</div>""",
        unsafe_allow_html=True,
    )


_REFRESH_INTERVALS: dict[str, int | None] = {
    "1 min": 60_000,
    "5 min": 300_000,
    "10 min": 600_000,
    "Manual": None,
}


def render_data_freshness_bar(latest_data_time: str | None) -> None:
    """Render data freshness info, interval selector, and manual refresh button."""
    from streamlit_autorefresh import st_autorefresh

    refreshed = datetime.now(_ET).strftime("%I:%M:%S %p ET").lstrip("0")
    data_label = (
        f"Data as of <span style='color:#D1D4DC;'>{latest_data_time}</span>"
        if latest_data_time
        else "Data time unavailable"
    )

    st.markdown(
        f"""<div style="text-align:right;color:#787B86;font-size:0.78rem;
            padding-top:14px;line-height:1.8;">
            {data_label}<br/>
            Refreshed&nbsp;{refreshed}
        </div>""",
        unsafe_allow_html=True,
    )

    col_sel, col_btn = st.columns([3, 1])
    with col_sel:
        interval_label = st.selectbox(
            "refresh_interval",
            list(_REFRESH_INTERVALS.keys()),
            index=0,
            key="refresh_interval",
            label_visibility="collapsed",
        )
    with col_btn:
        if st.button("↻", help="Refresh now", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

    interval_ms = _REFRESH_INTERVALS[interval_label]
    if interval_ms is not None:
        st_autorefresh(interval=interval_ms, key="auto_refresh_timer")
