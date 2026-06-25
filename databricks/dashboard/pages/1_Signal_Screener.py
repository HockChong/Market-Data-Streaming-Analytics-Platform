"""
Market Overview Page

Cross-layer screener to surface actionable trade setups by joining
fact_daily_market_adjusted_hc (split-adjusted prices) and dim_ticker_hc.
"""

import datetime

import streamlit as st
from utils.connection import fetch_latest_minute_timestamp
from utils.screener_data import fetch_available_dates, fetch_screener_base, fetch_sectors
from utils.screener_filters import (
    DEFAULT_GAIN_RANGE,
    DEFAULT_VOLUME_RANGE,
    GAIN_PERIOD_COLS,
    GAIN_SLIDER_MAX,
    GAIN_SLIDER_MIN,
    GAIN_SLIDER_STEP,
    MAX_RESULTS_OPTIONS,
    QUICK_FILTER_NONE,
    QUICK_FILTER_OPTIONS,
    SORT_COLUMN_OPTIONS,
    VOLUME_SLIDER_MAX,
    VOLUME_SLIDER_MIN,
    VOLUME_SLIDER_STEP,
    apply_screener_filters,
)
from utils.theme import apply_page_theme, fmt_dollar_compact, fmt_volume, render_data_freshness_bar

st.set_page_config(page_title="US Signal Screener", page_icon=":mag:", layout="wide")
apply_page_theme()

_col_title, _col_status = st.columns([7, 3])
with _col_title:
    st.title("US Signal Screener")
with _col_status:
    render_data_freshness_bar(fetch_latest_minute_timestamp())

# ── Date Filter ──────────────────────────────────────────────────────────────
available_dates_df = fetch_available_dates()

if available_dates_df.empty:
    st.error("No data found in fact_daily_market_adjusted_hc.")
    st.stop()

available_dates = available_dates_df["date"].tolist()
if isinstance(available_dates[0], str):
    available_dates = [datetime.date.fromisoformat(d) for d in available_dates]

sectors_df = fetch_sectors()


# ── Zone C: Screener Table ───────────────────────────────────────────────────
st.subheader("Screener")


def reset_sliders():
    if st.session_state.quick_filter != QUICK_FILTER_NONE:
        # We need to temporarily decouple the sliders to reset them
        # Streamlit session state callbacks fire BEFORE the rest of the script runs
        st.session_state.screener_volume_range = DEFAULT_VOLUME_RANGE


def handle_slider_change():
    # If a user manually moves a slider, we should turn off the quick filter
    # so they aren't confused why their manual slider changes are fighting a preset
    if st.session_state.quick_filter != QUICK_FILTER_NONE:
        st.session_state.quick_filter = QUICK_FILTER_NONE


# ── Fetch base data early so Symbol options are available inside the filter UI ─
_current_date = st.session_state.get("screener_selected_date", available_dates[0])
with st.spinner("Loading screener…"):
    base_df = fetch_screener_base(str(_current_date))

with st.container(border=True):
    quick_filter = st.radio(
        "Quick Screen Scenarios",
        QUICK_FILTER_OPTIONS,
        horizontal=True,
        key="quick_filter",
        on_change=reset_sliders,
    )

    st.divider()

    c1, c2, c3, c4, c5 = st.columns([2, 3, 2, 1, 4], vertical_alignment="bottom")

    with c1:
        selected_date = st.selectbox(
            "Date",
            options=available_dates,
            index=0,
            format_func=lambda d: d.strftime("%Y-%m-%d"),
            key="screener_selected_date",
        )
    with c2:
        with st.popover("⚙️ Filter Sectors & Symbols", use_container_width=True):
            sector_options = sectors_df["sector"].tolist() if not sectors_df.empty else []
            sector_options_with_all = ["All"] + sector_options
            selected_sectors = st.multiselect(
                "Sector", sector_options_with_all, default=["All"], key="screener_selected_sectors"
            )

            symbol_options = sorted(base_df["Symbol"].dropna().unique().tolist()) if not base_df.empty else []
            selected_symbols = st.multiselect("Symbol", symbol_options, default=[], key="screener_selected_symbols")
    with c3:
        sort_col = st.selectbox(
            "Sort By",
            options=SORT_COLUMN_OPTIONS,
            index=SORT_COLUMN_OPTIONS.index("Volume") if "Volume" in SORT_COLUMN_OPTIONS else 0,
            key="screener_sort_col",
        )
    with c4:
        max_results = st.selectbox(
            "Per Page",
            options=MAX_RESULTS_OPTIONS,
            index=0,
            key="screener_max_results",
        )
    with c5:
        if "screener_volume_range" not in st.session_state:
            st.session_state.screener_volume_range = DEFAULT_VOLUME_RANGE
        volume_range = st.slider(
            "Volume",
            min_value=VOLUME_SLIDER_MIN,
            max_value=VOLUME_SLIDER_MAX,
            step=VOLUME_SLIDER_STEP,
            key="screener_volume_range",
            format="%.1fM",
            on_change=handle_slider_change,
        )

# ── Period Return Filters ────────────────────────────────────────────────────
with st.expander("📈 Period Return Filters", expanded=False):
    _g_cols = st.columns(5)
    _gain_keys = ["gain_1w", "gain_1m", "gain_3m", "gain_6m", "gain_1y"]
    gain_ranges: dict[str, tuple[float, float]] = {}
    for col_label, key, container in zip(GAIN_PERIOD_COLS, _gain_keys, _g_cols):
        with container:
            if key not in st.session_state:
                st.session_state[key] = DEFAULT_GAIN_RANGE
            gain_ranges[col_label] = st.slider(
                col_label,
                min_value=GAIN_SLIDER_MIN,
                max_value=GAIN_SLIDER_MAX,
                step=GAIN_SLIDER_STEP,
                key=key,
                format="%.0f%%",
            )

st.divider()

# ── Apply filters in Python — zero warehouse cost per filter interaction ──────
filtered_df = apply_screener_filters(
    base_df=base_df,
    volume_range=volume_range,
    selected_sectors=selected_sectors,
    quick_filter=quick_filter,
    gain_ranges=gain_ranges,
    selected_symbols=selected_symbols,
)

filtered_df = filtered_df.sort_values(by=sort_col, ascending=False, na_position="last")
total_filtered = len(filtered_df)

# ── Pagination ────────────────────────────────────────────────────────────────
page_size = max_results
total_pages = max(1, (total_filtered + page_size - 1) // page_size)

# Reset to page 0 whenever any filter changes
_filter_key = (
    str(selected_date),
    tuple(sorted(selected_sectors)),
    tuple(sorted(selected_symbols)),
    sort_col,
    max_results,
    volume_range,
    quick_filter,
    tuple(sorted(gain_ranges.items())),
)
if st.session_state.get("_screener_filter_key") != _filter_key:
    st.session_state["screener_page"] = 0
    st.session_state["_screener_filter_key"] = _filter_key

if "screener_page" not in st.session_state:
    st.session_state["screener_page"] = 0

page = min(st.session_state["screener_page"], total_pages - 1)
st.session_state["screener_page"] = page

start = page * page_size
end = min(start + page_size, total_filtered)
screener_df = filtered_df.iloc[start:end].reset_index(drop=True)

# ── Display Results ───────────────────────────────────────────────────────────
if total_filtered > 0:
    st.caption(f"Showing {start + 1}–{end} of {total_filtered} matches  ·  Select a row to open the stock terminal →")
else:
    st.caption("No matches")

if not screener_df.empty:
    with st.expander("ℹ️ Metric Definitions"):
        st.markdown("""
        * **Chg %** — `(Close − Prev Close) / Prev Close`. Full-day return including the overnight gap. The standard daily change shown on Bloomberg and Yahoo Finance.
        * **Intraday %** — `(Close − Open) / Open`. Pure within-session move, gap excluded. Tells you what happened during regular trading hours only.
        * **Gap %** — `(Open − Prev Close) / Prev Close`. Overnight gap from yesterday's close to today's open. Positive = gap up, negative = gap down.
        * **RVOL** — Relative Volume. Today's volume ÷ 20-day average volume. >1 means above-average activity.
        * **Dollar Volume** — `Close × Volume`. Measures liquidity; higher values indicate a more actively traded session.

        _Prices and volume are split-adjusted (`fact_daily_market_adjusted_hc`), so period-return columns stay continuous across stock splits._
        """)

    display_df = screener_df.copy()
    display_df["Volume"] = display_df["Volume"].apply(fmt_volume)
    display_df["Dollar Volume"] = display_df["Dollar Volume"].apply(fmt_dollar_compact)

    def _color_pct(val):
        if not isinstance(val, (int, float)) or val != val:
            return ""
        return "color: #26a69a" if val >= 0 else "color: #ef5350"

    _gain_fmt = {col: "{:+.2f}%" for col in GAIN_PERIOD_COLS if col in display_df.columns}
    _gain_color_cols = [col for col in GAIN_PERIOD_COLS if col in display_df.columns]
    styled = display_df.style.format(
        {
            "Close": "${:.2f}",
            "Chg %": "{:+.2f}%",
            "Intraday %": "{:+.2f}%",
            "Gap %": "{:+.2f}%",
            "RVOL": "{:.2f}×",
            **_gain_fmt,
        },
        na_rep="—",
    ).applymap(_color_pct, subset=["Chg %", "Intraday %", "Gap %"] + _gain_color_cols)

    event = st.dataframe(
        styled,
        selection_mode="single-row",
        on_select="rerun",
        hide_index=True,
        use_container_width=True,
        height=600,
    )
    if len(event.selection.rows) > 0:
        selected_idx = event.selection.rows[0]
        st.session_state["selected_ticker"] = screener_df.iloc[selected_idx]["Symbol"]
        st.switch_page("pages/2_Stock_Deep_Dive.py")

    # ── Pagination controls ───────────────────────────────────────────────────
    pg_prev, pg_info, pg_next = st.columns([1, 4, 1])
    with pg_prev:
        if st.button("← Prev", disabled=(page == 0), use_container_width=True):
            st.session_state["screener_page"] = page - 1
            st.rerun()
    with pg_info:
        st.caption(f"Page {page + 1} of {total_pages}")
    with pg_next:
        if st.button("Next →", disabled=(page >= total_pages - 1), use_container_width=True):
            st.session_state["screener_page"] = page + 1
            st.rerun()

    # ── CSV Export (full filtered set, not just current page) ────────────────
    csv = filtered_df.to_csv(index=False)
    st.download_button(
        "Download CSV",
        csv,
        f"screener_{selected_date}.csv",
        "text/csv",
    )

else:
    st.info("No stocks match the selected criteria. Try relaxing the filters.")
