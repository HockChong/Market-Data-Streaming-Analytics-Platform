"""
Watchlist Page

Lets users save favourite US tickers and view their latest price, daily
change, and screener-style metrics at a glance. The watchlist is persisted
to watchlist.json in the dashboard directory so it survives page reloads
within the same Databricks App instance.
"""

import pandas as pd
import streamlit as st
from utils.connection import fetch_latest_minute_timestamp, get_minute_version
from utils.screener_filters import (
    QUICK_FILTER_NONE,
    SORT_COLUMN_OPTIONS,
    VOLUME_SLIDER_MAX,
    VOLUME_SLIDER_MIN,
    apply_screener_filters,
)
from utils.stock_terminal_charts import create_intraday_figure
from utils.stock_terminal_data import (
    fetch_intraday,
    fetch_latest_minute_date,
    fetch_prev_session_close,
)
from utils.theme import (
    BEARISH,
    BULLISH,
    apply_page_theme,
    fmt_dollar_compact,
    fmt_volume,
    render_data_freshness_bar,
)
from utils.watchlist_data import fetch_all_tickers, fetch_watchlist_quotes
from utils.watchlist_store import add_ticker, load_watchlist, remove_ticker

st.set_page_config(page_title="Watchlist", page_icon=":star:", layout="wide")
apply_page_theme()

# Keep ticker buttons on one line in the narrow Symbol column (e.g. "GOOD").
st.markdown(
    """
    <style>
    div[data-testid="stButton"] button {
        white-space: nowrap;
        padding-left: 0.5rem;
        padding-right: 0.5rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Page header ───────────────────────────────────────────────────────────────
_col_title, _col_status = st.columns([7, 3])
with _col_title:
    st.title("Watchlist")
with _col_status:
    render_data_freshness_bar(fetch_latest_minute_timestamp())

# ── Add stock ─────────────────────────────────────────────────────────────────
all_tickers_df = fetch_all_tickers()

watchlist = load_watchlist()

if not all_tickers_df.empty:
    ticker_map: dict[str, str] = dict(zip(all_tickers_df["symbol"], all_tickers_df["company_name"]))
    available_keys = [s for s in ticker_map if s not in watchlist]

    with st.container(border=True):
        col_input, col_btn = st.columns([5, 1], vertical_alignment="bottom")
        with col_input:
            chosen = st.selectbox(
                "Add stock to watchlist",
                options=available_keys,
                format_func=lambda k: f"{k} — {ticker_map.get(k, '')}",
                index=None,
                placeholder="Search ticker or company name…",
                key="watchlist_add_select",
            )
        with col_btn:
            if st.button("＋ Add", use_container_width=True, disabled=chosen is None):
                add_ticker(chosen)
                st.rerun()
else:
    ticker_map = {}

# ── Reload after any mutation ─────────────────────────────────────────────────
watchlist = load_watchlist()

if not watchlist:
    st.info("Your watchlist is empty. Search for a ticker above to add stocks.")
    st.stop()

# ── Fetch quotes for watchlisted tickers ─────────────────────────────────────
with st.spinner("Loading quotes…"):
    quotes_df = fetch_watchlist_quotes(watchlist)

quotes: dict[str, dict] = {}
if not quotes_df.empty:
    for _, row in quotes_df.iterrows():
        quotes[row["symbol"]] = row.to_dict()

# ── Screener-style filters (reuses apply_screener_filters) ──────────────────
# Map quote columns to the screener's column names so apply_screener_filters
# and SORT_COLUMN_OPTIONS work unchanged.
_SCREENER_COL_MAP = {
    "symbol": "Symbol",
    "sector": "Sector",
    "price": "Close",
    "chg_pct": "Chg %",
    "intraday_pct": "Intraday %",
    "gap_pct": "Gap %",
    "rvol": "RVOL",
    "volume": "Volume",
    "dollar_volume": "Dollar Volume",
    "gain_1w_pct": "1W Gain %",
    "gain_1m_pct": "1M Gain %",
    "gain_3m_pct": "3M Gain %",
}
_WATCHLIST_ORDER = "Watchlist Order"

# Left-join quotes onto the watchlist so tickers without a quote yet still render.
metrics_df = pd.DataFrame({"Symbol": watchlist})
if not quotes_df.empty:
    metrics_df = metrics_df.merge(quotes_df.rename(columns=_SCREENER_COL_MAP), on="Symbol", how="left")
else:
    metrics_df = metrics_df.reindex(columns=["Symbol", *[c for c in _SCREENER_COL_MAP.values() if c != "Symbol"]])

with st.container(border=True):
    _col_filter, _col_sort = st.columns([3, 2], vertical_alignment="bottom")
    with _col_filter:
        with st.popover("⚙️ Filter Sectors & Symbols", use_container_width=True):
            _sector_options = sorted(metrics_df["Sector"].dropna().unique().tolist())
            selected_sectors = st.multiselect(
                "Sector", ["All"] + _sector_options, default=["All"], key="watchlist_selected_sectors"
            )
            selected_symbols = st.multiselect("Symbol", sorted(watchlist), default=[], key="watchlist_selected_symbols")
    with _col_sort:
        _sort_options = [_WATCHLIST_ORDER] + [c for c in SORT_COLUMN_OPTIONS if c in metrics_df.columns]
        sort_col = st.selectbox("Sort By", options=_sort_options, index=0, key="watchlist_sort_col")

metrics_df = apply_screener_filters(
    base_df=metrics_df,
    volume_range=(VOLUME_SLIDER_MIN, VOLUME_SLIDER_MAX),  # unfiltered — watchlist has no volume slider
    selected_sectors=selected_sectors,
    quick_filter=QUICK_FILTER_NONE,
    selected_symbols=selected_symbols,
)
if sort_col != _WATCHLIST_ORDER:
    metrics_df = metrics_df.sort_values(by=sort_col, ascending=False, na_position="last")

visible_tickers = metrics_df["Symbol"].tolist()

# ── Render helpers ────────────────────────────────────────────────────────────
_COL_WEIGHTS = [2.4, 1.2, 1.5, 1.0, 0.9, 0.9, 0.9, 0.8, 0.8, 0.9, 0.8, 0.8, 0.8, 0.5]
_COL_LABELS = [
    "Company",
    "Symbol",
    "Sector",
    "Close",
    "Chg %",
    "Intra %",
    "Gap %",
    "RVOL",
    "Vol",
    "$ Vol",
    "1W %",
    "1M %",
    "3M %",
    "",
]
# Per-column text alignment: text columns left, numeric columns right (under
# right-aligned headers, like a trading terminal), remove button centered.
_COL_ALIGN = [
    "left",
    "left",
    "left",
    "right",
    "right",
    "right",
    "right",
    "right",
    "right",
    "right",
    "right",
    "right",
    "right",
    "center",
]
_MUTED = "#787B86"
_TEXT = "#D1D4DC"

# Ticker whose inline 1-min intraday chart is expanded (one at a time).
_EXPANDED_KEY = "watchlist_intraday_open"


def _has_value(val) -> bool:
    return val is not None and not pd.isna(val)


def _pct_html(val) -> str:
    if not _has_value(val):
        return f"<span style='color:{_MUTED}'>—</span>"
    color = BULLISH if val >= 0 else BEARISH
    sign = "+" if val >= 0 else ""
    return f"<span style='color:{color};font-weight:600'>{sign}{val:.2f}%</span>"


def _num_cell(col, text: str) -> None:
    col.markdown(
        f"<div style='color:{_TEXT};font-family:Courier New,monospace;font-size:0.85rem;"
        f"text-align:right;padding:10px 0'>{text}</div>",
        unsafe_allow_html=True,
    )


def _pct_cell(col, val) -> None:
    col.markdown(
        f"<div style='font-size:0.85rem;text-align:right;padding:10px 0'>{_pct_html(val)}</div>",
        unsafe_allow_html=True,
    )


def _render_intraday_chart(ticker: str, key_prefix: str) -> None:
    """Inline 1-min intraday chart for the live session, mirroring the Stock Terminal."""
    with st.container(border=True):
        with st.spinner(f"Loading intraday bars for {ticker}…"):
            # Sentinel for the minute fact: refetches bust as soon as a new 1-min bar lands.
            minute_version = get_minute_version()
            latest_minute_date = fetch_latest_minute_date(ticker, minute_version=minute_version)
            # Anchor the "Previous close" line to the live minute session shown here,
            # matching the Stock Terminal's intraday tab.
            prev_close = fetch_prev_session_close(ticker, latest_minute_date)
            intra_df = pd.DataFrame()
            if latest_minute_date is not None:
                intra_df = fetch_intraday(ticker, latest_minute_date, minute_version=minute_version).copy()

        if intra_df.empty:
            st.info(f"No intraday data found for {ticker}.")
            return

        # Normalize mixed timestamp formats (ms epoch / strings / tz-aware datetimes).
        ts_raw = intra_df["start_timestamp"]
        ts_ms = pd.to_datetime(pd.to_numeric(ts_raw, errors="coerce"), unit="ms", errors="coerce", utc=True)
        ts_any = pd.to_datetime(ts_raw, errors="coerce", utc=True)
        intra_df["start_timestamp"] = ts_ms.fillna(ts_any).dt.tz_convert("America/New_York").dt.tz_localize(None)

        # Guard against malformed rows that can collapse the chart.
        for c in ["open", "high", "low", "close", "volume"]:
            intra_df[c] = pd.to_numeric(intra_df[c], errors="coerce")
        intra_df = intra_df.dropna(subset=["start_timestamp", "open", "high", "low", "close", "volume"]).copy()
        if intra_df.empty:
            st.info("No valid intraday rows found for this session.")
            return
        intra_df = intra_df.sort_values("start_timestamp").reset_index(drop=True)

        # Cumulative session VWAP — resets at the open; typical-price (H+L+C)/3 basis,
        # matching the Stock Terminal's intraday VWAP.
        typical = (intra_df["high"] + intra_df["low"] + intra_df["close"]) / 3
        intra_df["vwap"] = (typical * intra_df["volume"]).cumsum() / intra_df["volume"].cumsum()

        session_label = pd.to_datetime(latest_minute_date).strftime("%Y-%m-%d")
        bar_count = len(intra_df)
        st.caption(
            f"{ticker} — 1-min intraday, live session {session_label} "
            f"({bar_count} bar{'s' if bar_count != 1 else ''} so far)"
        )

        fig = create_intraday_figure(
            intra_df,
            show_volume_profile=False,
            show_indicators=["VWAP"],
            chart_type="Candlestick",
            prev_close=prev_close,
        )
        st.plotly_chart(fig, use_container_width=True, key=f"{key_prefix}_intraday_{ticker}")

        if st.button("Open in Stock Terminal →", key=f"{key_prefix}_open_terminal_{ticker}"):
            st.session_state["selected_ticker"] = ticker
            st.switch_page("pages/2_Stock_Deep_Dive.py")


def _render_rows(tickers: list[str], key_prefix: str) -> None:
    if not tickers:
        st.info("No stocks to display.")
        return

    # Column header — block-level divs so text-align takes effect (a span is
    # inline and silently ignores it), aligned per column like the cells below.
    _h = st.columns(_COL_WEIGHTS)
    for header_col, label, align in zip(_h, _COL_LABELS, _COL_ALIGN):
        header_col.markdown(
            f"<div style='color:{_MUTED};font-size:0.72rem;white-space:nowrap;"
            f"text-transform:uppercase;letter-spacing:0.04em;text-align:{align}'>{label}</div>",
            unsafe_allow_html=True,
        )
    # Thin header underline — st.divider() adds too much vertical margin here.
    st.markdown(
        "<hr style='margin:0.15rem 0;border:none;border-top:1px solid #2B2B43'>",
        unsafe_allow_html=True,
    )

    for ticker in tickers:
        q = quotes.get(ticker, {})
        price = q.get("price")
        company = ticker_map.get(ticker, "")
        sector = q.get("sector")

        price_str = f"${price:,.2f}" if _has_value(price) else "—"
        rvol = q.get("rvol")
        rvol_str = f"{rvol:.2f}×" if _has_value(rvol) else "—"
        volume = q.get("volume")
        volume_str = fmt_volume(volume) if _has_value(volume) else "—"
        dollar_volume = q.get("dollar_volume")
        dollar_volume_str = fmt_dollar_compact(dollar_volume) if _has_value(dollar_volume) else "—"

        (
            col_company,
            col_ticker,
            col_sector,
            col_close,
            col_chg,
            col_intraday,
            col_gap,
            col_rvol,
            col_volume,
            col_dollar_volume,
            col_1w,
            col_1m,
            col_3m,
            col_rm,
        ) = st.columns(_COL_WEIGHTS, vertical_alignment="center")

        with col_company:
            st.markdown(
                f"<div style='padding:8px 0;line-height:1.5'>"
                f"<div style='color:{_TEXT};font-weight:500'>{company}</div>"
                f"<div style='color:{_MUTED};font-size:0.78rem'>US</div>"
                f"</div>",
                unsafe_allow_html=True,
            )

        with col_ticker:
            if st.button(
                ticker,
                key=f"{key_prefix}_goto_{ticker}",
                help=f"Toggle 1-min intraday chart for {ticker}",
            ):
                st.session_state[_EXPANDED_KEY] = None if st.session_state.get(_EXPANDED_KEY) == ticker else ticker

        col_sector.markdown(
            f"<div style='color:{_TEXT};font-size:0.85rem;padding:10px 0'>{sector if _has_value(sector) else '—'}</div>",
            unsafe_allow_html=True,
        )

        _num_cell(col_close, price_str)
        _pct_cell(col_chg, q.get("chg_pct"))
        _pct_cell(col_intraday, q.get("intraday_pct"))
        _pct_cell(col_gap, q.get("gap_pct"))
        _num_cell(col_rvol, rvol_str)
        _num_cell(col_volume, volume_str)
        _num_cell(col_dollar_volume, dollar_volume_str)
        _pct_cell(col_1w, q.get("gain_1w_pct"))
        _pct_cell(col_1m, q.get("gain_1m_pct"))
        _pct_cell(col_3m, q.get("gain_3m_pct"))

        with col_rm:
            if st.button(
                "✕",
                key=f"{key_prefix}_rm_{ticker}",
                help=f"Remove {ticker}",
                use_container_width=True,
            ):
                if st.session_state.get(_EXPANDED_KEY) == ticker:
                    st.session_state[_EXPANDED_KEY] = None
                remove_ticker(ticker)
                st.rerun()

        if st.session_state.get(_EXPANDED_KEY) == ticker:
            _render_intraday_chart(ticker, key_prefix)


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab_all, tab_us = st.tabs(["All", "US"])

with tab_all:
    _render_rows(visible_tickers, key_prefix="all")

with tab_us:
    _render_rows(visible_tickers, key_prefix="us")
