"""
Stock Terminal Page

Unified deep-dive for a single ticker: technicals, sentiment overlay, news feed.
Layout: Top ticker bar -> 70/30 horizontal split (Tabs | News & Sentiment).
"""

from datetime import date, timedelta

import pandas as pd
import streamlit as st
from utils.connection import fetch_latest_minute_timestamp, get_gold_version, get_minute_version
from utils.stock_terminal_charts import create_daily_figure, create_intraday_figure
from utils.stock_terminal_data import (
    fetch_active_tickers,
    fetch_daily_date_range,
    fetch_daily_market_data,
    fetch_intraday,
    fetch_latest_minute_date,
    fetch_prev_session_close,
    fetch_recent_news,
)
from utils.stock_terminal_indicators import (
    add_technical_indicators,
    compute_vol_ratio,
    compute_window_stats,
)
from utils.stock_terminal_render import render_latest_news, stat_row
from utils.theme import (
    apply_page_theme,
    fmt_dollar_compact,
    fmt_volume,
    render_data_freshness_bar,
)

st.set_page_config(page_title="US Stock Terminal", page_icon=":bar_chart:", layout="wide")
apply_page_theme()

LOOKBACK_MAP = {
    "30 days": 30,
    "60 days": 60,
    "90 days": 90,
    "6 months": 180,
    "1 year": 365,
    "3 years": 1095,
    "5 years": 1825,
}
RELATIVE_OPTIONS = list(LOOKBACK_MAP.keys()) + ["YTD", "MTD", "This Quarter", "Last Quarter"]


def _compute_fetch_days(lookback_days: int) -> int:
    """Snap to fixed fetch windows to maximise cache reuse across lookback changes.

    ≤365 days  → 420-day window (covers indicators + warmup).
    ≤1875 days → 1875-day window (3yr and 5yr share one entry).
    >1875 days → rounded up to nearest 90-day bucket (custom ranges going far back).
    """
    if lookback_days <= 365:
        return 420
    if lookback_days <= 1875:
        return 1875
    return ((lookback_days + 89) // 90) * 90


def _resolve_relative_dates(lookback: str) -> tuple[date, date]:
    """Return (start_date, end_date) for a named relative lookback option."""
    today = date.today()
    if lookback in LOOKBACK_MAP:
        return today - timedelta(days=LOOKBACK_MAP[lookback]), today
    if lookback == "YTD":
        return date(today.year, 1, 1), today
    if lookback == "MTD":
        return date(today.year, today.month, 1), today
    if lookback == "This Quarter":
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        return date(today.year, q_start_month, 1), today
    if lookback == "Last Quarter":
        q_start_month = ((today.month - 1) // 3) * 3 + 1
        q_start = date(today.year, q_start_month, 1)
        lq_end = q_start - timedelta(days=1)
        lq_start_month = ((lq_end.month - 1) // 3) * 3 + 1
        return date(lq_end.year, lq_start_month, 1), lq_end
    return today - timedelta(days=365), today


@st.cache_data(show_spinner=False)
def _get_enriched_market_data(symbol: str, fetch_days: int, gold_version: str) -> tuple:
    """Fetch, enrich with indicators, and compute window stats in one cached call.

    Keyed to (symbol, fetch_days, gold_version) so the cache busts automatically
    when new daily data lands, but sidebar-only reruns (chart type, indicator
    toggles) skip all rolling computation.
    """
    df = fetch_daily_market_data(symbol, fetch_days)
    stats = compute_window_stats(df)
    enriched = add_technical_indicators(df)
    return enriched, stats


# ── Sidebar Controls ──────────────────────────────────────────────────────────
with st.spinner("Loading tickers…"):
    tickers_df = fetch_active_tickers()

if tickers_df.empty:
    st.error("No tickers found in dim_ticker_hc.")
    st.stop()

ticker_options = tickers_df["symbol"].tolist()
default_ticker = st.session_state.get("selected_ticker", "AAPL")

st.sidebar.page_link("pages/1_Signal_Screener.py", label="← Back to Screener")
st.sidebar.markdown("---")
selected_symbol = st.sidebar.selectbox(
    "Select Symbol",
    ticker_options,
    index=ticker_options.index(default_ticker) if default_ticker in ticker_options else 0,
)

company_name = tickers_df.loc[tickers_df["symbol"] == selected_symbol, "company_name"].iloc[0]

data_min_date, data_max_date = fetch_daily_date_range(selected_symbol)
_today = date.today()
_data_min = data_min_date or (_today - timedelta(days=1825))
_data_max = data_max_date or _today

st.sidebar.markdown("**Date Range**")
st.sidebar.caption(f"Data available: {_data_min} → {_data_max}")
date_mode = st.sidebar.radio(
    "Mode",
    ["Relative", "Custom Range"],
    key="date_mode",
    horizontal=True,
)

if date_mode == "Relative":
    lookback = st.sidebar.selectbox(
        "Lookback Period",
        RELATIVE_OPTIONS,
        index=RELATIVE_OPTIONS.index("1 year"),
    )
    start_date, end_date = _resolve_relative_dates(lookback)
    # Clamp to actual data bounds so indicators don't silently go empty
    start_date = max(start_date, _data_min)
    end_date = min(end_date, _data_max)
    if start_date > end_date:
        st.warning(
            f"The **{lookback}** window falls entirely outside the available data "
            f"({_data_min} → {_data_max}). Try a longer lookback period."
        )
        st.stop()
    fetch_days = _compute_fetch_days((end_date - start_date).days)
else:
    _default_start = st.session_state.get("anchor_start", _data_max - timedelta(days=30))
    _default_end = st.session_state.get("anchor_end", _data_max)
    start_date = st.sidebar.date_input(
        "Start Date",
        value=max(_default_start, _data_min),
        min_value=_data_min,
        max_value=_data_max,
    )
    end_date = st.sidebar.date_input(
        "End Date",
        value=min(_default_end, _data_max),
        min_value=_data_min,
        max_value=_data_max,
    )
    if start_date >= end_date:
        st.sidebar.error("Start must be before End.")
        st.stop()
    lookback = f"{start_date} – {end_date}"
    fetch_days = _compute_fetch_days((_today - start_date).days + 60)

days = (end_date - start_date).days

st.sidebar.markdown("---")
st.sidebar.markdown("**Chart Options**")

chart_type = st.sidebar.radio("Chart Type", ["Candlestick", "Line (Close)"], index=0)

show_indicators = st.sidebar.multiselect(
    "Indicators",
    ["VWAP", "Volume"],
    default=["VWAP"],
)

show_52w_lines = st.sidebar.checkbox("Show 52W Hi/Lo Lines", value=False)
show_volume_profile = st.sidebar.checkbox("Volume Profile (Full Period)", value=True)

# ── Query + Enrich Market Data ────────────────────────────────────────────────
with st.spinner("Loading market data…"):
    market_df_full, window_stats = _get_enriched_market_data(selected_symbol, fetch_days, get_gold_version())

if market_df_full.empty:
    st.warning(
        f"No market data for {selected_symbol} in the last {lookback}. "
        "Try expanding the lookback period in the sidebar."
    )
    st.stop()

avg_vol_20d = window_stats["avg_vol_20d"]
high_52w = window_stats["high_52w"]
low_52w = window_stats["low_52w"]
pct_from_52w_high = window_stats["pct_from_52w_high"]
realized_vol_20d = window_stats["realized_vol_20d"]
max_drawdown_63d = window_stats["max_drawdown_63d"]

# Trim to the requested date window (handles both relative and custom ranges)
market_df = market_df_full[
    (pd.to_datetime(market_df_full["date"]) >= pd.Timestamp(start_date))
    & (pd.to_datetime(market_df_full["date"]) <= pd.Timestamp(end_date))
].reset_index(drop=True)

# ── Guard: trimmed window may be empty if lookback predates available data ────
if market_df.empty:
    st.warning(
        f"No data for **{selected_symbol}** in the selected window "
        f"({start_date} → {end_date}). "
        "The earliest available date is "
        f"**{_data_min}** and the latest is **{_data_max}**. "
        "Try a longer lookback period or switch to Custom Range."
    )
    st.stop()

# ── Query News ────────────────────────────────────────────────────────────────
news_df = fetch_recent_news(selected_symbol, since_date=str(start_date))

# ── Derived KPI Values ────────────────────────────────────────────────────────
latest = market_df.iloc[-1]
latest_dollar_volume = float(latest["close"] * latest["volume"])

vol_ratio = compute_vol_ratio(float(latest["volume"]), avg_vol_20d)

# ── Page Header ───────────────────────────────────────────────────────────────
_col_title, _col_status = st.columns([7, 3])
with _col_title:
    st.title("US Stock Terminal")
with _col_status:
    render_data_freshness_bar(fetch_latest_minute_timestamp())

# ── Ticker Identity Bar ───────────────────────────────────────────────────────
current_close = float(latest["close"])
# Header "today" badge: pairs this daily row's close with its OWN prev_adj_close, so it
# is correct for any selected window. The intraday tab does NOT reuse this — it anchors
# to the live session via fetch_prev_session_close (see the Intraday view below).
prev_close = float(latest.get("prev_close", current_close)) if pd.notna(latest.get("prev_close")) else current_close
change_val = current_close - prev_close
change_pct = (
    float(latest.get("price_change_pct", 0.0))
    if pd.notna(latest.get("price_change_pct"))
    else ((change_val / prev_close * 100) if prev_close else 0.0)
)

is_positive = change_val >= 0
color_class = "gf-green" if is_positive else "gf-red"
sign_char = "+" if is_positive else ""
arrow = "↑" if is_positive else "↓"

latest_date_str = pd.to_datetime(latest["date"]).strftime("%d %b")

ticker_bar_html = f"""
<div class="gf-header">
    <div class="gf-title">{company_name} ({selected_symbol})</div>
    <div class="gf-price-row">
        <span class="gf-price">{current_close:.2f}</span>
        <span class="gf-currency">USD</span>
    </div>
    <div class="gf-change-row {color_class}">
        {sign_char}{change_val:.2f} ({sign_char}{change_pct:.2f}%) {arrow} today
    </div>
    <div class="gf-timestamp">
        Closed: {latest_date_str} • Disclaimer
    </div>
</div>
"""
st.markdown(ticker_bar_html, unsafe_allow_html=True)

# ── KPI Tear Sheet ────────────────────────────────────────────────────────────
kpi1, kpi2, kpi3 = st.columns(3)

_last_sma = market_df["sma_20"].iloc[-1]
sma20 = float(_last_sma) if pd.notna(_last_sma) else None
if sma20 is not None and sma20 != 0:
    trend_pct = (float(latest["close"]) - sma20) / sma20 * 100
    kpi1.metric("Trend vs 20d SMA", f"${latest['close']:.2f}", f"{trend_pct:+.2f}%")
else:
    kpi1.metric("Trend vs 20d SMA", "—", "—")

if realized_vol_20d is not None:
    kpi2.metric(
        "20D Realized Vol (ann.)",
        f"{realized_vol_20d:.2f}%",
        "Higher = more volatile",
        delta_color="off",
    )
else:
    kpi2.metric("20D Realized Vol (ann.)", "—", "—")

if max_drawdown_63d is not None:
    kpi3.metric(
        "63D Max Drawdown",
        f"{max_drawdown_63d:.2f}%",
        "Worst peak-to-trough",
        delta_color="off",
    )
else:
    kpi3.metric("63D Max Drawdown", "—", "—")
st.markdown("---")

# ── Main Body Split ───────────────────────────────────────────────────────────
col_left, col_right = st.columns([7, 3])

with col_left:
    tab_intra, tab_daily, tab_stats = st.tabs(["⏱️ Intraday", "📈 Daily", "📊 Stats"])

    # ════════════════════════════════════════════════════════════════════════════
    # VIEW 1 — DAILY
    # ════════════════════════════════════════════════════════════════════════════
    with tab_daily:
        fig = create_daily_figure(
            market_df=market_df,
            chart_type=chart_type,
            show_indicators=show_indicators,
            show_52w_lines=show_52w_lines,
            show_volume_profile=show_volume_profile,
            high_52w=high_52w,
            low_52w=low_52w,
            latest=latest,
            avg_vol_20d=avg_vol_20d,
        )
        st.plotly_chart(fig, use_container_width=True)

    # ════════════════════════════════════════════════════════════════════════════
    # VIEW 2 — INTRADAY
    # ════════════════════════════════════════════════════════════════════════════
    with tab_intra:
        with st.spinner("Loading intraday bars…"):
            # Sentinel for the minute fact: refetches below bust as soon as a new
            # 1-min bar lands, while the page itself re-runs on the header timer.
            minute_version = get_minute_version()
            latest_minute_date = fetch_latest_minute_date(selected_symbol, minute_version=minute_version)
            # Anchor the "Previous close" line to the live minute session shown here,
            # not the daily date-range picker — otherwise a custom historical daily
            # window would draw a stale reference under live bars.
            intraday_prev_close = fetch_prev_session_close(selected_symbol, latest_minute_date)
            intra_df = pd.DataFrame()

            if latest_minute_date is not None:
                # Fetch the live session (even if it has only a few bars so far)
                intra_df = fetch_intraday(selected_symbol, latest_minute_date, minute_version=minute_version).copy()

        if not intra_df.empty:
            # Normalize mixed timestamp formats (ms epoch / strings / tz-aware datetimes).
            ts_raw = intra_df["start_timestamp"]
            ts_ms = pd.to_datetime(pd.to_numeric(ts_raw, errors="coerce"), unit="ms", errors="coerce", utc=True)
            ts_any = pd.to_datetime(ts_raw, errors="coerce", utc=True)
            ts = ts_ms.fillna(ts_any).dt.tz_convert("America/New_York").dt.tz_localize(None)
            intra_df["start_timestamp"] = ts

            # Guard against malformed rows that can collapse the chart.
            required_cols = ["start_timestamp", "open", "high", "low", "close", "volume"]
            for c in ["open", "high", "low", "close", "volume"]:
                intra_df[c] = pd.to_numeric(intra_df[c], errors="coerce")
            intra_df = intra_df.dropna(subset=required_cols).copy()
            if intra_df.empty:
                st.info("No valid intraday rows found for this session.")

            intra_df = intra_df.sort_values("start_timestamp").reset_index(drop=True)

            intra_df = add_technical_indicators(intra_df)

            # Cumulative session VWAP for the single live session — resets at the open.
            # Weights typical price (H+L+C)/3, the standard VWAP basis, matching the
            # daily-chart VWAP in add_technical_indicators (both use typical price).
            intra_df["vwap_num"] = (intra_df["high"] + intra_df["low"] + intra_df["close"]) / 3 * intra_df["volume"]
            intra_df["vwap"] = intra_df["vwap_num"].cumsum() / intra_df["volume"].cumsum()
            intra_df.drop(columns=["vwap_num"], inplace=True)

            if intra_df["start_timestamp"].nunique() <= 1:
                st.caption(
                    "Limited intraday bars available for this session; the chart will fill in as more minutes arrive."
                )
            else:
                live_label = pd.to_datetime(latest_minute_date).strftime("%Y-%m-%d")
                live_bar_count = len(intra_df)
                st.caption(
                    f"Live session {live_label} ({live_bar_count} bar{'s' if live_bar_count != 1 else ''} so far)"
                )

            fig_intra = create_intraday_figure(
                intra_df,
                show_volume_profile=show_volume_profile,
                show_indicators=show_indicators,
                chart_type=chart_type,
                prev_close=intraday_prev_close,
            )
            st.plotly_chart(fig_intra, use_container_width=True)
        else:
            st.info("No intraday data found.")

    # ════════════════════════════════════════════════════════════════════════════
    # VIEW 3 — STATS
    # ════════════════════════════════════════════════════════════════════════════
    with tab_stats:
        _s1, _s2 = st.columns(2)

        with _s1:
            _period_high = float(market_df["high"].max())
            _period_low = float(market_df["low"].min())
            _first_close = market_df["close"].iloc[0]
            _period_ret = (
                float((market_df["close"].iloc[-1] - _first_close) / _first_close * 100)
                if pd.notna(_first_close) and float(_first_close) != 0
                else 0.0
            )
            _first_close = float(_first_close) if pd.notna(_first_close) else 0.0
            _avg_daily_vol = float(market_df["volume"].mean())

            st.markdown("**Period Summary**")
            _actual_days = (pd.to_datetime(market_df["date"].iloc[-1]) - pd.to_datetime(market_df["date"].iloc[0])).days
            _period_label = (
                lookback
                if _actual_days >= days * 0.95
                else f"{market_df['date'].iloc[0]} – {market_df['date'].iloc[-1]}"
            )
            _rows = [
                (f"Period High ({_period_label})", f"${_period_high:.2f}"),
                (f"Period Low ({_period_label})", f"${_period_low:.2f}"),
                (f"Return ({_period_label})", f"{_period_ret:+.2f}%"),
                (f"Avg Daily Volume ({_period_label})", fmt_volume(_avg_daily_vol)),
                ("Latest Dollar Volume", fmt_dollar_compact(latest_dollar_volume)),
                ("20D Realized Vol (ann.)", f"{realized_vol_20d:.2f}%" if realized_vol_20d is not None else "—"),
                ("63D Max Drawdown", f"{max_drawdown_63d:.2f}%" if max_drawdown_63d is not None else "—"),
            ]
            if high_52w:
                _rows.append(("52W High", f"${high_52w:.2f}"))
            if low_52w:
                _rows.append(("52W Low", f"${low_52w:.2f}"))
            if pct_from_52w_high is not None:
                _rows.append(("% from 52W High", f"{pct_from_52w_high:.1f}%"))

            for _label, _val in _rows:
                st.markdown(stat_row(_label, _val), unsafe_allow_html=True)

        with _s2:
            st.markdown("**Technical Snapshot**")
            _tech_rows = [
                ("Vol / 20D Avg", f"{vol_ratio:.2f}×" if vol_ratio else "—"),
            ]

            for _label, _val in _tech_rows:
                st.markdown(stat_row(_label, _val), unsafe_allow_html=True)

with col_right:
    render_latest_news(news_df.reset_index(drop=True), selected_symbol)
