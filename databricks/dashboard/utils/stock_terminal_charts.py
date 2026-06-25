"""Chart-building helpers for the Stock Terminal page."""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.theme import (
    CHART_HEIGHT_LG,
    apply_rangebreaks,
    get_accent_color,
    get_bearish_color,
    get_bearish_rgba,
    get_bullish_color,
    get_bullish_rgba,
    get_cat_1,
)


def _volume_bar_colors(open_series: pd.Series, close_series: pd.Series) -> list[str]:
    """Return bullish/bearish colors based on open/close direction."""
    return [
        get_bullish_color() if close >= open_ else get_bearish_color()
        for open_, close in zip(open_series, close_series)
    ]


def _build_price_traces(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    chart_type: str,
    show_indicators: list,
    show_52w_lines: bool,
    high_52w,
    low_52w,
    latest: pd.Series,
    price_row: int,
) -> None:
    x_col = "date"

    if chart_type == "Candlestick":
        fig.add_trace(
            go.Candlestick(
                x=chart_df[x_col],
                open=chart_df["open"],
                high=chart_df["high"],
                low=chart_df["low"],
                close=chart_df["close"],
                name="OHLC",
                increasing_line_color=get_bullish_color(),
                decreasing_line_color=get_bearish_color(),
                increasing_line_width=2,
                decreasing_line_width=2,
            ),
            row=price_row,
            col=1,
            secondary_y=False,
        )
    else:
        if not chart_df.empty:
            baseline = float(chart_df["close"].iloc[0])
            latest_price = float(chart_df["close"].iloc[-1])
            is_bullish = latest_price >= baseline
            line_color = get_bullish_color() if is_bullish else get_bearish_color()
            fill_color = get_bullish_rgba(0.15) if is_bullish else get_bearish_rgba(0.15)

            # 1. Base line at first close
            fig.add_trace(
                go.Scatter(
                    x=chart_df[x_col],
                    y=[baseline] * len(chart_df),
                    mode="lines",
                    line=dict(width=0, color="rgba(0,0,0,0)"),
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=price_row,
                col=1,
                secondary_y=False,
            )

            # 2. The actual price line filling down to the previous trace
            fig.add_trace(
                go.Scatter(
                    x=chart_df[x_col],
                    y=chart_df["close"],
                    name="Close",
                    line=dict(color=line_color, width=2),
                    fill="tonexty",
                    fillcolor=fill_color,
                ),
                row=price_row,
                col=1,
                secondary_y=False,
            )

            # 3. Add dotted horizontal line for the baseline
            fig.add_hline(
                y=baseline,
                row=price_row,
                col=1,
                line_dash="dot",
                line_color="#787B86",
                line_width=1,
                opacity=0.7,
                annotation_text=f"Start<br>{baseline:.2f}",
                annotation_position="bottom right",
                annotation_font_size=10,
                annotation_font_color="#8B909D",
            )
        else:
            fig.add_trace(
                go.Scatter(
                    x=chart_df[x_col],
                    y=chart_df["close"],
                    name="Close",
                    line=dict(color=get_accent_color(), width=2),
                ),
                row=price_row,
                col=1,
                secondary_y=False,
            )

    if "VWAP" in show_indicators and "vwap" in chart_df.columns and chart_df["vwap"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=chart_df[x_col],
                y=chart_df["vwap"],
                name="VWAP (20D)",
                line=dict(color=get_cat_1(), width=1.5, dash="dot"),
            ),
            row=price_row,
            col=1,
            secondary_y=False,
        )

    if show_52w_lines and high_52w is not None:
        fig.add_hline(
            y=high_52w,
            row=price_row,
            col=1,
            line_dash="dot",
            line_color=get_bullish_color(),
            line_width=1,
            opacity=0.5,
            annotation_text=f"52W H ${high_52w:.0f}",
            annotation_position="right",
            annotation_font_size=10,
            annotation_font_color=get_bullish_color(),
        )
    if show_52w_lines and low_52w is not None:
        fig.add_hline(
            y=low_52w,
            row=price_row,
            col=1,
            line_dash="dot",
            line_color=get_bearish_color(),
            line_width=1,
            opacity=0.5,
            annotation_text=f"52W L ${low_52w:.0f}",
            annotation_position="right",
            annotation_font_size=10,
            annotation_font_color=get_bearish_color(),
        )


def _build_volume_trace(fig: go.Figure, chart_df: pd.DataFrame, vol_row: int, avg_vol_20d, x_col: str = "date") -> None:
    fig.add_trace(
        go.Bar(
            x=chart_df[x_col],
            y=chart_df["volume"],
            name="Volume",
            marker_color=_volume_bar_colors(chart_df["open"], chart_df["close"]),
            opacity=0.6,
        ),
        row=vol_row,
        col=1,
    )


def _add_volume_profile_overlay(
    fig: go.Figure,
    chart_df: pd.DataFrame,
    price_row: int,
    max_profile_width: float = 0.16,
    scope_label: str = "Full Period",
) -> None:
    """Overlay a left-side horizontal volume profile on the price pane.

    Each bar's volume is distributed uniformly across its full high-low range
    (not just the close), which is the standard industry approximation for daily
    OHLCV data. Marks the Point of Control (POC) and the 70% Value Area
    (VAH / VAL).
    """
    profile_df = chart_df[["high", "low", "volume"]].dropna().copy()
    profile_df = profile_df[profile_df["volume"] > 0]
    if profile_df.empty:
        return

    price_min = float(profile_df["low"].min())
    price_max = float(profile_df["high"].max())
    if price_max <= price_min:
        return

    # ATR-informed bin sizing: half a median bar range per bin,
    # clamped to [20, 100] for readability across all price levels.
    median_range = float((profile_df["high"] - profile_df["low"]).median())
    if median_range <= 0:
        median_range = (price_max - price_min) / 50
    n_bins = int(round((price_max - price_min) / (median_range / 2)))
    n_bins = min(max(n_bins, 20), 100)
    bin_size = (price_max - price_min) / n_bins

    # Distribute each bar's volume uniformly across all bins in [low, high].
    # np.clip ensures the highest bar never falls outside the last bin.
    lows = profile_df["low"].to_numpy()
    highs = profile_df["high"].to_numpy()
    volumes = profile_df["volume"].to_numpy(dtype=float)

    first_bins = np.clip(((lows - price_min) / bin_size).astype(int), 0, n_bins - 1)
    last_bins = np.clip(((highs - price_min) / bin_size).astype(int), 0, n_bins - 1)

    bin_volumes = np.zeros(n_bins)
    for i in range(len(profile_df)):
        n_overlap = last_bins[i] - first_bins[i] + 1
        bin_volumes[first_bins[i] : last_bins[i] + 1] += volumes[i] / n_overlap

    max_bin_vol = float(bin_volumes.max())
    if max_bin_vol <= 0:
        return

    bin_edges = [price_min + i * bin_size for i in range(n_bins + 1)]
    bin_centers = [(bin_edges[i] + bin_edges[i + 1]) / 2 for i in range(n_bins)]

    # Point of Control: bin with the highest accumulated volume.
    poc_idx = int(np.argmax(bin_volumes))
    poc_price = bin_centers[poc_idx]

    # Value Area: expand outward from POC until 70% of total volume is covered.
    # Standard market-profile rule — compare the next TWO bins on each side and
    # absorb whichever pair holds more volume (both bins of that pair) per step.
    total_vol = float(bin_volumes.sum())
    va_lo = poc_idx
    va_hi = poc_idx
    va_vol = float(bin_volumes[poc_idx])
    while va_vol < total_vol * 0.70 and (va_lo > 0 or va_hi < n_bins - 1):
        below = sum(float(bin_volumes[i]) for i in (va_lo - 1, va_lo - 2) if i >= 0)
        above = sum(float(bin_volumes[i]) for i in (va_hi + 1, va_hi + 2) if i <= n_bins - 1)
        expand_up = (above >= below and va_hi < n_bins - 1) or va_lo == 0
        if expand_up:
            for _ in range(2):
                if va_hi < n_bins - 1:
                    va_hi += 1
                    va_vol += float(bin_volumes[va_hi])
        else:
            for _ in range(2):
                if va_lo > 0:
                    va_lo -= 1
                    va_vol += float(bin_volumes[va_lo])

    vah = bin_edges[va_hi + 1]
    val = bin_edges[va_lo]

    y_axis_ref = "y" if price_row == 1 else f"y{price_row}"

    poc_line_color = "rgba(255,107,0,0.92)"
    va_line_color = "rgba(0,184,255,0.88)"
    poc_fill = "rgba(255,107,0,0.45)"
    va_fill = "rgba(0,184,255,0.24)"
    out_fill = "rgba(0,184,255,0.10)"

    # --- Profile bars ---------------------------------------------------------
    for i in range(n_bins):
        if bin_volumes[i] <= 0:
            continue
        bar_w = (bin_volumes[i] / max_bin_vol) * max_profile_width
        if i == poc_idx:
            fill = poc_fill
        elif va_lo <= i <= va_hi:
            fill = va_fill
        else:
            fill = out_fill
        fig.add_shape(
            type="rect",
            xref="paper",
            yref=y_axis_ref,
            x0=0.0,
            x1=bar_w,
            y0=bin_edges[i],
            y1=bin_edges[i + 1],
            fillcolor=fill,
            line=dict(width=0),
            layer="above",
        )

    # --- POC line (full chart width) ------------------------------------------
    fig.add_shape(
        type="line",
        xref="paper",
        yref=y_axis_ref,
        x0=0.0,
        x1=1.0,
        y0=poc_price,
        y1=poc_price,
        line=dict(color=poc_line_color, width=1.3, dash="dot"),
        layer="above",
    )

    # --- Value Area boundary lines (full chart width) -------------------------
    for level in (vah, val):
        fig.add_shape(
            type="line",
            xref="paper",
            yref=y_axis_ref,
            x0=0.0,
            x1=1.0,
            y0=level,
            y1=level,
            line=dict(color=va_line_color, width=1.1, dash="dash"),
            layer="above",
        )

    # --- Labels in the left margin (outside the chart area) -------------------
    label_x = -0.01
    fig.add_annotation(
        xref="paper",
        yref=y_axis_ref,
        x=label_x,
        y=poc_price,
        text=f"<b>POC</b> ${poc_price:.2f}",
        showarrow=False,
        font=dict(size=9, color=poc_line_color),
        xanchor="right",
        align="right",
        bgcolor="rgba(19,23,34,0.75)",
    )
    fig.add_annotation(
        xref="paper",
        yref=y_axis_ref,
        x=label_x,
        y=vah,
        text=f"VAH ${vah:.2f}",
        showarrow=False,
        font=dict(size=9, color=va_line_color),
        xanchor="right",
        align="right",
        bgcolor="rgba(19,23,34,0.75)",
    )
    fig.add_annotation(
        xref="paper",
        yref=y_axis_ref,
        x=label_x,
        y=val,
        text=f"VAL ${val:.2f}",
        showarrow=False,
        font=dict(size=9, color=va_line_color),
        xanchor="right",
        align="right",
        bgcolor="rgba(19,23,34,0.75)",
    )

    # --- "Full Period" scope label above the profile --------------------------
    fig.add_annotation(
        xref="paper",
        yref=y_axis_ref,
        x=max_profile_width / 2,
        y=price_max,
        text=f"Vol Profile · {scope_label}",
        showarrow=False,
        font=dict(size=8, color="rgba(180,180,180,0.65)"),
        xanchor="center",
        yanchor="bottom",
    )


def create_daily_figure(
    market_df: pd.DataFrame,
    chart_type: str,
    show_indicators: list,
    show_52w_lines: bool,
    show_volume_profile: bool,
    high_52w,
    low_52w,
    latest: pd.Series,
    avg_vol_20d,
) -> go.Figure:
    """Build the daily multi-row chart for the Stock Terminal."""
    subplot_rows = ["price"]
    if "Volume" in show_indicators:
        subplot_rows.append("volume")

    n_rows = len(subplot_rows)
    weights = {"price": 0.75, "volume": 0.25}
    total_w = sum(weights[r] for r in subplot_rows)
    row_heights = [round(weights[r] / total_w, 4) for r in subplot_rows]

    subplot_titles = {"price": "", "volume": "Volume"}
    specs = [[{"secondary_y": False}] for _ in range(n_rows)]

    fig = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=row_heights,
        subplot_titles=[subplot_titles[r] for r in subplot_rows],
        specs=specs,
    )

    price_row = 1
    _build_price_traces(
        fig,
        market_df,
        chart_type,
        show_indicators,
        show_52w_lines,
        high_52w,
        low_52w,
        latest,
        price_row,
    )
    if show_volume_profile:
        _add_volume_profile_overlay(fig, market_df, price_row=price_row)

    if "volume" in subplot_rows:
        _build_volume_trace(fig, market_df, subplot_rows.index("volume") + 1, avg_vol_20d)

    if chart_type == "Line (Close)" and not market_df.empty:
        baseline = float(market_df["close"].iloc[0])
        y_min = min(market_df["low"].min(), baseline)
        y_max = max(market_df["high"].max(), baseline)
        y_padding = (y_max - y_min) * 0.1 if y_max > y_min else y_min * 0.01

        fig.update_yaxes(
            title_text="", tickformat="$.2f", row=price_row, col=1, range=[y_min - y_padding, y_max + y_padding]
        )
    else:
        fig.update_yaxes(title_text="", tickformat="$.2f", row=price_row, col=1)
    if "volume" in subplot_rows:
        fig.update_yaxes(title_text="Vol", tickformat=".3s", row=subplot_rows.index("volume") + 1, col=1)

    fig.update_layout(
        height=CHART_HEIGHT_LG,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        hovermode="x unified",
        margin=dict(l=120, r=80, t=40, b=20),
        legend=dict(orientation="h", y=1.04, x=1, xanchor="right"),
    )

    apply_rangebreaks(fig, market_df["date"])
    return fig


def create_intraday_figure(
    intra_df: pd.DataFrame,
    show_volume_profile: bool = False,
    show_indicators: list | None = None,
    chart_type: str = "Candlestick",
    prev_close: float | None = None,
    session_boundary: pd.Timestamp | None = None,
) -> go.Figure:
    """Build intraday (1-minute) price + VWAP + volume chart."""
    if show_indicators is None:
        show_indicators = []

    subplot_rows = ["price"]
    if "Volume" in show_indicators:
        subplot_rows.append("volume")

    n_rows = len(subplot_rows)
    weights = {"price": 0.75, "volume": 0.25}
    total_w = sum(weights[r] for r in subplot_rows)
    row_heights = [round(weights[r] / total_w, 4) for r in subplot_rows]
    subplot_titles = {"price": "", "volume": "Volume"}

    fig_intra = make_subplots(
        rows=n_rows,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=row_heights,
        subplot_titles=[subplot_titles[r] for r in subplot_rows],
    )

    x = intra_df["start_timestamp"]
    line_close = intra_df["close"].copy()
    line_vwap = intra_df["vwap"].copy() if "vwap" in intra_df.columns else None
    session_gap_idx = None
    if session_boundary is not None and not intra_df.empty:
        # Break only line-style traces at the session boundary without mutating OHLC rows.
        idx_list = intra_df.index[intra_df["start_timestamp"] == session_boundary].tolist()
        if idx_list:
            session_gap_idx = idx_list[0]
            if session_gap_idx > 0:
                line_close.iloc[session_gap_idx] = np.nan
                if line_vwap is not None:
                    line_vwap.iloc[session_gap_idx] = np.nan

    if chart_type == "Candlestick" and not intra_df.empty:
        fig_intra.add_trace(
            go.Candlestick(
                x=x,
                open=intra_df["open"],
                high=intra_df["high"],
                low=intra_df["low"],
                close=intra_df["close"],
                name="OHLC",
                increasing_line_color=get_bullish_color(),
                decreasing_line_color=get_bearish_color(),
                increasing_line_width=2,
                decreasing_line_width=2,
            ),
            row=1,
            col=1,
        )
    elif chart_type != "Candlestick" and not intra_df.empty:
        # Determine bullish/bearish color for the area chart based on the latest price vs prev_close
        if prev_close is not None:
            latest_price = float(intra_df["close"].iloc[-1])
            is_bullish = latest_price >= prev_close
            line_color = get_bullish_color() if is_bullish else get_bearish_color()
            fill_color = get_bullish_rgba(0.15) if is_bullish else get_bearish_rgba(0.15)
            baseline = prev_close
        else:
            line_color = get_accent_color()
            fill_color = "rgba(41, 98, 255, 0.15)"  # ACCENT with alpha
            baseline = intra_df["close"].min()

        # 1. Base line at previous close
        base_y = [baseline if pd.notna(c) else np.nan for c in line_close]
        fig_intra.add_trace(
            go.Scatter(
                x=x,
                y=base_y,
                mode="lines",
                line=dict(width=0, color="rgba(0,0,0,0)"),
                showlegend=False,
                hoverinfo="skip",
            ),
            row=1,
            col=1,
        )

        # 2. The actual price line filling down to the previous trace
        fig_intra.add_trace(
            go.Scatter(
                x=x,
                y=line_close,
                name="Close",
                line=dict(color=line_color, width=2),
                fill="tonexty",  # Fill down to the baseline scatter
                fillcolor=fill_color,
            ),
            row=1,
            col=1,
        )

    if prev_close is not None:
        fig_intra.add_hline(
            y=prev_close,
            row=1,
            col=1,
            line_dash="dot",
            line_color="#787B86",
            line_width=1,
            opacity=0.7,
            annotation_text=f"Previous close<br>{prev_close:.2f}",
            annotation_position="bottom right",
            annotation_font_size=10,
            annotation_font_color="#8B909D",
        )

    if "VWAP" in show_indicators and line_vwap is not None and line_vwap.notna().any():
        fig_intra.add_trace(
            go.Scatter(
                x=x,
                y=line_vwap,
                name="Session VWAP",
                line=dict(color=get_cat_1(), width=1.5, dash="dot"),
            ),
            row=1,
            col=1,
        )

    # Guardrail: with very sparse intraday data (e.g., only opening bar),
    # the left-side volume profile can obscure the only candle.
    has_intraday_span = intra_df["start_timestamp"].nunique() > 1
    if show_volume_profile and has_intraday_span:
        _add_volume_profile_overlay(fig_intra, intra_df, price_row=1, scope_label="Session")

    if "volume" in subplot_rows:
        _build_volume_trace(
            fig_intra, intra_df, subplot_rows.index("volume") + 1, avg_vol_20d=None, x_col="start_timestamp"
        )

    if session_boundary is not None:
        boundary_x = pd.Timestamp(session_boundary)
        fig_intra.add_shape(
            type="line",
            xref="x",
            yref="paper",
            x0=boundary_x,
            x1=boundary_x,
            y0=0,
            y1=1,
            line=dict(color="#787B86", width=1, dash="dash"),
            opacity=0.6,
        )
        fig_intra.add_annotation(
            x=boundary_x,
            y=1,
            xref="x",
            yref="paper",
            text="Today",
            showarrow=False,
            xanchor="left",
            yanchor="top",
            font=dict(size=10, color="#8B909D"),
        )

    fig_intra.update_layout(
        height=CHART_HEIGHT_LG,
        xaxis_rangeslider_visible=False,
        showlegend=False,
        hovermode="x unified",
        margin=dict(l=120, r=40, t=40, b=20),
    )

    valid_ts = (
        intra_df["start_timestamp"].dropna()
        if "start_timestamp" in intra_df.columns
        else pd.Series(dtype="datetime64[ns]")
    )
    # Nudge the left edge ~3 min before the first bar so the 09:30 candle isn't
    # flush against the axis (looks clipped). The morning rangebreak bound below
    # is lowered to 9.4 (09:24) so this padding stays inside the visible domain
    # instead of being clamped away.
    _x_start = valid_ts.min() - pd.Timedelta(minutes=3) if not valid_ts.empty else None
    _x_end = valid_ts.max() + pd.Timedelta(minutes=15) if not valid_ts.empty else None

    rbreaks = [
        dict(bounds=["sat", "mon"]),
        dict(bounds=[16, 9.4], pattern="hour"),
    ]
    if not valid_ts.empty:
        # Find missing trading days (e.g. holidays) to exclude them from the x-axis.
        # Anchor at 09:30 with dvalue=6.5h so the removed band sits *inside* trading
        # hours; default dvalue=1d would span 00:00→24:00 and overlap the hour bound's
        # 16:00→09:30 removal, which collapses plotly's visible domain in 2-day mode.
        # Weekends are skipped — already removed by bounds=["sat","mon"].
        unique_dates = pd.to_datetime(valid_ts.dt.date.dropna().unique()).sort_values()
        if len(unique_dates) > 1:
            date_breaks = []
            for i in range(1, len(unique_dates)):
                delta_days = (unique_dates[i] - unique_dates[i - 1]).days
                for d in range(1, delta_days):
                    candidate = unique_dates[i - 1] + pd.Timedelta(days=d)
                    if candidate.weekday() < 5:
                        date_breaks.append(candidate)
            if date_breaks:
                rbreaks.append(
                    dict(
                        values=[d.strftime("%Y-%m-%d 09:30") for d in date_breaks],
                        dvalue=23_400_000,
                    )
                )

    fig_intra.update_xaxes(
        tickformat="%H:%M",
        rangebreaks=rbreaks,
        range=[_x_start, _x_end] if _x_start is not None and _x_end is not None else None,
    )
    # Determine appropriate Y-axis range to make the chart look dynamic
    if not intra_df.empty:
        y_min = intra_df["low"].min()
        y_max = intra_df["high"].max()
        if prev_close is not None:
            y_min = min(y_min, prev_close)
            y_max = max(y_max, prev_close)

        y_padding = (y_max - y_min) * 0.1 if y_max > y_min else y_min * 0.01

        fig_intra.update_yaxes(
            title_text="", tickformat="$.2f", row=1, col=1, range=[y_min - y_padding, y_max + y_padding]
        )
    else:
        fig_intra.update_yaxes(title_text="", tickformat="$.2f", row=1, col=1)
    if "volume" in subplot_rows:
        fig_intra.update_yaxes(title_text="Vol", tickformat=".3s", row=subplot_rows.index("volume") + 1, col=1)

    return fig_intra
