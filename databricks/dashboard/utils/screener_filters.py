"""Filter constants and in-memory filter helpers for Signal Screener."""

import pandas as pd

QUICK_FILTER_NONE = "None"
QUICK_FILTER_BREAKOUT = "🔥 Relative-Volume Leaders (Top RVOL + $VOL)"
QUICK_FILTER_LIQUIDITY = "💵 Top Dollar Liquidity (Top 10%)"
QUICK_FILTER_OPTIONS = [QUICK_FILTER_NONE, QUICK_FILTER_BREAKOUT, QUICK_FILTER_LIQUIDITY]

SORT_COLUMN_OPTIONS = [
    "Chg %",
    "Intraday %",
    "Gap %",
    "RVOL",
    "Dollar Volume",
    "Volume",
    "Close",
    "1W Gain %",
    "1M Gain %",
    "3M Gain %",
    "6M Gain %",
    "1Y Gain %",
    "Symbol",
]
SORT_DIRECTION_OPTIONS = ["Descending", "Ascending"]
PAGE_SIZE_OPTIONS = [100, 200, 500, 1000]

# Range sliders — values are in millions (M) so the slider label reads "1.0M".
# apply_screener_filters multiplies by _VOLUME_SCALE before comparing raw Volume.
VOLUME_SLIDER_MIN = 0.0  # 0 M
VOLUME_SLIDER_MAX = 500.0  # 500 M
VOLUME_SLIDER_STEP = 0.5  # 0.5 M
DEFAULT_VOLUME_RANGE = (1.0, VOLUME_SLIDER_MAX)  # (1 M, 500 M)
_VOLUME_SCALE = 1_000_000

MAX_RESULTS_OPTIONS = [50, 100, 250, 500]

# Period-return sliders — values in % (e.g. 20.0 = 20% gain).
# Stocks without enough history for a given period return NULL and are excluded
# only when the user actively narrows the slider past the default bounds.
GAIN_SLIDER_MIN = -100.0
GAIN_SLIDER_MAX = 300.0
GAIN_SLIDER_STEP = 5.0
DEFAULT_GAIN_RANGE = (GAIN_SLIDER_MIN, GAIN_SLIDER_MAX)

GAIN_PERIOD_COLS = ["1W Gain %", "1M Gain %", "3M Gain %", "6M Gain %", "1Y Gain %"]

RVOL_BREAKOUT_PERCENTILE = 0.80
DOLLAR_VOLUME_BREAKOUT_PERCENTILE = 0.70
DOLLAR_VOLUME_LIQUIDITY_PERCENTILE = 0.90
DOLLAR_VOLUME_FLOOR = 25_000_000.0


def volume_range_is_unfiltered(volume_range: tuple[int | float, int | float]) -> bool:
    """True when the slider still spans the full configured volume domain."""
    lo, hi = volume_range
    return lo <= VOLUME_SLIDER_MIN and hi >= VOLUME_SLIDER_MAX


def apply_screener_filters(
    base_df: pd.DataFrame,
    volume_range: tuple[int | float, int | float],
    selected_sectors: list[str],
    quick_filter: str,
    gain_ranges: dict[str, tuple[float, float]] | None = None,
    selected_symbols: list[str] | None = None,
) -> pd.DataFrame:
    """Apply all screener filters in Python to avoid additional warehouse round-trips."""
    filtered_df = base_df.copy()

    v_lo_m, v_hi_m = volume_range
    if v_lo_m > VOLUME_SLIDER_MIN:
        filtered_df = filtered_df[filtered_df["Volume"] >= v_lo_m * _VOLUME_SCALE]
    if v_hi_m < VOLUME_SLIDER_MAX:
        filtered_df = filtered_df[filtered_df["Volume"] <= v_hi_m * _VOLUME_SCALE]

    if selected_sectors and "All" not in selected_sectors:
        filtered_df = filtered_df[filtered_df["Sector"].isin(selected_sectors)]

    if selected_symbols:
        filtered_df = filtered_df[filtered_df["Symbol"].isin(selected_symbols)]

    if gain_ranges:
        for col_name, (g_lo, g_hi) in gain_ranges.items():
            if col_name not in filtered_df.columns:
                continue
            if g_lo > GAIN_SLIDER_MIN:
                # Stocks without enough history (NaN) are excluded when a minimum is set.
                filtered_df = filtered_df[filtered_df[col_name].notna() & (filtered_df[col_name] >= g_lo)]
            if g_hi < GAIN_SLIDER_MAX:
                filtered_df = filtered_df[filtered_df[col_name].notna() & (filtered_df[col_name] <= g_hi)]

    if filtered_df.empty:
        return filtered_df

    if quick_filter == QUICK_FILTER_BREAKOUT:
        rvol_series = filtered_df["RVOL"].dropna()
        dollar_vol_series = filtered_df["Dollar Volume"].dropna()

        if rvol_series.empty or dollar_vol_series.empty:
            return filtered_df.iloc[0:0]

        rvol_cutoff = rvol_series.quantile(RVOL_BREAKOUT_PERCENTILE)
        dollar_vol_cutoff = max(
            float(dollar_vol_series.quantile(DOLLAR_VOLUME_BREAKOUT_PERCENTILE)),
            DOLLAR_VOLUME_FLOOR,
        )

        filtered_df = filtered_df[
            (filtered_df["RVOL"] >= rvol_cutoff) & (filtered_df["Dollar Volume"] >= dollar_vol_cutoff)
        ]
    elif quick_filter == QUICK_FILTER_LIQUIDITY:
        dollar_vol_series = filtered_df["Dollar Volume"].dropna()
        if dollar_vol_series.empty:
            return filtered_df.iloc[0:0]
        dollar_vol_cutoff = max(
            float(dollar_vol_series.quantile(DOLLAR_VOLUME_LIQUIDITY_PERCENTILE)),
            DOLLAR_VOLUME_FLOOR,
        )
        filtered_df = filtered_df[filtered_df["Dollar Volume"] >= dollar_vol_cutoff]

    return filtered_df
