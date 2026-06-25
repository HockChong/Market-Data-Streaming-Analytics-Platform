"""Indicator and stats helpers for the Stock Terminal page."""

import numpy as np
import pandas as pd


def add_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy enriched with a 20-period SMA (``sma_20``) and rolling VWAP columns."""
    out = df.copy()
    out["sma_20"] = out["close"].rolling(window=20, min_periods=1).mean()

    # 20-period rolling VWAP using typical price (H+L+C)/3.
    # min_periods=1 so the line appears even when fewer than 20 bars exist.
    # For intraday data the page overwrites this column with the cumulative
    # session VWAP after calling this function.
    tp = (out["high"] + out["low"] + out["close"]) / 3
    out["vwap"] = (tp * out["volume"]).rolling(window=20, min_periods=1).sum() / out["volume"].rolling(
        window=20, min_periods=1
    ).sum()
    return out


def compute_window_stats(market_df: pd.DataFrame) -> dict[str, float | None]:
    """Compute rolling summary stats from the full pre-trim daily frame.

    All windows require at least as many rows as their target period; if fewer
    rows exist (e.g. a recently listed stock) the metric is set to None so the
    dashboard renders "—" instead of a silently wrong value.
    """
    # Exclude today so the 20D baseline matches the screener (ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING).
    # This prevents today's outsized volume from deflating its own RVOL ratio.
    prior = market_df.iloc[:-1]
    last_20 = prior.tail(20)
    avg_vol_20d = float(last_20["volume"].mean()) if len(last_20) == 20 else None

    stats_52w = market_df.tail(252)  # approx 252 trading days ≈ 1 year
    high_52w = float(stats_52w["high"].max()) if len(stats_52w) == 252 else None
    low_52w = float(stats_52w["low"].min()) if len(stats_52w) == 252 else None

    pct_from_52w_high = (
        (float(market_df["close"].iloc[-1]) - high_52w) / high_52w * 100
        if high_52w is not None and high_52w != 0
        else None
    )

    # Log returns (CFA convention), sample stdev (ddof=1), annualized by sqrt(252 trading days).
    log_returns = np.log(market_df["close"] / market_df["close"].shift(1)).dropna()
    realized_vol_20d = float(log_returns.tail(20).std(ddof=1) * (252**0.5) * 100) if len(log_returns) >= 20 else None

    trailing_63 = market_df.tail(63)
    if len(trailing_63) < 63:
        max_drawdown_63d = None
    else:
        rolling_peak = trailing_63["close"].cummax()
        drawdowns = (trailing_63["close"] / rolling_peak) - 1.0
        max_drawdown_63d = float(drawdowns.min() * 100)

    return {
        "avg_vol_20d": avg_vol_20d,
        "high_52w": high_52w,
        "low_52w": low_52w,
        "pct_from_52w_high": pct_from_52w_high,
        "realized_vol_20d": realized_vol_20d,
        "max_drawdown_63d": max_drawdown_63d,
    }


def compute_vol_ratio(latest_volume: float, avg_vol_20d: float | None) -> float | None:
    """Return latest-volume / 20D average ratio when valid."""
    if not avg_vol_20d or avg_vol_20d <= 0:
        return None
    return float(latest_volume) / avg_vol_20d
