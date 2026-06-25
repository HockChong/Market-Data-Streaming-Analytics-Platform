"""Rendering helpers for the Stock Terminal page."""

import html

import pandas as pd
import streamlit as st
from utils.theme import section_header


def stat_row(label: str, value: str) -> str:
    """Return a Bloomberg-style key/value row as an HTML string."""
    return (
        f'<div style="display:flex;align-items:center;'
        f'padding:7px 10px;border-bottom:1px solid #2B2B43;">'
        f'<span style="color:#787B86;font-size:0.85rem;flex-shrink:0">{label}</span>'
        f'<span style="flex:1;border-bottom:1px dotted rgba(120,123,134,0.35);'
        f'margin:0 10px;height:0;align-self:center"></span>'
        f"<span style=\"font-family:'Courier New',monospace;"
        f'font-size:0.9rem;color:#D1D4DC;flex-shrink:0">{value}</span></div>'
    )


def _normalize_optional_text(value, default: str = "—") -> str:
    """Normalize nullable text-like values used in UI rendering."""
    if value is None or pd.isna(value):
        return default
    text = str(value).strip()
    return text if text and text.lower() != "nan" else default


def _normalize_optional_url(value) -> str | None:
    """Normalize nullable URL values for safe anchor rendering."""
    if value is None or pd.isna(value):
        return None
    text = str(value).strip()
    return text if text and text.lower() != "nan" else None


def render_latest_news(news_df: pd.DataFrame, selected_symbol: str) -> None:
    """Render latest-news cards in a fixed-height scrollable panel."""
    section_header("Latest News")

    if news_df.empty:
        st.info(f"No news found for {selected_symbol}.")
        return

    cards = []
    for _, row in news_df.iterrows():
        url = _normalize_optional_url(row.get("article_url"))
        title = _normalize_optional_text(row.get("title"))
        title_esc = html.escape(title)

        title_md = (
            f'<a href="{url}" target="_blank" rel="noopener noreferrer" '
            f'style="color:#D1D4DC;text-decoration:none">{title_esc}</a>'
            if url
            else title_esc
        )

        publisher = _normalize_optional_text(row.get("publisher_name"))
        publisher_esc = html.escape(publisher)

        pub_date = _normalize_optional_text(row.get("published_date"))
        pub_date_esc = html.escape(pub_date)

        cards.append(
            f'<div class="news-card">'
            f'<div style="font-weight:600;font-size:0.92rem;line-height:1.4">{title_md}</div>'
            f'<div style="color:#787B86;font-size:0.72rem;margin-top:6px">'
            f"{publisher_esc} · {pub_date_esc}</div>"
            f"</div>"
        )

    scrollable = '<div style="height:620px;overflow-y:auto;padding-right:6px">' + "".join(cards) + "</div>"
    st.markdown(scrollable, unsafe_allow_html=True)
