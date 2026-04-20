"""Minimal Shinobu Streamlit app."""

from __future__ import annotations

import math

import streamlit as st

st.set_page_config(page_title="Shinobu Project", page_icon=":chart_with_upwards_trend:")

st.markdown(
    """
    <h1><span style="color:#d00000;">Shinobu</span> Project</h1>
    """,
    unsafe_allow_html=True,
)

st.write("Deployment verification page is live.")

st.subheader("Live Preview Chart")
prices: list[float] = []
for i in range(120):
    # Deterministic synthetic chart data for quick deployment verification.
    value = 100 + (i * 0.08) + (math.sin(i / 6) * 2.4) + (math.cos(i / 13) * 1.1)
    prices.append(round(value, 4))

st.line_chart({"price": prices}, height=320)
