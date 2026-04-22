"""Minimal Shinobu Streamlit app."""

from __future__ import annotations

import math

import streamlit as st

from shinobu.chart_component import render_shared_price_chart

st.set_page_config(page_title="Shinobu Project", page_icon=":chart_with_upwards_trend:")

st.markdown(
    """
    <h1><span style="color:#d00000;">Shinobu</span> Project</h1>
    """,
    unsafe_allow_html=True,
)

st.write("Deployment verification page is live.")

live_prices: list[float] = []
for i in range(120):
    value = 100 + (i * 0.08) + (math.sin(i / 6) * 2.4) + (math.cos(i / 13) * 1.1)
    live_prices.append(round(value, 4))

backtest_prices: list[float] = []
for i in range(120):
    value = 97 + (i * 0.05) + (math.sin(i / 8) * 1.6) + (math.cos(i / 15) * 0.9)
    backtest_prices.append(round(value, 4))

live_tab, backtest_tab, ai_signal_tab = st.tabs(["실전", "백테스팅", "ai신호탐색기"])

with live_tab:
    render_shared_price_chart("실전 차트", live_prices)

with backtest_tab:
    render_shared_price_chart("백테스팅 차트", backtest_prices)

with ai_signal_tab:
    render_shared_price_chart("ai신호탐색기 차트", backtest_prices)
