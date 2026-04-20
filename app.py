"""Minimal Shinobu Streamlit app."""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Shinobu Project", page_icon=":chart_with_upwards_trend:")

st.markdown(
    """
    <h1><span style="color:#d00000;">Shinobu</span> Project</h1>
    """,
    unsafe_allow_html=True,
)

st.write("Deployment verification page is live.")
