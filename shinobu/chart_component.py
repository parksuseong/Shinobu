"""Shared chart component for Shinobu tabs."""

from __future__ import annotations

from collections.abc import Sequence

import plotly.graph_objects as go
import streamlit as st


def render_shared_price_chart(
    title: str,
    prices: Sequence[float],
    *,
    height: int = 320,
) -> None:
    """Render a unified price chart used by both live and backtesting tabs."""
    st.subheader(title)
    fig = go.Figure(
        data=[
            go.Scatter(
                y=list(prices),
                mode="lines",
                name="price",
                line={"color": "#3b82f6", "width": 2},
            )
        ]
    )
    fig.update_layout(
        height=height,
        margin={"l": 8, "r": 8, "t": 12, "b": 8},
        xaxis={"rangeslider": {"visible": False}},
        dragmode="zoom",
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "scrollZoom": True,
            "displaylogo": False,
            "responsive": True,
        },
    )
