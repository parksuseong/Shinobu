from __future__ import annotations

import math

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


BUY_OPEN_COLOR = "#3b82f6"
BUY_CLOSE_COLOR = "#ef4444"
SCR_LINE_COLOR = "#e5e7eb"


def _asset_marker_symbol(symbol_code: str) -> str:
    if symbol_code == "252670.KS":
        return "star"
    return "circle"


def _build_x_values(frame: pd.DataFrame) -> list[int]:
    return list(range(len(frame)))


def _build_ticks(frame: pd.DataFrame, timeframe_label: str) -> tuple[list[int], list[str]]:
    x_values = _build_x_values(frame)
    total = len(x_values)
    if total == 0:
        return [], []

    step = max(1, math.ceil(total / 8))
    tick_values = [x_values[index] for index in range(0, total, step)]
    if x_values[-1] not in tick_values:
        tick_values.append(x_values[-1])

    if timeframe_label in {"일봉", "주봉", "월봉"}:
        tick_labels = [frame.index[index].strftime("%Y-%m") for index in range(0, total, step)]
        last_label = frame.index[-1].strftime("%Y-%m")
    else:
        tick_labels = [frame.index[index].strftime("%m-%d %H:%M") for index in range(0, total, step)]
        last_label = frame.index[-1].strftime("%m-%d %H:%M")

    if len(tick_labels) < len(tick_values):
        tick_labels.append(last_label)
    else:
        tick_labels[-1] = last_label

    return tick_values, tick_labels


def _apply_common_xaxis(figure: go.Figure, tick_values: list[int], tick_labels: list[str], max_x: int) -> None:
    figure.update_xaxes(
        type="linear",
        showgrid=False,
        showline=True,
        linecolor="#2a2e39",
        tickmode="array",
        tickvals=tick_values,
        ticktext=tick_labels,
        tickfont={"size": 11, "color": "#9aa4b2"},
        tickangle=0,
        ticks="outside",
        ticklen=6,
        tickcolor="#2a2e39",
        rangeslider={"visible": False},
        range=[-0.45, max_x + 0.45] if max_x >= 0 else None,
    )


def _hover_text(frame: pd.DataFrame, label: str) -> list[str]:
    return [f"{label}<br>{index:%Y-%m-%d %H:%M}<br>가격 {row['Close']:,.0f}" for index, row in frame.iterrows()]


def _candle_hover_text(frame: pd.DataFrame) -> list[str]:
    return [
        (
            f"{index:%Y-%m-%d %H:%M}<br>"
            f"시가 {row['Open']:,.0f}<br>"
            f"고가 {row['High']:,.0f}<br>"
            f"저가 {row['Low']:,.0f}<br>"
            f"종가 {row['Close']:,.0f}"
        )
        for index, row in frame.iterrows()
    ]


def _add_signal_markers(
    figure: go.Figure,
    rows: pd.DataFrame,
    x_positions: list[float],
    label: str,
    color: str,
    symbol: str,
    y_values,
    row_no: int,
) -> None:
    if rows.empty:
        return

    figure.add_trace(
        go.Scatter(
            x=x_positions,
            y=y_values,
            mode="markers+text",
            marker={
                "symbol": symbol,
                "size": 11,
                "color": color,
                "line": {"width": 1, "color": "#ffffff"},
            },
            text=[label] * len(rows),
            textposition="top center",
            textfont={"size": 10, "color": color},
            hovertext=_hover_text(rows, label),
            hovertemplate="%{hovertext}<extra></extra>",
            name=label,
        ),
        row=row_no,
        col=1,
    )


def _signal_rows_with_base_y(
    signal_frame: pd.DataFrame,
    base_frame: pd.DataFrame,
    signal_column: str,
    base_column: str,
    multiplier: float = 1.0,
) -> tuple[pd.DataFrame, pd.Series]:
    rows = signal_frame[signal_frame[signal_column]].copy()
    if rows.empty:
        return rows, pd.Series(dtype=float)

    aligned = base_frame.reindex(rows.index)
    valid_rows = aligned[base_column].notna()
    rows = rows.loc[valid_rows]
    if rows.empty:
        return rows, pd.Series(dtype=float)

    y_values = aligned.loc[rows.index, base_column] * multiplier
    return rows, y_values


def _add_asset_signal_group(
    figure: go.Figure,
    base_frame: pd.DataFrame,
    signal_frame: pd.DataFrame,
    asset_name: str,
    asset_symbol: str,
    include_scr_panel: bool,
    show_on_main: bool = True,
    show_on_indicator: bool = True,
) -> None:
    if "buy_open" not in signal_frame.columns or "buy_close" not in signal_frame.columns:
        return

    marker_symbol = _asset_marker_symbol(asset_symbol)
    base_positions = pd.Series(range(len(base_frame)), index=base_frame.index)

    buy_open_rows, buy_open_y = _signal_rows_with_base_y(signal_frame, base_frame, "buy_open", "Low", 0.985)
    buy_close_rows, buy_close_y = _signal_rows_with_base_y(signal_frame, base_frame, "buy_close", "High", 1.015)

    if show_on_main:
        _add_signal_markers(
            figure,
            buy_open_rows,
            base_positions.reindex(buy_open_rows.index).tolist(),
            f"buy open · {asset_name}",
            BUY_OPEN_COLOR,
            marker_symbol,
            buy_open_y,
            1,
        )
        _add_signal_markers(
            figure,
            buy_close_rows,
            base_positions.reindex(buy_close_rows.index).tolist(),
            f"buy close · {asset_name}",
            BUY_CLOSE_COLOR,
            marker_symbol,
            buy_close_y,
            1,
        )

    if show_on_indicator and include_scr_panel and "scr_line" in signal_frame.columns:
        indicator_open_rows = signal_frame[signal_frame["buy_open"]]
        indicator_close_rows = signal_frame[signal_frame["buy_close"]]
        _add_signal_markers(
            figure,
            indicator_open_rows,
            base_positions.reindex(indicator_open_rows.index).tolist(),
            f"buy open · {asset_name}",
            BUY_OPEN_COLOR,
            marker_symbol,
            indicator_open_rows["scr_line"] if not indicator_open_rows.empty else [],
            2,
        )
        _add_signal_markers(
            figure,
            indicator_close_rows,
            base_positions.reindex(indicator_close_rows.index).tolist(),
            f"buy close · {asset_name}",
            BUY_CLOSE_COLOR,
            marker_symbol,
            indicator_close_rows["scr_line"] if not indicator_close_rows.empty else [],
            2,
        )


def _add_candles(figure: go.Figure, frame: pd.DataFrame, x_values: list[int]) -> None:
    increasing = frame["Close"] >= frame["Open"]
    decreasing = ~increasing
    hover_text = _candle_hover_text(frame)

    if increasing.any():
        inc = frame.loc[increasing]
        inc_x = [x_values[index] for index, flag in enumerate(increasing.tolist()) if flag]
        inc_wick_x: list[float] = []
        inc_wick_y: list[float] = []
        for x_value, (_, row) in zip(inc_x, inc.iterrows(), strict=False):
            inc_wick_x.extend([x_value, x_value, None])
            inc_wick_y.extend([row["Low"], row["High"], None])
        figure.add_trace(
            go.Scatter(
                x=inc_wick_x,
                y=inc_wick_y,
                mode="lines",
                line={"color": "#089981", "width": 1},
                hoverinfo="skip",
                name="양봉 꼬리",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Bar(
                x=inc_x,
                y=(inc["Close"] - inc["Open"]).tolist(),
                base=inc["Open"].tolist(),
                width=0.98,
                marker={"color": "#089981", "line": {"color": "#089981", "width": 1}},
                customdata=[hover_text[index] for index, flag in enumerate(increasing.tolist()) if flag],
                hovertemplate="%{customdata}<extra></extra>",
                name="양봉",
            ),
            row=1,
            col=1,
        )

    if decreasing.any():
        dec = frame.loc[decreasing]
        dec_x = [x_values[index] for index, flag in enumerate(decreasing.tolist()) if flag]
        dec_wick_x: list[float] = []
        dec_wick_y: list[float] = []
        for x_value, (_, row) in zip(dec_x, dec.iterrows(), strict=False):
            dec_wick_x.extend([x_value, x_value, None])
            dec_wick_y.extend([row["Low"], row["High"], None])
        figure.add_trace(
            go.Scatter(
                x=dec_wick_x,
                y=dec_wick_y,
                mode="lines",
                line={"color": "#f23645", "width": 1},
                hoverinfo="skip",
                name="음봉 꼬리",
            ),
            row=1,
            col=1,
        )
        figure.add_trace(
            go.Bar(
                x=dec_x,
                y=(dec["Close"] - dec["Open"]).tolist(),
                base=dec["Open"].tolist(),
                width=0.98,
                marker={"color": "#f23645", "line": {"color": "#f23645", "width": 1}},
                customdata=[hover_text[index] for index, flag in enumerate(decreasing.tolist()) if flag],
                hovertemplate="%{customdata}<extra></extra>",
                name="음봉",
            ),
            row=1,
            col=1,
        )


def build_candlestick_chart(
    frame: pd.DataFrame,
    timeframe_label: str,
    symbol_name: str,
    symbol_code: str,
    pair_frame: pd.DataFrame | None = None,
    pair_name: str | None = None,
    pair_symbol_code: str | None = None,
) -> go.Figure:
    include_scr_panel = "scr_line" in frame.columns
    x_values = _build_x_values(frame)
    tick_values, tick_labels = _build_ticks(frame, timeframe_label)

    if include_scr_panel:
        figure = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            row_heights=[0.74, 0.26],
        )
    else:
        figure = make_subplots(rows=1, cols=1)

    _add_candles(figure, frame, x_values)

    _add_asset_signal_group(
        figure,
        frame,
        frame,
        symbol_name,
        symbol_code,
        include_scr_panel,
        show_on_main=True,
        show_on_indicator=True,
    )
    if pair_frame is not None and pair_name is not None and pair_symbol_code is not None:
        _add_asset_signal_group(
            figure,
            frame,
            pair_frame,
            pair_name,
            pair_symbol_code,
            include_scr_panel,
            show_on_main=True,
            show_on_indicator=True,
        )

    if include_scr_panel:
        figure.add_trace(
            go.Scatter(
                x=x_values,
                y=frame["scr_line"],
                mode="lines",
                line={"color": SCR_LINE_COLOR, "width": 1.7, "dash": "dot"},
                hovertemplate=f"{symbol_name} SCR " + "%{y:.2f}<extra></extra>",
                name=f"{symbol_name} SCR",
            ),
            row=2,
            col=1,
        )
        if pair_frame is not None and "scr_line" in pair_frame.columns:
            pair_scr = pair_frame.reindex(frame.index).ffill()
            figure.add_trace(
                go.Scatter(
                    x=x_values,
                    y=pair_scr["scr_line"],
                    mode="lines",
                    line={"color": "#f59e0b", "width": 1.4, "dash": "dot"},
                    hovertemplate=f"{pair_name or '인버스'} SCR " + "%{y:.2f}<extra></extra>",
                    name=f"{pair_name or '인버스'} SCR",
                ),
                row=2,
                col=1,
            )

    title_text = f"{symbol_name} · {timeframe_label}"
    if include_scr_panel:
        title_text += " · 실전"

    figure.update_layout(
        height=500 if include_scr_panel else 380,
        margin={"l": 2, "r": 56, "t": 42, "b": 12},
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        font={"color": "#d1d4dc", "family": "Malgun Gothic"},
        dragmode="pan",
        hovermode="closest",
        showlegend=False,
        bargap=0.0,
        hoverlabel={"bgcolor": "#1e222d", "font_color": "#d1d4dc"},
        annotations=[
            {
                "x": 0.01,
                "y": 1.04,
                "xref": "paper",
                "yref": "paper",
                "text": title_text,
                "showarrow": False,
                "font": {"size": 14, "color": "#e5e7eb", "family": "Malgun Gothic"},
                "align": "left",
            },
            *(
                [
                    {
                        "x": 0.01,
                        "y": 0.205,
                        "xref": "paper",
                        "yref": "paper",
                        "text": "보조지표 (흰색 점선: 레버리지 / 주황 점선: 인버스)",
                        "showarrow": False,
                        "font": {"size": 12, "color": "#9aa4b2", "family": "Malgun Gothic"},
                        "align": "left",
                    }
                ]
                if include_scr_panel
                else []
            ),
        ],
    )

    _apply_common_xaxis(figure, tick_values, tick_labels, len(frame) - 1)

    figure.update_yaxes(
        side="right",
        showgrid=True,
        gridcolor="rgba(42, 46, 57, 0.65)",
        showline=True,
        linecolor="#2a2e39",
        tickfont={"size": 12, "color": "#cbd5e1"},
        tickformat=",.0f",
        ticklabelposition="outside",
        ticks="outside",
        ticklen=6,
        tickcolor="#2a2e39",
        automargin=True,
        zeroline=False,
        fixedrange=False,
        autorange=True,
        row=1,
        col=1,
    )

    if include_scr_panel:
        figure.update_yaxes(
            title_text="보조지표",
            side="right",
            range=[-1.6, 1.6],
            tickmode="array",
            tickvals=[-1, 0, 1],
            ticktext=["하단", "0", "상단"],
            showgrid=True,
            gridcolor="rgba(42, 46, 57, 0.35)",
            showline=True,
            linecolor="#2a2e39",
            tickfont={"size": 10, "color": "#9aa4b2"},
            zeroline=False,
            fixedrange=False,
            row=2,
            col=1,
        )
        figure.add_hline(y=0, line_width=1, line_dash="dot", line_color="#2a2e39", row=2, col=1)
        figure.update_xaxes(showticklabels=False, row=1, col=1)
        figure.update_xaxes(showticklabels=True, row=2, col=1)
    else:
        figure.update_xaxes(showticklabels=True, row=1, col=1)

    return figure
