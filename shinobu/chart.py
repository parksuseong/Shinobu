from __future__ import annotations

import math

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


BUY_OPEN_COLOR = "#3b82f6"
BUY_CLOSE_COLOR = "#ef4444"
SCR_LINE_COLOR = "#e5e7eb"
PAIR_SCR_COLOR = "#f59e0b"

CANDLE_INC_WICK_INDEX = 0
CANDLE_INC_BODY_INDEX = 1
CANDLE_DEC_WICK_INDEX = 2
CANDLE_DEC_BODY_INDEX = 3
PRIMARY_OPEN_MAIN_INDEX = 4
PRIMARY_CLOSE_MAIN_INDEX = 5
PAIR_OPEN_MAIN_INDEX = 6
PAIR_CLOSE_MAIN_INDEX = 7
PRIMARY_OPEN_INDICATOR_INDEX = 8
PRIMARY_CLOSE_INDICATOR_INDEX = 9
PAIR_OPEN_INDICATOR_INDEX = 10
PAIR_CLOSE_INDICATOR_INDEX = 11
PRIMARY_SCR_INDEX = 12
PAIR_SCR_INDEX = 13


def _asset_marker_symbol(symbol_code: str) -> str:
    if symbol_code == "252670.KS":
        return "star"
    return "circle"


def _indicator_short_name(symbol_code: str | None, fallback: str) -> str:
    if symbol_code == "122630.KS":
        return "레버리지"
    if symbol_code == "252670.KS":
        return "곱버스"
    return fallback


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


def _empty_scatter(name: str, color: str, marker_symbol: str = "circle") -> go.Scatter:
    return go.Scatter(
        x=[],
        y=[],
        mode="markers+text",
        marker={"symbol": marker_symbol, "size": 11, "color": color, "line": {"width": 1, "color": "#ffffff"}},
        text=[],
        textposition="top center",
        textfont={"size": 10, "color": color},
        hovertext=[],
        hovertemplate="%{hovertext}<extra></extra>",
        name=name,
    )


def _empty_line(name: str, color: str, width: float = 1.4, dash: str = "dot") -> go.Scatter:
    return go.Scatter(
        x=[],
        y=[],
        mode="lines",
        line={"color": color, "width": width, "dash": dash},
        hovertemplate=name + " %{y:.2f}<extra></extra>",
        name=name,
    )


def _empty_bar(name: str, color: str) -> go.Bar:
    return go.Bar(
        x=[],
        y=[],
        base=[],
        width=0.98,
        marker={"color": color, "line": {"color": color, "width": 1}},
        customdata=[],
        hovertemplate="%{customdata}<extra></extra>",
        name=name,
    )


def _empty_wick(name: str, color: str) -> go.Scatter:
    return go.Scatter(
        x=[],
        y=[],
        mode="lines",
        line={"color": color, "width": 1},
        hoverinfo="skip",
        name=name,
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


def _build_signal_payload(
    base_frame: pd.DataFrame,
    signal_frame: pd.DataFrame | None,
    asset_name: str,
    asset_symbol: str,
    signal_column: str,
    row_mode: str,
) -> dict[str, list]:
    if signal_frame is None or signal_frame.empty or signal_column not in signal_frame.columns:
        return {"x": [], "y": [], "text": [], "hovertext": []}

    base_positions = pd.Series(range(len(base_frame)), index=base_frame.index)
    if row_mode == "main_open":
        rows, y_values = _signal_rows_with_base_y(signal_frame, base_frame, signal_column, "Low", 0.985)
    elif row_mode == "main_close":
        rows, y_values = _signal_rows_with_base_y(signal_frame, base_frame, signal_column, "High", 1.015)
    else:
        rows = signal_frame[signal_frame[signal_column]].copy()
        if rows.empty or "scr_line" not in rows.columns:
            return {"x": [], "y": [], "text": [], "hovertext": []}
        y_values = rows["scr_line"]

    if rows.empty:
        return {"x": [], "y": [], "text": [], "hovertext": []}

    action_label = "buy open" if signal_column == "buy_open" else "buy close"
    label = f"{action_label} · {asset_name}"
    return {
        "x": base_positions.reindex(rows.index).tolist(),
        "y": y_values.tolist() if hasattr(y_values, "tolist") else list(y_values),
        "text": [label] * len(rows),
        "hovertext": _hover_text(rows, label),
        "marker_symbol": _asset_marker_symbol(asset_symbol),
        "name": label,
    }


def _build_candle_payload(frame: pd.DataFrame, x_values: list[int], increasing: bool) -> dict[str, list]:
    mask = frame["Close"] >= frame["Open"] if increasing else frame["Close"] < frame["Open"]
    sliced = frame.loc[mask]
    selected_x = [x_values[index] for index, flag in enumerate(mask.tolist()) if flag]
    hover_text = _candle_hover_text(frame)

    wick_x: list[float] = []
    wick_y: list[float] = []
    for x_value, (_, row) in zip(selected_x, sliced.iterrows(), strict=False):
        wick_x.extend([x_value, x_value, None])
        wick_y.extend([row["Low"], row["High"], None])

    return {
        "wick_x": wick_x,
        "wick_y": wick_y,
        "body_x": selected_x,
        "body_y": (sliced["Close"] - sliced["Open"]).tolist(),
        "body_base": sliced["Open"].tolist(),
        "hovertext": [hover_text[index] for index, flag in enumerate(mask.tolist()) if flag],
    }


def _set_scatter_trace(
    trace: go.Scatter,
    *,
    x: list,
    y: list,
    text: list | None = None,
    hovertext: list | None = None,
    name: str | None = None,
    marker_symbol: str | None = None,
) -> None:
    trace.x = x
    trace.y = y
    trace.text = text or []
    trace.hovertext = hovertext or []
    if name is not None:
        trace.name = name
        if trace.hovertemplate == "%{hovertext}<extra></extra>":
            pass
    if marker_symbol is not None:
        trace.marker.symbol = marker_symbol
    trace.visible = True if x else "legendonly"


def _set_bar_trace(trace: go.Bar, *, x: list, y: list, base: list, hovertext: list) -> None:
    trace.x = x
    trace.y = y
    trace.base = base
    trace.customdata = hovertext
    trace.visible = True if x else "legendonly"


def _set_line_trace(trace: go.Scatter, *, x: list, y: list, name: str) -> None:
    trace.x = x
    trace.y = y
    trace.name = name
    trace.hovertemplate = name + " %{y:.2f}<extra></extra>"
    trace.visible = True if x else "legendonly"


def _create_figure_shell(include_scr_panel: bool) -> go.Figure:
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

    figure.add_trace(_empty_wick("상승 꼬리", "#089981"), row=1, col=1)
    figure.add_trace(_empty_bar("상승 봉", "#089981"), row=1, col=1)
    figure.add_trace(_empty_wick("하락 꼬리", "#f23645"), row=1, col=1)
    figure.add_trace(_empty_bar("하락 봉", "#f23645"), row=1, col=1)

    figure.add_trace(_empty_scatter("buy open · 레버리지", BUY_OPEN_COLOR), row=1, col=1)
    figure.add_trace(_empty_scatter("buy close · 레버리지", BUY_CLOSE_COLOR), row=1, col=1)
    figure.add_trace(_empty_scatter("buy open · 곱버스", BUY_OPEN_COLOR, "star"), row=1, col=1)
    figure.add_trace(_empty_scatter("buy close · 곱버스", BUY_CLOSE_COLOR, "star"), row=1, col=1)

    if include_scr_panel:
        figure.add_trace(_empty_scatter("buy open · 레버리지", BUY_OPEN_COLOR), row=2, col=1)
        figure.add_trace(_empty_scatter("buy close · 레버리지", BUY_CLOSE_COLOR), row=2, col=1)
        figure.add_trace(_empty_scatter("buy open · 곱버스", BUY_OPEN_COLOR, "star"), row=2, col=1)
        figure.add_trace(_empty_scatter("buy close · 곱버스", BUY_CLOSE_COLOR, "star"), row=2, col=1)
        figure.add_trace(_empty_line("레버리지 SCR", SCR_LINE_COLOR, 1.7), row=2, col=1)
        figure.add_trace(_empty_line("곱버스 SCR", PAIR_SCR_COLOR, 1.4), row=2, col=1)

    return figure


def update_candlestick_chart(
    figure: go.Figure,
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

    inc_payload = _build_candle_payload(frame, x_values, increasing=True)
    dec_payload = _build_candle_payload(frame, x_values, increasing=False)
    _set_scatter_trace(figure.data[CANDLE_INC_WICK_INDEX], x=inc_payload["wick_x"], y=inc_payload["wick_y"])
    _set_bar_trace(
        figure.data[CANDLE_INC_BODY_INDEX],
        x=inc_payload["body_x"],
        y=inc_payload["body_y"],
        base=inc_payload["body_base"],
        hovertext=inc_payload["hovertext"],
    )
    _set_scatter_trace(figure.data[CANDLE_DEC_WICK_INDEX], x=dec_payload["wick_x"], y=dec_payload["wick_y"])
    _set_bar_trace(
        figure.data[CANDLE_DEC_BODY_INDEX],
        x=dec_payload["body_x"],
        y=dec_payload["body_y"],
        base=dec_payload["body_base"],
        hovertext=dec_payload["hovertext"],
    )

    primary_open = _build_signal_payload(frame, frame, symbol_name, symbol_code, "buy_open", "main_open")
    primary_close = _build_signal_payload(frame, frame, symbol_name, symbol_code, "buy_close", "main_close")
    pair_open = _build_signal_payload(frame, pair_frame, pair_name or "곱버스", pair_symbol_code or "", "buy_open", "main_open")
    pair_close = _build_signal_payload(frame, pair_frame, pair_name or "곱버스", pair_symbol_code or "", "buy_close", "main_close")

    _set_scatter_trace(figure.data[PRIMARY_OPEN_MAIN_INDEX], **primary_open)
    _set_scatter_trace(figure.data[PRIMARY_CLOSE_MAIN_INDEX], **primary_close)
    _set_scatter_trace(figure.data[PAIR_OPEN_MAIN_INDEX], **pair_open)
    _set_scatter_trace(figure.data[PAIR_CLOSE_MAIN_INDEX], **pair_close)

    title_text = f"{symbol_name} · {timeframe_label}"
    if include_scr_panel:
        title_text += " · 실전"

    annotations = [
        {
            "x": 0.01,
            "y": 1.04,
            "xref": "paper",
            "yref": "paper",
            "text": title_text,
            "showarrow": False,
            "font": {"size": 14, "color": "#e5e7eb", "family": "Malgun Gothic"},
            "align": "left",
        }
    ]

    if include_scr_panel:
        primary_indicator_open = _build_signal_payload(frame, frame, symbol_name, symbol_code, "buy_open", "indicator")
        primary_indicator_close = _build_signal_payload(frame, frame, symbol_name, symbol_code, "buy_close", "indicator")
        pair_indicator_open = _build_signal_payload(
            frame,
            pair_frame,
            pair_name or "곱버스",
            pair_symbol_code or "",
            "buy_open",
            "indicator",
        )
        pair_indicator_close = _build_signal_payload(
            frame,
            pair_frame,
            pair_name or "곱버스",
            pair_symbol_code or "",
            "buy_close",
            "indicator",
        )

        _set_scatter_trace(figure.data[PRIMARY_OPEN_INDICATOR_INDEX], **primary_indicator_open)
        _set_scatter_trace(figure.data[PRIMARY_CLOSE_INDICATOR_INDEX], **primary_indicator_close)
        _set_scatter_trace(figure.data[PAIR_OPEN_INDICATOR_INDEX], **pair_indicator_open)
        _set_scatter_trace(figure.data[PAIR_CLOSE_INDICATOR_INDEX], **pair_indicator_close)

        _set_line_trace(figure.data[PRIMARY_SCR_INDEX], x=x_values, y=frame["scr_line"].tolist(), name=f"{symbol_name} SCR")
        if pair_frame is not None and "scr_line" in pair_frame.columns:
            pair_scr = pair_frame.reindex(frame.index).ffill()
            pair_scr_name = f"{_indicator_short_name(pair_symbol_code, pair_name or '곱버스')} SCR"
            _set_line_trace(figure.data[PAIR_SCR_INDEX], x=x_values, y=pair_scr["scr_line"].tolist(), name=pair_scr_name)
        else:
            _set_line_trace(figure.data[PAIR_SCR_INDEX], x=[], y=[], name="곱버스 SCR")

        annotations.append(
            {
                "x": 0.01,
                "y": 0.205,
                "xref": "paper",
                "yref": "paper",
                "text": "보조지표 (흰색 점선: 레버리지 / 주황 점선: 곱버스)",
                "showarrow": False,
                "font": {"size": 12, "color": "#9aa4b2", "family": "Malgun Gothic"},
                "align": "left",
            }
        )

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
        annotations=annotations,
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
        figure.update_xaxes(showticklabels=False, row=1, col=1)
        figure.update_xaxes(showticklabels=True, row=2, col=1)
    else:
        figure.update_xaxes(showticklabels=True, row=1, col=1)

    return figure


def build_candlestick_chart(
    frame: pd.DataFrame,
    timeframe_label: str,
    symbol_name: str,
    symbol_code: str,
    pair_frame: pd.DataFrame | None = None,
    pair_name: str | None = None,
    pair_symbol_code: str | None = None,
) -> go.Figure:
    figure = _create_figure_shell("scr_line" in frame.columns)
    return update_candlestick_chart(
        figure,
        frame,
        timeframe_label,
        symbol_name,
        symbol_code,
        pair_frame=pair_frame,
        pair_name=pair_name,
        pair_symbol_code=pair_symbol_code,
    )
