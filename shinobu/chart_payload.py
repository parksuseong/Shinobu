from __future__ import annotations

from typing import Any

import pandas as pd

from shinobu import data as market_data
from shinobu.live_trading import get_live_orders, get_live_started_at
from shinobu.strategy import StrategyAdjustments, calculate_scr_strategy


LIVE_TIMEFRAME = "5분봉"
MAX_LIVE_CHART_CANDLES = 100


def filter_frame_from_live_start(frame: pd.DataFrame) -> pd.DataFrame:
    started_at = get_live_started_at()
    if started_at is None:
        return frame.iloc[0:0].copy()

    before = frame.loc[frame.index < started_at].tail(MAX_LIVE_CHART_CANDLES)
    after = frame.loc[frame.index >= started_at]
    combined = pd.concat([before, after]).sort_index()
    if combined.empty and not frame.empty:
        return frame.tail(MAX_LIVE_CHART_CANDLES).copy()
    return combined.tail(MAX_LIVE_CHART_CANDLES).copy()


def _load_raw_frame(symbol: str, started_at: pd.Timestamp | None) -> pd.DataFrame:
    frame = market_data.load_live_chart_data(symbol, LIVE_TIMEFRAME)
    if started_at is None:
        return frame.tail(MAX_LIVE_CHART_CANDLES).copy()
    return filter_frame_from_live_start(frame)


def _load_strategy_frame(symbol: str, started_at: pd.Timestamp | None, adjustments: StrategyAdjustments, profile_name: str) -> pd.DataFrame:
    frame = market_data.load_live_chart_data(symbol, LIVE_TIMEFRAME)
    frame = calculate_scr_strategy(frame, adjustments, LIVE_TIMEFRAME, profile_name=profile_name)
    if started_at is None:
        return frame.tail(MAX_LIVE_CHART_CANDLES).copy()
    return filter_frame_from_live_start(frame)


def _pair_scr(frame: pd.DataFrame, pair_frame: pd.DataFrame | None) -> list[float | None]:
    if pair_frame is None or "scr_line" not in pair_frame.columns:
        return []
    aligned = pair_frame.reindex(frame.index).ffill()
    return [None if pd.isna(value) else float(value) for value in aligned["scr_line"].tolist()]


def _build_signal_markers(
    frame: pd.DataFrame,
    signal_frame: pd.DataFrame | None,
    label: str,
    signal_column: str,
    y_column: str,
    multiplier: float = 1.0,
) -> list[dict[str, Any]]:
    if signal_frame is None or signal_frame.empty or signal_column not in signal_frame.columns:
        return []

    rows = signal_frame[signal_frame[signal_column]].copy()
    if rows.empty:
        return []

    base_positions = pd.Series(range(len(frame)), index=frame.index)
    aligned = frame.reindex(rows.index)
    if y_column == "scr_line":
        y_values = rows["scr_line"]
    else:
        y_values = aligned[y_column] * multiplier

    result: list[dict[str, Any]] = []
    for timestamp, y_value in zip(rows.index, y_values, strict=False):
        x_value = base_positions.get(timestamp)
        if pd.isna(x_value) or pd.isna(y_value):
            continue
        row = rows.loc[timestamp]
        result.append(
            {
                "x": int(x_value),
                "y": float(y_value),
                "label": label,
                "time": pd.Timestamp(timestamp).strftime("%Y-%m-%d %H:%M"),
                "price": float(row.get("Close", 0) or 0),
                "scr": float(row.get("scr_line", 0) or 0),
                "signal": signal_column,
            }
        )
    return result


def _build_order_markers(frame: pd.DataFrame, symbols: list[str]) -> list[dict[str, Any]]:
    orders = get_live_orders()
    if not orders or frame.empty:
        return []

    order_frame = pd.DataFrame(orders)
    order_frame["candle_time"] = pd.to_datetime(order_frame["candle_time"])
    order_frame = order_frame[order_frame["symbol"].isin(symbols)]
    if order_frame.empty:
        return []

    aligned = frame.reindex(order_frame["candle_time"]).ffill()
    positions = pd.Series(range(len(frame)), index=frame.index)
    markers: list[dict[str, Any]] = []
    for (_, order), (_, candle) in zip(order_frame.iterrows(), aligned.iterrows(), strict=False):
        x_value = positions.get(order["candle_time"])
        if pd.isna(x_value):
            continue

        side = str(order.get("side", ""))
        y_value = float(candle["Low"]) * 0.985 if side == "buy" else float(candle["High"]) * 1.015
        label = f"실매수 - {market_data.display_name(order['symbol'])}" if side == "buy" else f"실매도 - {market_data.display_name(order['symbol'])}"
        markers.append(
            {
                "x": int(x_value),
                "y": y_value,
                "label": label,
                "side": side,
                "time": pd.Timestamp(order["candle_time"]).strftime("%Y-%m-%d %H:%M"),
                "price": float(order.get("price", 0) or 0),
                "reason": str(order.get("reason", "")),
            }
        )
    return markers


def _append_main_marker(
    bucket: list[dict[str, Any]],
    positions: pd.Series,
    timestamp: pd.Timestamp,
    price_row: pd.Series,
    label: str,
    marker_side: str,
) -> None:
    x_value = positions.get(timestamp)
    if pd.isna(x_value):
        return
    y_value = float(price_row["Low"]) * 0.985 if marker_side == "open" else float(price_row["High"]) * 1.015
    bucket.append(
        {
            "x": int(x_value),
            "y": y_value,
            "label": label,
            "time": pd.Timestamp(timestamp).strftime("%Y-%m-%d %H:%M"),
            "price": float(price_row.get("Close", 0) or 0),
        }
    )


def _append_indicator_marker(
    bucket: list[dict[str, Any]],
    positions: pd.Series,
    timestamp: pd.Timestamp,
    signal_row: pd.Series,
    label: str,
    signal_name: str,
) -> None:
    x_value = positions.get(timestamp)
    y_value = signal_row.get("scr_line")
    if pd.isna(x_value) or pd.isna(y_value):
        return
    bucket.append(
        {
            "x": int(x_value),
            "y": float(y_value),
            "label": label,
            "time": pd.Timestamp(timestamp).strftime("%Y-%m-%d %H:%M"),
            "price": float(signal_row.get("Close", 0) or 0),
            "scr": float(signal_row.get("scr_line", 0) or 0),
            "signal": signal_name,
        }
    )


def _build_position_signal_markers(frame: pd.DataFrame, symbol: str, pair_symbol: str | None, pair_frame: pd.DataFrame | None) -> dict[str, list[dict[str, Any]]]:
    empty = {
        "primaryOpenMain": [],
        "primaryCloseMain": [],
        "pairOpenMain": [],
        "pairCloseMain": [],
        "primaryOpenIndicator": [],
        "primaryCloseIndicator": [],
        "pairOpenIndicator": [],
        "pairCloseIndicator": [],
    }
    if frame.empty:
        return empty

    primary_name = market_data.display_name(symbol)
    pair_name = market_data.display_name(pair_symbol) if pair_symbol else "곱버스"
    positions = pd.Series(range(len(frame)), index=frame.index)
    aligned_pair = pair_frame.reindex(frame.index).ffill() if pair_frame is not None and not pair_frame.empty else None

    current_position: str | None = None

    for timestamp, primary_row in frame.iterrows():
        pair_row = aligned_pair.loc[timestamp] if aligned_pair is not None else None
        primary_open = bool(primary_row.get("buy_open", False))
        primary_close = bool(primary_row.get("buy_close", False))
        pair_open = bool(pair_row.get("buy_open", False)) if pair_row is not None else False
        pair_close = bool(pair_row.get("buy_close", False)) if pair_row is not None else False

        if current_position is None:
            if primary_open and pair_open and pair_row is not None:
                current_position = symbol if float(primary_row.get("scr_line", 0.0)) >= float(pair_row.get("scr_line", 0.0)) else (pair_symbol or symbol)
            elif primary_open:
                current_position = symbol
            elif pair_open:
                current_position = pair_symbol

            if current_position == symbol:
                label = f"전략 open - {primary_name}"
                _append_main_marker(empty["primaryOpenMain"], positions, timestamp, primary_row, label, "open")
                _append_indicator_marker(empty["primaryOpenIndicator"], positions, timestamp, primary_row, label, "buy_open")
            elif current_position == pair_symbol and pair_row is not None:
                label = f"전략 open - {pair_name}"
                _append_main_marker(empty["pairOpenMain"], positions, timestamp, primary_row, label, "open")
                _append_indicator_marker(empty["pairOpenIndicator"], positions, timestamp, pair_row, label, "buy_open")
            continue

        if current_position == symbol:
            if pair_open and pair_row is not None:
                close_label = f"전략 close - {primary_name}"
                open_label = f"전략 open - {pair_name}"
                _append_main_marker(empty["primaryCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["primaryCloseIndicator"], positions, timestamp, primary_row, close_label, "buy_close")
                _append_main_marker(empty["pairOpenMain"], positions, timestamp, primary_row, open_label, "open")
                _append_indicator_marker(empty["pairOpenIndicator"], positions, timestamp, pair_row, open_label, "buy_open")
                current_position = pair_symbol
            elif primary_close:
                close_label = f"전략 close - {primary_name}"
                _append_main_marker(empty["primaryCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["primaryCloseIndicator"], positions, timestamp, primary_row, close_label, "buy_close")
                current_position = None
            continue

        if current_position == pair_symbol and pair_row is not None:
            if primary_open:
                close_label = f"전략 close - {pair_name}"
                open_label = f"전략 open - {primary_name}"
                _append_main_marker(empty["pairCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["pairCloseIndicator"], positions, timestamp, pair_row, close_label, "buy_close")
                _append_main_marker(empty["primaryOpenMain"], positions, timestamp, primary_row, open_label, "open")
                _append_indicator_marker(empty["primaryOpenIndicator"], positions, timestamp, primary_row, open_label, "buy_open")
                current_position = symbol
            elif pair_close:
                close_label = f"전략 close - {pair_name}"
                _append_main_marker(empty["pairCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["pairCloseIndicator"], positions, timestamp, pair_row, close_label, "buy_close")
                current_position = None

    return empty


def build_chart_payload(
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments | None = None,
    profile_name: str = "original",
) -> dict[str, Any]:
    current_adjustments = adjustments or StrategyAdjustments()
    started_at = get_live_started_at()
    pair_name = market_data.display_name(pair_symbol) if pair_symbol else None

    if kind == "overlay":
        frame = _load_strategy_frame(symbol, started_at, current_adjustments, profile_name)
        pair_frame = _load_strategy_frame(pair_symbol, started_at, current_adjustments, profile_name) if pair_symbol else None
        include_scr = True
    else:
        frame = _load_raw_frame(symbol, started_at)
        pair_frame = _load_raw_frame(pair_symbol, started_at) if pair_symbol else None
        include_scr = False

    candles = [
        {
            "t": index.isoformat(),
            "o": float(row["Open"]),
            "h": float(row["High"]),
            "l": float(row["Low"]),
            "c": float(row["Close"]),
        }
        for index, row in frame.iterrows()
    ]
    tick_text = [index.strftime("%m-%d %H:%M") for index in frame.index]

    payload: dict[str, Any] = {
        "kind": kind,
        "symbol": symbol,
        "symbolName": market_data.display_name(symbol),
        "pairSymbol": pair_symbol,
        "pairName": pair_name,
        "includeScr": include_scr,
        "candles": candles,
        "tickText": tick_text,
        "orders": _build_order_markers(frame, [value for value in [symbol, pair_symbol] if value is not None]),
    }

    if include_scr:
        payload["scr"] = [None if pd.isna(value) else float(value) for value in frame["scr_line"].tolist()]
        payload["pairScr"] = _pair_scr(frame, pair_frame)
        payload["signals"] = _build_position_signal_markers(frame, symbol, pair_symbol, pair_frame)
    else:
        payload["signals"] = {}

    return payload
