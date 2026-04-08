from __future__ import annotations

from typing import Any

import pandas as pd

from shinobu import data as market_data
from shinobu.live_trading import get_live_orders, get_live_started_at
from shinobu.strategy import StrategyAdjustments, calculate_scr_strategy


LIVE_TIMEFRAME = "5분봉"
MAX_LIVE_CHART_CANDLES = 50


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


def _load_strategy_frame(symbol: str, started_at: pd.Timestamp | None, adjustments: StrategyAdjustments) -> pd.DataFrame:
    frame = market_data.load_live_chart_data(symbol, LIVE_TIMEFRAME)
    frame = calculate_scr_strategy(frame, adjustments, LIVE_TIMEFRAME)
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


def build_chart_payload(
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments | None = None,
) -> dict[str, Any]:
    current_adjustments = adjustments or StrategyAdjustments()
    started_at = get_live_started_at()
    pair_name = market_data.display_name(pair_symbol) if pair_symbol else None

    if kind == "overlay":
        frame = _load_strategy_frame(symbol, started_at, current_adjustments)
        pair_frame = _load_strategy_frame(pair_symbol, started_at, current_adjustments) if pair_symbol else None
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
        payload["signals"] = {
            "primaryOpenMain": _build_signal_markers(frame, frame, f"전략 open - {market_data.display_name(symbol)}", "buy_open", "Low", 0.985),
            "primaryCloseMain": _build_signal_markers(frame, frame, f"전략 close - {market_data.display_name(symbol)}", "buy_close", "High", 1.015),
            "pairOpenMain": _build_signal_markers(frame, pair_frame, f"전략 open - {pair_name or '곱버스'}", "buy_open", "Low", 0.985),
            "pairCloseMain": _build_signal_markers(frame, pair_frame, f"전략 close - {pair_name or '곱버스'}", "buy_close", "High", 1.015),
            "primaryOpenIndicator": _build_signal_markers(frame, frame, f"전략 open - {market_data.display_name(symbol)}", "buy_open", "scr_line"),
            "primaryCloseIndicator": _build_signal_markers(frame, frame, f"전략 close - {market_data.display_name(symbol)}", "buy_close", "scr_line"),
            "pairOpenIndicator": _build_signal_markers(frame, pair_frame, f"전략 open - {pair_name or '곱버스'}", "buy_open", "scr_line"),
            "pairCloseIndicator": _build_signal_markers(frame, pair_frame, f"전략 close - {pair_name or '곱버스'}", "buy_close", "scr_line"),
        }
    else:
        payload["signals"] = {}

    return payload
