from __future__ import annotations

from pathlib import Path
import threading
import time
from typing import Any

import pandas as pd

from shinobu import data as market_data
from shinobu.live_trading import (
    SIGNAL_TO_TRADE_SYMBOL,
    TRADE_TO_SIGNAL_SYMBOL,
    get_live_orders,
    get_live_started_at,
)
from shinobu.strategy import StrategyAdjustments, calculate_strategy
from shinobu.strategy_cache import calculate_strategy_cached


LIVE_TIMEFRAME = "5분봉"
MAX_LIVE_CHART_CANDLES = 1200
MAX_LIVE_CHART_BUSINESS_DAYS = 5
CHART_KST = market_data.KST
PAYLOAD_CACHE_DIR = Path(__file__).resolve().parent.parent / ".streamlit" / "payload_cache"
_PREWARM_LOCK = threading.Lock()
_PREWARM_STARTED_KEYS: set[str] = set()
_PREWARM_BUNDLE_KEYS: set[str] = set()


def filter_frame_from_live_start(frame: pd.DataFrame) -> pd.DataFrame:
    started_at = get_live_started_at()
    if started_at is None:
        return frame.iloc[0:0].copy()

    before = frame.loc[frame.index < started_at]
    after = frame.loc[frame.index >= started_at]
    combined = pd.concat([before, after]).sort_index()
    if combined.empty and not frame.empty:
        return limit_frame_to_recent_business_days(frame)
    return limit_frame_to_recent_business_days(combined)


def limit_frame_to_recent_business_days(frame: pd.DataFrame, max_days: int | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame
    current_max_days = int(max_days or MAX_LIVE_CHART_BUSINESS_DAYS)
    trade_days = pd.Index(pd.to_datetime(frame.index).normalize().unique()).sort_values()
    recent_days = trade_days[-current_max_days:]
    limited = frame.loc[frame.index.normalize().isin(recent_days)].copy()
    return limited.tail(MAX_LIVE_CHART_CANDLES).copy()


def _frame_position_map(frame: pd.DataFrame) -> pd.Series:
    return pd.Series(range(len(frame)), index=frame.index)


def _visible_index_set(frame: pd.DataFrame) -> set[pd.Timestamp]:
    return set(pd.DatetimeIndex(frame.index).tolist())


def _filter_markers_to_visible_range(
    markers: list[dict[str, Any]],
    visible_positions: pd.Series,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for marker in markers:
        try:
            timestamp = pd.Timestamp(marker.get("time"))
        except Exception:
            continue
        x_value = visible_positions.get(timestamp)
        if pd.isna(x_value):
            continue
        item = dict(marker)
        item["x"] = int(x_value)
        filtered.append(item)
    return filtered


def _filter_signal_bucket_map(
    signal_map: dict[str, list[dict[str, Any]]],
    visible_frame: pd.DataFrame,
) -> dict[str, list[dict[str, Any]]]:
    visible_positions = _frame_position_map(visible_frame)
    return {
        key: _filter_markers_to_visible_range(value, visible_positions)
        for key, value in signal_map.items()
    }


def _current_candle_status_from_timestamp(candle_start: pd.Timestamp | None) -> dict[str, Any]:
    if candle_start is None or pd.isna(candle_start):
        return {
            "isUnconfirmed": False,
            "candleTime": "",
            "remainingSeconds": 0,
            "remainingText": "",
            "progressPct": 100.0,
            "statusText": "봉 정보 없음",
        }

    now = pd.Timestamp.now(tz=CHART_KST).tz_localize(None)
    candle_start = pd.Timestamp(candle_start)
    candle_end = candle_start + pd.Timedelta(minutes=5)
    total_seconds = 300
    elapsed_seconds = max(0, min(int((now - candle_start).total_seconds()), total_seconds))
    remaining_seconds = max(0, int((candle_end - now).total_seconds()))
    is_unconfirmed = candle_start <= now < candle_end
    progress_pct = max(0.0, min((elapsed_seconds / total_seconds) * 100.0, 100.0))
    remaining_text = f"{remaining_seconds // 60:02d}:{remaining_seconds % 60:02d}"
    status_text = f"현재 봉 업데이트 남은 시간 {remaining_text}" if is_unconfirmed else "현재 봉 확정"
    return {
        "isUnconfirmed": is_unconfirmed,
        "candleTime": candle_start.strftime("%Y-%m-%d %H:%M"),
        "remainingSeconds": remaining_seconds,
        "remainingText": remaining_text,
        "progressPct": progress_pct,
        "statusText": status_text,
    }


def _current_candle_status(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return _current_candle_status_from_timestamp(None)
    return _current_candle_status_from_timestamp(pd.Timestamp(frame.index.max()))


def _payload_cache_path(cache_key: str) -> Path:
    PAYLOAD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    safe_key = "".join(char if char.isalnum() or char in "._-" else "_" for char in cache_key)
    return PAYLOAD_CACHE_DIR / f"{safe_key}.pkl"


def _build_payload_cache_key(
    *,
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments,
    strategy_name: str,
    visible_business_days: int,
) -> str:
    started_at = get_live_started_at()
    started_at_text = started_at.isoformat() if started_at is not None else ""
    return "|".join(
        [
            kind,
            symbol,
            pair_symbol or "",
            strategy_name,
            str(int(visible_business_days)),
            f"s{adjustments.stoch_pct}_c{adjustments.cci_pct}_r{adjustments.rsi_pct}",
            started_at_text,
        ]
    )


def _read_cached_payload(cache_key: str) -> dict[str, Any] | None:
    cache_path = _payload_cache_path(cache_key)
    if not cache_path.exists():
        return None
    try:
        payload = pd.read_pickle(cache_path)
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    candles = payload.get("candles") or []
    if candles:
        try:
            payload["currentCandle"] = _current_candle_status_from_timestamp(pd.Timestamp(candles[-1]["t"]))
        except Exception:
            pass
    return payload


def _write_cached_payload(cache_key: str, payload: dict[str, Any]) -> None:
    pd.to_pickle(payload, _payload_cache_path(cache_key))


def _merge_series_payload(
    cached_values: list[Any] | None,
    current_values: list[Any],
    *,
    keep_tail_overlap: int = 3,
) -> list[Any]:
    if not cached_values:
        return list(current_values)
    if not current_values:
        return []
    if len(cached_values) > len(current_values):
        return list(current_values)

    prefix_length = max(0, min(len(cached_values), len(current_values)) - keep_tail_overlap)
    if prefix_length == 0:
        return list(current_values)
    if list(cached_values[:prefix_length]) != list(current_values[:prefix_length]):
        return list(current_values)
    return list(cached_values[:prefix_length]) + list(current_values[prefix_length:])


def _merge_payload_arrays(
    cached_payload: dict[str, Any] | None,
    *,
    candles: list[dict[str, Any]],
    tick_text: list[str],
    scr_values: list[float | None] | None = None,
    pair_scr_values: list[float | None] | None = None,
) -> tuple[list[dict[str, Any]], list[str], list[float | None] | None, list[float | None] | None]:
    if not cached_payload:
        return candles, tick_text, scr_values, pair_scr_values

    merged_candles = _merge_series_payload(cached_payload.get("candles"), candles)
    merged_tick_text = _merge_series_payload(cached_payload.get("tickText"), tick_text)
    merged_scr = None if scr_values is None else _merge_series_payload(cached_payload.get("scr"), scr_values)
    merged_pair_scr = None if pair_scr_values is None else _merge_series_payload(cached_payload.get("pairScr"), pair_scr_values)
    return merged_candles, merged_tick_text, merged_scr, merged_pair_scr


def _load_raw_frame(symbol: str, started_at: pd.Timestamp | None) -> pd.DataFrame:
    frame = market_data.load_live_chart_data_for_strategy(symbol, LIVE_TIMEFRAME, "src_v2_adx")
    if started_at is None:
        return limit_frame_to_recent_business_days(frame)
    return limit_frame_to_recent_business_days(filter_frame_from_live_start(frame))


def _load_strategy_frame(symbol: str, started_at: pd.Timestamp | None, adjustments: StrategyAdjustments, strategy_name: str) -> pd.DataFrame:
    frame = market_data.load_live_chart_data_for_strategy(symbol, LIVE_TIMEFRAME, strategy_name)
    frame = calculate_strategy_cached(
        frame,
        adjustments,
        LIVE_TIMEFRAME,
        strategy_name=strategy_name,
        symbol=symbol,
    )
    if started_at is None:
        return limit_frame_to_recent_business_days(frame)
    return limit_frame_to_recent_business_days(filter_frame_from_live_start(frame))


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
    candidate_symbols: set[str] = set()
    for symbol in symbols:
        if not symbol:
            continue
        candidate_symbols.add(symbol)
        candidate_symbols.add(SIGNAL_TO_TRADE_SYMBOL.get(symbol, symbol))
        candidate_symbols.add(TRADE_TO_SIGNAL_SYMBOL.get(symbol, symbol))
    order_frame = order_frame[order_frame["symbol"].isin(candidate_symbols)]
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


def _is_upper_main_marker(marker: dict[str, Any]) -> bool:
    side = str(marker.get("side", "") or "").lower()
    label = str(marker.get("label", "") or "").lower()
    signal = str(marker.get("signal", "") or "").lower()
    return side == "sell" or "close" in label or signal == "buy_close"


def _apply_main_marker_vertical_offsets(
    frame: pd.DataFrame,
    signal_map: dict[str, list[dict[str, Any]]],
    order_markers: list[dict[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    if frame.empty:
        return signal_map, order_markers

    frame_by_time = {timestamp.strftime("%Y-%m-%d %H:%M"): row for timestamp, row in frame.iterrows()}
    marker_refs: list[tuple[str, int, dict[str, Any]]] = []

    for key in ["primaryOpenMain", "primaryCloseMain", "pairOpenMain", "pairCloseMain"]:
        for index, marker in enumerate(signal_map.get(key, [])):
            marker_refs.append((key, index, marker))
    for index, marker in enumerate(order_markers):
        marker_refs.append(("orders", index, marker))

    grouped: dict[tuple[str, str], list[tuple[str, int, dict[str, Any]]]] = {}
    for ref in marker_refs:
        _, _, marker = ref
        time_key = str(marker.get("time", "") or "")
        region = "upper" if _is_upper_main_marker(marker) else "lower"
        grouped.setdefault((time_key, region), []).append(ref)

    for (time_key, region), markers in grouped.items():
        if len(markers) <= 1:
            continue
        candle_row = frame_by_time.get(time_key)
        if candle_row is None:
            continue
        high_price = float(candle_row.get("High", candle_row.get("Close", 0)) or 0)
        low_price = float(candle_row.get("Low", candle_row.get("Close", 0)) or 0)
        close_price = float(candle_row.get("Close", 0) or 0)
        candle_range = max(high_price - low_price, close_price * 0.0025, 1.0)
        step = candle_range * 0.42

        sorted_markers = sorted(markers, key=lambda item: (0 if item[0] == "orders" else 1, int(item[2].get("x", 0))))
        for offset_index, (bucket_name, marker_index, marker) in enumerate(sorted_markers):
            updated = dict(marker)
            if region == "upper":
                updated["y"] = high_price + candle_range * 0.15 + step * offset_index
            else:
                updated["y"] = low_price - candle_range * 0.15 - step * offset_index
            if bucket_name == "orders":
                order_markers[marker_index] = updated
            else:
                signal_map[bucket_name][marker_index] = updated

    return signal_map, order_markers


def _marker_label(prefix: str, instrument_name: str, signal_row: pd.Series) -> str:
    detail = str(signal_row.get("signal_detail", "") or "").strip()
    if detail:
        return f"{prefix} - {instrument_name} ({detail})"
    return f"{prefix} - {instrument_name}"


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
                label = _marker_label("전략 open", primary_name, primary_row)
                _append_main_marker(empty["primaryOpenMain"], positions, timestamp, primary_row, label, "open")
                _append_indicator_marker(empty["primaryOpenIndicator"], positions, timestamp, primary_row, label, "buy_open")
            elif current_position == pair_symbol and pair_row is not None:
                label = _marker_label("전략 open", pair_name, pair_row)
                _append_main_marker(empty["pairOpenMain"], positions, timestamp, primary_row, label, "open")
                _append_indicator_marker(empty["pairOpenIndicator"], positions, timestamp, pair_row, label, "buy_open")
            continue

        if current_position == symbol:
            if pair_open and pair_row is not None:
                close_label = _marker_label("전략 close", primary_name, primary_row)
                open_label = _marker_label("전략 open", pair_name, pair_row)
                _append_main_marker(empty["primaryCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["primaryCloseIndicator"], positions, timestamp, primary_row, close_label, "buy_close")
                _append_main_marker(empty["pairOpenMain"], positions, timestamp, primary_row, open_label, "open")
                _append_indicator_marker(empty["pairOpenIndicator"], positions, timestamp, pair_row, open_label, "buy_open")
                current_position = pair_symbol
            elif primary_close:
                close_label = _marker_label("전략 close", primary_name, primary_row)
                _append_main_marker(empty["primaryCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["primaryCloseIndicator"], positions, timestamp, primary_row, close_label, "buy_close")
                current_position = None
            continue

        if current_position == pair_symbol and pair_row is not None:
            if primary_open:
                close_label = _marker_label("전략 close", pair_name, pair_row)
                open_label = _marker_label("전략 open", primary_name, primary_row)
                _append_main_marker(empty["pairCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["pairCloseIndicator"], positions, timestamp, pair_row, close_label, "buy_close")
                _append_main_marker(empty["primaryOpenMain"], positions, timestamp, primary_row, open_label, "open")
                _append_indicator_marker(empty["primaryOpenIndicator"], positions, timestamp, primary_row, open_label, "buy_open")
                current_position = symbol
            elif pair_close:
                close_label = _marker_label("전략 close", pair_name, pair_row)
                _append_main_marker(empty["pairCloseMain"], positions, timestamp, primary_row, close_label, "close")
                _append_indicator_marker(empty["pairCloseIndicator"], positions, timestamp, pair_row, close_label, "buy_close")
                current_position = None

    return empty


def build_chart_payload(
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments | None = None,
    strategy_name: str = "src_v2_adx",
    visible_business_days: int = MAX_LIVE_CHART_BUSINESS_DAYS,
) -> dict[str, Any]:
    current_adjustments = adjustments or StrategyAdjustments()
    cache_key = _build_payload_cache_key(
        kind=kind,
        symbol=symbol,
        pair_symbol=pair_symbol,
        adjustments=current_adjustments,
        strategy_name=strategy_name,
        visible_business_days=visible_business_days,
    )
    cached_payload = _read_cached_payload(cache_key)

    started_at = get_live_started_at()
    pair_name = market_data.display_name(pair_symbol) if pair_symbol else None

    if kind == "overlay":
        full_frame = _load_strategy_frame(symbol, None, current_adjustments, strategy_name)
        full_pair_frame = _load_strategy_frame(pair_symbol, None, current_adjustments, strategy_name) if pair_symbol else None
        frame = limit_frame_to_recent_business_days(filter_frame_from_live_start(full_frame) if started_at is not None else full_frame, visible_business_days)
        pair_frame = (
            limit_frame_to_recent_business_days(filter_frame_from_live_start(full_pair_frame), visible_business_days)
            if started_at is not None and full_pair_frame is not None
            else limit_frame_to_recent_business_days(full_pair_frame, visible_business_days) if full_pair_frame is not None else None
        )
        include_scr = True
    else:
        full_frame = _load_raw_frame(symbol, None)
        full_pair_frame = _load_raw_frame(pair_symbol, None) if pair_symbol else None
        frame = limit_frame_to_recent_business_days(filter_frame_from_live_start(full_frame) if started_at is not None else full_frame, visible_business_days)
        pair_frame = (
            limit_frame_to_recent_business_days(filter_frame_from_live_start(full_pair_frame), visible_business_days)
            if started_at is not None and full_pair_frame is not None
            else limit_frame_to_recent_business_days(full_pair_frame, visible_business_days) if full_pair_frame is not None else None
        )
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
    visible_orders = _filter_markers_to_visible_range(
        _build_order_markers(full_frame, [value for value in [symbol, pair_symbol] if value is not None]),
        _frame_position_map(frame),
    )
    visible_signals = (
        _filter_signal_bucket_map(
            _build_position_signal_markers(full_frame, symbol, pair_symbol, full_pair_frame),
            frame,
        )
        if include_scr
        else {}
    )
    if include_scr:
        visible_signals, visible_orders = _apply_main_marker_vertical_offsets(frame, visible_signals, visible_orders)
    scr_values = [None if pd.isna(value) else float(value) for value in frame["scr_line"].tolist()] if include_scr else None
    pair_scr_values = _pair_scr(frame, pair_frame) if include_scr else None
    candles, tick_text, scr_values, pair_scr_values = _merge_payload_arrays(
        cached_payload,
        candles=candles,
        tick_text=tick_text,
        scr_values=scr_values,
        pair_scr_values=pair_scr_values,
    )

    payload: dict[str, Any] = {
        "kind": kind,
        "symbol": symbol,
        "symbolName": market_data.display_name(symbol),
        "pairSymbol": pair_symbol,
        "pairName": pair_name,
        "includeScr": include_scr,
        "candles": candles,
        "tickText": tick_text,
        "orders": visible_orders,
        "debug": {
            "max_candles": MAX_LIVE_CHART_CANDLES,
            "business_days": visible_business_days,
            "frame_rows": len(frame),
            "first_time": frame.index.min().isoformat() if not frame.empty else "",
            "last_time": frame.index.max().isoformat() if not frame.empty else "",
            "trade_days": [day.strftime("%Y-%m-%d") for day in pd.Index(frame.index.normalize().unique()).sort_values()],
        },
    }

    payload["currentCandle"] = _current_candle_status(full_frame)

    if include_scr:
        payload["scr"] = scr_values or []
        payload["pairScr"] = pair_scr_values or []
        payload["signals"] = visible_signals
    else:
        payload["signals"] = {}

    _write_cached_payload(cache_key, payload)
    return payload


def ensure_live_chart_prewarm(
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments | None = None,
    *,
    strategy_name: str = "src_v2_adx",
    visible_business_days: int = MAX_LIVE_CHART_BUSINESS_DAYS,
) -> None:
    current_adjustments = adjustments or StrategyAdjustments()
    prewarm_key = _build_payload_cache_key(
        kind="overlay",
        symbol=symbol,
        pair_symbol=pair_symbol,
        adjustments=current_adjustments,
        strategy_name=strategy_name,
        visible_business_days=visible_business_days,
    )

    with _PREWARM_LOCK:
        if prewarm_key in _PREWARM_STARTED_KEYS:
            return
        _PREWARM_STARTED_KEYS.add(prewarm_key)

    def _runner() -> None:
        try:
            build_chart_payload(
                "overlay",
                symbol,
                pair_symbol,
                current_adjustments,
                strategy_name=strategy_name,
                visible_business_days=visible_business_days,
            )
        except Exception:
            with _PREWARM_LOCK:
                _PREWARM_STARTED_KEYS.discard(prewarm_key)

    thread = threading.Thread(target=_runner, daemon=True, name=f"shinobu-prewarm-{strategy_name}")
    thread.start()


def ensure_live_chart_prewarm_bundle(
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments | None = None,
    *,
    current_strategy_name: str,
    visible_business_days: int = MAX_LIVE_CHART_BUSINESS_DAYS,
    all_strategy_names: list[str] | None = None,
) -> None:
    current_adjustments = adjustments or StrategyAdjustments()
    strategy_names = list(dict.fromkeys([current_strategy_name] + list(all_strategy_names or [])))
    bundle_key = "|".join(
        [
            symbol,
            pair_symbol or "",
            current_strategy_name,
            str(int(visible_business_days)),
            ",".join(strategy_names),
            f"s{current_adjustments.stoch_pct}_c{current_adjustments.cci_pct}_r{current_adjustments.rsi_pct}",
        ]
    )

    with _PREWARM_LOCK:
        if bundle_key in _PREWARM_BUNDLE_KEYS:
            return
        _PREWARM_BUNDLE_KEYS.add(bundle_key)

    ensure_live_chart_prewarm(
        symbol,
        pair_symbol,
        current_adjustments,
        strategy_name=current_strategy_name,
        visible_business_days=visible_business_days,
    )

    def _runner() -> None:
        try:
            for strategy_name in strategy_names:
                if strategy_name == current_strategy_name:
                    continue
                time.sleep(0.6)
                ensure_live_chart_prewarm(
                    symbol,
                    pair_symbol,
                    current_adjustments,
                    strategy_name=strategy_name,
                    visible_business_days=visible_business_days,
                )
        except Exception:
            pass

    thread = threading.Thread(target=_runner, daemon=True, name=f"shinobu-prewarm-bundle-{current_strategy_name}")
    thread.start()
