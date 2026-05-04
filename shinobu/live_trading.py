from __future__ import annotations

import json
import tempfile
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

from shinobu.data import display_name, load_live_chart_data, load_live_chart_data_cached_only
from shinobu.kis import KisApiError, cancel_domestic_order, fetch_domestic_balance, place_domestic_order
from shinobu.strategy import (
    DEFAULT_STRATEGY_NAME,
    StrategyAdjustments,
    calculate_strategy,
    get_strategy_history_business_days,
    normalize_strategy_name,
)
from shinobu.strategy_cache import calculate_strategy_cached


SIGNAL_TO_TRADE_SYMBOL = {
    "122630.KS": "069500.KS",
    "252670.KS": "114800.KS",
}
TRADE_TO_SIGNAL_SYMBOL = {value: key for key, value in SIGNAL_TO_TRADE_SYMBOL.items()}
EXECUTION_MODE_X1 = "x1"
EXECUTION_MODE_SIGNAL = "signal"
DEFAULT_EXECUTION_MODE = EXECUTION_MODE_X1
MAX_LIVE_ORDERS = 200
MAX_ASSET_HISTORY = 240
LIVE_FILL_CONFIRM_TIMEOUT_SECONDS = 4.0
LIVE_FILL_CONFIRM_POLL_SECONDS = 0.35
LIVE_ORDER_MAX_RETRIES = 12
LIVE_ORDER_RETRY_DELAY_SECONDS = 0.35
LIVE_ORDER_PARTIAL_RETRY_DELAY_SECONDS = 0.15
RETRYABLE_ORDER_ERROR_PATTERNS = (
    "getaddrinfo failed",
    "name or service not known",
    "network is unreachable",
    "connection reset",
    "remote end closed",
    "temporarily unavailable",
    "timed out",
    "timeout",
)
LIVE_STATE_FILE = Path(__file__).resolve().parent.parent / ".streamlit" / "live_state.json"
LIVE_LOG_FILE = Path(__file__).resolve().parent.parent / ".streamlit" / "live_trading.log"
_LIVE_STATE_LOCK = threading.RLock()
KST = ZoneInfo("Asia/Seoul")
PREMARKET_MONITOR_HOUR = 8
REGULAR_MARKET_OPEN_HOUR = 9
REGULAR_MARKET_CLOSE_HOUR = 15
REGULAR_MARKET_CLOSE_MINUTE = 30
AFTER_HOURS_CLOSE_HOUR = 18
PRE_CLOSE_FORCE_EXIT_MINUTES = 15
LIVE_SWITCH_CONFIRM_BARS = 2
LIVE_MAX_HOLD_BARS = 180


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _default_state() -> dict[str, Any]:
    return {
        "enabled": False,
        "started_at": "",
        "strategy_name": DEFAULT_STRATEGY_NAME,
        "chart_business_days": 2,
        "execution_mode": DEFAULT_EXECUTION_MODE,
        "last_checked_candle": "",
        "last_cycle_at": "",
        "last_order_at": "",
        "last_asset_snapshot_order_at": "",
        "last_status": "stopped",
        "last_error": "",
        "pending_target_mode": "none",
        "pending_target_symbol": "",
        "pending_target_reason": "",
        "pending_target_candle": "",
        "switch_confirm_symbol": "",
        "switch_confirm_count": 0,
        "switch_confirm_last_candle": "",
        "position_entry_symbol": "",
        "position_entry_candle": "",
        "last_regular_close_cleanup_date": "",
        "last_forced_exit_date": "",
        "deferred_open_signal_symbol": "",
        "deferred_open_trade_symbol": "",
        "deferred_open_candle": "",
        "deferred_open_set_date": "",
        "orders": [],
        "asset_history": [],
    }


def _ensure_state_file() -> None:
    LIVE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not LIVE_STATE_FILE.exists():
        _write_state(_default_state())
    if not LIVE_LOG_FILE.exists():
        LIVE_LOG_FILE.touch()


def _read_state() -> dict[str, Any]:
    _ensure_state_file()
    try:
        data = json.loads(LIVE_STATE_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        data = _default_state()

    state = _default_state()
    state.update(data if isinstance(data, dict) else {})
    if not isinstance(state["orders"], list):
        state["orders"] = []
    if not isinstance(state["asset_history"], list):
        state["asset_history"] = []
    return state


def _write_state(state: dict[str, Any]) -> None:
    LIVE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, ensure_ascii=False, indent=2)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False, dir=str(LIVE_STATE_FILE.parent)) as temp_file:
        temp_file.write(payload)
        temp_path = Path(temp_file.name)
    temp_path.replace(LIVE_STATE_FILE)


def init_live_state() -> None:
    with _LIVE_STATE_LOCK:
        _ensure_state_file()
        _write_state(_read_state())


def _append_log(level: str, message: str) -> None:
    with LIVE_LOG_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_text()}  [{level}]  {message}\n")
        log_file.flush()


def append_live_log(level: str, message: str) -> None:
    with _LIVE_STATE_LOCK:
        _ensure_state_file()
        _append_log(level, message)


def _set_status(state: dict[str, Any], status: str, error: str = "") -> None:
    state["last_status"] = status
    state["last_error"] = error
    state["last_cycle_at"] = _now_text()


def _append_order(
    state: dict[str, Any],
    symbol: str,
    side: str,
    quantity: int,
    price: float,
    reason: str,
    candle_time: pd.Timestamp,
    execution_tag: str = "",
) -> dict[str, Any]:
    entry = {
        "symbol": symbol,
        "side": side,
        "quantity": quantity,
        "price": float(price),
        "reason": reason,
        "execution_tag": str(execution_tag or ""),
        "candle_time": candle_time.strftime("%Y-%m-%d %H:%M"),
        "timestamp": _now_text(),
        "order_no": "",
        "order_orgno": "",
        "filled": False,
        "fill_message": "",
        "canceled": False,
        "cancel_message": "",
    }
    state["orders"].append(entry)
    if len(state["orders"]) > MAX_LIVE_ORDERS:
        del state["orders"][:-MAX_LIVE_ORDERS]
    state["last_order_at"] = _now_text()
    return entry


def _set_pending_target(
    state: dict[str, Any],
    mode: str,
    symbol: str = "",
    reason: str = "",
    candle_time: pd.Timestamp | None = None,
) -> None:
    state["pending_target_mode"] = mode
    state["pending_target_symbol"] = symbol
    state["pending_target_reason"] = reason
    state["pending_target_candle"] = candle_time.strftime("%Y-%m-%d %H:%M") if candle_time is not None else ""


def _clear_pending_target(state: dict[str, Any]) -> None:
    _set_pending_target(state, "none")


def _set_deferred_open(
    state: dict[str, Any],
    signal_symbol: str,
    trade_symbol: str,
    candle_time: pd.Timestamp,
    now: pd.Timestamp | None = None,
) -> None:
    current = pd.Timestamp(now) if now is not None else _now_kst_naive()
    state["deferred_open_signal_symbol"] = str(signal_symbol or "")
    state["deferred_open_trade_symbol"] = str(trade_symbol or "")
    state["deferred_open_candle"] = candle_time.strftime("%Y-%m-%d %H:%M")
    state["deferred_open_set_date"] = current.strftime("%Y-%m-%d")


def _clear_deferred_open(state: dict[str, Any]) -> None:
    state["deferred_open_signal_symbol"] = ""
    state["deferred_open_trade_symbol"] = ""
    state["deferred_open_candle"] = ""
    state["deferred_open_set_date"] = ""


def _clear_switch_confirm_state(state: dict[str, Any]) -> None:
    state["switch_confirm_symbol"] = ""
    state["switch_confirm_count"] = 0
    state["switch_confirm_last_candle"] = ""


def _register_switch_confirm(
    state: dict[str, Any],
    target_signal_symbol: str,
    candle_key: str,
    required_bars: int,
) -> bool:
    required = max(int(required_bars), 1)
    last_symbol = str(state.get("switch_confirm_symbol", "") or "")
    last_candle = str(state.get("switch_confirm_last_candle", "") or "")
    try:
        last_count = int(state.get("switch_confirm_count", 0) or 0)
    except (TypeError, ValueError):
        last_count = 0

    if last_symbol != target_signal_symbol:
        last_count = 0
    if last_candle != candle_key:
        last_count += 1

    state["switch_confirm_symbol"] = target_signal_symbol
    state["switch_confirm_count"] = last_count
    state["switch_confirm_last_candle"] = candle_key
    return last_count >= required


def _parse_candle_text(value: str) -> pd.Timestamp | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        ts = pd.Timestamp(text)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    return ts


def _bars_since_entry(entry_candle_text: str, current_candle: pd.Timestamp) -> int:
    entry_ts = _parse_candle_text(entry_candle_text)
    if entry_ts is None:
        return 0
    delta_minutes = int((pd.Timestamp(current_candle) - entry_ts).total_seconds() // 60)
    if delta_minutes <= 0:
        return 0
    return delta_minutes // 5


def set_live_enabled(enabled: bool) -> None:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        state["enabled"] = bool(enabled)
        state["last_checked_candle"] = ""
        state["last_error"] = ""
        state["last_cycle_at"] = _now_text()
        if enabled:
            state["started_at"] = _now_text()
            state["last_status"] = "running"
            _append_log("상태", "실전 투자 시작")
        else:
            state["last_status"] = "stopped"
            _append_log("상태", "실전 투자 중지")
        _write_state(state)


def is_live_enabled() -> bool:
    with _LIVE_STATE_LOCK:
        return bool(_read_state()["enabled"])


def get_live_logs(limit: int | None = None, lookback_days: int = 5) -> list[str]:
    with _LIVE_STATE_LOCK:
        _ensure_state_file()
        try:
            lines = LIVE_LOG_FILE.read_text(encoding="utf-8").splitlines()
        except OSError:
            lines = []

    cutoff = datetime.now() - timedelta(days=max(int(lookback_days), 1))
    filtered: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            timestamp = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
            if timestamp < cutoff:
                continue
        except Exception:
            pass
        filtered.append(line)

    if isinstance(limit, int) and limit > 0:
        filtered = filtered[-limit:]
    return list(reversed(filtered))


def get_live_orders() -> list[dict[str, Any]]:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        return list(state["orders"])


def get_live_started_at() -> pd.Timestamp | None:
    with _LIVE_STATE_LOCK:
        started_at = _read_state().get("started_at", "")
    if not started_at:
        return None
    return pd.Timestamp(started_at)


def get_live_runtime_state() -> dict[str, str]:
    with _LIVE_STATE_LOCK:
        state = _read_state()
    return {
        "last_checked_candle": str(state.get("last_checked_candle", "")),
        "last_cycle_at": str(state.get("last_cycle_at", "")),
        "last_order_at": str(state.get("last_order_at", "")),
        "last_status": str(state.get("last_status", "stopped")),
        "last_error": str(state.get("last_error", "")),
    }


def get_live_strategy_name() -> str:
    with _LIVE_STATE_LOCK:
        state = _read_state()
    return normalize_strategy_name(str(state.get("strategy_name", DEFAULT_STRATEGY_NAME)))


def set_live_strategy_name(strategy_name: str) -> None:
    normalized = normalize_strategy_name(strategy_name)
    with _LIVE_STATE_LOCK:
        state = _read_state()
        state["strategy_name"] = normalized
        _write_state(state)


def get_live_chart_business_days() -> int:
    with _LIVE_STATE_LOCK:
        state = _read_state()
    try:
        return max(1, min(int(state.get("chart_business_days", 2)), 5))
    except (TypeError, ValueError):
        return 2


def set_live_chart_business_days(days: int) -> None:
    next_value = max(1, min(int(days), 5))
    with _LIVE_STATE_LOCK:
        state = _read_state()
        state["chart_business_days"] = next_value
        _write_state(state)


def normalize_execution_mode(mode: str | None) -> str:
    candidate = str(mode or DEFAULT_EXECUTION_MODE).strip().lower()
    if candidate in {EXECUTION_MODE_X1, EXECUTION_MODE_SIGNAL}:
        return candidate
    return DEFAULT_EXECUTION_MODE


def get_live_execution_mode() -> str:
    with _LIVE_STATE_LOCK:
        state = _read_state()
    return normalize_execution_mode(str(state.get("execution_mode", DEFAULT_EXECUTION_MODE)))


def set_live_execution_mode(mode: str) -> None:
    normalized = normalize_execution_mode(mode)
    with _LIVE_STATE_LOCK:
        state = _read_state()
        state["execution_mode"] = normalized
        _write_state(state)


def record_asset_snapshot(total_assets: float) -> None:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        last_order_at = str(state.get("last_order_at", "") or "")
        last_snapshot_order_at = str(state.get("last_asset_snapshot_order_at", "") or "")
        history = list(state.get("asset_history", []))
        if not last_order_at and history:
            return
        if last_order_at and last_order_at == last_snapshot_order_at:
            return

        timestamp = _now_text()
        if history and history[-1].get("timestamp") == timestamp:
            history[-1]["total_assets"] = float(total_assets)
        else:
            history.append({"timestamp": timestamp, "total_assets": float(total_assets)})
        if len(history) > MAX_ASSET_HISTORY:
            history = history[-MAX_ASSET_HISTORY:]
        state["asset_history"] = history
        state["last_asset_snapshot_order_at"] = last_order_at
        _write_state(state)


def get_asset_history() -> list[dict[str, Any]]:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        return list(state.get("asset_history", []))


def _load_strategy(symbol: str, adjustments: StrategyAdjustments, strategy_name: str) -> pd.DataFrame:
    try:
        from shinobu.data import load_live_chart_data_for_strategy, _business_days_to_lookback_days

        lookback_days = _business_days_to_lookback_days(get_strategy_history_business_days(strategy_name))
        frame = load_live_chart_data_cached_only(symbol, "5분봉", lookback_days=lookback_days)
        now_kst = _now_kst_naive()
        needs_refresh = frame.empty
        if not frame.empty and _market_phase(now_kst) == "regular":
            latest_ts = pd.Timestamp(frame.index.max())
            # During market hours, treat same-day but lagging intraday cache as stale.
            # We allow a small lag window and force refresh when cache falls behind.
            latest_closed_candle = now_kst.floor("5min") - pd.Timedelta(minutes=5)
            stale_cutoff = latest_closed_candle - pd.Timedelta(minutes=10)
            needs_refresh = (latest_ts.date() < now_kst.date()) or (latest_ts < stale_cutoff)
        if needs_refresh:
            frame = load_live_chart_data_for_strategy(symbol, "5분봉", strategy_name)
    except Exception:
        frame = load_live_chart_data(symbol, "5분봉")
    return calculate_strategy_cached(
        frame,
        adjustments,
        "5분봉",
        strategy_name=strategy_name,
        symbol=symbol,
    )


def _now_kst_naive() -> pd.Timestamp:
    return pd.Timestamp.now(tz=KST).tz_localize(None)


def _is_business_day(timestamp: pd.Timestamp) -> bool:
    return pd.Timestamp(timestamp).dayofweek < 5


def _market_phase(now: pd.Timestamp | None = None) -> str:
    current = pd.Timestamp(now) if now is not None else _now_kst_naive()
    if not _is_business_day(current):
        return "closed"

    current_minutes = current.hour * 60 + current.minute
    premarket_monitor_minutes = PREMARKET_MONITOR_HOUR * 60
    regular_open_minutes = REGULAR_MARKET_OPEN_HOUR * 60
    regular_close_minutes = REGULAR_MARKET_CLOSE_HOUR * 60 + REGULAR_MARKET_CLOSE_MINUTE
    after_hours_close_minutes = AFTER_HOURS_CLOSE_HOUR * 60

    if premarket_monitor_minutes <= current_minutes < regular_open_minutes:
        return "premarket"
    if regular_open_minutes <= current_minutes < regular_close_minutes:
        return "regular"
    if regular_close_minutes <= current_minutes < after_hours_close_minutes:
        return "after_hours"
    return "closed"


def _is_pre_close_window(now: pd.Timestamp | None = None) -> bool:
    current = pd.Timestamp(now) if now is not None else _now_kst_naive()
    if _market_phase(current) != "regular":
        return False
    current_minutes = current.hour * 60 + current.minute
    regular_close_minutes = REGULAR_MARKET_CLOSE_HOUR * 60 + REGULAR_MARKET_CLOSE_MINUTE
    return current_minutes >= (regular_close_minutes - PRE_CLOSE_FORCE_EXIT_MINUTES)


def _is_closed_5m_candle(candle_start: pd.Timestamp, now: pd.Timestamp | None = None) -> bool:
    current = now if now is not None else _now_kst_naive()
    candle_start = pd.Timestamp(candle_start)
    candle_end = candle_start + pd.Timedelta(minutes=5)
    return current >= candle_end


def _parse_candle_key(candle_key: str) -> pd.Timestamp | None:
    text = str(candle_key or "").strip()
    if not text:
        return None
    try:
        parsed = pd.Timestamp(text)
    except Exception:
        return None
    return parsed if not pd.isna(parsed) else None


def _get_target_rows(
    primary: pd.DataFrame,
    secondary: pd.DataFrame,
    last_checked_candle: str = "",
) -> tuple[pd.Timestamp, pd.Series, pd.Series, bool] | None:
    combined_index = primary.index.union(secondary.index).sort_values()
    if len(combined_index) < 2:
        return None

    latest_time = pd.Timestamp(combined_index[-1])
    max_closed_index = len(combined_index) - 1 if _is_closed_5m_candle(latest_time) else len(combined_index) - 2
    if max_closed_index < 0:
        return None

    closed_times = [pd.Timestamp(value) for value in combined_index[: max_closed_index + 1]]
    target_time = closed_times[-1]
    last_checked_time = _parse_candle_key(last_checked_candle)
    if last_checked_time is not None:
        for candidate in closed_times:
            if candidate > last_checked_time:
                target_time = candidate
                break
    has_backlog = bool(target_time < closed_times[-1])

    aligned_primary = primary.reindex(combined_index).ffill()
    aligned_secondary = secondary.reindex(combined_index).ffill()
    return target_time, aligned_primary.loc[target_time], aligned_secondary.loc[target_time], has_backlog


def _find_current_pair_position(positions: pd.DataFrame, symbols: list[str]) -> dict[str, Any] | None:
    if positions.empty or "code" not in positions.columns:
        return None

    filtered = positions.copy()
    if "quantity" in filtered.columns:
        quantities = pd.to_numeric(filtered["quantity"], errors="coerce").fillna(0)
        filtered = filtered.loc[quantities > 0].copy()
    if filtered.empty:
        return None

    target_symbols: list[str] = []
    for symbol in symbols:
        if symbol not in target_symbols:
            target_symbols.append(symbol)
        mapped = SIGNAL_TO_TRADE_SYMBOL.get(symbol, symbol)
        if mapped not in target_symbols:
            target_symbols.append(mapped)
    target_codes = [symbol.replace(".KS", "") for symbol in target_symbols]
    matches = filtered[filtered["code"].isin(target_codes)]
    if matches.empty:
        return None

    sort_column = "eval_amount" if "eval_amount" in matches.columns else "quantity"
    row = matches.sort_values(sort_column, ascending=False).iloc[0]
    held_trade_symbol = f"{row['code']}.KS"
    return {
        "symbol": held_trade_symbol,
        "signal_symbol": TRADE_TO_SIGNAL_SYMBOL.get(held_trade_symbol, held_trade_symbol),
        "name": row.get("name", ""),
        "quantity": int(float(row.get("quantity", 0))),
        "current_price": float(row.get("current_price", 0)),
    }


def _choose_open_candidate(
    primary_symbol: str,
    secondary_symbol: str,
    primary_row: pd.Series,
    secondary_row: pd.Series,
    allow_raw_open: bool = False,
) -> tuple[str, pd.Series] | None:
    candidates = []
    primary_open = bool(primary_row.get("buy_open", False)) or (
        allow_raw_open and bool(primary_row.get("raw_buy_open", False))
    )
    secondary_open = bool(secondary_row.get("buy_open", False)) or (
        allow_raw_open and bool(secondary_row.get("raw_buy_open", False))
    )
    if primary_open:
        candidates.append((primary_symbol, primary_row))
    if secondary_open:
        candidates.append((secondary_symbol, secondary_row))
    if not candidates:
        return None
    return max(candidates, key=lambda item: float(item[1].get("scr_line", 0.0)))


def _allocation_quantity(orderable_cash: float, price: float) -> int:
    if price <= 0:
        return 0
    return int(max(float(orderable_cash), 0.0) // float(price))


def _is_cash_exceeded_error(exc: Exception) -> bool:
    message = str(exc)
    return "APBK0952" in message or "주문가능금액을 초과" in message


def _position_quantity_for_symbol(positions: pd.DataFrame, symbol: str) -> int:
    if positions.empty or "code" not in positions.columns:
        return 0
    code = symbol.replace(".KS", "")
    matches = positions.loc[positions["code"] == code]
    if matches.empty:
        return 0
    return int(float(matches.iloc[0].get("quantity", 0)))


def _format_order_response(order_output: dict[str, Any]) -> str:
    if not order_output:
        return "응답 없음"
    parts: list[str] = []
    branch = str(order_output.get("KRX_FWDG_ORD_ORGNO") or order_output.get("ORD_GNO_BRNO") or "").strip()
    order_no = str(order_output.get("ODNO") or "").strip()
    order_time = str(order_output.get("ORD_TMD") or "").strip()
    if branch:
        parts.append(f"지점 {branch}")
    if order_no:
        parts.append(f"주문번호 {order_no}")
    if order_time:
        parts.append(f"주문시각 {order_time}")
    if not parts:
        return json.dumps(order_output, ensure_ascii=False)
    return ", ".join(parts)


def _extract_order_numbers(order_output: dict[str, Any]) -> tuple[str, str]:
    order_orgno = str(order_output.get("KRX_FWDG_ORD_ORGNO") or order_output.get("ORD_GNO_BRNO") or "").strip()
    order_no = str(order_output.get("ODNO") or "").strip()
    return order_orgno, order_no


def _is_retryable_order_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(pattern in message for pattern in RETRYABLE_ORDER_ERROR_PATTERNS)


def _confirm_fill_after_order(symbol: str, side: str, baseline_quantity: int, expected_quantity: int) -> tuple[bool, str]:
    deadline = time.monotonic() + LIVE_FILL_CONFIRM_TIMEOUT_SECONDS
    while time.monotonic() <= deadline:
        try:
            fetch_domestic_balance.clear()
            positions, _ = fetch_domestic_balance()
        except Exception as exc:
            if time.monotonic() <= deadline:
                _append_log("경고", f"{display_name(symbol)} 체결 확인 재시도: {exc}")
                time.sleep(LIVE_FILL_CONFIRM_POLL_SECONDS)
                continue
            return False, f"체결 확인 실패: {exc}"
        current_quantity = _position_quantity_for_symbol(positions, symbol)
        if side == "buy":
            if current_quantity >= baseline_quantity + max(expected_quantity, 1):
                return True, f"체결 확인: 보유수량 {baseline_quantity}주 -> {current_quantity}주"
        else:
            target_quantity = max(baseline_quantity - expected_quantity, 0)
            if current_quantity <= target_quantity:
                return True, f"체결 확인: 보유수량 {baseline_quantity}주 -> {current_quantity}주"
        time.sleep(LIVE_FILL_CONFIRM_POLL_SECONDS)
    return False, "체결 확인 대기 시간 초과"


def _cancel_pending_orders_after_regular_close(state: dict[str, Any], now: pd.Timestamp | None = None) -> None:
    current = pd.Timestamp(now) if now is not None else _now_kst_naive()
    current_date = current.strftime("%Y-%m-%d")
    if state.get("last_regular_close_cleanup_date", "") == current_date:
        return

    canceled_any = False
    for order in reversed(state.get("orders", [])):
        order_timestamp = str(order.get("timestamp", "") or "")
        if not order_timestamp.startswith(current_date):
            continue
        if bool(order.get("filled", False)) or bool(order.get("canceled", False)):
            continue

        order_orgno = str(order.get("order_orgno", "") or "").strip()
        order_no = str(order.get("order_no", "") or "").strip()
        symbol = str(order.get("symbol", "") or "").strip()
        quantity = int(order.get("quantity", 0) or 0)
        if not order_orgno or not order_no or not symbol or quantity <= 0:
            continue

        try:
            cancel_output = cancel_domestic_order(
                symbol=symbol.replace(".KS", ""),
                order_orgno=order_orgno,
                order_no=order_no,
                quantity=quantity,
            )
            order["canceled"] = True
            order["cancel_message"] = _format_order_response(cancel_output)
            canceled_any = True
            _append_log("정보", f"{display_name(symbol)} 미체결 주문을 장 종료 후 취소했습니다. ({order['cancel_message']})")
        except Exception as exc:
            order["cancel_message"] = str(exc)
            _append_log("경고", f"{display_name(symbol)} 미체결 주문 취소 실패: {exc}")

    if state.get("pending_target_mode", "none") != "none":
        _append_log("정보", "미체결 정리 이후 보정 목표를 초기화했습니다.")
        _clear_pending_target(state)

    state["last_regular_close_cleanup_date"] = current_date
    if canceled_any:
        _set_status(state, "after_hours_cleanup")
    else:
        _set_status(state, "after_hours")
    _write_state(state)


def _mark_pending_orders_for_monitor(state: dict[str, Any], now: pd.Timestamp | None = None) -> None:
    current = pd.Timestamp(now) if now is not None else _now_kst_naive()
    current_date = current.strftime("%Y-%m-%d")
    pending_count = 0
    for order in state.get("orders", []):
        order_timestamp = str(order.get("timestamp", "") or "")
        if not order_timestamp.startswith(current_date):
            continue
        if bool(order.get("filled", False)) or bool(order.get("canceled", False)):
            continue
        pending_count += 1

    if pending_count > 0:
        _set_status(state, "premarket_pending")
        _append_log("정보", f"장 시작 전 미체결 주문 {pending_count}건을 감시 중입니다.")
    else:
        _set_status(state, "premarket")
    _write_state(state)


def _submit_live_order(
    state: dict[str, Any],
    symbol: str,
    side: str,
    quantity: int,
    expected_price: float,
    reason: str,
    candle_time: pd.Timestamp,
    baseline_quantity: int,
    execution_tag: str = "",
) -> None:
    current_baseline = int(baseline_quantity)
    round_count = 0
    buy_half_ladder_mode = False

    while True:
        try:
            fetch_domestic_balance.clear()
            positions, latest_summary = fetch_domestic_balance()
        except Exception as exc:
            _append_log("오류", f"{display_name(symbol)} 주문 전 잔고 조회 실패: {exc}")
            raise
        current_quantity = _position_quantity_for_symbol(positions, symbol)

        if side == "buy":
            latest_cash = float(latest_summary.get("orderable_cash", 0) or 0)
            affordable_quantity = _allocation_quantity(latest_cash, expected_price)
            if buy_half_ladder_mode:
                working_quantity = max(affordable_quantity // 2, 1) if affordable_quantity > 0 else 0
            else:
                working_quantity = affordable_quantity
            if working_quantity <= 0:
                if round_count == 0:
                    _set_status(state, "waiting_cash", "주문 가능 금액 부족")
                    _append_log("경고", f"{display_name(symbol)} 주문 가능 수량이 없어 대기합니다.")
                    _write_state(state)
                    raise KisApiError("주문 가능 수량이 없습니다.")
                return
            baseline_for_fill = current_baseline
        else:
            working_quantity = current_quantity
            if working_quantity <= 0:
                return
            baseline_for_fill = current_quantity

        if round_count > 0:
            action_text = "추가 매수" if side == "buy" else "추가 매도"
            _append_log("정보", f"{display_name(symbol)} {action_text} {working_quantity}주를 재시도합니다.")

        used_cash_fallback = _submit_live_order_once(
            state=state,
            symbol=symbol,
            side=side,
            quantity=working_quantity,
            expected_price=expected_price,
            reason=reason,
            candle_time=candle_time,
            baseline_quantity=baseline_for_fill,
            execution_tag=execution_tag,
        )
        if side == "buy" and used_cash_fallback:
            buy_half_ladder_mode = True

        try:
            fetch_domestic_balance.clear()
            positions, latest_summary = fetch_domestic_balance()
        except Exception as exc:
            _append_log("오류", f"{display_name(symbol)} 주문 후 잔고 조회 실패: {exc}")
            raise
        updated_quantity = _position_quantity_for_symbol(positions, symbol)

        if side == "buy":
            latest_cash = float(latest_summary.get("orderable_cash", 0) or 0)
            if updated_quantity <= current_baseline:
                _append_log("경고", f"{display_name(symbol)} 매수 후 보유수량 증가가 확인되지 않아 추가 매수를 중단합니다.")
                return
            current_baseline = updated_quantity
            round_count += 1
            if latest_cash < max(float(expected_price), 1.0):
                return
            time.sleep(LIVE_ORDER_PARTIAL_RETRY_DELAY_SECONDS)
            continue

        if updated_quantity >= current_quantity:
            _append_log("경고", f"{display_name(symbol)} 매도 후 보유수량 감소가 확인되지 않아 추가 매도를 중단합니다.")
            return
        round_count += 1
        if updated_quantity <= 0:
            return
        time.sleep(LIVE_ORDER_PARTIAL_RETRY_DELAY_SECONDS)


def _submit_live_order_once(
    state: dict[str, Any],
    symbol: str,
    side: str,
    quantity: int,
    expected_price: float,
    reason: str,
    candle_time: pd.Timestamp,
    baseline_quantity: int,
    execution_tag: str = "",
) -> bool:
    last_error: Exception | None = None
    working_quantity = max(int(quantity), 0)
    attempt = 0
    used_cash_fallback = False
    while attempt < LIVE_ORDER_MAX_RETRIES:
        attempt += 1
        try:
            if working_quantity <= 0:
                raise KisApiError("주문 수량이 0주가 되어 주문을 중단합니다.")

            if side == "buy":
                fetch_domestic_balance.clear()
                _, latest_summary = fetch_domestic_balance()
                latest_cash = float(latest_summary.get("orderable_cash", 0) or 0)
                recalculated_quantity = _allocation_quantity(latest_cash, expected_price)
                if recalculated_quantity <= 0:
                    raise KisApiError("주문 직전 재조회 기준 매수 가능 수량이 없습니다.")
                if recalculated_quantity < working_quantity:
                    _append_log(
                        "정보",
                        f"{display_name(symbol)} 주문 직전 재조회로 수량을 {working_quantity}주 -> {recalculated_quantity}주로 조정합니다.",
                    )
                    working_quantity = recalculated_quantity

            broker_output = place_domestic_order(symbol.replace(".KS", ""), side, working_quantity)
            order_entry = _append_order(
                state,
                symbol,
                side,
                working_quantity,
                expected_price,
                reason,
                candle_time,
                execution_tag=execution_tag,
            )
            order_orgno, order_no = _extract_order_numbers(broker_output)
            order_entry["order_orgno"] = order_orgno
            order_entry["order_no"] = order_no
            if attempt > 1:
                _append_log("정보", f"{display_name(symbol)} {side.upper()} 주문 재시도 성공 ({attempt}/{LIVE_ORDER_MAX_RETRIES})")
            _append_log(
                "주문",
                f"{display_name(symbol)} {side.upper()} {working_quantity}주 시장가 주문 접수 ({_format_order_response(broker_output)})",
            )
            filled, fill_message = _confirm_fill_after_order(symbol, side, baseline_quantity, working_quantity)
            order_entry["filled"] = bool(filled)
            order_entry["fill_message"] = fill_message
            _append_log("체결" if filled else "경고", f"{display_name(symbol)} {side.upper()} {working_quantity}주 {fill_message}")
            return used_cash_fallback
        except Exception as exc:
            last_error = exc
            if side == "buy" and _is_cash_exceeded_error(exc) and working_quantity > 1:
                fetch_domestic_balance.clear()
                _, latest_summary = fetch_domestic_balance()
                latest_cash = float(latest_summary.get("orderable_cash", 0) or 0)
                recalculated_quantity = int(latest_cash // max(float(expected_price) * 1.001, 1.0))
                reduced_quantity = max(working_quantity // 2, 1)
                if 0 < recalculated_quantity < reduced_quantity:
                    reduced_quantity = recalculated_quantity
                _append_log(
                    "경고",
                    f"{display_name(symbol)} BUY 주문가능금액 초과로 수량을 {working_quantity}주 -> {reduced_quantity}주(50%)로 낮춰 즉시 재시도합니다.",
                )
                used_cash_fallback = True
                working_quantity = reduced_quantity
                time.sleep(0.2)
                continue
            if attempt < LIVE_ORDER_MAX_RETRIES and _is_retryable_order_error(exc):
                _append_log(
                    "경고",
                    f"{display_name(symbol)} {side.upper()} 주문 재시도 예정 ({attempt}/{LIVE_ORDER_MAX_RETRIES}): {exc}",
                )
                time.sleep(LIVE_ORDER_RETRY_DELAY_SECONDS * attempt)
                continue
            break

    message = str(last_error) if last_error is not None else "알 수 없는 주문 오류"
    _set_status(state, "error", message)
    _append_log("오류", f"{display_name(symbol)} {side.upper()} 주문 실패: {message}")
    _write_state(state)
    raise KisApiError(message)


def _trade_symbol(signal_symbol: str, execution_mode: str) -> str:
    normalized_mode = normalize_execution_mode(execution_mode)
    if normalized_mode == EXECUTION_MODE_SIGNAL:
        return signal_symbol
    return SIGNAL_TO_TRADE_SYMBOL.get(signal_symbol, signal_symbol)


def process_live_trading_cycle(
    primary_symbol: str,
    secondary_symbol: str,
    adjustments: StrategyAdjustments,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
) -> None:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        if not state["enabled"]:
            return
        execution_mode = normalize_execution_mode(state.get("execution_mode", DEFAULT_EXECUTION_MODE))
        now_kst = _now_kst_naive()

        phase = _market_phase(now_kst)
        if phase != "regular":
            if phase == "premarket":
                _mark_pending_orders_for_monitor(state)
            elif phase == "after_hours":
                _cancel_pending_orders_after_regular_close(state)
            else:
                _set_status(state, "market_closed")
                _write_state(state)
            return

        _set_status(state, "checking")

        try:
            primary = _load_strategy(primary_symbol, adjustments, strategy_name)
            secondary = _load_strategy(secondary_symbol, adjustments, strategy_name)
        except Exception as exc:
            _set_status(state, "error", str(exc))
            _append_log("오류", f"실전 데이터 조회 실패: {exc}")
            _write_state(state)
            raise

        target_rows = _get_target_rows(primary, secondary, str(state.get("last_checked_candle", "") or ""))
        if target_rows is None:
            _set_status(state, "waiting_data")
            _append_log("대기", "완료된 5분봉이 아직 충분하지 않아 다음 주기를 기다립니다.")
            _write_state(state)
            return

        target_time, primary_row, secondary_row, has_backlog = target_rows
        candle_key = target_time.strftime("%Y-%m-%d %H:%M")
        if has_backlog:
            _append_log("정보", f"엔진 재개로 누락된 봉을 순차 처리 중입니다. 현재 처리 봉: {candle_key}")
        try:
            positions, summary = fetch_domestic_balance()
        except Exception as exc:
            _set_status(state, "error", str(exc))
            _append_log("오류", f"실계좌 조회 실패: {exc}")
            _write_state(state)
            raise
        current_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
        if current_position is not None:
            current_signal_symbol = current_position.get("signal_symbol", current_position["symbol"])
            opposite_symbol = secondary_symbol if current_signal_symbol == primary_symbol else primary_symbol
            opposite_row = secondary_row if opposite_symbol == secondary_symbol else primary_row
            opposite_open = bool(opposite_row.get("buy_open", False)) or bool(opposite_row.get("raw_buy_open", False))
            if opposite_open:
                # Force switching when opposite-side open signal exists.
                chosen_open = (opposite_symbol, opposite_row)
            else:
                chosen_open = _choose_open_candidate(
                    primary_symbol,
                    secondary_symbol,
                    primary_row,
                    secondary_row,
                    allow_raw_open=True,
                )
        else:
            chosen_open = _choose_open_candidate(
                primary_symbol,
                secondary_symbol,
                primary_row,
                secondary_row,
                allow_raw_open=False,
            )

        # Execute deferred open only on next business day and only if open signal still valid.
        deferred_signal_symbol = str(state.get("deferred_open_signal_symbol", "") or "")
        deferred_trade_symbol = str(state.get("deferred_open_trade_symbol", "") or "")
        deferred_set_date = str(state.get("deferred_open_set_date", "") or "")
        today_text = now_kst.strftime("%Y-%m-%d")
        if deferred_signal_symbol:
            if today_text <= deferred_set_date:
                pass
            elif current_position is not None:
                _append_log("정보", "이월 진입 대기 신호를 해제합니다. 이미 보유 포지션이 있습니다.")
                _clear_deferred_open(state)
            else:
                deferred_row = primary_row if deferred_signal_symbol == primary_symbol else secondary_row
                deferred_open_valid = bool(deferred_row.get("buy_open", False))
                deferred_close_active = bool(deferred_row.get("buy_close", False))
                if deferred_open_valid and not deferred_close_active:
                    _set_pending_target(
                        state,
                        "symbol",
                        deferred_trade_symbol or _trade_symbol(deferred_signal_symbol, execution_mode),
                        "장마감 이월 진입",
                        target_time,
                    )
                    _append_log("정보", f"{candle_key} 기준 이월 신호가 유지되어 다음날 진입을 진행합니다.")
                else:
                    _append_log("정보", f"{candle_key} 기준 이월 신호가 유지되지 않아 다음날 진입을 취소합니다.")
                _clear_deferred_open(state)

        # Pre-close risk rule:
        # 1) If holding position in last 10 minutes, force close to avoid overnight gap risk.
        # 2) If flat and new open signal appears in last 10 minutes, defer to next business day.
        if _is_pre_close_window(now_kst):
            forced_exit_date = str(state.get("last_forced_exit_date", "") or "")
            if current_position is not None and forced_exit_date != today_text:
                current_symbol = current_position["symbol"]
                current_signal_symbol = current_position.get("signal_symbol", current_symbol)
                active_row = primary_row if current_signal_symbol == primary_symbol else secondary_row
                current_quantity = int(current_position["quantity"])
                _append_log("정보", f"{candle_key} 기준 장마감 10분 전 리스크 관리 청산을 실행합니다.")
                _submit_live_order(
                    state,
                    current_symbol,
                    "sell",
                    current_quantity,
                    float(active_row["Close"]),
                    "장마감 10분 전 강제 청산",
                    target_time,
                    baseline_quantity=current_quantity,
                    execution_tag="eod_force_exit",
                )
                fetch_domestic_balance.clear()
                positions, _ = fetch_domestic_balance()
                remaining_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
                if remaining_position is None:
                    state["last_forced_exit_date"] = today_text
                else:
                    _append_log("경고", "장마감 강제 청산 후 보유가 남아 있어 같은 날 재시도 대상으로 유지합니다.")
                _clear_pending_target(state)
                _clear_deferred_open(state)
                _set_status(state, "ordered")
                _write_state(state)
                return

            if current_position is None and chosen_open is not None:
                deferred_signal = chosen_open[0]
                deferred_trade = _trade_symbol(deferred_signal, execution_mode)
                _set_deferred_open(state, deferred_signal, deferred_trade, target_time, now_kst)
                _clear_pending_target(state)
                _set_status(state, "idle")
                _append_log("정보", f"{candle_key} 기준 장마감 10분 전 신규 진입은 다음 영업일로 이월합니다.")
                _write_state(state)
                return
        pending_mode = str(state.get("pending_target_mode", "none") or "none")
        pending_symbol = str(state.get("pending_target_symbol", "") or "")
        pending_reason = str(state.get("pending_target_reason", "") or "")
        pending_candle = str(state.get("pending_target_candle", "") or "")

        if current_position is not None:
            current_signal_symbol = current_position.get("signal_symbol", current_position["symbol"])
            active_row = primary_row if current_signal_symbol == primary_symbol else secondary_row
            if chosen_open is not None and chosen_open[0] != current_signal_symbol:
                _set_pending_target(
                    state,
                    "symbol",
                    _trade_symbol(chosen_open[0], execution_mode),
                    "반대 ETF 스위치",
                    target_time,
                )
            elif bool(active_row.get("buy_close", False)):
                _set_pending_target(state, "cash", reason="지표 과열 청산", candle_time=target_time)
        elif chosen_open is not None:
            _set_pending_target(
                state,
                "symbol",
                _trade_symbol(chosen_open[0], execution_mode),
                "buy open 진입",
                target_time,
            )

        pending_mode = str(state.get("pending_target_mode", "none") or "none")
        pending_symbol = str(state.get("pending_target_symbol", "") or "")
        pending_reason = str(state.get("pending_target_reason", "") or "")
        pending_candle = str(state.get("pending_target_candle", "") or "")

        should_reconcile = pending_mode != "none"
        if state["last_checked_candle"] == candle_key and not should_reconcile:
            _set_status(state, "idle")
            _write_state(state)
            return

        state["last_checked_candle"] = candle_key

        if pending_mode == "cash":
            if current_position is None:
                _clear_pending_target(state)
                _set_status(state, "idle")
                _append_log("정보", f"{pending_candle or candle_key} 기준 청산 목표 달성")
                _write_state(state)
                return

            current_symbol = current_position["symbol"]
            current_signal_symbol = current_position.get("signal_symbol", current_symbol)
            active_row = primary_row if current_signal_symbol == primary_symbol else secondary_row
            # Keep close execution paired with a currently valid buy_close signal.
            # If close signal is no longer true, drop stale pending-close reconciliation.
            if not bool(active_row.get("buy_close", False)):
                _append_log("정보", f"{pending_candle or candle_key} 기준 청산 보정 해제: 현재 봉 buy_close 미충족")
                _clear_pending_target(state)
                _set_status(state, "idle")
                _write_state(state)
                return
            current_quantity = int(current_position["quantity"])
            _append_log("정보", f"{pending_candle or candle_key} 기준 미완료 청산 감지, 보정 주문을 재시도합니다.")
            _submit_live_order(
                state,
                current_symbol,
                "sell",
                current_quantity,
                float(active_row["Close"]),
                pending_reason or "지표 과열 청산",
                target_time,
                baseline_quantity=current_quantity,
                execution_tag="reconcile_close",
            )
            fetch_domestic_balance.clear()
            positions, _ = fetch_domestic_balance()
            if _find_current_pair_position(positions, [primary_symbol, secondary_symbol]) is None:
                _clear_pending_target(state)
            _set_status(state, "ordered")
            _write_state(state)
            return

        if pending_mode == "symbol" and pending_symbol:
            if current_position is None:
                target_signal_symbol = TRADE_TO_SIGNAL_SYMBOL.get(pending_symbol, pending_symbol)
                target_row = primary_row if target_signal_symbol == primary_symbol else secondary_row
                buy_price = float(target_row["Close"])
                buy_quantity = _allocation_quantity(summary.get("orderable_cash", 0), buy_price)
                if buy_quantity <= 0:
                    _set_status(state, "waiting_cash")
                    _append_log(
                        "경고",
                        f"{display_name(pending_symbol)} 진입 가능 수량이 없어 다음 주기에 다시 확인합니다.",
                    )
                    _write_state(state)
                    return

                _append_log(
                    "정보",
                    f"{pending_candle or candle_key} 기준 목표 포지션 미달성 감지, 진입 주문을 재시도합니다.",
                )
                _submit_live_order(
                    state,
                    pending_symbol,
                    "buy",
                    buy_quantity,
                    buy_price,
                    pending_reason or "buy open 진입",
                    target_time,
                    baseline_quantity=0,
                    execution_tag="reconcile_open",
                )
                fetch_domestic_balance.clear()
                positions, _ = fetch_domestic_balance()
                current_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
                if current_position is not None and current_position["symbol"] == pending_symbol:
                    _clear_pending_target(state)
                _set_status(state, "ordered")
                _write_state(state)
                return

            if current_position is not None and current_position["symbol"] == pending_symbol:
                _clear_pending_target(state)
                if chosen_open is not None and _trade_symbol(chosen_open[0], execution_mode) == pending_symbol:
                    active_row = primary_row if chosen_open[0] == primary_symbol else secondary_row
                    current_quantity = int(current_position["quantity"])
                    add_price = float(active_row["Close"])
                    add_quantity = _allocation_quantity(summary.get("orderable_cash", 0), add_price)
                    if add_quantity > 0:
                        _submit_live_order(
                            state,
                            pending_symbol,
                            "buy",
                            add_quantity,
                            add_price,
                            "동일 방향 추가매수",
                            target_time,
                            baseline_quantity=current_quantity,
                        )
                        _set_status(state, "ordered")
                    else:
                        _set_status(state, "holding")
                        _append_log("대기", f"{candle_key} 기준 보유 유지")
                    _write_state(state)
                    return
                _set_status(state, "holding")
                _append_log("대기", f"{candle_key} 기준 보유 유지")
                _write_state(state)
                return

            if current_position is not None and current_position["symbol"] != pending_symbol:
                current_symbol = current_position["symbol"]
                current_signal_symbol = current_position.get("signal_symbol", current_symbol)
                active_row = primary_row if current_signal_symbol == primary_symbol else secondary_row
                current_quantity = int(current_position["quantity"])
                _append_log("정보", f"{pending_candle or candle_key} 기준 목표 포지션 불일치 감지, 기존 보유분을 먼저 청산합니다.")
                _submit_live_order(
                    state,
                    current_symbol,
                    "sell",
                    current_quantity,
                    float(active_row["Close"]),
                    pending_reason or "목표 포지션 보정 청산",
                    target_time,
                    baseline_quantity=current_quantity,
                )
                fetch_domestic_balance.clear()
                positions, summary = fetch_domestic_balance()
                current_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
                if current_position is not None and current_position["symbol"] != pending_symbol:
                    _set_status(state, "holding")
                    _append_log("경고", "기존 포지션 청산이 아직 완전히 끝나지 않아 다음 주기에 스위칭을 이어서 시도합니다.")
                    _write_state(state)
                    return
                if current_position is not None and current_position["symbol"] == pending_symbol:
                    _clear_pending_target(state)
                    _set_status(state, "ordered")
                    _append_log("정보", f"{pending_candle or candle_key} 기준 반대 포지션 스위칭이 이미 반영되어 보유 상태를 유지합니다.")
                    _write_state(state)
                    return

                target_signal_symbol = TRADE_TO_SIGNAL_SYMBOL.get(pending_symbol, pending_symbol)
                target_row = primary_row if target_signal_symbol == primary_symbol else secondary_row
                buy_price = float(target_row["Close"])
                buy_quantity = _allocation_quantity(summary.get("orderable_cash", 0), buy_price)
                if buy_quantity <= 0:
                    _set_status(state, "waiting_cash")
                    _append_log("경고", f"{display_name(pending_symbol)} 스위칭 진입 가능 수량이 없어 다음 주기에 다시 확인합니다.")
                    _write_state(state)
                    return

                _append_log("정보", f"{pending_candle or candle_key} 기준 반대 포지션 청산 완료, {display_name(pending_symbol)} 진입을 이어서 시도합니다.")
                _append_log("정보", f"{pending_candle or candle_key} 기준 목표 포지션 미달성 감지, 진입 주문을 재시도합니다.")
                _submit_live_order(
                    state,
                    pending_symbol,
                    "buy",
                buy_quantity,
                buy_price,
                pending_reason or "buy open 진입",
                target_time,
                baseline_quantity=0,
            )
            fetch_domestic_balance.clear()
            positions, _ = fetch_domestic_balance()
            current_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
            if current_position is not None and current_position["symbol"] == pending_symbol:
                _clear_pending_target(state)
            _set_status(state, "ordered")
            _write_state(state)
            return

        if current_position is not None:
            _set_status(state, "holding")
            _append_log("대기", f"{candle_key} 기준 보유 유지")
            _write_state(state)
            return

        _set_status(state, "idle")
        _append_log("대기", f"{candle_key} 기준 진입 신호 없음")
        _write_state(state)
