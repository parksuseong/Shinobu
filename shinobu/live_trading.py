from __future__ import annotations

import json
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from shinobu.data import display_name, load_live_chart_data
from shinobu.kis import KisApiError, fetch_domestic_balance, place_domestic_order
from shinobu.strategy import StrategyAdjustments, calculate_scr_strategy


LIVE_ALLOCATION_KRW = 500_000.0
MAX_LIVE_ORDERS = 200
MAX_ASSET_HISTORY = 240
LIVE_FILL_CONFIRM_TIMEOUT_SECONDS = 8.0
LIVE_FILL_CONFIRM_POLL_SECONDS = 1.0
LIVE_STATE_FILE = Path(__file__).resolve().parent.parent / ".streamlit" / "live_state.json"
LIVE_LOG_FILE = Path(__file__).resolve().parent.parent / ".streamlit" / "live_trading.log"
_LIVE_STATE_LOCK = threading.RLock()


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _default_state() -> dict[str, Any]:
    return {
        "enabled": False,
        "started_at": "",
        "last_checked_candle": "",
        "last_cycle_at": "",
        "last_order_at": "",
        "last_asset_snapshot_order_at": "",
        "last_status": "stopped",
        "last_error": "",
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


def _append_log(level: str, message: str) -> None:
    with LIVE_LOG_FILE.open("a", encoding="utf-8") as log_file:
        log_file.write(f"{_now_text()}  [{level}]  {message}\n")
        log_file.flush()


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
) -> None:
    state["orders"].append(
        {
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": float(price),
            "reason": reason,
            "candle_time": candle_time.strftime("%Y-%m-%d %H:%M"),
            "timestamp": _now_text(),
        }
    )
    if len(state["orders"]) > MAX_LIVE_ORDERS:
        del state["orders"][:-MAX_LIVE_ORDERS]
    state["last_order_at"] = _now_text()


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


def get_live_logs(limit: int = 20) -> list[str]:
    with _LIVE_STATE_LOCK:
        _ensure_state_file()
        try:
            lines = LIVE_LOG_FILE.read_text(encoding="utf-8").splitlines()
        except OSError:
            lines = []
    lines = [line for line in lines if line.strip()]
    return list(reversed(lines[-limit:]))


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


def _load_strategy(symbol: str, adjustments: StrategyAdjustments) -> pd.DataFrame:
    frame = load_live_chart_data(symbol, "5분봉")
    return calculate_scr_strategy(frame, adjustments, "5분봉")


def _get_target_rows(primary: pd.DataFrame, secondary: pd.DataFrame) -> tuple[pd.Timestamp, pd.Series, pd.Series] | None:
    combined_index = primary.index.union(secondary.index).sort_values()
    if len(combined_index) < 2:
        return None

    target_time = combined_index[-2]
    aligned_primary = primary.reindex(combined_index).ffill()
    aligned_secondary = secondary.reindex(combined_index).ffill()
    return target_time, aligned_primary.loc[target_time], aligned_secondary.loc[target_time]


def _find_current_pair_position(positions: pd.DataFrame, symbols: list[str]) -> dict[str, Any] | None:
    if positions.empty or "code" not in positions.columns:
        return None

    target_codes = [symbol.replace(".KS", "") for symbol in symbols]
    matches = positions[positions["code"].isin(target_codes)]
    if matches.empty:
        return None

    sort_column = "eval_amount" if "eval_amount" in matches.columns else "quantity"
    row = matches.sort_values(sort_column, ascending=False).iloc[0]
    return {
        "symbol": f"{row['code']}.KS",
        "name": row.get("name", ""),
        "quantity": int(float(row.get("quantity", 0))),
        "current_price": float(row.get("current_price", 0)),
    }


def _choose_open_candidate(
    primary_symbol: str,
    secondary_symbol: str,
    primary_row: pd.Series,
    secondary_row: pd.Series,
) -> tuple[str, pd.Series] | None:
    candidates = []
    if bool(primary_row.get("buy_open", False)):
        candidates.append((primary_symbol, primary_row))
    if bool(secondary_row.get("buy_open", False)):
        candidates.append((secondary_symbol, secondary_row))
    if not candidates:
        return None
    return max(candidates, key=lambda item: float(item[1].get("scr_line", 0.0)))


def _allocation_quantity(orderable_cash: float, price: float) -> int:
    if price <= 0:
        return 0
    budget = min(float(orderable_cash), LIVE_ALLOCATION_KRW)
    return int(budget // price)


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


def _confirm_fill_after_order(symbol: str, side: str, baseline_quantity: int, expected_quantity: int) -> tuple[bool, str]:
    deadline = time.monotonic() + LIVE_FILL_CONFIRM_TIMEOUT_SECONDS
    while time.monotonic() <= deadline:
        fetch_domestic_balance.clear()
        positions, _ = fetch_domestic_balance()
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


def _submit_live_order(
    state: dict[str, Any],
    symbol: str,
    side: str,
    quantity: int,
    expected_price: float,
    reason: str,
    candle_time: pd.Timestamp,
    baseline_quantity: int,
) -> None:
    broker_output = place_domestic_order(symbol.replace(".KS", ""), side, quantity)
    _append_order(state, symbol, side, quantity, expected_price, reason, candle_time)
    _append_log(
        "주문",
        f"{display_name(symbol)} {side.upper()} {quantity}주 시장가 주문 접수 ({_format_order_response(broker_output)})",
    )
    filled, fill_message = _confirm_fill_after_order(symbol, side, baseline_quantity, quantity)
    _append_log("체결" if filled else "경고", f"{display_name(symbol)} {side.upper()} {quantity}주 {fill_message}")


def process_live_trading_cycle(
    primary_symbol: str,
    secondary_symbol: str,
    adjustments: StrategyAdjustments,
) -> None:
    with _LIVE_STATE_LOCK:
        state = _read_state()
        if not state["enabled"]:
            return

        _set_status(state, "checking")

        try:
            primary = _load_strategy(primary_symbol, adjustments)
            secondary = _load_strategy(secondary_symbol, adjustments)
        except Exception as exc:
            _set_status(state, "error", str(exc))
            _append_log("오류", f"실전 데이터 조회 실패: {exc}")
            _write_state(state)
            raise

        target_rows = _get_target_rows(primary, secondary)
        if target_rows is None:
            _set_status(state, "waiting_data")
            _append_log("대기", "완료된 5분봉이 아직 충분하지 않아 다음 주기를 기다립니다.")
            _write_state(state)
            return

        target_time, primary_row, secondary_row = target_rows
        candle_key = target_time.strftime("%Y-%m-%d %H:%M")
        if state["last_checked_candle"] == candle_key:
            _set_status(state, "idle")
            _write_state(state)
            return

        state["last_checked_candle"] = candle_key
        positions, summary = fetch_domestic_balance()
        current_position = _find_current_pair_position(positions, [primary_symbol, secondary_symbol])
        chosen_open = _choose_open_candidate(primary_symbol, secondary_symbol, primary_row, secondary_row)

        if current_position is not None:
            current_symbol = current_position["symbol"]
            current_quantity = int(current_position["quantity"])
            active_row = primary_row if current_symbol == primary_symbol else secondary_row

            if chosen_open is not None and chosen_open[0] != current_symbol:
                target_symbol, target_row = chosen_open
                _submit_live_order(
                    state,
                    current_symbol,
                    "sell",
                    current_quantity,
                    float(active_row["Close"]),
                    "반대 ETF 스위치 청산",
                    target_time,
                    baseline_quantity=current_quantity,
                )

                fetch_domestic_balance.clear()
                _, summary = fetch_domestic_balance()
                buy_price = float(target_row["Close"])
                buy_quantity = _allocation_quantity(summary.get("orderable_cash", 0), buy_price)
                if buy_quantity > 0:
                    _submit_live_order(
                        state,
                        target_symbol,
                        "buy",
                        buy_quantity,
                        buy_price,
                        "반대 ETF 진입",
                        target_time,
                        baseline_quantity=0,
                    )
                    _set_status(state, "ordered")
                else:
                    _set_status(state, "waiting_cash")
                    _append_log("경고", f"{display_name(target_symbol)} 매수 가능 수량이 없습니다.")
                _write_state(state)
                return

            if bool(active_row.get("buy_close", False)):
                _submit_live_order(
                    state,
                    current_symbol,
                    "sell",
                    current_quantity,
                    float(active_row["Close"]),
                    "지표 과열 청산",
                    target_time,
                    baseline_quantity=current_quantity,
                )
                _set_status(state, "ordered")
                _write_state(state)
                return

            _set_status(state, "holding")
            _append_log("대기", f"{candle_key} 기준 보유 유지")
            _write_state(state)
            return

        if chosen_open is None:
            _set_status(state, "idle")
            _append_log("대기", f"{candle_key} 기준 진입 신호 없음")
            _write_state(state)
            return

        target_symbol, target_row = chosen_open
        buy_price = float(target_row["Close"])
        buy_quantity = _allocation_quantity(summary.get("orderable_cash", 0), buy_price)
        if buy_quantity <= 0:
            _set_status(state, "waiting_cash")
            _append_log("경고", f"{display_name(target_symbol)} 매수 가능 수량이 없습니다.")
            _write_state(state)
            return

        _submit_live_order(
            state,
            target_symbol,
            "buy",
            buy_quantity,
            buy_price,
            "buy open 진입",
            target_time,
            baseline_quantity=0,
        )
        _set_status(state, "ordered")
        _write_state(state)
