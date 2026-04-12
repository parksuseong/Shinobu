from __future__ import annotations

import re
import threading
from pathlib import Path
from typing import Any

import pandas as pd

from shinobu.strategy import (
    StrategyAdjustments,
    calculate_strategy,
    get_strategy_history_business_days,
    normalize_strategy_name,
)


CACHE_VERSION = 2
_CACHE_LOCK = threading.RLock()
_CACHE_DIR = Path(__file__).resolve().parent.parent / ".streamlit" / "strategy_cache"

_ROWS_PER_BUSINESS_DAY = {
    "1분봉": 400,
    "5분봉": 80,
    "15분봉": 30,
    "30분봉": 20,
    "1시간봉": 10,
    "4시간봉": 3,
    "일봉": 1,
    "주봉": 1,
    "월봉": 1,
}


def _sanitize(text: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", text)


def _normalize_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [column for column in ("Open", "High", "Low", "Close", "Volume") if column in frame.columns]
    normalized = frame.loc[:, columns].copy()
    normalized.index = pd.DatetimeIndex(frame.index)
    return normalized


def _cache_path(
    *,
    symbol: str,
    timeframe_label: str,
    strategy_name: str,
    adjustments: StrategyAdjustments,
) -> Path:
    adjustment_key = f"s{adjustments.stoch_pct}_c{adjustments.cci_pct}_r{adjustments.rsi_pct}"
    filename = "_".join(
        [
            _sanitize(symbol),
            _sanitize(timeframe_label),
            _sanitize(normalize_strategy_name(strategy_name)),
            adjustment_key,
        ]
    )
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / f"{filename}.pkl"


def _source_signature(frame: pd.DataFrame) -> dict[str, Any]:
    if frame.empty:
        return {"rows": 0}

    last_row = frame.iloc[-1]
    return {
        "rows": int(len(frame)),
        "start": pd.Timestamp(frame.index.min()).isoformat(),
        "end": pd.Timestamp(frame.index.max()).isoformat(),
        "last_open": float(last_row.get("Open", 0) or 0),
        "last_high": float(last_row.get("High", 0) or 0),
        "last_low": float(last_row.get("Low", 0) or 0),
        "last_close": float(last_row.get("Close", 0) or 0),
        "last_volume": float(last_row.get("Volume", 0) or 0),
    }


def _rows_per_business_day(timeframe_label: str | None) -> int:
    return int(_ROWS_PER_BUSINESS_DAY.get(timeframe_label or "", 80))


def _recalc_warmup_rows(strategy_name: str, timeframe_label: str | None) -> int:
    rows_per_day = _rows_per_business_day(timeframe_label)
    history_days = get_strategy_history_business_days(strategy_name)
    if normalize_strategy_name(strategy_name) == "src_v2_adx":
        return max(rows_per_day * history_days, rows_per_day * 5)
    return max(rows_per_day * 4, 200)


def _is_prefix_match(current_frame: pd.DataFrame, cached_source: pd.DataFrame) -> bool:
    if len(cached_source) > len(current_frame):
        return False
    if len(cached_source) == 0:
        return True
    current_prefix = _normalize_source_frame(current_frame.iloc[: len(cached_source)])
    cached_prefix = _normalize_source_frame(cached_source)
    return current_prefix.equals(cached_prefix)


def _derive_initial_state(
    source_frame: pd.DataFrame,
    strategy_frame: pd.DataFrame,
) -> dict[str, object]:
    in_position = False
    entry_price: float | None = None
    highest_price: float | None = None

    for timestamp in source_frame.index:
        signal_row = strategy_frame.loc[timestamp]
        row = source_frame.loc[timestamp]
        high_price = float(row.get("High", 0) or 0)
        close_price = float(row.get("Close", 0) or 0)

        if bool(signal_row.get("buy_open", False)):
            in_position = True
            entry_price = close_price
            highest_price = high_price
            continue

        if in_position:
            highest_price = max(float(highest_price or high_price), high_price)
            if bool(signal_row.get("buy_close", False)):
                in_position = False
                entry_price = None
                highest_price = None

    return {
        "in_position": in_position,
        "entry_price": entry_price,
        "highest_price": highest_price,
    }


def _calculate_incremental(
    frame: pd.DataFrame,
    cached_source: pd.DataFrame,
    cached_frame: pd.DataFrame,
    *,
    adjustments: StrategyAdjustments,
    timeframe_label: str | None,
    strategy_name: str,
) -> pd.DataFrame:
    appended_rows = len(frame) - len(cached_source)
    warmup_rows = _recalc_warmup_rows(strategy_name, timeframe_label)
    merge_start = max(0, len(cached_source) - appended_rows - warmup_rows)

    prefix_source = _normalize_source_frame(frame.iloc[:merge_start])
    prefix_strategy = cached_frame.iloc[:merge_start].copy()
    initial_state = _derive_initial_state(prefix_source, prefix_strategy) if merge_start > 0 else {}

    tail_frame = frame.iloc[merge_start:].copy()
    recalculated_tail = calculate_strategy(
        frame=tail_frame,
        adjustments=adjustments,
        timeframe_label=timeframe_label,
        strategy_name=strategy_name,
        initial_state=initial_state,
    )

    if merge_start <= 0:
        return recalculated_tail

    merged = pd.concat([prefix_strategy, recalculated_tail], axis=0)
    merged = merged[~merged.index.duplicated(keep="last")]
    return merged.sort_index()


def _write_payload(cache_path: Path, source_frame: pd.DataFrame, strategy_frame: pd.DataFrame) -> None:
    pd.to_pickle(
        {
            "version": CACHE_VERSION,
            "signature": _source_signature(source_frame),
            "source_frame": _normalize_source_frame(source_frame),
            "frame": strategy_frame,
        },
        cache_path,
    )


def calculate_strategy_cached(
    frame: pd.DataFrame,
    adjustments: StrategyAdjustments | None = None,
    timeframe_label: str | None = None,
    strategy_name: str | None = None,
    symbol: str = "",
) -> pd.DataFrame:
    current_adjustments = adjustments or StrategyAdjustments()
    normalized_strategy = normalize_strategy_name(strategy_name)
    cache_path = _cache_path(
        symbol=symbol or "unknown",
        timeframe_label=timeframe_label or "unknown",
        strategy_name=normalized_strategy,
        adjustments=current_adjustments,
    )
    normalized_source = _normalize_source_frame(frame)
    signature = _source_signature(normalized_source)

    with _CACHE_LOCK:
        try:
            payload = pd.read_pickle(cache_path)
        except Exception:
            payload = None

        if isinstance(payload, dict) and payload.get("version") == CACHE_VERSION:
            cached_frame = payload.get("frame")
            cached_source = payload.get("source_frame")
            if isinstance(cached_frame, pd.DataFrame) and isinstance(cached_source, pd.DataFrame):
                if payload.get("signature") == signature:
                    return cached_frame
                if _is_prefix_match(normalized_source, cached_source):
                    merged_result = _calculate_incremental(
                        frame=frame,
                        cached_source=cached_source,
                        cached_frame=cached_frame,
                        adjustments=current_adjustments,
                        timeframe_label=timeframe_label,
                        strategy_name=normalized_strategy,
                    )
                    _write_payload(cache_path, normalized_source, merged_result)
                    return merged_result

        result = calculate_strategy(
            frame=frame,
            adjustments=current_adjustments,
            timeframe_label=timeframe_label,
            strategy_name=normalized_strategy,
        )
        _write_payload(cache_path, normalized_source, result)
        return result
