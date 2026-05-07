#!/usr/bin/env python3
from __future__ import annotations

import signal
import sys
import time
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shinobu import data as market_data
from shinobu.cache_db import (
    acquire_named_lock,
    align_raw_intraday_pair_to_intersection,
    clear_chart_payload_caches,
    has_raw_intraday_mismatch,
    release_named_lock,
)
from shinobu.live_trading import (
    StrategyAdjustments,
    append_live_log,
    get_live_strategy_name,
    init_live_state,
    is_live_enabled,
    process_live_trading_cycle,
    set_live_enabled,
)
from shinobu.strategy_cache import calculate_strategy_cached

PRIMARY_SYMBOL = "122630.KS"
PAIR_SYMBOL = "252670.KS"
LOOP_SECONDS = 5.0
PAIR_RECOVERY_INTERVAL_SECONDS = 60.0
PAIR_RECOVERY_IGNORE_RECENT_MINUTES = 10
PAIR_RECOVERY_LOCK_NAME = "pair_candle_recovery"

_running = True
_pair_recovery_last_run_monotonic = 0.0


def _stop_handler(signum: int, frame) -> None:  # type: ignore[no-untyped-def]
    global _running
    _running = False


def _get_pair_recovery_ignore_recent_minutes(now: pd.Timestamp | None = None) -> int:
    ts = pd.Timestamp.now(tz=market_data.KST) if now is None else pd.Timestamp(now)
    if ts.tzinfo is None:
        ts = ts.tz_localize(market_data.KST)
    else:
        ts = ts.tz_convert(market_data.KST)
    if ts.weekday() >= 5:
        return 0
    current_minutes = int(ts.hour) * 60 + int(ts.minute)
    market_open_minutes = 9 * 60
    market_close_minutes = 15 * 60 + 30
    if current_minutes < market_open_minutes or current_minutes >= market_close_minutes:
        return 0
    return PAIR_RECOVERY_IGNORE_RECENT_MINUTES


def _run_pair_candle_recovery_if_due(adjustments: StrategyAdjustments, strategy_name: str) -> None:
    global _pair_recovery_last_run_monotonic
    now_monotonic = time.monotonic()
    if (now_monotonic - _pair_recovery_last_run_monotonic) < PAIR_RECOVERY_INTERVAL_SECONDS:
        return
    _pair_recovery_last_run_monotonic = now_monotonic

    if not acquire_named_lock(PAIR_RECOVERY_LOCK_NAME, stale_after_seconds=120):
        return
    try:
        for symbol in (PRIMARY_SYMBOL, PAIR_SYMBOL):
            market_data._load_live_chart_data_impl(symbol, "5분봉", lookback_days=7)

        raw_primary = market_data.display_symbol(PRIMARY_SYMBOL)
        raw_pair = market_data.display_symbol(PAIR_SYMBOL)
        ignore_recent_minutes = _get_pair_recovery_ignore_recent_minutes()

        if not has_raw_intraday_mismatch(
            symbol_a=raw_primary,
            symbol_b=raw_pair,
            timeframe="5분봉",
            ignore_recent_minutes=ignore_recent_minutes,
        ):
            return

        recovery = align_raw_intraday_pair_to_intersection(
            symbol_a=raw_primary,
            symbol_b=raw_pair,
            timeframe="5분봉",
            ignore_recent_minutes=ignore_recent_minutes,
        )
        deleted_total = int(recovery.get("deleted_total", 0) or 0)
        if deleted_total <= 0:
            return

        lookback_days = max(int(pd.Timestamp.now(tz=market_data.KST).dayofyear * 2), 7)
        for symbol in (PRIMARY_SYMBOL, PAIR_SYMBOL):
            source_frame = market_data._load_live_chart_data_impl(symbol, "5분봉", lookback_days=lookback_days)
            calculate_strategy_cached(
                source_frame,
                adjustments,
                "5분봉",
                strategy_name=strategy_name,
                symbol=symbol,
            )
        clear_chart_payload_caches()
        append_live_log(
            "복구",
            f"캔들 리커버리 완료: 총 {deleted_total}개 정리 "
            f"(롱삭제:{int(recovery.get('deleted_a', 0) or 0)}, 숏삭제:{int(recovery.get('deleted_b', 0) or 0)})",
        )
    except Exception as exc:
        append_live_log("오류", f"캔들 리커버리 실패: {exc}")
    finally:
        release_named_lock(PAIR_RECOVERY_LOCK_NAME)


def main() -> None:
    global _running
    signal.signal(signal.SIGINT, _stop_handler)
    signal.signal(signal.SIGTERM, _stop_handler)

    init_live_state()
    if not is_live_enabled():
        set_live_enabled(True)

    adjustments = StrategyAdjustments(stoch_pct=0, cci_pct=0, rsi_pct=0)
    append_live_log("정보", "백그라운드 실전 엔진 시작")

    while _running:
        strategy_name = get_live_strategy_name()
        try:
            _run_pair_candle_recovery_if_due(adjustments, strategy_name)
            process_live_trading_cycle(
                PRIMARY_SYMBOL,
                PAIR_SYMBOL,
                adjustments,
                strategy_name=strategy_name,
            )
        except Exception as exc:
            append_live_log("오류", f"백그라운드 엔진 사이클 실패: {exc}")
        time.sleep(LOOP_SECONDS)

    append_live_log("정보", "백그라운드 실전 엔진 종료")


if __name__ == "__main__":
    main()
