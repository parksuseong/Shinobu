#!/usr/bin/env python3
from __future__ import annotations

import signal
import sys
import time
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from shinobu.live_trading import (
    StrategyAdjustments,
    append_live_log,
    get_live_strategy_name,
    init_live_state,
    is_live_enabled,
    process_live_trading_cycle,
    set_live_enabled,
)

PRIMARY_SYMBOL = "122630.KS"
PAIR_SYMBOL = "252670.KS"
LOOP_SECONDS = 5.0

_running = True


def _stop_handler(signum: int, frame) -> None:  # type: ignore[no-untyped-def]
    global _running
    _running = False


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
