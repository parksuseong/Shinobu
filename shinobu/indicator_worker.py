from __future__ import annotations

import threading
from typing import Iterable

from shinobu import data as market_data
from shinobu.strategy import (
    StrategyAdjustments,
    get_strategy_history_business_days,
    normalize_strategy_name,
)
from shinobu.strategy_cache import calculate_strategy_cached


_WORKER_LOCK = threading.Lock()
_STARTED_KEYS: set[str] = set()


def _dedupe(items: Iterable[str | None]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def precompute_indicator_data(
    *,
    symbols: list[str],
    strategy_names: list[str],
    adjustments: StrategyAdjustments | None = None,
    timeframe_label: str = "5분봉",
) -> None:
    current_adjustments = adjustments or StrategyAdjustments()
    normalized_strategies = _dedupe([normalize_strategy_name(name) for name in strategy_names])
    normalized_symbols = _dedupe(symbols)
    if not normalized_symbols or not normalized_strategies:
        return

    # Reuse the largest-history frame to reduce duplicate collection latency.
    seed_strategy = max(normalized_strategies, key=get_strategy_history_business_days)

    for symbol in normalized_symbols:
        base_frame = market_data.load_live_chart_data_for_strategy(symbol, timeframe_label, seed_strategy)
        for strategy_name in normalized_strategies:
            calculate_strategy_cached(
                base_frame,
                adjustments=current_adjustments,
                timeframe_label=timeframe_label,
                strategy_name=strategy_name,
                symbol=symbol,
            )


def ensure_indicator_worker_bundle(
    *,
    primary_symbol: str,
    pair_symbol: str | None,
    strategy_names: list[str],
    adjustments: StrategyAdjustments | None = None,
    timeframe_label: str = "5분봉",
) -> None:
    current_adjustments = adjustments or StrategyAdjustments()
    symbols = _dedupe([primary_symbol, pair_symbol])
    strategies = _dedupe([normalize_strategy_name(name) for name in strategy_names])
    if not symbols or not strategies:
        return

    bundle_key = "|".join(
        [
            ",".join(symbols),
            ",".join(strategies),
            timeframe_label,
            f"s{current_adjustments.stoch_pct}_c{current_adjustments.cci_pct}_r{current_adjustments.rsi_pct}",
        ]
    )

    with _WORKER_LOCK:
        if bundle_key in _STARTED_KEYS:
            return
        _STARTED_KEYS.add(bundle_key)

    def _runner() -> None:
        try:
            precompute_indicator_data(
                symbols=symbols,
                strategy_names=strategies,
                adjustments=current_adjustments,
                timeframe_label=timeframe_label,
            )
        except Exception:
            with _WORKER_LOCK:
                _STARTED_KEYS.discard(bundle_key)

    thread = threading.Thread(target=_runner, daemon=True, name="shinobu-indicator-worker")
    thread.start()
