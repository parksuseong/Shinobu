from __future__ import annotations

from typing import Any

from shinobu.chart_payload import build_chart_payload
from shinobu.strategy import StrategyAdjustments, normalize_strategy_name


def build_chart_payload_controlled(
    *,
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments,
    strategy_name: str,
    visible_business_days: int,
    include_markers: bool = True,
) -> dict[str, Any]:
    normalized_kind = kind if kind in {"raw", "overlay"} else "raw"
    normalized_strategy = normalize_strategy_name(strategy_name)
    normalized_visible_days = max(1, min(int(visible_business_days), 5))
    return build_chart_payload(
        normalized_kind,
        symbol,
        pair_symbol,
        adjustments,
        strategy_name=normalized_strategy,
        visible_business_days=normalized_visible_days,
        include_markers=bool(include_markers),
    )
