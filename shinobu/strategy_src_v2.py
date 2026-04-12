from __future__ import annotations

import pandas as pd

from shinobu.strategy_src import SrcAdjustments
from shinobu.strategy_src_v2_core import SrcV2Config, calculate_src_v2_core_strategy


SRC_V2_NORMAL_CONFIG = SrcV2Config(
    strategy_key="src_v2_normal",
    profile_label="SRC V2 Normal",
    stoch_oversold=10.0,
    stoch_overbought=80.0,
    cci_oversold=-130.0,
    cci_overbought=150.0,
    rsi_oversold=44.0,
    rsi_overbought=75.0,
    open_prev_need=3,
    open_cross_need=2,
    close_need=3,
    stop_loss_pct=0.03,
    trailing_stop_pct=0.05,
)


def calculate_src_v2_strategy(
    frame: pd.DataFrame,
    adjustments: SrcAdjustments | None = None,
    timeframe_label: str | None = None,
    initial_state: dict[str, object] | None = None,
) -> pd.DataFrame:
    return calculate_src_v2_core_strategy(
        frame=frame,
        config=SRC_V2_NORMAL_CONFIG,
        adjustments=adjustments,
        timeframe_label=timeframe_label,
        initial_state=initial_state,
    )
