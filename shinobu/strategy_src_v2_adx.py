from __future__ import annotations

from shinobu.strategy_src import SrcAdjustments
from shinobu.strategy_src_v2_core import SrcV2Config, calculate_src_v2_core_strategy


SRC_V2_ADX_CONFIG = SrcV2Config(
    strategy_key="src_v2_adx",
    profile_label="SRC V2 ADX",
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
    use_daily_adx_filter=True,
    adx_threshold=25.0,
    strict_stoch_oversold=8.0,
    strict_cci_oversold=-150.0,
    strict_rsi_oversold=42.0,
    strict_open_prev_need=3,
    strict_open_cross_need=3,
    strict_scr_threshold=0.20,
)


def calculate_src_v2_adx_strategy(
    frame,
    adjustments: SrcAdjustments | None = None,
    timeframe_label: str | None = None,
    initial_state: dict[str, object] | None = None,
):
    return calculate_src_v2_core_strategy(
        frame=frame,
        config=SRC_V2_ADX_CONFIG,
        adjustments=adjustments,
        timeframe_label=timeframe_label,
        initial_state=initial_state,
    )
