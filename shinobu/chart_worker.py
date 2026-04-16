from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from shinobu import data as market_data
from shinobu.live_trading import get_live_started_at
from shinobu.strategy import StrategyAdjustments
from shinobu.strategy_cache import calculate_strategy_cached


LIVE_TIMEFRAME = "5분봉"


@dataclass(frozen=True)
class ChartFrameBundle:
    include_scr: bool
    full_frame: pd.DataFrame
    full_pair_frame: pd.DataFrame | None
    visible_frame: pd.DataFrame
    visible_pair_frame: pd.DataFrame | None


def _filter_frame_from_live_start(frame: pd.DataFrame, started_at: pd.Timestamp | None) -> pd.DataFrame:
    if started_at is None:
        return frame
    before = frame.loc[frame.index < started_at]
    after = frame.loc[frame.index >= started_at]
    return pd.concat([before, after]).sort_index()


def _limit_frame_to_recent_business_days(
    frame: pd.DataFrame,
    *,
    max_days: int,
    max_candles: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    trade_days = pd.Index(pd.to_datetime(frame.index).normalize().unique()).sort_values()
    recent_days = trade_days[-int(max_days) :]
    limited = frame.loc[frame.index.normalize().isin(recent_days)].copy()
    return limited.tail(int(max_candles)).copy()


def _load_raw_frame(symbol: str) -> pd.DataFrame:
    frame = market_data.load_live_chart_data_for_strategy(symbol, LIVE_TIMEFRAME, "src_v2_adx")
    return frame.sort_index()


def _load_strategy_frame(
    symbol: str,
    adjustments: StrategyAdjustments,
    strategy_name: str,
) -> pd.DataFrame:
    frame = market_data.load_live_chart_data_for_strategy(symbol, LIVE_TIMEFRAME, strategy_name)
    calculated = calculate_strategy_cached(
        frame,
        adjustments,
        LIVE_TIMEFRAME,
        strategy_name=strategy_name,
        symbol=symbol,
    )
    return calculated.sort_index()


def collect_chart_frames(
    *,
    kind: str,
    symbol: str,
    pair_symbol: str | None,
    adjustments: StrategyAdjustments,
    strategy_name: str,
    visible_business_days: int,
    max_candles: int,
) -> ChartFrameBundle:
    started_at = get_live_started_at()
    include_scr = kind == "overlay"

    if include_scr:
        full_frame = _load_strategy_frame(symbol, adjustments, strategy_name)
        full_pair_frame = _load_strategy_frame(pair_symbol, adjustments, strategy_name) if pair_symbol else None
    else:
        full_frame = _load_raw_frame(symbol)
        full_pair_frame = _load_raw_frame(pair_symbol) if pair_symbol else None

    filtered_frame = _filter_frame_from_live_start(full_frame, started_at)
    visible_frame = _limit_frame_to_recent_business_days(
        filtered_frame,
        max_days=visible_business_days,
        max_candles=max_candles,
    )

    visible_pair_frame: pd.DataFrame | None = None
    if full_pair_frame is not None:
        filtered_pair = _filter_frame_from_live_start(full_pair_frame, started_at)
        visible_pair_frame = _limit_frame_to_recent_business_days(
            filtered_pair,
            max_days=visible_business_days,
            max_candles=max_candles,
        )

    return ChartFrameBundle(
        include_scr=include_scr,
        full_frame=full_frame,
        full_pair_frame=full_pair_frame,
        visible_frame=visible_frame,
        visible_pair_frame=visible_pair_frame,
    )
