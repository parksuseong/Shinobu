from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from shinobu import data as market_data
from shinobu.live_trading import get_live_started_at
from shinobu.strategy import (
    DEFAULT_STRATEGY_NAME,
    StrategyAdjustments,
    get_strategy_history_business_days,
)
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


def _limit_frame_to_date_range(
    frame: pd.DataFrame,
    *,
    start_date: str,
    end_date: str,
    max_candles: int,
) -> pd.DataFrame:
    if frame.empty:
        return frame
    if not start_date or not end_date:
        return frame.tail(int(max_candles)).copy()
    try:
        start_ts = pd.Timestamp(start_date)
        end_ts = pd.Timestamp(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    except Exception:
        return frame.tail(int(max_candles)).copy()
    limited = frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()
    # For explicit date-range queries, return the full requested window.
    # Truncating to max_candles makes older candles disappear unexpectedly.
    return limited


def _load_raw_frame(symbol: str) -> pd.DataFrame:
    lookback_days = market_data._business_days_to_lookback_days(get_strategy_history_business_days(DEFAULT_STRATEGY_NAME))
    frame = market_data.load_live_chart_data_cached_only(symbol, LIVE_TIMEFRAME, lookback_days=lookback_days)
    return frame.sort_index()


def _load_strategy_frame(
    symbol: str,
    adjustments: StrategyAdjustments,
    strategy_name: str,
    lookback_days_override: int | None = None,
) -> pd.DataFrame:
    lookback_days = int(lookback_days_override or market_data._business_days_to_lookback_days(get_strategy_history_business_days(strategy_name)))
    frame = market_data.load_live_chart_data_cached_only(symbol, LIVE_TIMEFRAME, lookback_days=lookback_days)
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
    start_date: str = "",
    end_date: str = "",
    max_candles: int,
) -> ChartFrameBundle:
    started_at = get_live_started_at()
    include_scr = kind == "overlay"
    lookback_days_override: int | None = None
    if start_date:
        try:
            start_ts = pd.Timestamp(start_date)
            now_ts = pd.Timestamp.now(tz=None)
            lookback_days_override = max(int((now_ts - start_ts).days) + 3, 5)
        except Exception:
            lookback_days_override = None

    if include_scr:
        full_frame = _load_strategy_frame(symbol, adjustments, strategy_name, lookback_days_override=lookback_days_override)
        full_pair_frame = (
            _load_strategy_frame(pair_symbol, adjustments, strategy_name, lookback_days_override=lookback_days_override)
            if pair_symbol
            else None
        )
    else:
        if lookback_days_override is None:
            full_frame = _load_raw_frame(symbol)
            full_pair_frame = _load_raw_frame(pair_symbol) if pair_symbol else None
        else:
            full_frame = market_data.load_live_chart_data_cached_only(symbol, LIVE_TIMEFRAME, lookback_days=lookback_days_override).sort_index()
            full_pair_frame = (
                market_data.load_live_chart_data_cached_only(pair_symbol, LIVE_TIMEFRAME, lookback_days=lookback_days_override).sort_index()
                if pair_symbol
                else None
            )

    filtered_frame = _filter_frame_from_live_start(full_frame, started_at)
    if start_date and end_date:
        visible_frame = _limit_frame_to_date_range(
            filtered_frame,
            start_date=start_date,
            end_date=end_date,
            max_candles=max_candles,
        )
    else:
        visible_frame = _limit_frame_to_recent_business_days(
            filtered_frame,
            max_days=visible_business_days,
            max_candles=max_candles,
        )

    visible_pair_frame: pd.DataFrame | None = None
    if full_pair_frame is not None:
        filtered_pair = _filter_frame_from_live_start(full_pair_frame, started_at)
        if start_date and end_date:
            visible_pair_frame = _limit_frame_to_date_range(
                filtered_pair,
                start_date=start_date,
                end_date=end_date,
                max_candles=max_candles,
            )
        else:
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
