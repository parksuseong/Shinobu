from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from shinobu.strategy_src import (
    SrcAdjustments,
    _calculate_cci,
    _calculate_rsi,
    _calculate_stochastic_fast,
)


ADX_PERIOD = 14


@dataclass(frozen=True)
class SrcV2Config:
    strategy_key: str
    profile_label: str
    stoch_oversold: float
    stoch_overbought: float
    cci_oversold: float
    cci_overbought: float
    rsi_oversold: float
    rsi_overbought: float
    open_prev_need: int
    open_cross_need: int
    close_need: int
    stop_loss_pct: float
    trailing_stop_pct: float
    use_daily_adx_filter: bool = False
    adx_threshold: float = 25.0
    strict_stoch_oversold: float = 8.0
    strict_cci_oversold: float = -150.0
    strict_rsi_oversold: float = 42.0
    strict_open_prev_need: int = 3
    strict_open_cross_need: int = 3
    strict_scr_threshold: float = 0.2


def _calculate_scr_line(strategy: pd.DataFrame) -> pd.Series:
    scr_line = (
        ((strategy["stoch"] - 50.0) / 50.0)
        + (strategy["cci"] / 200.0)
        + ((strategy["rsi"] - 50.0) / 50.0)
    ) / 3.0
    return scr_line.clip(-1.5, 1.5)


def _calculate_adx(frame: pd.DataFrame, period: int = ADX_PERIOD) -> pd.Series:
    high = frame["High"]
    low = frame["Low"]
    close = frame["Close"]

    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = up_move.where((up_move > down_move) & (up_move > 0), 0.0)
    minus_dm = down_move.where((down_move > up_move) & (down_move > 0), 0.0)

    true_range = pd.concat(
        [
            high - low,
            (high - close.shift(1)).abs(),
            (low - close.shift(1)).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = true_range.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, pd.NA))
    minus_di = 100 * (minus_dm.ewm(alpha=1 / period, adjust=False, min_periods=period).mean() / atr.replace(0, pd.NA))
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, pd.NA)) * 100
    return dx.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()


def _join_daily_adx(frame: pd.DataFrame) -> pd.DataFrame:
    daily = frame.resample("1D").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    ).dropna()
    daily["daily_adx"] = _calculate_adx(daily)

    enriched = frame.copy()
    enriched["trade_day"] = enriched.index.normalize()
    enriched = enriched.join(daily[["daily_adx"]], on="trade_day")
    enriched["daily_adx"] = enriched["daily_adx"].fillna(0.0)
    return enriched.drop(columns=["trade_day"])


def calculate_src_v2_core_strategy(
    frame: pd.DataFrame,
    config: SrcV2Config,
    adjustments: SrcAdjustments | None = None,
    timeframe_label: str | None = None,
    initial_state: dict[str, object] | None = None,
) -> pd.DataFrame:
    _ = adjustments
    _ = timeframe_label

    strategy = frame.copy()
    strategy["stoch"] = _calculate_stochastic_fast(strategy)
    strategy["cci"] = _calculate_cci(strategy)
    strategy["rsi"] = _calculate_rsi(strategy["Close"])
    strategy["scr_line"] = _calculate_scr_line(strategy)

    if config.use_daily_adx_filter:
        strategy = _join_daily_adx(strategy)
    else:
        strategy["daily_adx"] = 0.0

    strategy["oversold_count"] = (
        (strategy["stoch"] <= config.stoch_oversold).astype(int)
        + (strategy["cci"] <= config.cci_oversold).astype(int)
        + (strategy["rsi"] <= config.rsi_oversold).astype(int)
    )
    strategy["overbought_count"] = (
        (strategy["stoch"] >= config.stoch_overbought).astype(int)
        + (strategy["cci"] >= config.cci_overbought).astype(int)
        + (strategy["rsi"] >= config.rsi_overbought).astype(int)
    )

    if config.use_daily_adx_filter:
        strategy["strict_oversold_count"] = (
            (strategy["stoch"] <= config.strict_stoch_oversold).astype(int)
            + (strategy["cci"] <= config.strict_cci_oversold).astype(int)
            + (strategy["rsi"] <= config.strict_rsi_oversold).astype(int)
        )
    previous = strategy.shift(1)
    cross_up_count = (
        ((previous["stoch"] <= config.stoch_oversold) & (strategy["stoch"] > config.stoch_oversold)).astype(int)
        + ((previous["cci"] <= config.cci_oversold) & (strategy["cci"] > config.cci_oversold)).astype(int)
        + ((previous["rsi"] <= config.rsi_oversold) & (strategy["rsi"] > config.rsi_oversold)).astype(int)
    )
    raw_buy_open = (
        (previous["oversold_count"].fillna(0).astype(int) >= config.open_prev_need)
        & (cross_up_count >= config.open_cross_need)
    ).fillna(False)

    if config.use_daily_adx_filter:
        strict_cross_up_count = (
            ((previous["stoch"] <= config.strict_stoch_oversold) & (strategy["stoch"] > config.strict_stoch_oversold)).astype(int)
            + ((previous["cci"] <= config.strict_cci_oversold) & (strategy["cci"] > config.strict_cci_oversold)).astype(int)
            + ((previous["rsi"] <= config.strict_rsi_oversold) & (strategy["rsi"] > config.strict_rsi_oversold)).astype(int)
        )
        raw_buy_open = (
            (~(strategy["daily_adx"] >= config.adx_threshold) & raw_buy_open)
            | (
                (strategy["daily_adx"] >= config.adx_threshold)
                & (previous["strict_oversold_count"].fillna(0).astype(int) >= config.strict_open_prev_need)
                & (strict_cross_up_count >= config.strict_open_cross_need)
                & (strategy["scr_line"] >= config.strict_scr_threshold)
            )
        ).fillna(False)

    raw_overheat_close = (
        (previous["overbought_count"].fillna(0).astype(int) < config.close_need)
        & (strategy["overbought_count"] >= config.close_need)
    ).fillna(False)

    buy_open_flags: list[bool] = []
    buy_close_flags: list[bool] = []
    signal_detail: list[str] = []
    state = initial_state or {}
    entry_price = float(state["entry_price"]) if state.get("entry_price") is not None else None
    highest_price = float(state["highest_price"]) if state.get("highest_price") is not None else None
    in_position = bool(state.get("in_position", False))

    for timestamp, row in strategy.iterrows():
        buy_open = False
        buy_close = False
        detail = ""

        close_price = float(row["Close"])
        high_price = float(row["High"])

        if not in_position and bool(raw_buy_open.loc[timestamp]):
            buy_open = True
            detail = "과매도 반등 진입"
            if config.use_daily_adx_filter and float(row.get("daily_adx", 0.0)) >= config.adx_threshold:
                detail = f"ADX {config.adx_threshold:.0f} 필터 진입"
            in_position = True
            entry_price = close_price
            highest_price = high_price
        elif in_position:
            highest_price = max(float(highest_price or high_price), high_price)
            stop_loss_hit = entry_price is not None and close_price <= entry_price * (1 - config.stop_loss_pct)
            trailing_stop_hit = highest_price is not None and close_price <= highest_price * (1 - config.trailing_stop_pct)
            overheat_hit = bool(raw_overheat_close.loc[timestamp])

            if stop_loss_hit:
                buy_close = True
                detail = f"손절 -{config.stop_loss_pct * 100:.1f}%"
            elif trailing_stop_hit:
                buy_close = True
                detail = f"트레일링 스탑 -{config.trailing_stop_pct * 100:.1f}%"
            elif overheat_hit:
                buy_close = True
                detail = "과열 청산"

            if buy_close:
                in_position = False
                entry_price = None
                highest_price = None

        buy_open_flags.append(buy_open)
        buy_close_flags.append(buy_close)
        signal_detail.append(detail)

    strategy["buy_open"] = pd.Series(buy_open_flags, index=strategy.index, dtype=bool)
    strategy["buy_close"] = pd.Series(buy_close_flags, index=strategy.index, dtype=bool)
    strategy["sell_open"] = False
    strategy["sell_close"] = False
    strategy["signal"] = ""
    strategy["signal_detail"] = pd.Series(signal_detail, index=strategy.index, dtype="string").fillna("")
    strategy.loc[strategy["buy_open"], "signal"] = "buy open"
    strategy.loc[strategy["buy_close"], "signal"] = "buy close"

    strategy.attrs["thresholds"] = {
        "stoch_oversold": config.stoch_oversold,
        "stoch_overbought": config.stoch_overbought,
        "cci_oversold": config.cci_oversold,
        "cci_overbought": config.cci_overbought,
        "rsi_oversold": config.rsi_oversold,
        "rsi_overbought": config.rsi_overbought,
        "open_prev_need": config.open_prev_need,
        "open_cross_need": config.open_cross_need,
        "close_need": config.close_need,
        "stop_loss_pct": config.stop_loss_pct * 100.0,
        "trailing_stop_pct": config.trailing_stop_pct * 100.0,
        "use_daily_adx_filter": config.use_daily_adx_filter,
        "adx_threshold": config.adx_threshold if config.use_daily_adx_filter else None,
        "strict_open_prev_need": config.strict_open_prev_need if config.use_daily_adx_filter else None,
        "strict_open_cross_need": config.strict_open_cross_need if config.use_daily_adx_filter else None,
        "strict_scr_threshold": config.strict_scr_threshold if config.use_daily_adx_filter else None,
        "profile_label": config.profile_label,
        "profile_name": config.strategy_key,
        "strategy_name": "SRC_V2",
    }
    return strategy
