from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


STOCH_PERIOD = 14
CCI_PERIOD = 20
RSI_PERIOD = 14


@dataclass(frozen=True)
class SrcAdjustments:
    stoch_pct: int = 0
    cci_pct: int = 0
    rsi_pct: int = 0


@dataclass(frozen=True)
class SrcThresholds:
    stoch_oversold: float
    stoch_overbought: float
    cci_oversold: float
    cci_overbought: float
    rsi_oversold: float
    rsi_overbought: float


@dataclass(frozen=True)
class SrcProfile:
    stoch_oversold: float
    stoch_overbought: float
    cci_oversold: float
    cci_overbought: float
    rsi_oversold: float
    rsi_overbought: float
    open_prev_need: int
    open_cross_need: int
    close_need: int
    label: str


DEFAULT_SRC_PROFILE_NAME = "normal"

NORMAL_5M_PROFILE = SrcProfile(
    stoch_oversold=20.0,
    stoch_overbought=80.0,
    cci_oversold=-100.0,
    cci_overbought=100.0,
    rsi_oversold=30.0,
    rsi_overbought=70.0,
    open_prev_need=3,
    open_cross_need=3,
    close_need=3,
    label="Normal",
)

ACTIVE_5M_PROFILE = SrcProfile(
    stoch_oversold=38.0,
    stoch_overbought=78.0,
    cci_oversold=-60.0,
    cci_overbought=96.0,
    rsi_oversold=42.0,
    rsi_overbought=66.0,
    open_prev_need=2,
    open_cross_need=1,
    close_need=2,
    label="Active",
)

DEFAULT_SRC_PROFILE = NORMAL_5M_PROFILE

SRC_PROFILE_OPTIONS = {
    "normal": NORMAL_5M_PROFILE,
    "active": ACTIVE_5M_PROFILE,
}

_LEGACY_PROFILE_ALIASES = {
    "original": "normal",
    "aggressive": "active",
    "defensive": "normal",
}


def _calculate_rsi(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)
    avg_gain = gain.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    avg_loss = loss.ewm(alpha=1 / period, adjust=False, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))


def _calculate_cci(frame: pd.DataFrame, period: int = CCI_PERIOD) -> pd.Series:
    typical_price = (frame["High"] + frame["Low"] + frame["Close"]) / 3
    sma = typical_price.rolling(period).mean()
    mean_deviation = typical_price.rolling(period).apply(
        lambda values: (abs(values - values.mean())).mean(),
        raw=False,
    )
    return (typical_price - sma) / (0.015 * mean_deviation.replace(0, pd.NA))


def _calculate_stochastic_fast(frame: pd.DataFrame, period: int = STOCH_PERIOD) -> pd.Series:
    lowest_low = frame["Low"].rolling(period).min()
    highest_high = frame["High"].rolling(period).max()
    denominator = (highest_high - lowest_low).replace(0, pd.NA)
    return ((frame["Close"] - lowest_low) / denominator) * 100


def normalize_src_profile_name(profile_name: str | None) -> str:
    candidate = (profile_name or DEFAULT_SRC_PROFILE_NAME).strip().lower()
    candidate = _LEGACY_PROFILE_ALIASES.get(candidate, candidate)
    return candidate if candidate in SRC_PROFILE_OPTIONS else DEFAULT_SRC_PROFILE_NAME


def get_src_profile_label(profile_name: str | None) -> str:
    return SRC_PROFILE_OPTIONS[normalize_src_profile_name(profile_name)].label


def _get_src_profile(timeframe_label: str | None, profile_name: str | None = None) -> SrcProfile:
    _ = timeframe_label
    return SRC_PROFILE_OPTIONS[normalize_src_profile_name(profile_name)]


def _build_thresholds(
    adjustments: SrcAdjustments | None,
    profile: SrcProfile,
) -> SrcThresholds:
    current = adjustments or SrcAdjustments()
    stoch_oversold = profile.stoch_oversold * (1 + (current.stoch_pct / 100))
    stoch_overbought = profile.stoch_overbought * (1 - (current.stoch_pct / 100))
    cci_oversold = profile.cci_oversold * (1 - (current.cci_pct / 100))
    cci_overbought = profile.cci_overbought * (1 - (current.cci_pct / 100))
    rsi_oversold = profile.rsi_oversold * (1 + (current.rsi_pct / 100))
    rsi_overbought = profile.rsi_overbought * (1 - (current.rsi_pct / 100))

    return SrcThresholds(
        stoch_oversold=stoch_oversold,
        stoch_overbought=stoch_overbought,
        cci_oversold=cci_oversold,
        cci_overbought=cci_overbought,
        rsi_oversold=rsi_oversold,
        rsi_overbought=rsi_overbought,
    )


def _build_raw_conditions(
    strategy: pd.DataFrame,
    thresholds: SrcThresholds,
    profile: SrcProfile,
) -> tuple[pd.Series, pd.Series]:
    strategy["oversold_count"] = (
        (strategy["stoch"] <= thresholds.stoch_oversold).astype(int)
        + (strategy["cci"] <= thresholds.cci_oversold).astype(int)
        + (strategy["rsi"] <= thresholds.rsi_oversold).astype(int)
    )
    strategy["overbought_count"] = (
        (strategy["stoch"] >= thresholds.stoch_overbought).astype(int)
        + (strategy["cci"] >= thresholds.cci_overbought).astype(int)
        + (strategy["rsi"] >= thresholds.rsi_overbought).astype(int)
    )

    previous = strategy.shift(1)
    cross_up_count = (
        ((previous["stoch"] <= thresholds.stoch_oversold) & (strategy["stoch"] > thresholds.stoch_oversold)).astype(int)
        + ((previous["cci"] <= thresholds.cci_oversold) & (strategy["cci"] > thresholds.cci_oversold)).astype(int)
        + ((previous["rsi"] <= thresholds.rsi_oversold) & (strategy["rsi"] > thresholds.rsi_oversold)).astype(int)
    )
    raw_buy_open = (
        (previous["oversold_count"].fillna(0).astype(int) >= profile.open_prev_need)
        & (cross_up_count >= profile.open_cross_need)
    )
    raw_buy_close = (
        (previous["overbought_count"].fillna(0).astype(int) < profile.close_need)
        & (strategy["overbought_count"] >= profile.close_need)
    )
    return raw_buy_open.fillna(False), raw_buy_close.fillna(False)


def calculate_src_strategy(
    frame: pd.DataFrame,
    adjustments: SrcAdjustments | None = None,
    timeframe_label: str | None = None,
    profile_name: str | None = None,
    initial_state: dict[str, object] | None = None,
) -> pd.DataFrame:
    strategy = frame.copy()
    strategy["stoch"] = _calculate_stochastic_fast(strategy)
    strategy["cci"] = _calculate_cci(strategy)
    strategy["rsi"] = _calculate_rsi(strategy["Close"])

    strategy["scr_line"] = (
        ((strategy["stoch"] - 50.0) / 50.0)
        + (strategy["cci"] / 200.0)
        + ((strategy["rsi"] - 50.0) / 50.0)
    ) / 3.0
    strategy["scr_line"] = strategy["scr_line"].clip(-1.5, 1.5)

    profile = _get_src_profile(timeframe_label, profile_name)
    thresholds = _build_thresholds(adjustments, profile)
    raw_buy_open, raw_buy_close = _build_raw_conditions(strategy, thresholds, profile)

    buy_open_flags: list[bool] = []
    buy_close_flags: list[bool] = []
    in_position = bool((initial_state or {}).get("in_position", False))

    for open_signal, close_signal in zip(raw_buy_open.tolist(), raw_buy_close.tolist(), strict=False):
        buy_open = False
        buy_close = False
        if not in_position and open_signal:
            buy_open = True
            in_position = True
        elif in_position and close_signal:
            buy_close = True
            in_position = False
        buy_open_flags.append(buy_open)
        buy_close_flags.append(buy_close)

    strategy["buy_open"] = pd.Series(buy_open_flags, index=strategy.index, dtype=bool)
    strategy["buy_close"] = pd.Series(buy_close_flags, index=strategy.index, dtype=bool)
    strategy["sell_open"] = False
    strategy["sell_close"] = False
    strategy["signal"] = ""
    strategy.loc[strategy["buy_open"], "signal"] = "buy open"
    strategy.loc[strategy["buy_close"], "signal"] = "buy close"

    strategy.attrs["thresholds"] = {
        "stoch_oversold": thresholds.stoch_oversold,
        "stoch_overbought": thresholds.stoch_overbought,
        "cci_oversold": thresholds.cci_oversold,
        "cci_overbought": thresholds.cci_overbought,
        "rsi_oversold": thresholds.rsi_oversold,
        "rsi_overbought": thresholds.rsi_overbought,
        "open_prev_need": profile.open_prev_need,
        "open_cross_need": profile.open_cross_need,
        "close_need": profile.close_need,
        "profile_label": profile.label,
        "profile_name": normalize_src_profile_name(profile_name),
        "strategy_name": "SRC",
    }
    return strategy


def build_signal_logs(frame: pd.DataFrame, timeframe_label: str) -> list[str]:
    logs: list[str] = []
    signal_rows = frame[frame["signal"] != ""].tail(12)

    for timestamp, row in signal_rows.iterrows():
        time_text = timestamp.strftime("%Y-%m-%d %H:%M")
        price_text = f"{row['Close']:,.0f}"
        logs.append(
            f"{time_text}  {timeframe_label}  {row['signal']}  "
            f"(가격 {price_text} / SCR {row['scr_line']:.2f})"
        )

    if not logs and not frame.empty:
        latest = frame.iloc[-1]
        logs.append(
            f"{frame.index[-1].strftime('%Y-%m-%d %H:%M')}  아직 신호 없음  "
            f"(가격 {latest['Close']:,.0f} / SCR {latest['scr_line']:.2f})"
        )

    return list(reversed(logs))
