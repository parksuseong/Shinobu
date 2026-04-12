from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from shinobu.strategy_src import SrcAdjustments, calculate_src_strategy
from shinobu.strategy_src_v2 import calculate_src_v2_strategy
from shinobu.strategy_src_v2_adx import calculate_src_v2_adx_strategy


StrategyAdjustments = SrcAdjustments


@dataclass(frozen=True)
class StrategyOption:
    key: str
    label: str
    title: str
    help_text: str


DEFAULT_STRATEGY_NAME = "src_v2_adx"
DEFAULT_STRATEGY_HISTORY_BUSINESS_DAYS = 5

STRATEGY_HISTORY_BUSINESS_DAYS = {
    "src_active": 5,
    "src_normal": 5,
    "src_v2_normal": 5,
    "src_v2_adx": 25,
}


STRATEGY_OPTIONS = {
    "src_active": StrategyOption(
        key="src_active",
        label="SRC Active",
        title="Active",
        help_text=(
            "SRC Active\n"
            "- 지표 기간: Stochastic 14 / CCI 20 / RSI 14\n"
            "- 과매도: Stochastic 38 이하, CCI -60 이하, RSI 42 이하\n"
            "- 과열: Stochastic 78 이상, CCI 96 이상, RSI 66 이상\n"
            "- 진입: 직전 봉에서 2개 이상 과매도, 현재 봉에서 1개 이상 상향 돌파\n"
            "- 청산: 2개 이상 과열이면 청산\n"
            "- 특징: 이전 Active보다 매매를 조금 줄이면서도 Normal보다 훨씬 빠르게 반응합니다."
        ),
    ),
    "src_normal": StrategyOption(
        key="src_normal",
        label="SRC Normal",
        title="Normal",
        help_text=(
            "SRC Normal\n"
            "- 지표 기간: Stochastic 14 / CCI 20 / RSI 14\n"
            "- 과매도: Stochastic 20 이하, CCI -100 이하, RSI 30 이하\n"
            "- 과열: Stochastic 80 이상, CCI 100 이상, RSI 70 이상\n"
            "- 진입: 직전 봉에서 3개 모두 과매도, 현재 봉에서 3개 모두 상향 돌파\n"
            "- 청산: 3개가 모두 과열이어야 청산합니다.\n"
            "- 특징: Active보다 진입과 청산 모두 더 보수적이라 포지션을 더 길게 가져갑니다."
        ),
    ),
    "src_v2_normal": StrategyOption(
        key="src_v2_normal",
        label="SRC V2 Normal",
        title="V2 Normal",
        help_text=(
            "SRC V2 Normal\n"
            "- 지표 기간: Stochastic 14 / CCI 20 / RSI 14\n"
            "- 과매도: Stochastic 10 이하, CCI -130 이하, RSI 44 이하\n"
            "- 과열: Stochastic 80 이상, CCI 150 이상, RSI 75 이상\n"
            "- 진입: 직전 봉에서 3개 모두 과매도, 현재 봉에서 2개 이상 상향 돌파\n"
            "- 청산: 3개 모두 과열이면 청산\n"
            "- 추가: 손절 -3.0%, 트레일링 스탑 -5.0%\n"
            "- 특징: 손실은 더 짧게 제한하고, 이익 구간은 더 길게 보유하도록 조정했습니다."
        ),
    ),
    "src_v2_adx": StrategyOption(
        key="src_v2_adx",
        label="SRC V2 ADX",
        title="V2 ADX",
        help_text=(
            "SRC V2 ADX\n"
            "- 지표 기간: Stochastic 14 / CCI 20 / RSI 14\n"
            "- 기본 진입/청산: SRC V2 Normal과 동일\n"
            "- 일봉 ADX 25 미만: 기존 SRC V2 Normal 진입 조건 사용\n"
            "- 일봉 ADX 25 이상: 역풍 구간으로 보고 진입을 더 엄격하게 제한\n"
            "- 엄격 진입: Stochastic 8 이하, CCI -150 이하, RSI 42 이하 + 3개 동시 상향 돌파 + SCR 0.20 이상\n"
            "- 청산: 3개 모두 과열, 손절 -3.0%, 트레일링 스탑 -5.0%\n"
            "- 특징: 강한 추세장에서 애매한 반등 진입을 줄여 손절 누적을 완화합니다."
        ),
    ),
}


_STRATEGY_ALIASES = {
    "active": "src_active",
    "normal": "src_normal",
    "src active": "src_active",
    "src normal": "src_normal",
    "src_v2": "src_v2_normal",
    "src v2": "src_v2_normal",
    "v2": "src_v2_normal",
    "src_v2_normal": "src_v2_normal",
    "src v2 normal": "src_v2_normal",
    "src_v2_adx": "src_v2_adx",
    "src v2 adx": "src_v2_adx",
}


def normalize_strategy_name(strategy_name: str | None) -> str:
    candidate = (strategy_name or DEFAULT_STRATEGY_NAME).strip().lower()
    candidate = _STRATEGY_ALIASES.get(candidate, candidate)
    return candidate if candidate in STRATEGY_OPTIONS else DEFAULT_STRATEGY_NAME


def get_strategy_label(strategy_name: str | None) -> str:
    return STRATEGY_OPTIONS[normalize_strategy_name(strategy_name)].label


def get_strategy_title(strategy_name: str | None) -> str:
    return STRATEGY_OPTIONS[normalize_strategy_name(strategy_name)].title


def get_strategy_help_text(strategy_name: str | None) -> str:
    return STRATEGY_OPTIONS[normalize_strategy_name(strategy_name)].help_text


def list_strategy_options() -> list[StrategyOption]:
    return [
        STRATEGY_OPTIONS["src_active"],
        STRATEGY_OPTIONS["src_normal"],
        STRATEGY_OPTIONS["src_v2_normal"],
        STRATEGY_OPTIONS["src_v2_adx"],
    ]


def get_strategy_history_business_days(strategy_name: str | None) -> int:
    normalized = normalize_strategy_name(strategy_name)
    return int(STRATEGY_HISTORY_BUSINESS_DAYS.get(normalized, DEFAULT_STRATEGY_HISTORY_BUSINESS_DAYS))


def calculate_strategy(
    frame: pd.DataFrame,
    adjustments: StrategyAdjustments | None = None,
    timeframe_label: str | None = None,
    strategy_name: str | None = None,
    initial_state: dict[str, object] | None = None,
) -> pd.DataFrame:
    normalized = normalize_strategy_name(strategy_name)
    if normalized == "src_active":
        return calculate_src_strategy(frame, adjustments, timeframe_label, profile_name="active", initial_state=initial_state)
    if normalized == "src_v2_normal":
        return calculate_src_v2_strategy(frame, adjustments, timeframe_label, initial_state=initial_state)
    if normalized == "src_v2_adx":
        return calculate_src_v2_adx_strategy(frame, adjustments, timeframe_label, initial_state=initial_state)
    return calculate_src_strategy(frame, adjustments, timeframe_label, profile_name="normal", initial_state=initial_state)
