from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from shinobu.strategy_src import SrcAdjustments, calculate_src_strategy


StrategyAdjustments = SrcAdjustments


@dataclass(frozen=True)
class StrategyOption:
    key: str
    label: str
    title: str
    help_text: str


DEFAULT_STRATEGY_NAME = "src"
DEFAULT_STRATEGY_HISTORY_BUSINESS_DAYS = 5

STRATEGY_HISTORY_BUSINESS_DAYS = {
    "src": 5,
}

STRATEGY_OPTIONS = {
    "src": StrategyOption(
        key="src",
        label="SRC",
        title="SRC",
        help_text=(
            "SCR(Stochastic/CCI/RSI) 동시 확인 전략\n"
            "- 과매도: Stochastic 20 이하, CCI -100 이하, RSI 35 이하\n"
            "- 과열: Stochastic 80 이상, CCI 100 이상, RSI 70 이상\n"
            "- 진입: 직전 봉에서 3개 모두 과매도 + 현재 봉에서 3개 모두 상향 돌파\n"
            "- 청산: 3개 모두 과열 진입 시 청산\n"
            "- 레버리지/인버스 스위칭은 엔진 로직을 그대로 유지"
        ),
    ),
}

_STRATEGY_ALIASES = {
    "src": "src",
    "src_blog_scr": "src",
    "src_blg_scr": "src",
    "blog": "src",
    "scr": "src",
    "src_normal": "src",
    "src_v2_normal": "src",
    "src_v2_adx": "src",
    "normal": "src",
    "v2": "src",
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
    return [STRATEGY_OPTIONS["src"]]


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
    _ = normalize_strategy_name(strategy_name)
    return calculate_src_strategy(
        frame,
        adjustments,
        timeframe_label,
        profile_name="blog_scr",
        initial_state=initial_state,
    )
