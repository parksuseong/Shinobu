from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.utils import PlotlyJSONEncoder
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image
import yfinance as yf

from config import get_secret, has_kis_account
from shinobu import data as market_data
from shinobu.cache_db import (
    acquire_named_lock,
    align_raw_intraday_pair_to_intersection,
    acquire_startup_init_lock,
    clear_chart_payload_caches,
    clear_all_cache_data,
    get_raw_intraday_range,
    has_raw_intraday_mismatch,
    is_named_lock_locked,
    is_startup_init_locked,
    is_startup_initialized,
    mark_startup_initialized,
    release_named_lock,
    release_startup_init_lock,
)
from shinobu.chart import build_candlestick_chart, update_candlestick_chart
from shinobu.chart_payload import ensure_live_chart_prewarm_bundle, run_live_chart_prewarm_sync
from shinobu.live_chart_component import build_live_chart_html
from shinobu.backtest_engine import (
    build_long_short_signals,
    get_backtest_job,
    get_backtest_timeframe_labels,
    get_backtest_timeframe_max_days,
    submit_backtest_job,
)
from shinobu.recommendation_engine import load_recommendation_history, load_recommendations_for
from shinobu.kis import KisApiError, fetch_domestic_balance, fetch_domestic_daily_ccld
from shinobu.strategy_cache import calculate_strategy_cached
from shinobu.live_trading import (
    EXECUTION_MODE_SIGNAL,
    EXECUTION_MODE_X1,
    append_live_log,
    get_asset_history,
    get_live_execution_mode,
    get_live_logs,
    get_live_orders,
    get_live_runtime_state,
    get_live_strategy_name,
    get_live_started_at,
    init_live_state,
    is_live_enabled,
    process_live_trading_cycle,
    record_asset_snapshot,
    set_live_enabled,
    set_live_execution_mode,
    set_live_strategy_name,
)
from shinobu.strategy import (
    DEFAULT_STRATEGY_NAME,
    StrategyAdjustments,
    calculate_strategy,
    get_strategy_help_text,
    get_strategy_label,
    get_strategy_title,
    list_strategy_options,
    normalize_strategy_name,
)

LIVE_TIMEFRAME = "5분봉"
PRIMARY_SYMBOL = "122630.KS"
LIVE_CHART_STATE_KEY = "live_chart_state"
LIVE_FIGURE_STATE_KEY = "live_figure_state"
LIVE_CHART_NONCE_KEY = "live_chart_nonce"
MAX_LIVE_CHART_CANDLES = 1200
MAX_LIVE_CHART_BUSINESS_DAYS = 5
STRATEGY_PROFILE_STATE_KEY = "strategy_profile"
CHART_START_DATE_STATE_KEY = "chart_start_date"
CHART_END_DATE_STATE_KEY = "chart_end_date"
EXECUTION_MODE_STATE_KEY = "execution_mode"
ACCOUNT_PANEL_CACHE_KEY = "account_panel_cache"
ACCOUNT_SUMMARY_CACHE_KEY = "account_summary_cache"
ACCOUNT_SUMMARY_FETCHED_AT_KEY = "account_summary_fetched_at"
ACCOUNT_PANEL_LAST_ORDER_KEY = "account_panel_last_order_at"
CLOSED_TRADES_CACHE_KEY = "closed_trades_cache"
CLOSED_TRADES_LAST_ORDER_KEY = "closed_trades_last_order_at"
ACCOUNT_FETCH_TIMEOUT_SECONDS = 1.2
ACCOUNT_SUMMARY_REFRESH_SECONDS = 30.0
PAIR_RECOVERY_INTERVAL_SECONDS = 60.0
PAIR_RECOVERY_IGNORE_RECENT_MINUTES = 10
PAIR_RECOVERY_LOCK_NAME = "pair_candle_recovery"
BACKTEST_RESULT_STATE_KEY = "backtest_result"
BACKTEST_JOB_ID_STATE_KEY = "backtest_job_id"
BACKTEST_SAJU_RESULT_STATE_KEY = "backtest_saju_result"
BACKTEST_SAJU_RUNNING_STATE_KEY = "backtest_saju_running"
LIVE_CHART_COMPONENT_VERSION = 1
SAJU_ANALYSIS_LOCK = threading.Lock()
SAJU_GLOBAL_LOCK_NAME = "backtest_saju_analysis"
SAJU_GLOBAL_LOCK_STALE_SECONDS = 900
SAJU_TIMEFRAME_WINDOWS: list[tuple[str, str, str]] = [
    ("1h", "60m", "60d"),
    ("1d", "1d", "1y"),
    ("1w", "1wk", "3y"),
]
SAJU_REFERENCE_SOURCES: list[dict[str, str]] = [
    {
        "title": "Enhanced Momentum with Momentum Transformers (2024, arXiv)",
        "url": "https://arxiv.org/abs/2412.12516",
        "note": "모멘텀 + 트랜스포머 기반 신호 구성",
    },
    {
        "title": "TrendFolios (2025, arXiv)",
        "url": "https://arxiv.org/abs/2506.09330",
        "note": "포트폴리오 단위 추세/모멘텀 설계",
    },
    {
        "title": "Improving Time-Series Momentum Strategies (CME, 2025)",
        "url": "https://www.cmegroup.com/content/dam/cmegroup/education/files/improving-time-series-momentum-strategies.pdf",
        "note": "변동성 타게팅과 신호 선택 효과",
    },
    {
        "title": "Constructing time-series momentum portfolios with deep multi-task learning (2023)",
        "url": "https://www.sciencedirect.com/science/article/abs/pii/S0957417423010898",
        "note": "딥러닝 멀티태스크 기반 시계열 모멘텀",
    },
]


def _get_pair_recovery_ignore_recent_minutes(now: pd.Timestamp | None = None) -> int:
    """Use strict matching after market close so 15:30 close bars are recovered immediately."""
    ts = pd.Timestamp.now(tz=market_data.KST) if now is None else pd.Timestamp(now)
    if ts.tzinfo is None:
        ts = ts.tz_localize(market_data.KST)
    else:
        ts = ts.tz_convert(market_data.KST)

    # Weekend or outside regular market session: no in-flight candles to ignore.
    if ts.weekday() >= 5:
        return 0
    current_minutes = int(ts.hour) * 60 + int(ts.minute)
    market_open_minutes = 9 * 60
    market_close_minutes = 15 * 60 + 30
    if current_minutes < market_open_minutes or current_minutes >= market_close_minutes:
        return 0
    return PAIR_RECOVERY_IGNORE_RECENT_MINUTES
_RESET_LOCK = threading.Lock()
_PAIR_RECOVERY_STATE_LOCK = threading.Lock()
_RESET_STATE: dict[str, object] = {
    "running": False,
    "done": False,
    "error": "",
    "message": "",
    "started_monotonic": 0.0,
    "eta_seconds": 120,
    "current_step": 0,
    "total_steps": 0,
}
ASSET_DIR = Path(__file__).resolve().parent / "assets"
POSITIVE_IMAGE_PATH = ASSET_DIR / "shinobu_positive.mp4"
NEGATIVE_IMAGE_PATH = ASSET_DIR / "shinobu_negative.mp4"
NEUTRAL_IMAGE_PATH = ASSET_DIR / "shinobu_neutral.mp4"
POSITIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_positive.svg"
NEGATIVE_FALLBACK_PATH = ASSET_DIR / "shinobu_negative.svg"
NEUTRAL_FALLBACK_PATH = ASSET_DIR / "shinobu_positive.svg"
_PAIR_RECOVERY_LAST_RUN_MONOTONIC = 0.0
_PAIR_RECOVERY_STATE: dict[str, str] = {
    "checked_at": "-",
    "message": "대기 중",
}


st.set_page_config(page_title="Shinobu Project", page_icon="S", layout="wide")


display_name = market_data.display_name
get_pair_symbol = market_data.get_pair_symbol


def render_header(profile_name: str) -> None:
    st.title("Shinobu Project")
    st.caption("\uC2E4\uC804 5\uBD84\uBD09 \uC790\uB3D9\uB9E4\uB9E4")


def init_strategy_profile_state() -> None:
    if STRATEGY_PROFILE_STATE_KEY not in st.session_state:
        st.session_state[STRATEGY_PROFILE_STATE_KEY] = get_live_strategy_name()


def init_chart_date_range_state() -> None:
    if CHART_START_DATE_STATE_KEY in st.session_state and CHART_END_DATE_STATE_KEY in st.session_state:
        return
    fallback_today = pd.Timestamp.now().date()
    default_end_date = fallback_today
    try:
        raw_symbol = market_data.display_symbol(PRIMARY_SYMBOL)
        _, max_ts = get_raw_intraday_range(raw_symbol, LIVE_TIMEFRAME)
        if max_ts is not None:
            default_end_date = pd.Timestamp(max_ts).date()
    except Exception:
        default_end_date = fallback_today
    st.session_state[CHART_END_DATE_STATE_KEY] = default_end_date
    st.session_state[CHART_START_DATE_STATE_KEY] = default_end_date - pd.Timedelta(days=1)


def init_execution_mode_state() -> None:
    if EXECUTION_MODE_STATE_KEY not in st.session_state:
        st.session_state[EXECUTION_MODE_STATE_KEY] = get_live_execution_mode()


def get_current_chart_date_range() -> tuple[date, date]:
    init_chart_date_range_state()
    start_value = st.session_state.get(CHART_START_DATE_STATE_KEY)
    end_value = st.session_state.get(CHART_END_DATE_STATE_KEY)
    start_date = pd.Timestamp(start_value).date()
    end_date = pd.Timestamp(end_value).date()
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    return start_date, end_date


def _set_chart_date_range(start_value: date, end_value: date) -> None:
    start_date = pd.Timestamp(start_value).date()
    end_date = pd.Timestamp(end_value).date()
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    st.session_state[CHART_START_DATE_STATE_KEY] = start_date
    st.session_state[CHART_END_DATE_STATE_KEY] = end_date


def get_current_strategy_profile() -> str:
    init_strategy_profile_state()
    return normalize_strategy_name(st.session_state.get(STRATEGY_PROFILE_STATE_KEY))


def get_current_execution_mode() -> str:
    init_execution_mode_state()
    current = str(st.session_state.get(EXECUTION_MODE_STATE_KEY, EXECUTION_MODE_X1))
    return current if current in {EXECUTION_MODE_X1, EXECUTION_MODE_SIGNAL} else EXECUTION_MODE_X1


def _set_strategy_profile(profile_name: str) -> None:
    normalized = normalize_strategy_name(profile_name)
    current = normalize_strategy_name(st.session_state.get(STRATEGY_PROFILE_STATE_KEY))
    st.session_state[STRATEGY_PROFILE_STATE_KEY] = normalized
    set_live_strategy_name(normalized)
    if current != normalized:
        init_live_chart_state()
        st.session_state[LIVE_CHART_STATE_KEY] = {"started_at": "", "frames": {}}
        st.session_state[LIVE_FIGURE_STATE_KEY] = {}


def _set_execution_mode(mode: str) -> None:
    normalized = mode if mode in {EXECUTION_MODE_X1, EXECUTION_MODE_SIGNAL} else EXECUTION_MODE_X1
    st.session_state[EXECUTION_MODE_STATE_KEY] = normalized
    set_live_execution_mode(normalized)


def render_live_selector_bar() -> str:
    current_profile = normalize_strategy_name(DEFAULT_STRATEGY_NAME)
    _set_strategy_profile(current_profile)
    current_start_date, current_end_date = get_current_chart_date_range()
    current_execution_mode = get_current_execution_mode()
    range_col, execution_col, _ = st.columns([1.3, 0.9, 2.8], vertical_alignment="top")
    with range_col:
        st.caption("\uCC28\uD2B8 \uAE30\uAC04")
        start_col, end_col = st.columns([1, 1], gap="small")
        with start_col:
            selected_start_date = st.date_input(
                "\uC2DC\uC791\uC77C",
                value=current_start_date,
                key="chart-start-date-input",
                label_visibility="collapsed",
            )
        with end_col:
            selected_end_date = st.date_input(
                "\uC885\uB8CC\uC77C",
                value=current_end_date,
                key="chart-end-date-input",
                label_visibility="collapsed",
            )
    with execution_col:
        st.caption("\uC2E4\uC81C \uC8FC\uBB38")
        selected_execution_mode = st.selectbox(
            "\uC2E4\uC81C \uC8FC\uBB38 \uD0C0\uC785",
            options=[EXECUTION_MODE_X1, EXECUTION_MODE_SIGNAL],
            index=0 if current_execution_mode == EXECUTION_MODE_X1 else 1,
            format_func=lambda value: "x1 ETF" if value == EXECUTION_MODE_X1 else "\uB808\uBC84\uB9AC\uC9C0/\uACF1\uBC84\uC2A4",
            help="\uC2E4\uC81C \uC2DC\uADF8\uB110 \uC8FC\uBB38\uC744 x1 ETF \uB610\uB294 \uB808\uBC84\uB9AC\uC9C0/\uACF1\uBC84\uC2A4\uB85C \uC2E4\uD589\uD569\uB2C8\uB2E4.",
            label_visibility="collapsed",
        )

    selected_start = pd.Timestamp(selected_start_date).date()
    selected_end = pd.Timestamp(selected_end_date).date()
    if selected_start != current_start_date or selected_end != current_end_date:
        _set_chart_date_range(selected_start, selected_end)
    if selected_execution_mode != current_execution_mode:
        _set_execution_mode(selected_execution_mode)

    mode_label = "x1 ETF" if get_current_execution_mode() == EXECUTION_MODE_X1 else "\uB808\uBC84\uB9AC\uC9C0/\uACF1\uBC84\uC2A4"
    chart_start_date, chart_end_date = get_current_chart_date_range()
    st.caption(
        f"\uCC28\uD2B8 \uD45C\uC2DC: {chart_start_date.isoformat()} ~ {chart_end_date.isoformat()} | \uC2E4\uC81C \uC8FC\uBB38: {mode_label}"
    )
    st.caption("\uB9C8\uCEE4 \uD45C\uC2DC \uD544\uD130\uB294 \uCC28\uD2B8 \uC0C1\uB2E8\uC5D0\uC11C \uBC14\uB85C \uD1A0\uAE00\uD569\uB2C8\uB2E4.")
    st.caption("매매법 : 心を燃やせ")
    return get_current_strategy_profile()
def _get_reset_state() -> dict[str, object]:
    with _RESET_LOCK:
        return dict(_RESET_STATE)


def _set_reset_state(**kwargs: object) -> None:
    with _RESET_LOCK:
        _RESET_STATE.update(kwargs)


def _set_pair_recovery_state(message: str) -> None:
    now_text = pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S")
    with _PAIR_RECOVERY_STATE_LOCK:
        _PAIR_RECOVERY_STATE["checked_at"] = now_text
        _PAIR_RECOVERY_STATE["message"] = str(message)


def _get_pair_recovery_state_text() -> str:
    with _PAIR_RECOVERY_STATE_LOCK:
        checked_at = _PAIR_RECOVERY_STATE.get("checked_at", "-")
        message = _PAIR_RECOVERY_STATE.get("message", "대기 중")
    return f"마지막 점검 {checked_at} · {message}"


def _lookback_days_from_current_year_start() -> int:
    now = pd.Timestamp.now(tz="Asia/Seoul")
    year_start = pd.Timestamp(year=now.year, month=1, day=1, tz="Asia/Seoul")
    return max(5, int((now - year_start).days) + 3)


def _run_startup_initialization(primary_symbol: str, pair_symbol: str | None) -> None:
    symbols = [value for value in [primary_symbol, pair_symbol] if value]
    prewarm_days = [1, 2, 3, 4, 5]
    lookback_days = _lookback_days_from_current_year_start()
    total_steps = 1 + len(symbols) + len(prewarm_days) + 1
    completed_steps = 0
    try:
        _set_reset_state(message="\uAE30\uC874 \uCE90\uC2DC\uC640 SQLite \uB370\uC774\uD130\uB97C \uC815\uB9AC\uD569\uB2C8\uB2E4.")
        clear_all_cache_data()
        st.cache_data.clear()
        completed_steps += 1
        _set_reset_state(current_step=completed_steps, total_steps=total_steps)

        _set_reset_state(message="\uC62C\uD574 1\uC6D4 1\uC77C\uBD80\uD130 \uCE94\uB4E4\uC744 \uC218\uC9D1\uD558\uACE0 \uC804\uB7B5 \uACC4\uC0B0\uC744 \uC218\uD589\uD569\uB2C8\uB2E4.")
        for symbol in symbols:
            source_frame = market_data._load_live_chart_data_impl(symbol, LIVE_TIMEFRAME, lookback_days=lookback_days)
            calculate_strategy_cached(
                source_frame,
                StrategyAdjustments(),
                LIVE_TIMEFRAME,
                strategy_name=DEFAULT_STRATEGY_NAME,
                symbol=symbol,
            )
            completed_steps += 1
            _set_reset_state(
                message=f"{market_data.display_name(symbol)} \uCE94\uB4E4/\uC804\uB7B5 \uC800\uC7A5 \uC644\uB8CC",
                current_step=completed_steps,
                total_steps=total_steps,
            )

        _set_reset_state(message="\uCC28\uD2B8/\uB9C8\uCEE4 \uCE90\uC2DC\uB97C \uBBF8\uB9AC \uC900\uBE44\uD569\uB2C8\uB2E4.")
        for day in prewarm_days:
            run_live_chart_prewarm_sync(
                primary_symbol,
                pair_symbol,
                StrategyAdjustments(),
                strategy_name=DEFAULT_STRATEGY_NAME,
                visible_business_days_list=[day],
            )
            completed_steps += 1
            _set_reset_state(
                message=f"\uCC28\uD2B8/\uB9C8\uCEE4 \uD504\uB9AC\uC6DC {day}\uC77C \uC644\uB8CC",
                current_step=completed_steps,
                total_steps=total_steps,
            )
        ensure_live_chart_prewarm_bundle(
            primary_symbol,
            pair_symbol,
            StrategyAdjustments(),
            current_strategy_name=DEFAULT_STRATEGY_NAME,
            visible_business_days=5,
            all_strategy_names=[DEFAULT_STRATEGY_NAME],
        )
        completed_steps += 1
        _set_reset_state(current_step=completed_steps, total_steps=total_steps)

        _set_reset_state(
            running=False,
            done=True,
            error="",
            message="\uCD08\uAE30\uD654\uAC00 \uC644\uB8CC\uB418\uC5C8\uC2B5\uB2C8\uB2E4.",
            current_step=total_steps,
            total_steps=total_steps,
        )
        mark_startup_initialized(True)
    except Exception as exc:
        mark_startup_initialized(False)
        _set_reset_state(
            running=False,
            done=False,
            error=str(exc),
            message="\uCD08\uAE30\uD654 \uC911 \uC624\uB958\uAC00 \uBC1C\uC0DD\uD588\uC2B5\uB2C8\uB2E4. \uC790\uB3D9 \uC7AC\uC2DC\uB3C4\uD569\uB2C8\uB2E4.",
            current_step=completed_steps,
            total_steps=total_steps,
        )
    finally:
        release_startup_init_lock()


def _ensure_startup_initialization(primary_symbol: str, pair_symbol: str | None) -> None:
    if is_startup_initialized():
        _set_reset_state(
            running=False,
            done=True,
            error="",
            message="\uCD08\uAE30\uD654\uAC00 \uC644\uB8CC\uB418\uC5C8\uC2B5\uB2C8\uB2E4.",
        )
        return

    with _RESET_LOCK:
        running = bool(_RESET_STATE.get("running", False))
        done = bool(_RESET_STATE.get("done", False))
        if running or done:
            return

    if not acquire_startup_init_lock():
        if is_startup_init_locked():
            _set_reset_state(
                running=True,
                done=False,
                error="",
                message="\uB2E4\uB978 \uC138\uC158\uC5D0\uC11C \uCD08\uAE30\uD654\uAC00 \uC9C4\uD589 \uC911\uC785\uB2C8\uB2E4.",
                started_monotonic=time.monotonic(),
                current_step=0,
                total_steps=0,
            )
            return
        # Stale lock was likely recovered by another process. Retry once.
        if not acquire_startup_init_lock():
            _set_reset_state(
                running=True,
                done=False,
                error="",
                message="\uCD08\uAE30\uD654 \uB77D \uD655\uBCF4\uB97C \uB300\uAE30 \uC911\uC785\uB2C8\uB2E4.",
                started_monotonic=time.monotonic(),
                current_step=0,
                total_steps=0,
            )
            return

    if is_startup_initialized():
        release_startup_init_lock()
        _set_reset_state(
            running=False,
            done=True,
            error="",
            message="\uCD08\uAE30\uD654\uAC00 \uC644\uB8CC\uB418\uC5C8\uC2B5\uB2C8\uB2E4.",
        )
        return

    with _RESET_LOCK:
        running = bool(_RESET_STATE.get("running", False))
        done = bool(_RESET_STATE.get("done", False))
        if running or done:
            release_startup_init_lock()
            return
        _RESET_STATE.update(
            running=True,
            done=False,
            error="",
            message="\uCD08\uAE30\uD654 \uC911\uC785\uB2C8\uB2E4.",
            started_monotonic=time.monotonic(),
            eta_seconds=120,
            current_step=0,
            total_steps=0,
        )
    worker = threading.Thread(
        target=_run_startup_initialization,
        args=(primary_symbol, pair_symbol),
        daemon=True,
        name="shinobu-startup-init-worker",
    )
    worker.start()


def init_live_chart_state() -> None:
    if LIVE_CHART_STATE_KEY not in st.session_state:
        st.session_state[LIVE_CHART_STATE_KEY] = {"started_at": "", "frames": {}}
    if LIVE_FIGURE_STATE_KEY not in st.session_state:
        st.session_state[LIVE_FIGURE_STATE_KEY] = {}
    if LIVE_CHART_NONCE_KEY not in st.session_state:
        st.session_state[LIVE_CHART_NONCE_KEY] = 0


@st.cache_data(ttl=5, show_spinner=False)
def get_cached_raw_frame(symbol: str, timeframe_label: str, profile_name: str) -> pd.DataFrame:
    if hasattr(market_data, "load_ui_chart_data_for_strategy"):
        return market_data.load_ui_chart_data_for_strategy(symbol, timeframe_label, profile_name)
    return market_data.load_ui_chart_data(symbol, timeframe_label)


@st.cache_data(ttl=5, show_spinner=False)
def get_cached_strategy_frame(
    symbol: str,
    timeframe_label: str,
    stoch_pct: int,
    cci_pct: int,
    rsi_pct: int,
    profile_name: str,
) -> pd.DataFrame:
    adjustments = StrategyAdjustments(stoch_pct=stoch_pct, cci_pct=cci_pct, rsi_pct=rsi_pct)
    raw = get_cached_raw_frame(symbol, timeframe_label, profile_name)
    return calculate_strategy_cached(
        raw,
        adjustments,
        timeframe_label,
        strategy_name=profile_name,
        symbol=symbol,
    )


def filter_frame_from_live_start(frame: pd.DataFrame) -> pd.DataFrame:
    started_at = get_live_started_at()
    if started_at is None:
        return frame.iloc[0:0].copy()

    before = frame.loc[frame.index < started_at]
    after = frame.loc[frame.index >= started_at]
    combined = pd.concat([before, after]).sort_index()
    if combined.empty and not frame.empty:
        return limit_frame_to_recent_business_days(frame)
    return limit_frame_to_recent_business_days(combined)


def limit_frame_to_recent_business_days(frame: pd.DataFrame, max_days: int | None = None) -> pd.DataFrame:
    if frame.empty:
        return frame
    current_max_days = int(max_days or MAX_LIVE_CHART_BUSINESS_DAYS)
    trade_days = pd.Index(pd.to_datetime(frame.index).normalize().unique()).sort_values()
    recent_days = trade_days[-current_max_days:]
    limited = frame.loc[frame.index.normalize().isin(recent_days)].copy()
    return limited.tail(MAX_LIVE_CHART_CANDLES).copy()


def _empty_live_frame(template: pd.DataFrame | None = None) -> pd.DataFrame:
    if template is None:
        return pd.DataFrame()
    return template.iloc[0:0].copy()


def _merge_live_frame(cache_frame: pd.DataFrame, latest_frame: pd.DataFrame) -> pd.DataFrame:
    if latest_frame.empty:
        return cache_frame
    if cache_frame.empty:
        return latest_frame.tail(MAX_LIVE_CHART_CANDLES).copy()

    last_index = cache_frame.index.max()
    appended = latest_frame.loc[latest_frame.index > last_index]
    if appended.empty:
        return cache_frame.tail(MAX_LIVE_CHART_CANDLES).copy()

    merged = pd.concat([cache_frame, appended]).sort_index()
    merged = merged[~merged.index.duplicated(keep="last")]
    return limit_frame_to_recent_business_days(merged)


def get_live_chart_frame(symbol: str, adjustments: StrategyAdjustments, profile_name: str) -> pd.DataFrame:
    init_live_chart_state()
    started_at = get_live_started_at()
    state = st.session_state[LIVE_CHART_STATE_KEY]
    started_key = started_at.isoformat() if started_at is not None else ""

    if state.get("started_at") != started_key:
        state["started_at"] = started_key
        state["frames"] = {}

    latest_frame = get_cached_strategy_frame(
        symbol,
        LIVE_TIMEFRAME,
        adjustments.stoch_pct,
        adjustments.cci_pct,
        adjustments.rsi_pct,
        profile_name,
    )
    latest_frame = limit_frame_to_recent_business_days(filter_frame_from_live_start(latest_frame))

    if started_at is None:
        return _empty_live_frame(latest_frame)

    frames = state["frames"]
    cache_frame = frames.get(symbol)
    if cache_frame is None:
        frames[symbol] = limit_frame_to_recent_business_days(latest_frame)
    else:
        frames[symbol] = _merge_live_frame(cache_frame, latest_frame)
    return frames[symbol]


def get_preview_chart_frame(symbol: str, adjustments: StrategyAdjustments, profile_name: str) -> pd.DataFrame:
    frame = get_cached_strategy_frame(
        symbol,
        LIVE_TIMEFRAME,
        adjustments.stoch_pct,
        adjustments.cci_pct,
        adjustments.rsi_pct,
        profile_name,
    )
    return limit_frame_to_recent_business_days(frame)


def get_preview_raw_chart_frame(symbol: str) -> pd.DataFrame:
    frame = get_cached_raw_frame(symbol, LIVE_TIMEFRAME, get_current_strategy_profile())
    return limit_frame_to_recent_business_days(frame)


def get_live_raw_chart_frame(symbol: str) -> pd.DataFrame:
    frame = get_cached_raw_frame(symbol, LIVE_TIMEFRAME, get_current_strategy_profile())
    return limit_frame_to_recent_business_days(filter_frame_from_live_start(frame))


def _get_chart_figure(
    chart_kind: str,
    frame: pd.DataFrame,
    symbol: str,
    pair_symbol: str | None,
    pair_frame: pd.DataFrame | None,
) -> go.Figure:
    init_live_chart_state()
    state = st.session_state[LIVE_FIGURE_STATE_KEY]
    include_scr_panel = "scr_line" in frame.columns
    figure_key = f"{chart_kind}:{symbol}:{pair_symbol or ''}:{include_scr_panel}"
    symbol_name = display_name(symbol)
    pair_name = display_name(pair_symbol) if pair_symbol else None

    cached_figure = state.get(figure_key)
    if cached_figure is None:
        cached_figure = build_candlestick_chart(
            frame,
            LIVE_TIMEFRAME,
            symbol_name,
            symbol,
            pair_frame=pair_frame,
            pair_name=pair_name,
            pair_symbol_code=pair_symbol,
        )
        state[figure_key] = cached_figure
        return cached_figure

    return update_candlestick_chart(
        cached_figure,
        frame,
        LIVE_TIMEFRAME,
        symbol_name,
        symbol,
        pair_frame=pair_frame,
        pair_name=pair_name,
        pair_symbol_code=pair_symbol,
    )


def _add_live_order_markers(figure: go.Figure, order_frame: pd.DataFrame, price_frame: pd.DataFrame) -> None:
    if order_frame.empty or price_frame.empty:
        return

    aligned = price_frame.reindex(order_frame["candle_time"]).ffill()
    if aligned.empty:
        return

    working = order_frame.copy()
    x_positions = pd.Series(range(len(price_frame)), index=price_frame.index)
    y_values = []
    for (_, order), (_, candle) in zip(working.iterrows(), aligned.iterrows(), strict=False):
        y_values.append(float(candle["Low"]) * 0.985 if order["side"] == "buy" else float(candle["High"]) * 1.015)

    working["x"] = x_positions.reindex(working["candle_time"]).tolist()
    working["y"] = y_values
    working = working[working["x"].notna()].copy()
    if working.empty:
        return

    color_map = {"buy": "#3b82f6", "sell": "#ef4444"}
    label_map = {"buy": "실제 매수", "sell": "실제 매도"}
    for (side, order_symbol), group in working.groupby(["side", "symbol"]):
        color = color_map.get(side, "#9aa4b2")
        label = label_map.get(side, side)
        figure.add_trace(
            go.Scatter(
                x=group["x"],
                y=group["y"],
                mode="markers+text",
                marker={"symbol": "heart", "size": 15, "color": color, "line": {"width": 1, "color": "#ffffff"}},
                text=[f"{label} · {display_name(order_symbol)}" for _ in range(len(group))],
                textposition="top center",
                textfont={"size": 10, "color": color},
                hovertemplate="%{text}<extra></extra>",
                name=f"{label} {display_name(order_symbol)}",
            ),
            row=1,
            col=1,
        )


def render_live_trade_header(symbol: str, pair_symbol: str | None) -> None:
    if pair_symbol is None:
        st.subheader(f"실전 매매 차트 · {display_name(symbol)}")
        return
    st.subheader(f"실전 매매 차트 · {display_name(symbol)} / {display_name(pair_symbol)}")


def mask_account_number(account_number: str) -> str:
    if not account_number:
        return ""
    return f"{account_number[:2]}{'*' * max(len(account_number) - 2, 0)}"


def _load_balance_quick(refresh: bool = True) -> tuple[pd.DataFrame, dict, bool]:
    cached = st.session_state.get(ACCOUNT_PANEL_CACHE_KEY)
    fallback_positions = pd.DataFrame()
    fallback_summary: dict = {}
    has_fallback = False

    if isinstance(cached, dict):
        maybe_positions = cached.get("positions")
        maybe_summary = cached.get("summary")
        if isinstance(maybe_positions, pd.DataFrame) and isinstance(maybe_summary, dict):
            fallback_positions = maybe_positions
            fallback_summary = maybe_summary
            has_fallback = True
    if not refresh:
        if has_fallback:
            return fallback_positions, fallback_summary, False
        return pd.DataFrame(), {}, True

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fetch_domestic_balance)
    try:
        positions, summary = future.result(timeout=ACCOUNT_FETCH_TIMEOUT_SECONDS)
        positions = _dedupe_positions_frame(positions)
        st.session_state[ACCOUNT_PANEL_CACHE_KEY] = {"positions": positions, "summary": summary}
        return positions, summary, False
    except (FuturesTimeoutError, Exception):
        if has_fallback:
            return fallback_positions, fallback_summary, True
        return pd.DataFrame(), {}, True
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _load_account_summary_quick() -> tuple[dict, bool]:
    cached_summary = st.session_state.get(ACCOUNT_SUMMARY_CACHE_KEY)
    if not isinstance(cached_summary, dict):
        cached_summary = {}
    fetched_at = float(st.session_state.get(ACCOUNT_SUMMARY_FETCHED_AT_KEY, 0.0) or 0.0)
    now = time.monotonic()
    should_refresh = (not cached_summary) or ((now - fetched_at) >= ACCOUNT_SUMMARY_REFRESH_SECONDS)
    if not should_refresh:
        return cached_summary, False

    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(fetch_domestic_balance)
    try:
        positions, summary = future.result(timeout=ACCOUNT_FETCH_TIMEOUT_SECONDS)
        st.session_state[ACCOUNT_SUMMARY_CACHE_KEY] = dict(summary)
        st.session_state[ACCOUNT_SUMMARY_FETCHED_AT_KEY] = now

        # First render convenience: populate position cache if missing.
        if ACCOUNT_PANEL_CACHE_KEY not in st.session_state:
            st.session_state[ACCOUNT_PANEL_CACHE_KEY] = {
                "positions": _dedupe_positions_frame(positions),
                "summary": summary,
            }
        return dict(summary), False
    except (FuturesTimeoutError, Exception):
        return cached_summary, True
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _dedupe_positions_frame(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return positions

    normalized = positions.copy()
    if {"code", "name"}.issubset(normalized.columns):
        aggregations = {
            "quantity": "sum",
            "avg_price": "last",
            "current_price": "last",
            "eval_amount": "sum",
            "profit_amount": "sum",
            "profit_rate": "last",
        }
        aggregations = {key: value for key, value in aggregations.items() if key in normalized.columns}
        normalized = (
            normalized.groupby(["code", "name"], as_index=False)
            .agg(aggregations)
            .sort_values(["eval_amount", "quantity"], ascending=False)
            .reset_index(drop=True)
        )
    return normalized


def _format_positions_frame(positions: pd.DataFrame) -> pd.DataFrame:
    if positions.empty:
        return positions

    normalized = _dedupe_positions_frame(positions)

    display_columns = ["code", "name", "quantity", "avg_price", "current_price", "eval_amount", "profit_amount", "profit_rate"]
    view = normalized.loc[:, [column for column in display_columns if column in normalized.columns]].copy()
    view = view.rename(
        columns={
            "code": "종목코드",
            "name": "종목명",
            "quantity": "보유수량",
            "avg_price": "평균단가",
            "current_price": "현재가",
            "eval_amount": "평가금액",
            "profit_amount": "평가손익",
            "profit_rate": "수익률(%)",
        }
    )
    for column in ["보유수량", "평균단가", "현재가", "평가금액", "평가손익"]:
        if column in view.columns:
            view[column] = view[column].map(lambda value: f"{float(value):,.0f}")
    if "수익률(%)" in view.columns:
        view["수익률(%)"] = view["수익률(%)"].map(lambda value: f"{float(value):+.2f}")
    return view


def _format_five_min_bucket_label(timestamp: pd.Timestamp) -> str:
    start = pd.Timestamp(timestamp).floor("5min")
    end = start + pd.Timedelta(minutes=5)
    return f"{start.strftime('%m-%d %H:%M')}~{end.strftime('%H:%M')}"


def _group_execution_ledger_by_5m(executions: pd.DataFrame) -> pd.DataFrame:
    if executions.empty:
        return executions

    ledger = executions.copy()
    ledger["bucket_start"] = pd.to_datetime(ledger["timestamp"]).dt.floor("5min")
    grouped = (
        ledger.groupby(["bucket_start", "symbol", "name", "side"], as_index=False)
        .agg(
            quantity=("quantity", "sum"),
            amount=("amount", "sum"),
            order_count=("order_no", "nunique"),
        )
    )
    grouped["price"] = grouped["amount"] / grouped["quantity"]
    grouped["time_range"] = grouped["bucket_start"].map(_format_five_min_bucket_label)
    return grouped.sort_values("bucket_start", ascending=False).reset_index(drop=True)


def _account_return_rate(summary: dict) -> float:
    purchase_amount = float(summary.get("매입금액", 0) or 0)
    profit = float(summary.get("평가손익", 0) or 0)
    if purchase_amount <= 0:
        return 0.0
    return (profit / purchase_amount) * 100


def _emotion_image_path(image_path: Path, fallback_path: Path) -> Path | None:
    if image_path.exists():
        return image_path
    if fallback_path.exists():
        return fallback_path
    return None


@st.cache_data(ttl=3600, show_spinner=False)
def _get_thumbnail_base64(image_path: str, max_width: int = 280, max_height: int = 320) -> str:
    path = Path(image_path)
    with Image.open(path) as image:
        converted = image.convert("RGB")
        converted.thumbnail((max_width, max_height))
        from io import BytesIO

        buffer = BytesIO()
        converted.save(buffer, format="JPEG", quality=72, optimize=True)
        return base64.b64encode(buffer.getvalue()).decode("ascii")


@st.cache_data(ttl=3600, show_spinner=False)
def _get_video_base64(video_path: str) -> str:
    return base64.b64encode(Path(video_path).read_bytes()).decode("ascii")


def _render_emotion_card(title: str, caption: str, image_path: Path, fallback_path: Path, highlighted: bool, tone: str) -> None:
    active_border = "#94a3b8"
    active_background = "rgba(19, 23, 34, 0.92)"
    inactive_background = "rgba(19, 23, 34, 0.82)"
    border = active_border if highlighted else "#2a2e39"
    background = active_background if highlighted else inactive_background
    accent = "#d1d4dc"
    text_shadow = "none"
    header_shadow = "none"
    image_overlay = "rgba(15,20,32,0.16)"
    image_opacity = "1" if highlighted else "0.42"
    image_filter = "saturate(1.08) contrast(1.04)" if highlighted else "grayscale(0.22) saturate(0.72) brightness(0.78)"

    caption_size = "20px" if highlighted else "18px"
    caption_weight = "900" if highlighted else "700"
    header_min_height = "122px"
    title_min_height = "20px"
    caption_min_height = "74px"
    st.markdown(
        f"""
        <div style="border:2px solid {border};background:{background};border-radius:14px;padding:10px 10px 6px 10px;box-shadow:{header_shadow};transition:all 0.2s ease;min-height:{header_min_height};display:flex;flex-direction:column;">
            <div style="font-size:13px;color:#e5e7eb;font-weight:700;margin-bottom:4px;min-height:{title_min_height};">{title}</div>
            <div style="font-size:{caption_size};font-weight:{caption_weight};color:{accent};margin-bottom:0;text-align:center;text-shadow:{text_shadow};letter-spacing:-0.02em;min-height:{caption_min_height};display:flex;align-items:center;justify-content:center;line-height:1.25;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{caption}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    card_id = f"emotion-{tone}"
    st.markdown(
        f"""
        <style>
        #{card_id} {{
            width: 100%;
            aspect-ratio: 4 / 4.6;
            max-height: 260px;
            border-radius: 12px;
            overflow: hidden;
            margin-top: 8px;
            position: relative;
            background: {background};
            border: 2px solid {border};
            box-shadow: {header_shadow};
            display: flex;
            align-items: center;
            justify-content: center;
        }}
        #{card_id}::after {{
            content: "";
            position: absolute;
            inset: 0;
            background: {image_overlay if highlighted else "rgba(15,20,32,0.42)"};
            pointer-events: none;
        }}
        #{card_id} img, #{card_id} svg {{
            width: 100%;
            height: 100%;
            object-fit: contain;
            display: block;
            opacity: {image_opacity};
            filter: {image_filter};
        }}
        #{card_id} video {{
            width: 100%;
            height: 100%;
            object-fit: cover;
            display: block;
            opacity: {image_opacity};
            filter: {image_filter};
            background: transparent;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    resolved = _emotion_image_path(image_path, fallback_path)
    if resolved:
        resolved_suffix = resolved.suffix.lower()
        if resolved_suffix == ".svg":
            svg_text = resolved.read_text(encoding="utf-8")
            st.markdown(f'<div id="{card_id}">{svg_text}</div>', unsafe_allow_html=True)
        elif resolved_suffix in {".mp4", ".webm", ".mov"}:
            video_base64 = _get_video_base64(str(resolved))
            video_mime = "video/mp4" if resolved_suffix in {".mp4", ".mov"} else "video/webm"
            video_attrs = "autoplay loop muted playsinline preload=\"auto\"" if highlighted else "muted playsinline preload=\"metadata\""
            st.markdown(
                (
                    f'<div id="{card_id}"><video {video_attrs}>'
                    f'<source src="data:{video_mime};base64,{video_base64}" type="{video_mime}">'
                    "</video></div>"
                ),
                unsafe_allow_html=True,
            )
        else:
            image_base64 = _get_thumbnail_base64(str(resolved))
            st.markdown(
                f'<div id="{card_id}"><img src="data:image/jpeg;base64,{image_base64}"></div>',
                unsafe_allow_html=True,
            )
        return

    fallback_symbol_map = {"positive": ":-)", "negative": ">:(", "neutral": "~_~"}
    fallback_symbol = fallback_symbol_map.get(tone, ":-)")
    st.markdown(
        f"""
        <div id="{card_id}" style="border:1px dashed {border};color:#d1d4dc;font-size:42px;">
            {fallback_symbol}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _emotion_by_position(positions: pd.DataFrame) -> str:
    if positions.empty:
        return "neutral"

    code_column = "code" if "code" in positions.columns else None
    if code_column is None:
        return "neutral"

    codes = positions[code_column].astype(str).tolist()
    if "122630" in codes or "069500" in codes:
        return "positive"
    if "252670" in codes or "114800" in codes:
        return "negative"
    return "neutral"
def _extract_total_assets(summary: dict) -> float:
    for key in ["total_assets", "총자산"]:
        if key in summary:
            try:
                return float(summary.get(key) or 0)
            except (TypeError, ValueError):
                return 0.0
    return 0.0
def _build_asset_history_figure() -> go.Figure | None:
    history = get_asset_history()
    if not history:
        return None

    frame = pd.DataFrame(history)
    if frame.empty or "timestamp" not in frame.columns or "total_assets" not in frame.columns:
        return None

    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="coerce")
    frame["total_assets"] = pd.to_numeric(frame["total_assets"], errors="coerce")
    frame = frame.dropna().tail(80)
    if frame.empty:
        return None

    figure = go.Figure()
    figure.add_trace(
        go.Scatter(
            x=frame["timestamp"],
            y=frame["total_assets"],
            mode="lines",
            line={"color": "#f59e0b", "width": 2},
            fill="tozeroy",
            fillcolor="rgba(245,158,11,0.12)",
            hovertemplate="%{x|%m-%d %H:%M}<br>총자산 %{y:,.0f}원<extra></extra>",
            name="자산 추이",
        )
    )
    figure.update_layout(
        height=318,
        margin={"l": 26, "r": 26, "t": 52, "b": 26},
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        font={"color": "#d1d4dc", "family": "Malgun Gothic"},
        showlegend=False,
        title={"text": "자산 상승 그래프", "x": 0.02, "font": {"size": 13}},
    )
    figure.update_xaxes(showgrid=False, tickfont={"size": 10, "color": "#9aa4b2"}, automargin=True)
    figure.update_yaxes(
        side="right",
        showgrid=True,
        gridcolor="rgba(42,46,57,0.35)",
        tickformat=",.0f",
        tickfont={"size": 10, "color": "#9aa4b2"},
        automargin=True,
    )
    return figure


@st.cache_data(ttl=60, show_spinner=False)
def get_live_trade_history(lookback_days: int = 5) -> pd.DataFrame:
    history_window_start = pd.Timestamp.now(tz=None).normalize() - pd.Timedelta(days=max(int(lookback_days), 1) - 1)
    fetch_start = history_window_start
    start_date = fetch_start.strftime("%Y%m%d")
    end_date = pd.Timestamp.now(tz=None).strftime("%Y%m%d")
    frames = []
    for symbol in ["069500.KS", "114800.KS"]:
        frame = fetch_domestic_daily_ccld(start_date, end_date, symbol=symbol, max_pages=2)
        if not frame.empty:
            frames.append(frame)
    executions = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if executions.empty:
        return executions

    executions = executions.copy()
    executions["timestamp"] = pd.to_datetime(executions["timestamp"], errors="coerce")
    executions = executions.dropna(subset=["timestamp"])
    runtime_orders = get_live_runtime_state().get("orders", [])
    sell_reason_by_order_no: dict[str, str] = {}
    sell_runtime_orders: list[dict[str, object]] = []
    if isinstance(runtime_orders, list):
        for item in runtime_orders:
            if not isinstance(item, dict):
                continue
            if str(item.get("side", "") or "").lower() != "sell":
                continue
            order_no = str(item.get("order_no", "") or "").strip()
            reason = str(item.get("reason", "") or "").strip()
            if order_no and reason:
                sell_reason_by_order_no[order_no] = reason
            runtime_symbol = str(item.get("symbol", "") or "").strip()
            runtime_ts = pd.to_datetime(item.get("timestamp"), errors="coerce")
            if reason and runtime_symbol and pd.notna(runtime_ts):
                sell_runtime_orders.append(
                    {
                        "symbol": runtime_symbol,
                        "timestamp": pd.Timestamp(runtime_ts),
                        "reason": reason,
                    }
                )
    dedupe_keys = [column for column in ["symbol", "side", "quantity", "price", "timestamp", "order_no"] if column in executions.columns]
    if dedupe_keys:
        executions = executions.drop_duplicates(subset=dedupe_keys, keep="first")
    executions = executions.loc[executions["symbol"].isin(["069500.KS", "114800.KS"])].sort_values("timestamp")
    if executions.empty:
        return pd.DataFrame()

    trades: list[dict[str, object]] = []
    open_lots: dict[str, dict[str, object]] = {}

    for execution in executions.itertuples(index=False):
        symbol = str(execution.symbol)
        side = str(execution.side)
        quantity = float(execution.quantity)
        price = float(execution.price)
        name = str(execution.name or display_name(symbol))
        timestamp = pd.Timestamp(execution.timestamp)

        if side == "buy":
            lot = open_lots.get(symbol)
            if lot is None:
                open_lots[symbol] = {
                    "symbol": symbol,
                    "name": name,
                    "entry_time": timestamp,
                    "entry_qty": quantity,
                    "entry_amount": quantity * price,
                }
            else:
                lot["entry_qty"] = float(lot["entry_qty"]) + quantity
                lot["entry_amount"] = float(lot["entry_amount"]) + (quantity * price)
            continue

        lot = open_lots.get(symbol)
        if lot is None:
            continue

        entry_qty = float(lot["entry_qty"])
        entry_amount = float(lot["entry_amount"])
        matched_qty = min(entry_qty, quantity)
        if matched_qty <= 0:
            continue

        entry_avg = entry_amount / entry_qty if entry_qty > 0 else 0.0
        exit_amount = matched_qty * price
        pnl_amount = exit_amount - (matched_qty * entry_avg)
        pnl_rate = (pnl_amount / (matched_qty * entry_avg) * 100.0) if entry_avg > 0 else 0.0
        execution_reason = str(getattr(execution, "reason", "") or "").strip()
        execution_order_no = str(getattr(execution, "order_no", "") or "").strip()
        exit_reason = sell_reason_by_order_no.get(execution_order_no, execution_reason)
        if not str(exit_reason or "").strip():
            nearest_reason = ""
            nearest_gap: float | None = None
            for runtime_sell in sell_runtime_orders:
                if str(runtime_sell.get("symbol", "")) != symbol:
                    continue
                runtime_ts = pd.Timestamp(runtime_sell["timestamp"])
                gap_seconds = abs((runtime_ts - timestamp).total_seconds())
                if gap_seconds > 1800:
                    continue
                if nearest_gap is None or gap_seconds < nearest_gap:
                    nearest_gap = gap_seconds
                    nearest_reason = str(runtime_sell.get("reason", "") or "").strip()
            if nearest_reason:
                exit_reason = nearest_reason
        trades.append(
            {
                "symbol": symbol,
                "name": name,
                "entry_time": pd.Timestamp(lot["entry_time"]),
                "exit_time": timestamp,
                "quantity": matched_qty,
                "entry_price": entry_avg,
                "exit_price": price,
                "pnl_amount": pnl_amount,
                "pnl_rate": pnl_rate,
                "result": "승" if pnl_amount > 0 else "패" if pnl_amount < 0 else "보합",
                "exit_reason": exit_reason,
            }
        )

        remaining_qty = entry_qty - matched_qty
        if remaining_qty > 0:
            open_lots[symbol] = {
                "symbol": symbol,
                "name": name,
                "entry_time": lot["entry_time"],
                "entry_qty": remaining_qty,
                "entry_amount": remaining_qty * entry_avg,
            }
        else:
            del open_lots[symbol]

    history = pd.DataFrame(trades)
    if history.empty:
        return history
    history["exit_time"] = pd.to_datetime(history["exit_time"], errors="coerce")
    history = history.loc[history["exit_time"] >= history_window_start]
    return history.reset_index(drop=True)


@st.cache_data(ttl=60, show_spinner=False)
def get_recent_execution_ledger(lookback_days: int = 7) -> pd.DataFrame:
    start = (pd.Timestamp.now(tz=None).normalize() - pd.Timedelta(days=max(int(lookback_days), 1) - 1)).strftime("%Y%m%d")
    end = pd.Timestamp.now(tz=None).strftime("%Y%m%d")
    frames = []
    for symbol in ["069500.KS", "114800.KS"]:
        frame = fetch_domestic_daily_ccld(start, end, symbol=symbol, max_pages=2)
        if not frame.empty:
            frames.append(frame)
    executions = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if executions.empty:
        return executions
    executions = executions.copy()
    executions = executions.loc[executions["symbol"].isin(["069500.KS", "114800.KS"])]
    if executions.empty:
        return executions
    executions["timestamp"] = pd.to_datetime(executions["timestamp"], errors="coerce")
    dedupe_keys = [column for column in ["symbol", "side", "quantity", "price", "timestamp", "order_no"] if column in executions.columns]
    if dedupe_keys:
        executions = executions.drop_duplicates(subset=dedupe_keys, keep="first")
    executions = executions.dropna(subset=["timestamp"]).sort_values("timestamp", ascending=False)
    return executions.reset_index(drop=True)


def _render_open_live_positions() -> None:
    runtime = get_live_runtime_state()
    last_order_at = str(runtime.get("last_order_at", "") or "")
    cached_order_at = str(st.session_state.get(ACCOUNT_PANEL_LAST_ORDER_KEY, "") or "")
    should_refresh = last_order_at != cached_order_at or ACCOUNT_PANEL_CACHE_KEY not in st.session_state
    current_positions, _, is_stale = _load_balance_quick(refresh=should_refresh)
    if should_refresh and not is_stale:
        st.session_state[ACCOUNT_PANEL_LAST_ORDER_KEY] = last_order_at

    trade_codes = {"069500", "114800"}
    if not current_positions.empty and "code" in current_positions.columns:
        open_view = current_positions[current_positions["code"].astype(str).isin(trade_codes)].copy()
        open_view = _dedupe_positions_frame(open_view)
    else:
        open_view = pd.DataFrame()

    st.markdown("##### 미청산 주문 / 보유중")
    if open_view.empty:
        st.caption("현재 보유 중인 실전 포지션이 없습니다.")
    else:
        st.dataframe(_format_positions_frame(open_view), width="stretch", hide_index=True)


def _render_closed_live_trades() -> None:
    st.markdown("##### 청산 완료 거래")
    st.caption("최근 5일 기준")
    runtime = get_live_runtime_state()
    last_order_at = str(runtime.get("last_order_at", "") or "")
    cached_history = st.session_state.get(CLOSED_TRADES_CACHE_KEY)
    cached_order_at = str(st.session_state.get(CLOSED_TRADES_LAST_ORDER_KEY, "") or "")
    try:
        if isinstance(cached_history, pd.DataFrame) and cached_order_at == last_order_at:
            history = cached_history
        else:
            history = get_live_trade_history(5)
            st.session_state[CLOSED_TRADES_CACHE_KEY] = history
            st.session_state[CLOSED_TRADES_LAST_ORDER_KEY] = last_order_at
    except KisApiError as exc:
        st.caption(f"거래내역 조회 오류: {exc}")
        return
    except Exception as exc:
        st.caption(f"거래내역 집계 오류: {exc}")
        return
    if history.empty:
        st.caption("최근 5일 기준으로 집계된 청산 완료 거래가 없습니다.")
    else:
        wins = int((history["pnl_amount"] > 0).sum())
        draws = int((history["pnl_amount"] == 0).sum())
        losses = int((history["pnl_amount"] < 0).sum())
        total = len(history)
        win_rate = (wins / total * 100.0) if total else 0.0
        realized = float(history["pnl_amount"].sum())

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("거래수", f"{total}")
        col2.metric("승/무/패", f"{wins}/{draws}/{losses}")
        col3.metric("승률", f"{win_rate:.1f}%")
        col4.metric("실현손익", f"{realized:,.0f}원")

        view = history.sort_values("exit_time", ascending=False).copy()
        view["진입구간"] = pd.to_datetime(view["entry_time"]).map(_format_five_min_bucket_label)
        view["청산구간"] = pd.to_datetime(view["exit_time"]).map(_format_five_min_bucket_label)
        view["entry_time"] = pd.to_datetime(view["entry_time"]).dt.strftime("%m-%d %H:%M")
        view["exit_time"] = pd.to_datetime(view["exit_time"]).dt.strftime("%m-%d %H:%M")
        view["quantity"] = view["quantity"].map(lambda value: f"{float(value):,.0f}")
        view["entry_price"] = view["entry_price"].map(lambda value: f"{float(value):,.0f}")
        view["exit_price"] = view["exit_price"].map(lambda value: f"{float(value):,.0f}")
        view["pnl_amount"] = view["pnl_amount"].map(lambda value: f"{float(value):+,.0f}")
        view["pnl_rate"] = view["pnl_rate"].map(lambda value: f"{float(value):+.2f}%")
        if "exit_reason" not in view.columns:
            view["exit_reason"] = ""
        view["exit_reason"] = view["exit_reason"].fillna("").astype(str).str.strip().replace("", "-")
        view = view.rename(
            columns={
                "name": "종목",
                "entry_time": "진입",
                "exit_time": "청산",
                "quantity": "수량",
                "entry_price": "진입가",
                "exit_price": "청산가",
                "pnl_amount": "손익",
                "pnl_rate": "수익률",
                "result": "결과",
                "exit_reason": "청산사유",
            }
        )
        st.dataframe(
            view[["종목", "진입구간", "청산구간", "수량", "진입가", "청산가", "손익", "수익률", "결과", "청산사유"]].head(20),
            width="stretch",
            hide_index=True,
        )


def render_emotion_panel(positions: pd.DataFrame, summary: dict) -> None:
    total_assets = _extract_total_assets(summary)
    if total_assets > 0:
        record_asset_snapshot(total_assets)

    emotion_state = _emotion_by_position(positions)
    positive = emotion_state == "positive"
    neutral = emotion_state == "neutral"
    negative = emotion_state == "negative"

    emotion_left, emotion_center, emotion_right = st.columns(3)
    with emotion_left:
        _render_emotion_card(
            "\uB871\uD3EC\uC9C0\uC158",
            "\uD654\uC5FC\uC758 \uD638\uD761: \uC81C 9\uD615 \u300C\uC624\uC758\u300D",
            POSITIVE_IMAGE_PATH,
            POSITIVE_FALLBACK_PATH,
            positive,
            "positive",
        )
    with emotion_center:
        _render_emotion_card(
            "\uBB34\uD3EC\uC9C0\uC158",
            "\uBB3C\uC758 \uD638\uD761: \uC81C11\uD615\u300C\uC794\uC794\uD55C \uBB3C\uACB0\u300D",
            NEUTRAL_IMAGE_PATH,
            NEUTRAL_FALLBACK_PATH,
            neutral,
            "neutral",
        )
    with emotion_right:
        _render_emotion_card(
            "\uC20F\uD3EC\uC9C0\uC158",
            "\uBC8C\uB808\uC758 \uD638\uD761: \uB098\uBE44\uC758 \uCDA4 \u300C\uC7A5\uB09C\u300D",
            NEGATIVE_IMAGE_PATH,
            NEGATIVE_FALLBACK_PATH,
            negative,
            "negative",
        )


@st.fragment(run_every="30s")

def render_live_account_panel() -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDataFrame"] div[role="columnheader"],
        div[data-testid="stDataFrame"] div[role="gridcell"] {
            font-size: 11px !important;
            line-height: 1.15 !important;
            min-height: 22px !important;
            padding-top: 1px !important;
            padding-bottom: 1px !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("#### 실계좌")
    if not has_kis_account():
        st.info("한투 계좌 정보가 없어 계좌 화면을 표시할 수 없습니다.")
        return

    if ACCOUNT_PANEL_CACHE_KEY not in st.session_state:
        positions, fallback_summary, positions_stale = _load_balance_quick(refresh=True)
    else:
        positions, fallback_summary, positions_stale = _load_balance_quick(refresh=False)
    summary, summary_stale = _load_account_summary_quick()
    if not summary and isinstance(fallback_summary, dict):
        summary = fallback_summary
    is_stale = bool(positions_stale or summary_stale)
    if not summary and positions.empty:
        st.caption("계좌 정보 로딩 중입니다. 차트는 먼저 표시됩니다.")
        return
    if is_stale:
        st.caption("계좌 패널은 최근 캐시 기준으로 먼저 표시 중입니다.")

    purchase_amount = float(summary.get("purchase_amount", 0) or 0)
    profit_amount = float(summary.get("profit_amount", 0) or 0)
    profit_rate = (profit_amount / purchase_amount * 100.0) if purchase_amount > 0 else 0.0

    col1, col2 = st.columns(2)
    col1.metric("총자산", f"{summary.get('total_assets', 0):,.0f}원")
    col2.metric("평가금액", f"{summary.get('eval_amount', 0):,.0f}원")
    col3, col4 = st.columns(2)
    col3.metric("평가손익", f"{profit_amount:,.0f}원")
    col4.metric("수익률", f"{profit_rate:+.2f}%")
    st.caption(f"계좌 {mask_account_number(summary.get('account_number', ''))}")

    st.markdown("##### 보유종목")
    if positions.empty:
        st.info("현재 보유 포지션이 없습니다.")
    else:
        st.dataframe(_format_positions_frame(positions), width="stretch", hide_index=True)


@st.fragment(run_every="5s")
def render_live_trade_history_panel() -> None:
    st.markdown("#### 실전 거래 내역")
    _render_open_live_positions()


@st.fragment(run_every="5s")
def render_closed_live_trade_history_panel() -> None:
    _render_closed_live_trades()


@st.fragment(run_every="60s")
def render_emotion_section() -> None:
    positions, fallback_summary, _ = _load_balance_quick(refresh=False)
    summary = st.session_state.get(ACCOUNT_SUMMARY_CACHE_KEY)
    if not isinstance(summary, dict) or not summary:
        summary = fallback_summary if isinstance(fallback_summary, dict) else {}
    if positions.empty and not summary:
        return
    render_emotion_panel(positions, summary)
def render_live_trade_chart(symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments, profile_name: str) -> None:
    init_live_chart_state()
    strategy_label = "비공개"
    visible_start_date, visible_end_date = get_current_chart_date_range()
    runtime = get_live_runtime_state()
    if runtime["last_status"] in {"checking", "waiting_data"} or not runtime["last_checked_candle"]:
        st.info("엔진이 계산하고 있습니다. 차트와 시그널을 준비하는 중입니다.")
    st.caption(f"표시 기간: {visible_start_date.isoformat()} ~ {visible_end_date.isoformat()}")
    components.html(
        build_live_chart_html(
            server_url="",
            symbol=symbol,
            pair_symbol=pair_symbol,
            stoch_pct=adjustments.stoch_pct,
            cci_pct=adjustments.cci_pct,
            rsi_pct=adjustments.rsi_pct,
            strategy_name=profile_name,
            strategy_label=strategy_label,
            start_date=visible_start_date.isoformat(),
            end_date=visible_end_date.isoformat(),
            render_nonce=LIVE_CHART_COMPONENT_VERSION,
        ),
        height=740,
    )
    return

    server_url = ensure_chart_server()
    pair_query = pair_symbol or ""
    component_key = f"live-chart-{symbol}-{pair_query}-{adjustments.stoch_pct}-{adjustments.cci_pct}-{adjustments.rsi_pct}"
    html = f"""
    <div id="chart-root" style="width:100%;height:560px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <script>
    const root = document.getElementById("chart-root");
    const endpoint = "{server_url}/chart?kind=overlay&symbol={symbol}&pair_symbol={pair_query}&stoch_pct={adjustments.stoch_pct}&cci_pct={adjustments.cci_pct}&rsi_pct={adjustments.rsi_pct}";
    let initialized = false;

    function markerTrace(markers, color, symbol, axisSuffix = "") {{
      const indicatorMode = axisSuffix === "2";
      return {{
        type: "scatter",
        mode: indicatorMode ? "markers" : "markers+text",
        x: markers.map((item) => item.x),
        y: markers.map((item) => item.y),
        text: indicatorMode ? [] : markers.map((item) => item.label),
        textposition: "top center",
        textfont: {{ size: 10, color }},
        marker: {{ color, size: indicatorMode ? 7 : 10, symbol, opacity: indicatorMode ? 0.42 : 1, line: {{ color: "#ffffff", width: 1 }} }},
        hoverinfo: "text",
        hovertext: markers.map((item) => item.label),
        xaxis: axisSuffix ? `x${{axisSuffix}}` : "x",
        yaxis: axisSuffix ? `y${{axisSuffix}}` : "y",
        showlegend: false
      }};
    }}

    function buildFigure(data) {{
      const x = data.candles.map((_, index) => index);
      const tickSource = data.tickText || [];
      const maxTickLabels = 6;
      const interval = Math.max(1, Math.ceil(Math.max(1, x.length - 1) / Math.max(1, maxTickLabels - 1)));
      const tickIndices = [];
      for (let i = 0; i < x.length; i += interval) {{
        tickIndices.push(i);
      }}
      if (x.length > 0) {{
        const lastIndex = x.length - 1;
        const lastSelected = tickIndices.length ? tickIndices[tickIndices.length - 1] : -1;
        if (lastIndex - lastSelected >= Math.max(1, Math.floor(interval * 0.7))) {{
          tickIndices.push(lastIndex);
        }}
      }}
      const tickvals = tickIndices.map((idx) => x[idx]);
      const ticktext = tickIndices.map((idx) => tickSource[idx] ?? "");

      return {{
        data: [
          {{
            type: "candlestick",
            x,
            open: data.candles.map((item) => item.o),
            high: data.candles.map((item) => item.h),
            low: data.candles.map((item) => item.l),
            close: data.candles.map((item) => item.c),
            increasing: {{ line: {{ color: "#089981" }}, fillcolor: "#089981" }},
            decreasing: {{ line: {{ color: "#f23645" }}, fillcolor: "#f23645" }},
            xaxis: "x",
            yaxis: "y",
            hovertemplate: "시가 %{{open:,.0f}}<br>고가 %{{high:,.0f}}<br>저가 %{{low:,.0f}}<br>종가 %{{close:,.0f}}<extra></extra>",
            showlegend: false
          }},
          markerTrace(data.signals.primaryOpenMain || [], "#3b82f6", "circle"),
          markerTrace(data.signals.primaryCloseMain || [], "#ef4444", "circle"),
          markerTrace(data.signals.pairOpenMain || [], "#3b82f6", "star"),
          markerTrace(data.signals.pairCloseMain || [], "#ef4444", "star"),
          markerTrace(data.orders || [], "#f59e0b", "diamond"),
          {{
            type: "scatter", mode: "lines", x, y: data.scr || [], xaxis: "x2", yaxis: "y2",
            line: {{ color: "#ffffff", width: 3.1, dash: "solid" }}, showlegend: false,
            hovertemplate: `${{data.symbolName}} SCR %{{y:.2f}}<extra></extra>`
          }},
          {{
            type: "scatter", mode: "lines", x, y: data.pairScr || [], xaxis: "x2", yaxis: "y2",
            line: {{ color: "#f59e0b", width: 2.5, dash: "dot" }}, showlegend: false,
            hovertemplate: `${{data.pairName || "곱버스"}} SCR %{{y:.2f}}<extra></extra>`
          }},
          markerTrace(data.signals.primaryOpenIndicator || [], "#3b82f6", "circle", "2"),
          markerTrace(data.signals.primaryCloseIndicator || [], "#ef4444", "circle", "2"),
          markerTrace(data.signals.pairOpenIndicator || [], "#3b82f6", "star", "2"),
          markerTrace(data.signals.pairCloseIndicator || [], "#ef4444", "star", "2")
        ],
        layout: {{
          paper_bgcolor: "#131722",
          plot_bgcolor: "#131722",
          font: {{ color: "#d1d4dc", family: "Malgun Gothic" }},
          margin: {{ l: 36, r: 56, t: 42, b: 64 }},
          height: 600,
          dragmode: false,
          hovermode: "x unified",
          hoverdistance: 30,
          spikedistance: 30,
          hoverlabel: {{ bgcolor: "#1e222d", font: {{ color: "#d1d4dc" }} }},
          showlegend: false,
          uirevision: "shinobu-live-chart",
          xaxis: {{
            domain: [0, 1],
            anchor: "y",
            tickmode: "array",
            tickvals,
            ticktext,
            tickangle: 0,
            tickfont: {{ size: 11, color: "#9aa4b2" }},
            showgrid: false,
            showticklabels: true,
            automargin: true,
            ticks: "outside",
            ticklen: 4,
            showline: true,
            linecolor: "rgba(75,85,99,0.8)",
            range: [-0.45, Math.max(x.length - 0.55, 1)],
            fixedrange: true,
            showspikes: true,
            spikemode: "across",
            spikecolor: "#4b5563",
            spikethickness: 1
          }},
          yaxis: {{ domain: [0.31, 1], side: "right", showgrid: true, gridcolor: "rgba(42,46,57,0.65)", fixedrange: true }},
          xaxis2: {{ domain: [0, 1], anchor: "y2", tickmode: "array", tickvals, ticktext, showticklabels: false, showgrid: false, range: [-0.45, Math.max(x.length - 0.55, 1)], fixedrange: true, showspikes: true, spikemode: "across", spikecolor: "#4b5563", spikethickness: 1 }},
          yaxis2: {{ domain: [0, 0.24], side: "right", range: [-1.6, 1.6], tickmode: "array", tickvals: [-1, 0, 1], ticktext: ["하단", "0", "상단"], showgrid: true, gridcolor: "rgba(42,46,57,0.35)" }},
          annotations: [
            {{ x: 0.012, y: 1.04, xref: "paper", yref: "paper", xanchor: "left", showarrow: false, text: `${{data.symbolName}} · 5분봉 · 실전`, font: {{ size: 14, color: "#e5e7eb", family: "Malgun Gothic" }} }},
            {{ x: 0.012, y: 0.27, xref: "paper", yref: "paper", xanchor: "left", showarrow: false, text: "보조지표 (흰색 점선: 레버리지 / 주황 점선: 곱버스)", font: {{ size: 12, color: "#9aa4b2", family: "Malgun Gothic" }} }}
          ]
        }}
      }};
    }}

    async function refreshChart() {{
      const response = await fetch(endpoint, {{ cache: "no-store" }});
      if (!response.ok) throw new Error(`HTTP ${{response.status}}`);
      const payload = await response.json();
      const figure = buildFigure(payload);
      const config = {{ responsive: true, displaylogo: false, displayModeBar: false, scrollZoom: false, modeBarButtonsToRemove: ["zoom2d", "pan2d", "lasso2d", "select2d", "zoomIn2d", "zoomOut2d", "autoScale2d", "resetScale2d"] }};
      if (!initialized) {{
        await Plotly.newPlot(root, figure.data, figure.layout, config);
        initialized = true;
      }} else {{
        await Plotly.react(root, figure.data, figure.layout, config);
      }}
    }}

    refreshChart();
    setInterval(refreshChart, 5000);
    </script>
    """
    components.html(html, height=640)
@st.fragment(run_every="5s")
def run_live_engine(loaded_symbol: str, pair_symbol: str | None, adjustments: StrategyAdjustments, profile_name: str) -> None:
    if os.getenv("SHINOBU_EXTERNAL_ENGINE", "").strip().lower() in {"1", "true", "yes", "on"}:
        return
    if pair_symbol is None:
        return

    try:
        _run_pair_candle_recovery_if_due(loaded_symbol, pair_symbol, adjustments, profile_name)
        process_live_trading_cycle(loaded_symbol, pair_symbol, adjustments, strategy_name=profile_name)
    except KisApiError:
        return
    except Exception:
        return


def ensure_live_engine_running() -> None:
    if not is_live_enabled():
        set_live_enabled(True)


def _run_pair_candle_recovery_if_due(
    primary_symbol: str,
    pair_symbol: str,
    adjustments: StrategyAdjustments,
    profile_name: str,
) -> None:
    global _PAIR_RECOVERY_LAST_RUN_MONOTONIC
    now_monotonic = time.monotonic()
    if (now_monotonic - _PAIR_RECOVERY_LAST_RUN_MONOTONIC) < PAIR_RECOVERY_INTERVAL_SECONDS:
        return
    _PAIR_RECOVERY_LAST_RUN_MONOTONIC = now_monotonic

    if not acquire_named_lock(PAIR_RECOVERY_LOCK_NAME, stale_after_seconds=120):
        _set_pair_recovery_state("다른 세션에서 리커버리 실행 중")
        return

    try:
        raw_primary = market_data.display_symbol(primary_symbol)
        raw_pair = market_data.display_symbol(pair_symbol)
        # Refresh both symbols before alignment so latest close bar can be backfilled first.
        for symbol in (primary_symbol, pair_symbol):
            market_data._load_live_chart_data_impl(symbol, LIVE_TIMEFRAME, lookback_days=7)
        ignore_recent_minutes = _get_pair_recovery_ignore_recent_minutes()
        if not has_raw_intraday_mismatch(
            symbol_a=raw_primary,
            symbol_b=raw_pair,
            timeframe=LIVE_TIMEFRAME,
            ignore_recent_minutes=ignore_recent_minutes,
        ):
            _set_pair_recovery_state("정상 (불일치 없음)")
            return
        recovery = align_raw_intraday_pair_to_intersection(
            symbol_a=raw_primary,
            symbol_b=raw_pair,
            timeframe=LIVE_TIMEFRAME,
            ignore_recent_minutes=ignore_recent_minutes,
        )
        deleted_total = int(recovery.get("deleted_total", 0) or 0)
        only_a_count = int(recovery.get("only_a_count", 0) or 0)
        only_b_count = int(recovery.get("only_b_count", 0) or 0)
        only_a_head = ", ".join((recovery.get("only_a", []) or [])[:3]) or "-"
        only_b_head = ", ".join((recovery.get("only_b", []) or [])[:3]) or "-"
        if deleted_total <= 0:
            _set_pair_recovery_state(
                f"불일치 감지(정리 0건) 롱단독:{only_a_count} / 숏단독:{only_b_count}"
            )
            append_live_log(
                "복구",
                (
                    "캔들 리커버리 이슈 감지: 불일치가 있으나 정리 대상 없음 "
                    f"(롱단독:{only_a_count}, 숏단독:{only_b_count}, "
                    f"롱예시:{only_a_head}, 숏예시:{only_b_head})"
                ),
            )
            return

        lookback_days = _lookback_days_from_current_year_start()
        for symbol in (primary_symbol, pair_symbol):
            source_frame = market_data._load_live_chart_data_impl(symbol, LIVE_TIMEFRAME, lookback_days=lookback_days)
            calculate_strategy_cached(
                source_frame,
                adjustments,
                LIVE_TIMEFRAME,
                strategy_name=profile_name,
                symbol=symbol,
            )
        clear_chart_payload_caches()
        st.cache_data.clear()
        _set_pair_recovery_state(
            f"복구 완료 (삭제 {deleted_total}개, 롱:{int(recovery.get('deleted_a', 0) or 0)} / 숏:{int(recovery.get('deleted_b', 0) or 0)})"
        )
        append_live_log(
            "복구",
            (
                "캔들 리커버리 완료: "
                f"총 {deleted_total}개 정리 "
                f"(롱삭제:{int(recovery.get('deleted_a', 0) or 0)}, 숏삭제:{int(recovery.get('deleted_b', 0) or 0)}, "
                f"롱단독:{only_a_count}, 숏단독:{only_b_count}, "
                f"롱예시:{only_a_head}, 숏예시:{only_b_head})"
            ),
        )
    except Exception as exc:
        _set_pair_recovery_state(f"복구 실패: {exc}")
        append_live_log("오류", f"캔들 리커버리 실패: {exc}")
        raise
    finally:
        release_named_lock(PAIR_RECOVERY_LOCK_NAME)


def render_live_trading_panel(pair_symbol: str | None) -> None:
    st.markdown("#### 실전 투자")
    execution_mode = get_current_execution_mode()
    execution_label = "x1 ETF" if execution_mode == EXECUTION_MODE_X1 else "레버리지/곱버스"
    if pair_symbol is None:
        st.warning("실전 투자는 레버리지/인버스 페어 종목에서만 실행됩니다.")

    status_text = "항상 실행 중"
    status_color = "#3b82f6"
    st.markdown(
        f"""
        <div style="margin-bottom:10px;">
            <span style="display:inline-block;padding:5px 10px;border-radius:999px;background:{status_color}22;color:{status_color};font-size:12px;">
                상태: {status_text}
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(
        f"실전 주문은 5분봉 기준으로만 처리하고, 5초마다 최신 완료 봉을 확인합니다. "
        f"현재 전략은 {get_strategy_label(get_current_strategy_profile())}, 실제 주문 대상은 {execution_label}이며, 매수 시 주문가능현금을 최대한 사용합니다."
    )
    st.caption(
        f"캔들 리커버리(최근 {_get_pair_recovery_ignore_recent_minutes()}분 제외): {_get_pair_recovery_state_text()}"
    )

    runtime = get_live_runtime_state()
    status_name = {
        "running": "실행 중",
        "stopped": "중지됨",
        "checking": "봉 확인 중",
        "waiting_data": "데이터 대기",
        "idle": "신호 대기",
        "holding": "보유 유지",
        "ordered": "주문 완료",
        "waiting_cash": "주문 가능 금액 대기",
        "error": "오류",
    }.get(runtime["last_status"], runtime["last_status"] or "-")
    st.markdown("##### 엔진 상태")
    info_left, info_right = st.columns(2)
    info_left.caption(f"마지막 확인: {runtime['last_cycle_at'] or '-'}")
    info_right.caption(f"마지막 주문: {runtime['last_order_at'] or '-'}")
    st.caption(f"마지막 완료 봉: {runtime['last_checked_candle'] or '-'}")
    st.caption(f"엔진 상태: {status_name}")
    if runtime["last_error"]:
        st.warning(runtime["last_error"])

    if pair_symbol is None:
        st.warning("현재 종목은 실전 페어 전략 대상이 아닙니다.")

    logs = get_live_logs()
    log_html = "".join(
        f'<div style="padding:10px 0;border-top:1px solid #1e222d;color:#d1d4dc;font-size:14px;">{message}</div>'
        for message in logs
    )
    if not log_html:
        log_html = '<div style="padding:10px 0;color:#9aa4b2;font-size:14px;">실전 매매 로그가 아직 없습니다.</div>'

    st.markdown(
        f"""
        <div style="background:#131722;border:1px solid #2a2e39;border-radius:12px;padding:14px;height:280px;overflow-y:auto;">
            <div style="font-size:13px;color:#9aa4b2;margin-bottom:12px;">실전 매매 로그</div>
            {log_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def _filter_frame_by_date(frame: pd.DataFrame, start_value: date, end_value: date) -> pd.DataFrame:
    if frame.empty:
        return frame
    index = pd.DatetimeIndex(frame.index)
    start_ts = pd.Timestamp(start_value)
    end_ts = pd.Timestamp(end_value) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    if index.tz is not None:
        start_ts = start_ts.tz_localize(index.tz) if start_ts.tzinfo is None else start_ts.tz_convert(index.tz)
        end_ts = end_ts.tz_localize(index.tz) if end_ts.tzinfo is None else end_ts.tz_convert(index.tz)
    else:
        if start_ts.tzinfo is not None:
            start_ts = start_ts.tz_convert(None)
        if end_ts.tzinfo is not None:
            end_ts = end_ts.tz_convert(None)
    return frame.loc[(index >= start_ts) & (index <= end_ts)].copy()


def _marker_y(frame: pd.DataFrame, mask: pd.Series, region: str, extra_scale: float = 1.0) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    span = (frame["High"] - frame["Low"]).astype(float)
    fallback = frame["Close"].abs().astype(float) * 0.01
    offset = span.where(span > 0, fallback).fillna(fallback).replace(0, 1.0) * 0.55 * float(extra_scale)
    if region == "upper":
        return (frame["High"] + offset).where(mask)
    return (frame["Low"] - offset).where(mask)


def _spread_marker_y(
    frame: pd.DataFrame,
    base_y: pd.Series,
    mask: pd.Series,
    *,
    region: str,
    level: int,
) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    span = (frame["High"] - frame["Low"]).astype(float)
    fallback = frame["Close"].abs().astype(float) * 0.01
    gap = span.where(span > 0, fallback).fillna(fallback).replace(0, 1.0) * 0.90
    distance = gap * max(int(level), 0)
    if region == "upper":
        return (base_y + distance).where(mask)
    return (base_y - distance).where(mask)


def _backtest_combined_performance(frame: pd.DataFrame) -> tuple[int, int, float, float]:
    if frame.empty:
        return 0, 0, 0.0, 0.0
    position: str | None = None
    entry_price = 0.0
    trade_returns: list[float] = []
    closed_returns: list[float] = []

    for _, row in frame.iterrows():
        close_price = float(row.get("Close", 0.0) or 0.0)
        if close_price <= 0:
            continue
        long_open = bool(row.get("long_open", False))
        long_close = bool(row.get("long_close", False))
        short_open = bool(row.get("short_open", False))
        short_close = bool(row.get("short_close", False))

        # Close first so same-candle switch (close + open) is reflected in order.
        if position == "long" and long_close:
            ret = (close_price / entry_price) - 1.0 if entry_price > 0 else 0.0
            trade_returns.append(ret)
            closed_returns.append(ret)
            position = None
            entry_price = 0.0
        elif position == "short" and short_close:
            ret = (entry_price / close_price) - 1.0 if entry_price > 0 else 0.0
            trade_returns.append(ret)
            closed_returns.append(ret)
            position = None
            entry_price = 0.0

        if position is not None:
            continue

        if long_open and not short_open:
            position = "long"
            entry_price = close_price
            continue
        if short_open and not long_open:
            position = "short"
            entry_price = close_price

    if position is not None and entry_price > 0:
        last_close = float(frame.iloc[-1].get("Close", 0.0) or 0.0)
        if last_close > 0:
            if position == "long":
                trade_returns.append((last_close / entry_price) - 1.0)
            elif position == "short":
                trade_returns.append((entry_price / last_close) - 1.0)

    if not trade_returns:
        return 0, 0, 0.0, 0.0
    win_rate = (
        float(sum(1 for value in closed_returns if value > 0) / len(closed_returns) * 100.0)
        if closed_returns
        else 0.0
    )
    cumulative = 1.0
    for value in trade_returns:
        cumulative *= 1.0 + value
    cumulative_return = (cumulative - 1.0) * 100.0
    return len(closed_returns), len(trade_returns), win_rate, cumulative_return


def _wait_backtest_job(job_id: str, timeout_seconds: float = 90.0) -> dict[str, object]:
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while time.monotonic() <= deadline:
        job = get_backtest_job(job_id)
        if isinstance(job, dict):
            status = str(job.get("status", "queued"))
            if status in {"succeeded", "failed"}:
                return job
        time.sleep(0.25)
    return {"status": "failed", "error": f"timeout ({timeout_seconds:.0f}s)"}


def _wait_named_lock_release(lock_name: str, timeout_seconds: float = 600.0, poll_seconds: float = 0.5) -> bool:
    started = time.monotonic()
    while (time.monotonic() - started) < timeout_seconds:
        if not is_named_lock_locked(lock_name, stale_after_seconds=SAJU_GLOBAL_LOCK_STALE_SECONDS):
            return True
        time.sleep(max(poll_seconds, 0.1))
    return False


def _load_saju_price_frame(symbol: str, interval: str, period: str) -> pd.DataFrame:
    candidates: list[str] = []
    upper_symbol = str(symbol).strip().upper()
    if upper_symbol:
        candidates.append(upper_symbol)
    if upper_symbol.endswith(".KS"):
        candidates.append(upper_symbol.replace(".KS", ".KQ"))
        candidates.append(upper_symbol.replace(".KS", ""))
    elif upper_symbol.endswith(".KQ"):
        candidates.append(upper_symbol.replace(".KQ", ".KS"))
        candidates.append(upper_symbol.replace(".KQ", ""))
    elif len(upper_symbol) == 6 and upper_symbol.isdigit():
        candidates.append(f"{upper_symbol}.KS")
        candidates.append(f"{upper_symbol}.KQ")

    # Keep order while removing duplicates.
    unique_candidates = list(dict.fromkeys(candidates))
    raw = pd.DataFrame()
    expected_seconds: int | None = None
    if interval in {"5m", "15m", "30m", "60m", "1h"}:
        minute = 60 if interval in {"60m", "1h"} else int(interval.replace("m", ""))
        expected_seconds = minute * 60

    for candidate in unique_candidates:
        raw = yf.download(
            candidate,
            interval=interval,
            period=period,
            auto_adjust=False,
            progress=False,
            prepost=False,
            threads=False,
        )
        if raw.empty:
            continue
        # Some symbols (e.g. wrong market suffix) may return daily bars for intraday requests.
        if expected_seconds is not None and len(raw.index) >= 3:
            idx = pd.to_datetime(raw.index, errors="coerce")
            idx = idx[~idx.isna()]
            if len(idx) >= 3:
                deltas = pd.Series(idx[1:].values - idx[:-1].values)
                median_delta_sec = float(pd.to_timedelta(deltas).dt.total_seconds().median())
                if median_delta_sec > expected_seconds * 4:
                    continue
        break
    if raw.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    frame = raw.copy()
    if isinstance(frame.columns, pd.MultiIndex):
        frame.columns = frame.columns.get_level_values(0)
    rename_map = {str(column): str(column).title() for column in frame.columns}
    frame = frame.rename(columns=rename_map)
    required = ["Open", "High", "Low", "Close", "Volume"]
    if any(col not in frame.columns for col in required):
        return pd.DataFrame(columns=required)
    out = frame.loc[:, required].copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()].sort_index()
    if isinstance(out.index, pd.DatetimeIndex) and out.index.tz is not None:
        out.index = out.index.tz_convert("Asia/Seoul").tz_localize(None)
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def _summarize_saju_ohlcv(
    frame: pd.DataFrame,
    *,
    timeframe: str,
    interval: str,
    period: str,
) -> dict[str, object]:
    if frame.empty or len(frame) < 20:
        return {
            "timeframe": timeframe,
            "interval": interval,
            "period": period,
            "status": "failed",
            "error": "OHLCV 데이터가 부족합니다.",
        }
    recent = frame.tail(min(len(frame), 8))
    recent_candles = [
        (
            f"{pd.Timestamp(ts).strftime('%Y-%m-%d %H:%M')} "
            f"O:{float(row['Open']):.1f} H:{float(row['High']):.1f} "
            f"L:{float(row['Low']):.1f} C:{float(row['Close']):.1f} V:{float(row['Volume']):.0f}"
        )
        for ts, row in recent.iterrows()
    ]
    first_close = float(frame["Close"].iloc[0] or 0.0)
    last_close = float(frame["Close"].iloc[-1] or 0.0)
    return_pct = ((last_close / first_close) - 1.0) * 100.0 if first_close > 0 else 0.0
    scr_latest = None
    scr_mean_20 = None
    scr_delta_5 = None
    try:
        strategy_frame = calculate_strategy(
            frame.copy(),
            adjustments=None,
            timeframe_label=timeframe,
            strategy_name=DEFAULT_STRATEGY_NAME,
            initial_state=None,
        )
        if isinstance(strategy_frame, pd.DataFrame) and "scr_line" in strategy_frame.columns:
            scr_series = pd.to_numeric(strategy_frame["scr_line"], errors="coerce").dropna()
            if not scr_series.empty:
                scr_latest = float(scr_series.iloc[-1])
                scr_mean_20 = float(scr_series.tail(min(20, len(scr_series))).mean())
                if len(scr_series) >= 6:
                    scr_delta_5 = float(scr_series.iloc[-1] - scr_series.iloc[-6])
                elif len(scr_series) >= 2:
                    scr_delta_5 = float(scr_series.iloc[-1] - scr_series.iloc[0])
    except Exception:
        pass

    high = frame["High"].astype(float)
    low = frame["Low"].astype(float)
    tenkan = (high.rolling(9).max() + low.rolling(9).min()) / 2.0
    kijun = (high.rolling(26).max() + low.rolling(26).min()) / 2.0
    senkou_a = (tenkan + kijun) / 2.0
    senkou_b = (high.rolling(52).max() + low.rolling(52).min()) / 2.0
    senkou_a_now = senkou_a.shift(26)
    senkou_b_now = senkou_b.shift(26)
    cloud_top = float(pd.concat([senkou_a_now, senkou_b_now], axis=1).max(axis=1).iloc[-1])
    cloud_bottom = float(pd.concat([senkou_a_now, senkou_b_now], axis=1).min(axis=1).iloc[-1])
    if pd.isna(cloud_top) or pd.isna(cloud_bottom):
        cloud_top = float(pd.concat([senkou_a, senkou_b], axis=1).max(axis=1).iloc[-1])
        cloud_bottom = float(pd.concat([senkou_a, senkou_b], axis=1).min(axis=1).iloc[-1])
    bull_cloud = bool(
        pd.notna(senkou_a_now.iloc[-1]) and pd.notna(senkou_b_now.iloc[-1]) and (senkou_a_now.iloc[-1] > senkou_b_now.iloc[-1])
    )
    if last_close > cloud_top:
        cloud_position = "above"
    elif last_close < cloud_bottom:
        cloud_position = "below"
    else:
        cloud_position = "inside"

    return {
        "timeframe": timeframe,
        "interval": interval,
        "period": period,
        "status": "ok",
        "bars": int(len(frame)),
        "first_ts": pd.Timestamp(frame.index[0]).strftime("%Y-%m-%d %H:%M"),
        "last_ts": pd.Timestamp(frame.index[-1]).strftime("%Y-%m-%d %H:%M"),
        "latest_close": last_close,
        "period_high": float(frame["High"].max() or 0.0),
        "period_low": float(frame["Low"].min() or 0.0),
        "avg_volume": float(frame["Volume"].mean() or 0.0),
        "latest_volume": float(frame["Volume"].iloc[-1] or 0.0),
        "recent_return_pct": float(return_pct),
        "scr_latest": scr_latest,
        "scr_mean_20": scr_mean_20,
        "scr_delta_5": scr_delta_5,
        "cloud_top": float(cloud_top) if pd.notna(cloud_top) else None,
        "cloud_bottom": float(cloud_bottom) if pd.notna(cloud_bottom) else None,
        "cloud_bull": bull_cloud,
        "cloud_position": cloud_position,
        "recent_candles": recent_candles,
    }


def _build_saju_codex_prompt(
    *,
    symbol_name: str,
    symbol_code: str,
    summaries: list[dict[str, object]],
) -> str:
    current_row = next(
        (row for row in summaries if str(row.get("timeframe")) == "1h" and str(row.get("status")) == "ok"),
        None,
    )
    current_price = float(current_row.get("latest_close", 0.0) or 0.0) if isinstance(current_row, dict) else 0.0
    lines: list[str] = [
        "[입력 데이터]",
        f"종목: {symbol_name} ({symbol_code})",
        "고정 타임프레임/기간: 1h(60일), 1d(1년), 1w(3년)",
        f"현재가(1h 최신 종가 기준): {current_price:.4f}" if current_price > 0 else "현재가: 확인 불가",
        "",
        "[OHLCV 요약]",
    ]
    for row in summaries:
        timeframe = str(row.get("timeframe", ""))
        if str(row.get("status", "failed")) != "ok":
            lines.append(f"- {timeframe}: 실패 ({row.get('error', 'unknown')})")
            continue
        scr_latest = row.get("scr_latest")
        scr_mean_20 = row.get("scr_mean_20")
        scr_delta_5 = row.get("scr_delta_5")
        cloud_top = row.get("cloud_top")
        cloud_bottom = row.get("cloud_bottom")
        cloud_bull = bool(row.get("cloud_bull", False))
        cloud_position = str(row.get("cloud_position", "unknown"))
        scr_text = (
            f"SCR latest={float(scr_latest):.2f}, mean20={float(scr_mean_20):.2f}, delta5={float(scr_delta_5):+.2f}"
            if scr_latest is not None and scr_mean_20 is not None and scr_delta_5 is not None
            else "SCR n/a"
        )
        cloud_text = (
            f"Cloud top/bottom={float(cloud_top):.1f}/{float(cloud_bottom):.1f}, "
            f"state={'bull' if cloud_bull else 'bear'}, price_pos={cloud_position}"
            if cloud_top is not None and cloud_bottom is not None
            else "Cloud n/a"
        )
        lines.append(
            "- {tf}({interval}/{period}): bars={bars}, 구간={first}~{last}, 최신종가={last_close:.4f}, "
            "수익률={ret:.2f}%, 고가최대={high:.4f}, 저가최소={low:.4f}, 평균거래량={avg_vol:.0f}, 최신거래량={last_vol:.0f}, {scr_text}, {cloud_text}".format(
                tf=timeframe,
                interval=str(row.get("interval", "")),
                period=str(row.get("period", "")),
                bars=int(row.get("bars", 0)),
                first=str(row.get("first_ts", "-")),
                last=str(row.get("last_ts", "-")),
                last_close=float(row.get("latest_close", 0.0) or 0.0),
                ret=float(row.get("recent_return_pct", 0.0) or 0.0),
                high=float(row.get("period_high", 0.0) or 0.0),
                low=float(row.get("period_low", 0.0) or 0.0),
                avg_vol=float(row.get("avg_volume", 0.0) or 0.0),
                last_vol=float(row.get("latest_volume", 0.0) or 0.0),
                scr_text=scr_text,
                cloud_text=cloud_text,
            )
        )
        recent_candles = row.get("recent_candles", [])
        if isinstance(recent_candles, list) and recent_candles:
            lines.append(f"  최근 캔들: {' | '.join(str(value) for value in recent_candles)}")

    lines.extend(["", "[전략 참고 출처]"])
    for source in SAJU_REFERENCE_SOURCES:
        lines.append(f"- {source['title']} | {source['note']} | {source['url']}")

    lines.extend(
        [
            "",
            "[요청사항]",
            "인터넷에서 널리 쓰이는 최신 기술적 분석 접근(추세/모멘텀/거래량/변동성/파동)을 참고해, 위 OHLCV + SCR + 구름대(이치모쿠) 요약으로 해석해줘.",
            "SRC 전체 시그널 카운트/체결 정보는 사용하지 마.",
            "반드시 한국어로, 숫자/가격 중심으로 간결하고 직관적으로 작성해줘.",
            "",
            "1) 단기(1h) / 중기(1d) / 장기(1w) 판단",
            "- 각 구간별 방향(상승/하락/중립), 신뢰도(0~100), 핵심 근거 1개",
            "",
            "2) 종합 판단",
            "- 최종 방향(상승/하락/중립), 종합 신뢰도(0~100)",
            "",
            "3) 상승 시나리오 (현재가 기준)",
            "- 1차/2차 목표가(가격과 %), 무효화 가격",
            "",
            "4) 하락 시나리오 (현재가 기준)",
            "- 1차/2차 하락 목표가(가격과 %), 최소 반등 예상 구간(가격 범위)",
            "",
            "5) 엘리어트 파동 관점",
            "- 현재를 1~5파 또는 ABC 어디로 보는지, 기준 가격 포함",
            "- 피보나치(되돌림/확장) 기준 최소 반등 구간과 목표가 포함",
            "- 무효화 조건(이 가격 이탈 시 시나리오 폐기) 포함",
            "",
            "6) 즉시 행동 가이드",
            "- 매수/관망/분할매수 중 1개와 리스크 경고 2개",
            "",
            "7) 한 줄 결론",
        ]
    )
    return "\n".join(lines)


def _call_saju_codex_analysis(prompt_text: str) -> str:
    codex_bin = shutil.which("codex")
    if not codex_bin:
        raise RuntimeError("Codex CLI를 찾을 수 없습니다. EC2에서 `npm i -g @openai/codex` 설치 후 다시 시도해주세요.")

    model = str(get_secret("OPENAI_MODEL", "gpt-5.4") or "").strip()
    output_path = ""
    try:
        with tempfile.NamedTemporaryFile(prefix="saju-codex-", suffix=".txt", delete=False) as output_file:
            output_path = output_file.name

        cmd = [
            codex_bin,
            "exec",
            "--ephemeral",
            "--skip-git-repo-check",
            "--output-last-message",
            output_path,
            "-",
        ]
        if model:
            cmd[2:2] = ["-m", model]

        completed = subprocess.run(
            cmd,
            input=prompt_text,
            text=True,
            capture_output=True,
            timeout=240,
            cwd=str(Path(__file__).resolve().parent),
            env=os.environ.copy(),
            check=False,
        )
        stdout_text = str(completed.stdout or "")
        stderr_text = str(completed.stderr or "")
        combined = f"{stdout_text}\n{stderr_text}".strip()
        if completed.returncode != 0:
            lowered = combined.lower()
            if "403" in lowered or "forbidden" in lowered or "login" in lowered:
                raise RuntimeError("Codex CLI 인증이 필요합니다. EC2에서 `codex login --device-auth`를 먼저 완료해주세요.")
            raise RuntimeError(f"Codex CLI 실행 실패(returncode={completed.returncode}): {combined[-500:]}")

        content = Path(output_path).read_text(encoding="utf-8", errors="ignore").strip()
        if not content:
            raise RuntimeError("Codex 응답 텍스트를 추출하지 못했습니다.")
        return content
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Codex CLI 분석 시간이 초과되었습니다(240초).") from exc
    finally:
        if output_path:
            try:
                Path(output_path).unlink(missing_ok=True)
            except Exception:
                pass


def _render_reference_sources(sources: object) -> None:
    if not isinstance(sources, list) or not sources:
        return
    st.markdown("**참고 출처**")
    for source in sources:
        if not isinstance(source, dict):
            continue
        title = str(source.get("title", "") or "").strip()
        url = str(source.get("url", "") or "").strip()
        note = str(source.get("note", "") or "").strip()
        if title and url:
            st.markdown(f"- [{title}]({url}) : {note}")


def _run_saju_analysis_once(symbol_input: str) -> dict[str, object]:
    resolved_symbol, resolved_name = market_data.resolve_symbol(symbol_input)
    rows: list[dict[str, object]] = []
    with st.spinner("종목 사주보기 계산 중입니다..."):
        for tf, interval, period in SAJU_TIMEFRAME_WINDOWS:
            try:
                price_frame = _load_saju_price_frame(resolved_symbol, interval=interval, period=period)
                rows.append(
                    _summarize_saju_ohlcv(
                        price_frame,
                        timeframe=tf,
                        interval=interval,
                        period=period,
                    )
                )
            except Exception as tf_exc:
                rows.append(
                    {
                        "timeframe": tf,
                        "interval": interval,
                        "period": period,
                        "status": "failed",
                        "error": str(tf_exc),
                    }
                )

        prompt = _build_saju_codex_prompt(
            symbol_name=resolved_name,
            symbol_code=resolved_symbol,
            summaries=rows,
        )
        analysis_text = _call_saju_codex_analysis(prompt)

    return {
        "ok": True,
        "symbol": resolved_symbol,
        "name": resolved_name,
        "rows": rows,
        "analysis": analysis_text,
        "generated_at": pd.Timestamp.now(tz="Asia/Seoul").strftime("%Y-%m-%d %H:%M:%S"),
        "sources": SAJU_REFERENCE_SOURCES,
    }


def render_backtest_tab(profile_name: str, adjustments: StrategyAdjustments) -> None:
    st.markdown("#### 백테스팅")
    st.caption("yfinance 기반 `5m/15m/30m/60m/1h/4h/1d`로 SRC 신호를 계산하고 long/short open·close를 표시합니다.")

    today = pd.Timestamp.now().date()
    timeframe_options = get_backtest_timeframe_labels()
    current_timeframe = str(st.session_state.get("backtest-timeframe-input", "30m") or "30m")
    default_timeframe_index = timeframe_options.index(current_timeframe) if current_timeframe in timeframe_options else 2
    col_a, col_b = st.columns([1.2, 1.0], vertical_alignment="top")
    with col_a:
        symbol_input = st.text_input(
            "종목",
            value="에이비엘바이오",
            key="backtest-symbol-input",
            help="예: 122630, 252670, 005930, BTC-USD, 삼성전자",
        )
        timeframe = st.selectbox(
            "타임프레임",
            options=timeframe_options,
            index=default_timeframe_index,
            key="backtest-timeframe-input",
        )
        max_days = get_backtest_timeframe_max_days(timeframe)
        if max_days is None:
            st.caption("조회 가능 기간: 제한 없음 (1d)")
        else:
            st.caption(f"조회 가능 기간: 최근 {int(max_days)}일")
        if timeframe == "4h":
            st.caption("4h는 yfinance 60m 데이터를 `resample('4h')`로 집계해 계산합니다.")
    max_days = get_backtest_timeframe_max_days(timeframe)
    if max_days is None:
        min_date = date(1990, 1, 1)
        default_start = max(min_date, (today - pd.Timedelta(days=365)))
    else:
        min_date = (today - pd.Timedelta(days=max(int(max_days), 1) - 1))
        default_start = max(min_date, (today - pd.Timedelta(days=min(30, max(int(max_days), 1) - 1))))
    default_end = today
    with col_b:
        start_date = st.date_input(
            "시작일",
            value=default_start,
            min_value=min_date,
            max_value=today,
            key="backtest-start-date-input",
        )
        end_date = st.date_input(
            "종료일",
            value=default_end,
            min_value=min_date,
            max_value=today,
            key="backtest-end-date-input",
        )
    if start_date > end_date:
        start_date, end_date = end_date, start_date
    if max_days is not None:
        selected_days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days + 1
        if selected_days > int(max_days):
            start_date = (pd.Timestamp(end_date) - pd.Timedelta(days=int(max_days) - 1)).date()
            st.warning(f"{timeframe}는 최근 {int(max_days)}일만 조회할 수 있어 시작일을 자동 보정했습니다.")

    action_col1, action_col2 = st.columns([1, 1], vertical_alignment="center")
    with action_col1:
        run_clicked = st.button("신호 계산", type="primary", key="backtest-run-button", use_container_width=True)

    saju_running = (
        bool(st.session_state.get(BACKTEST_SAJU_RUNNING_STATE_KEY, False))
        or SAJU_ANALYSIS_LOCK.locked()
        or is_named_lock_locked(SAJU_GLOBAL_LOCK_NAME, stale_after_seconds=SAJU_GLOBAL_LOCK_STALE_SECONDS)
    )
    with action_col2:
        saju_clicked = st.button(
            "종목 사주보기",
            key="backtest-saju-button",
            use_container_width=True,
            disabled=saju_running,
        )
    if run_clicked:
        try:
            resolved_symbol, resolved_name = market_data.resolve_symbol(symbol_input)
            job_id = submit_backtest_job(
                symbol=resolved_symbol,
                timeframe=timeframe,
                strategy_name=profile_name,
                adjustments=adjustments,
            )
            st.session_state[BACKTEST_JOB_ID_STATE_KEY] = job_id
            st.session_state[BACKTEST_RESULT_STATE_KEY] = {
                "symbol": resolved_symbol,
                "name": resolved_name,
                "input_symbol": str(symbol_input).strip(),
                "timeframe": timeframe,
                "job_id": job_id,
                "requested_start": pd.Timestamp(start_date).date().isoformat(),
                "requested_end": pd.Timestamp(end_date).date().isoformat(),
            }
        except Exception as exc:
            st.session_state[BACKTEST_RESULT_STATE_KEY] = {"error": str(exc)}
            st.session_state[BACKTEST_JOB_ID_STATE_KEY] = ""

    if saju_clicked:
        if not SAJU_ANALYSIS_LOCK.acquire(blocking=False):
            st.warning("다른 종목 사주보기 계산이 진행 중입니다. 잠시 후 다시 시도해주세요.")
        else:
            has_global_lock = False
            st.session_state[BACKTEST_SAJU_RUNNING_STATE_KEY] = True
            try:
                has_global_lock = acquire_named_lock(
                    SAJU_GLOBAL_LOCK_NAME,
                    stale_after_seconds=SAJU_GLOBAL_LOCK_STALE_SECONDS,
                )
                if not has_global_lock:
                    with st.spinner("다른 사용자의 종목 사주보기 계산이 끝날 때까지 대기 중입니다..."):
                        released = _wait_named_lock_release(
                            SAJU_GLOBAL_LOCK_NAME,
                            timeout_seconds=600.0,
                            poll_seconds=0.5,
                        )
                    if not released:
                        raise RuntimeError("대기 시간이 초과되었습니다. 잠시 후 다시 시도해주세요.")
                    has_global_lock = acquire_named_lock(
                        SAJU_GLOBAL_LOCK_NAME,
                        stale_after_seconds=SAJU_GLOBAL_LOCK_STALE_SECONDS,
                    )
                    if not has_global_lock:
                        raise RuntimeError("종목 사주보기 글로벌 락 획득에 실패했습니다. 다시 시도해주세요.")

                st.session_state[BACKTEST_SAJU_RESULT_STATE_KEY] = _run_saju_analysis_once(symbol_input)
            except Exception as exc:
                st.session_state[BACKTEST_SAJU_RESULT_STATE_KEY] = {"ok": False, "error": str(exc)}
            finally:
                st.session_state[BACKTEST_SAJU_RUNNING_STATE_KEY] = False
                if has_global_lock:
                    release_named_lock(SAJU_GLOBAL_LOCK_NAME)
                SAJU_ANALYSIS_LOCK.release()

    saju_result = st.session_state.get(BACKTEST_SAJU_RESULT_STATE_KEY)
    if isinstance(saju_result, dict):
        st.markdown("##### 종목 사주보기")
        if not bool(saju_result.get("ok", False)):
            st.error(f"종목 사주보기 실패: {saju_result.get('error', '알 수 없는 오류')}")
        else:
            st.caption(
                f"{saju_result.get('name')} ({saju_result.get('symbol')}) · 생성시각: {saju_result.get('generated_at', '-')}"
            )
            st.markdown(str(saju_result.get("analysis", "")))
            with st.expander("타임프레임별 계산 요약", expanded=False):
                rows_df = pd.DataFrame(saju_result.get("rows", []))
                if rows_df.empty:
                    st.caption("요약 데이터가 없습니다.")
                else:
                    st.dataframe(rows_df, use_container_width=True, hide_index=True)
            _render_reference_sources(saju_result.get("sources"))
        # 질문 1회당 세션 1개 원칙: 출력 직후 결과 세션 제거
        st.session_state.pop(BACKTEST_SAJU_RESULT_STATE_KEY, None)

    result = st.session_state.get(BACKTEST_RESULT_STATE_KEY)
    if not isinstance(result, dict):
        st.info("입력값을 정하고 `신호 계산`을 눌러주세요.")
        return
    if result.get("error"):
        st.error(f"백테스트 실패: {result['error']}")
        return

    current_start = pd.Timestamp(start_date).date()
    current_end = pd.Timestamp(end_date).date()
    if current_start > current_end:
        current_start, current_end = current_end, current_start
    result_timeframe = str(result.get("timeframe") or timeframe)
    requested_start = str(result.get("requested_start", ""))
    requested_end = str(result.get("requested_end", ""))
    if (
        str(result.get("input_symbol", "")).strip() != str(symbol_input).strip()
        or result_timeframe != timeframe
        or (requested_start and requested_start != current_start.isoformat())
        or (requested_end and requested_end != current_end.isoformat())
    ):
        st.info("입력 조건이 변경되었습니다. 변경한 조건으로 `신호 계산`을 다시 눌러주세요.")
        return

    st.caption(
        f"{result['name']} ({result['symbol']}) · {result_timeframe} · {current_start.isoformat()} ~ {current_end.isoformat()}"
    )
    job_id = str(result.get("job_id") or st.session_state.get(BACKTEST_JOB_ID_STATE_KEY) or "")
    if not job_id:
        st.info("신호 계산을 먼저 실행해주세요.")
        return
    job = get_backtest_job(job_id)
    if not isinstance(job, dict):
        st.error("백테스트 작업 정보를 찾을 수 없습니다. 다시 실행해주세요.")
        return
    status = str(job.get("status", "queued"))
    if status in {"queued", "running"}:
        started_at_text = str(job.get("started_at", "") or "")
        elapsed_text = "-"
        if started_at_text:
            try:
                started_at = pd.Timestamp(started_at_text)
                now_utc = pd.Timestamp.utcnow()
                if getattr(now_utc, "tzinfo", None) is not None:
                    now_utc = now_utc.tz_localize(None)
                if getattr(started_at, "tzinfo", None) is not None:
                    started_at = started_at.tz_localize(None)
                elapsed_seconds = max(0, int((now_utc - started_at).total_seconds()))
                elapsed_text = f"{elapsed_seconds}초"
            except Exception:
                elapsed_text = "-"
        st.info(f"백테스트 계산 실행 중 (경과: {elapsed_text}). 완료되면 자동으로 결과를 갱신합니다.")
        time.sleep(1.2)
        st.rerun()
        return
    if status == "failed":
        st.error(f"백테스트 실패: {job.get('error', '알 수 없는 오류')}")
        return
    strategy_frame = job.get("strategy_frame")
    if not isinstance(strategy_frame, pd.DataFrame):
        st.error("백테스트 데이터가 유효하지 않습니다. 다시 실행해주세요.")
        return
    filtered_frame = _filter_frame_by_date(strategy_frame, current_start, current_end)
    if filtered_frame.empty:
        st.warning("선택한 기간에 데이터가 없습니다. 기간을 넓혀주세요.")
        return
    frame = build_long_short_signals(filtered_frame)
    if frame.empty:
        st.warning("선택한 조건으로 계산된 봉이 없습니다. 조건을 다시 확인해주세요.")
        return
    closed_trade_count, trade_count, win_rate_pct, cumulative_return_pct = _backtest_combined_performance(frame)
    metric_cols = st.columns(4)
    metric_cols[0].metric("Long Open", f"{int(frame['long_open'].sum())}")
    metric_cols[1].metric("Long Close", f"{int(frame['long_close'].sum())}")
    metric_cols[2].metric("Short Open", f"{int(frame['short_open'].sum())}")
    metric_cols[3].metric("Short Close", f"{int(frame['short_close'].sum())}")
    perf_cols = st.columns(3)
    perf_cols[0].metric("거래 수", f"{closed_trade_count}/{trade_count}")
    perf_cols[1].metric("승률(청산기준)", f"{win_rate_pct:.1f}%")
    perf_cols[2].metric("누적 수익률", f"{cumulative_return_pct:.2f}%")

    price_fig = go.Figure()
    tick_source = pd.to_datetime(frame.index)
    is_intraday = result_timeframe in {"5m", "15m", "30m", "60m", "1h", "4h"}
    if is_intraday:
        x_keys = tick_source.strftime("%Y-%m-%d %H:%M").tolist()
        candle_hover_times = x_keys
    else:
        x_keys = tick_source.strftime("%Y-%m-%d").tolist()
        candle_hover_times = x_keys
    x_map = pd.Series(x_keys, index=frame.index)
    max_tick_labels = 6
    step = max(1, len(x_keys) // max(1, max_tick_labels - 1))
    tick_indices = list(range(0, len(x_keys), step))
    if len(x_keys) > 0 and tick_indices[-1] != (len(x_keys) - 1):
        tick_indices.append(len(x_keys) - 1)
    tickvals = [x_keys[idx] for idx in tick_indices]
    if is_intraday:
        ticktext = [tick_source[idx].strftime("%m-%d %H:%M") for idx in tick_indices]
    else:
        ticktext = [tick_source[idx].strftime("%Y-%m-%d") for idx in tick_indices]

    price_fig.add_trace(
        go.Candlestick(
            x=x_keys,
            open=frame["Open"],
            high=frame["High"],
            low=frame["Low"],
            close=frame["Close"],
            text=[
                f"Time {t}<br>Open {o:,.1f}<br>High {h:,.1f}<br>Low {l:,.1f}<br>Close {c:,.1f}"
                for t, o, h, l, c in zip(candle_hover_times, frame["Open"], frame["High"], frame["Low"], frame["Close"], strict=False)
            ],
            hovertext=[
                f"Time {t}<br>Open {o:,.1f}<br>High {h:,.1f}<br>Low {l:,.1f}<br>Close {c:,.1f}"
                for t, o, h, l, c in zip(candle_hover_times, frame["Open"], frame["High"], frame["Low"], frame["Close"], strict=False)
            ],
            hoverinfo="text",
            name="price",
            increasing={"line": {"color": "#089981"}, "fillcolor": "#089981"},
            decreasing={"line": {"color": "#f23645"}, "fillcolor": "#f23645"},
            showlegend=False,
        )
    )
    long_open = frame.loc[frame["long_open"]]
    long_close = frame.loc[frame["long_close"]]
    short_open = frame.loc[frame["short_open"]]
    short_close = frame.loc[frame["short_close"]]
    long_open_y = _marker_y(frame, frame["long_open"], "lower", 1.40)
    long_close_y = _marker_y(frame, frame["long_close"], "upper", 1.40)
    short_open_y = _marker_y(frame, frame["short_open"], "upper", 1.40)
    short_close_y = _marker_y(frame, frame["short_close"], "lower", 1.40)
    # Keep close above open when markers overlap on the same candle.
    # Lower region: close should be closer to price (upper), open farther down.
    long_open_y = _spread_marker_y(frame, long_open_y, frame["long_open"], region="lower", level=2)
    short_close_y = _spread_marker_y(frame, short_close_y, frame["short_close"], region="lower", level=0)
    # Upper region: close farther up, open closer to price (lower).
    long_close_y = _spread_marker_y(frame, long_close_y, frame["long_close"], region="upper", level=2)
    short_open_y = _spread_marker_y(frame, short_open_y, frame["short_open"], region="upper", level=0)

    if not long_open.empty:
        price_fig.add_trace(
            go.Scatter(
                x=x_map.loc[long_open.index].tolist(),
                y=long_open_y.loc[long_open.index],
                mode="markers",
                name="long_open",
                marker={"color": "#16a34a", "symbol": "circle", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not long_close.empty:
        price_fig.add_trace(
            go.Scatter(
                x=x_map.loc[long_close.index].tolist(),
                y=long_close_y.loc[long_close.index],
                mode="markers",
                name="long_close",
                marker={"color": "#16a34a", "symbol": "circle-open", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not short_open.empty:
        price_fig.add_trace(
            go.Scatter(
                x=x_map.loc[short_open.index].tolist(),
                y=short_open_y.loc[short_open.index],
                mode="markers",
                name="short_open",
                marker={"color": "#dc2626", "symbol": "star", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not short_close.empty:
        price_fig.add_trace(
            go.Scatter(
                x=x_map.loc[short_close.index].tolist(),
                y=short_close_y.loc[short_close.index],
                mode="markers",
                name="short_close",
                marker={"color": "#dc2626", "symbol": "star-open", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    price_fig.update_layout(
        height=400,
        paper_bgcolor="#131722",
        plot_bgcolor="#131722",
        font={"color": "#d1d4dc", "family": "Malgun Gothic"},
        margin={"l": 24, "r": 56, "t": 42, "b": 18},
        dragmode="pan",
        hovermode="closest",
        showlegend=False,
        bargap=0,
        bargroupgap=0,
        xaxis={
            "tickmode": "array",
            "tickvals": tickvals,
            "ticktext": ticktext,
            "type": "category",
            "categoryorder": "array",
            "categoryarray": x_keys,
            "range": [x_keys[0], x_keys[-1]] if x_keys else None,
            "showgrid": False,
            "fixedrange": False,
            "rangeslider": {"visible": False},
            "tickfont": {"size": 11, "color": "#9aa4b2"},
        },
        yaxis={
            "side": "right",
            "showgrid": True,
            "gridcolor": "rgba(42,46,57,0.65)",
            "fixedrange": False,
        },
        annotations=[
            {
                "x": 0.01,
                "y": 1.04,
                "xref": "paper",
                "yref": "paper",
                "xanchor": "left",
                "showarrow": False,
                "text": f"{result['name']} | {timeframe} | 백테스트 가격",
                "font": {"size": 14, "color": "#e5e7eb", "family": "Malgun Gothic"},
            },
            {
                "x": 0.99,
                "y": 1.04,
                "xref": "paper",
                "yref": "paper",
                "xanchor": "right",
                "showarrow": False,
                "text": "비공개",
                "font": {"size": 13, "color": "#60a5fa", "family": "Malgun Gothic"},
            },
        ],
    )
    backtest_chart_id = f"backtest-chart-{int(time.time() * 1000)}"
    figure_json = json.dumps(price_fig.to_plotly_json(), cls=PlotlyJSONEncoder)
    backtest_chart_html = f"""
    <div id="{backtest_chart_id}" style="width:100%;height:560px;background:#131722;border:1px solid #2a2e39;border-radius:12px;"></div>
    <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
    <script>
      const fig = {figure_json};
      const config = {{
        responsive: true,
        displaylogo: false,
        displayModeBar: false,
        doubleClick: false,
        scrollZoom: true
      }};
      Plotly.newPlot("{backtest_chart_id}", fig.data, fig.layout, config);
    </script>
    """
    components.html(backtest_chart_html, height=460)

    st.markdown("##### 신호 로그")
    signal_rows = frame.loc[
        frame["long_open"] | frame["long_close"] | frame["short_open"] | frame["short_close"],
        ["Close", "long_open", "long_close", "short_open", "short_close"],
    ].copy()
    if signal_rows.empty:
        st.caption("기간 내 신호가 없습니다.")
    else:
        signal_rows = signal_rows.reset_index()
        time_col = signal_rows.columns[0]
        signal_rows[time_col] = pd.to_datetime(signal_rows[time_col]).dt.strftime("%Y-%m-%d")
        st.dataframe(
            signal_rows,
            use_container_width=True,
            hide_index=True,
            column_config={
                time_col: st.column_config.TextColumn("일자"),
                "Close": st.column_config.NumberColumn("종가", format="%.2f"),
                "long_open": st.column_config.CheckboxColumn("Long Open"),
                "long_close": st.column_config.CheckboxColumn("Long Close"),
                "short_open": st.column_config.CheckboxColumn("Short Open"),
                "short_close": st.column_config.CheckboxColumn("Short Close"),
            },
        )


def render_stock_recommendation_tab() -> None:
    st.markdown("#### 종목추천")
    st.warning("이 종목은 투자 권유가 아닙니다. 개인적으로 공부용입니다.")
    st.caption("장 열리는 평일 06:00(KST)에 하루 1회 계산하며, 당일에는 저장된 추천 결과만 표시합니다.")

    now_kst = pd.Timestamp.now(tz="Asia/Seoul")
    today = now_kst.date()
    payload = load_recommendations_for(today)

    if payload is None:
        st.info("오늘 추천 데이터가 아직 없습니다. 백엔드 스케줄러(평일 06:00 KST) 실행 후 표시됩니다.")
        return

    generated_at = str(payload.get("generated_at", "") or "")
    run_date = str(payload.get("run_date", "") or today.isoformat())
    meta = payload.get("meta", {})
    items = payload.get("items", [])
    if generated_at:
        st.caption(f"기준일: {run_date} · 생성시각: {generated_at}")

    if isinstance(meta, dict):
        market_message = str(meta.get("message", "") or "").strip()
        if market_message:
            st.caption(market_message)
        stats = []
        for key in ("universe_scanned", "weekly_pass", "daily_checked", "selected"):
            if key in meta:
                stats.append(f"{key}={meta.get(key)}")
        if stats:
            st.caption(" · ".join(stats))

    if not isinstance(items, list) or not items:
        st.warning("추천 조건을 만족한 종목이 없습니다.")
    else:
        rows = pd.DataFrame(items)
        view_cols = [
            col
            for col in [
                "name",
                "symbol",
                "daily_close",
                "daily_alignment",
                "score",
                "wave_target_1",
                "wave_target_2",
                "invalidation",
            ]
            if col in rows.columns
        ]
        if view_cols:
            st.dataframe(
                rows[view_cols].rename(
                    columns={
                        "name": "종목명",
                        "symbol": "심볼",
                        "daily_close": "현재가",
                        "daily_alignment": "일봉 배열",
                        "score": "점수",
                        "wave_target_1": "파동 목표가1",
                        "wave_target_2": "파동 목표가2",
                        "invalidation": "무효화 가격",
                    }
                ),
                use_container_width=True,
                hide_index=True,
            )

        st.markdown("##### 추천 사유")
        for idx, row in enumerate(items, start=1):
            name = str(row.get("name", "") or row.get("symbol", ""))
            symbol = str(row.get("symbol", "") or "")
            reason = str(row.get("reason", "") or "")
            score = float(row.get("score", 0.0) or 0.0)
            st.markdown(f"**{idx}위 {name} ({symbol}) · 점수 {score:.2f}**")
            st.write(reason)

    st.markdown("---")
    st.markdown("#### 추천 이력 조회")
    lookback_days = st.selectbox(
        "조회 기간",
        options=[30, 60, 90, 180, 365],
        index=2,
        format_func=lambda value: f"최근 {int(value)}일",
        key="stock-reco-history-lookback",
    )

    history = load_recommendation_history(days=int(lookback_days))
    totals = history.get("totals", {}) if isinstance(history, dict) else {}
    daily_rows = history.get("daily", []) if isinstance(history, dict) else []
    symbol_rows = history.get("symbols", []) if isinstance(history, dict) else []

    metric_cols = st.columns(3)
    metric_cols[0].metric("조회된 일수", f"{int(totals.get('days_loaded', 0))}")
    metric_cols[1].metric("누적 추천 종목 수", f"{int(totals.get('unique_symbols', 0))}")
    metric_cols[2].metric("가장 최근 추천 종목 수", f"{int(totals.get('active_today', 0))}")

    if daily_rows:
        st.markdown("##### 일자별 등장/이탈")
        daily_df = pd.DataFrame(daily_rows).rename(
            columns={
                "date": "일자",
                "count": "추천수",
                "added_count": "신규진입",
                "removed_count": "이탈",
                "added_symbols": "신규 종목(일부)",
                "removed_symbols": "이탈 종목(일부)",
            }
        )
        st.dataframe(daily_df, use_container_width=True, hide_index=True)
    else:
        st.info("추천 이력이 아직 없습니다.")

    if symbol_rows:
        st.markdown("##### 종목별 누적 추천 횟수")
        symbol_df = pd.DataFrame(symbol_rows).rename(
            columns={
                "name": "종목명",
                "symbol": "심볼",
                "count": "추천 횟수",
                "first_date": "최초 추천일",
                "last_date": "최근 추천일",
                "active_today": "최근일 포함",
                "last_removed_date": "최근 이탈일",
            }
        )
        ordered_cols = [
            col
            for col in [
                "종목명",
                "심볼",
                "추천 횟수",
                "최초 추천일",
                "최근 추천일",
                "최근일 포함",
                "최근 이탈일",
            ]
            if col in symbol_df.columns
        ]
        st.dataframe(symbol_df[ordered_cols], use_container_width=True, hide_index=True)


@st.fragment(run_every="1s")
def render_reset_running_page() -> None:
    state = _get_reset_state()
    started = float(state.get("started_monotonic", 0.0) or 0.0)
    elapsed = int(max(0.0, time.monotonic() - started))
    message = str(state.get("message", "") or "\uCD08\uAE30\uD654 \uC911\uC785\uB2C8\uB2E4.")
    current_step = int(state.get("current_step", 0) or 0)
    total_steps = int(state.get("total_steps", 0) or 0)

    st.title("\uCD08\uAE30\uD654\uC911\uC785\uB2C8\uB2E4")
    st.warning("\uCD5C\uCD08 \uAD6C\uB3D9 \uCD08\uAE30\uD654 \uC791\uC5C5\uC774 \uC9C4\uD589 \uC911\uC785\uB2C8\uB2E4. \uC7A0\uC2DC\uB9CC \uAE30\uB2E4\uB824\uC8FC\uC138\uC694.")
    if total_steps > 0:
        st.caption(f"{message} ({min(current_step, total_steps)}/{total_steps} \uB2E8\uACC4, {elapsed}\uCD08 \uACBD\uACFC)")
        progress = min(1.0, max(0.0, current_step / max(total_steps, 1)))
    else:
        st.caption(f"{message} ({elapsed}\uCD08 \uACBD\uACFC)")
        progress = 0.0
    error_text = str(state.get("error", "") or "")
    if error_text:
        st.caption(f"\uC7AC\uC2DC\uB3C4 \uC0AC\uC720: {error_text}")
    st.progress(max(0.0, min(1.0, progress)))


def main() -> None:
    init_live_state()
    ensure_live_engine_running()
    loaded_symbol = PRIMARY_SYMBOL
    pair_symbol = get_pair_symbol(loaded_symbol)
    _ensure_startup_initialization(loaded_symbol, pair_symbol)
    reset_state = _get_reset_state()
    if bool(reset_state.get("running", False)) or not bool(reset_state.get("done", False)):
        render_reset_running_page()
        return

    init_live_chart_state()
    init_strategy_profile_state()
    init_chart_date_range_state()
    init_execution_mode_state()
    adjustments = StrategyAdjustments(stoch_pct=0, cci_pct=0, rsi_pct=0)
    base_profile_name = get_current_strategy_profile()

    st.markdown(
        """
        <style>
        div[data-testid="stTabs"] button[role="tab"] {
            font-size: 1.18rem;
            font-weight: 800;
            padding: 0.85rem 1.4rem;
            min-height: 3.1rem;
        }
        div[data-testid="stTabs"] button[role="tab"] p {
            font-size: 1.18rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    live_tab, backtest_tab = st.tabs(["실전", "백테스팅"])

    with live_tab:
        render_header(base_profile_name)
        live_profile_name = render_live_selector_bar()
        left, right = st.columns([2.2, 1], vertical_alignment="top")
        with right:
            render_live_account_panel()
            run_live_engine(loaded_symbol, pair_symbol, adjustments, live_profile_name)
            render_live_trading_panel(pair_symbol)
        with left:
            render_live_trade_header(loaded_symbol, pair_symbol)
            chart_slot = st.empty()
            emotion_slot = st.empty()
            history_slot = st.empty()
            with emotion_slot.container():
                render_emotion_section()
            with chart_slot.container():
                render_live_trade_chart(loaded_symbol, pair_symbol, adjustments, live_profile_name)
            with history_slot.container():
                st.markdown("---")
                render_live_trade_history_panel()
                render_closed_live_trade_history_panel()

    with backtest_tab:
        render_backtest_tab(get_current_strategy_profile(), adjustments)



if __name__ == "__main__":
    main()
