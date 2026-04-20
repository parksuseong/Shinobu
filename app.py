from __future__ import annotations

import base64
import threading
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import date
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from PIL import Image

from config import has_kis_account
from shinobu import data as market_data
from shinobu.cache_db import (
    acquire_named_lock,
    align_raw_intraday_pair_to_intersection,
    acquire_startup_init_lock,
    clear_chart_payload_caches,
    clear_all_cache_data,
    get_raw_intraday_range,
    has_raw_intraday_mismatch,
    is_startup_init_locked,
    is_startup_initialized,
    mark_startup_initialized,
    release_named_lock,
    release_startup_init_lock,
)
from shinobu.chart import build_candlestick_chart, update_candlestick_chart
from shinobu.chart_payload import ensure_live_chart_prewarm_bundle, run_live_chart_prewarm_sync
from shinobu.live_chart_component import build_live_chart_html
from shinobu.backtest_engine import build_long_short_signals, get_backtest_job, submit_backtest_job
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
    st.markdown(f"\uD604\uC7AC \uC804\uB7B5: **{get_strategy_label(profile_name)}**")


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
    option_map = {option.key: option for option in list_strategy_options()}

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

    active_option = option_map[get_current_strategy_profile()]
    mode_label = "x1 ETF" if get_current_execution_mode() == EXECUTION_MODE_X1 else "\uB808\uBC84\uB9AC\uC9C0/\uACF1\uBC84\uC2A4"
    chart_start_date, chart_end_date = get_current_chart_date_range()
    st.caption(
        f"\uD604\uC7AC \uC804\uB7B5: {active_option.label} | \uCC28\uD2B8 \uD45C\uC2DC: {chart_start_date.isoformat()} ~ {chart_end_date.isoformat()} | \uC2E4\uC81C \uC8FC\uBB38: {mode_label}"
    )
    st.caption("\uB9C8\uCEE4 \uD45C\uC2DC \uD544\uD130\uB294 \uCC28\uD2B8 \uC0C1\uB2E8\uC5D0\uC11C \uBC14\uB85C \uD1A0\uAE00\uD569\uB2C8\uB2E4.")
    st.caption(get_strategy_help_text(active_option.key).replace("\n", " | "))
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
            }
        )
        st.dataframe(
            view[["종목", "진입구간", "청산구간", "수량", "진입가", "청산가", "손익", "수익률", "결과"]].head(20),
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
    strategy_label = get_strategy_label(profile_name)
    visible_start_date, visible_end_date = get_current_chart_date_range()
    runtime = get_live_runtime_state()
    if runtime["last_status"] in {"checking", "waiting_data"} or not runtime["last_checked_candle"]:
        st.info("엔진이 계산하고 있습니다. 차트와 시그널을 준비하는 중입니다.")
    st.caption(f"차트 반영 전략: {strategy_label} · 표시 기간: {visible_start_date.isoformat()} ~ {visible_end_date.isoformat()}")
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
            render_nonce=0,
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
    start_ts = pd.Timestamp(start_value)
    end_ts = pd.Timestamp(end_value) + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
    return frame.loc[(frame.index >= start_ts) & (frame.index <= end_ts)].copy()


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


def _backtest_long_performance(frame: pd.DataFrame) -> tuple[int, float, float]:
    if frame.empty:
        return 0, 0.0, 0.0
    in_position = False
    entry_price = 0.0
    trade_returns: list[float] = []

    for _, row in frame.iterrows():
        close_price = float(row.get("Close", 0.0) or 0.0)
        if close_price <= 0:
            continue
        long_open = bool(row.get("long_open", False))
        long_close = bool(row.get("long_close", False))

        if not in_position and long_open:
            in_position = True
            entry_price = close_price
            continue
        if in_position and long_close:
            trade_returns.append((close_price / entry_price) - 1.0 if entry_price > 0 else 0.0)
            in_position = False
            entry_price = 0.0

    if in_position and entry_price > 0:
        last_close = float(frame.iloc[-1].get("Close", 0.0) or 0.0)
        if last_close > 0:
            trade_returns.append((last_close / entry_price) - 1.0)

    if not trade_returns:
        return 0, 0.0, 0.0
    win_rate = float(sum(1 for value in trade_returns if value > 0) / len(trade_returns) * 100.0)
    cumulative = 1.0
    for value in trade_returns:
        cumulative *= 1.0 + value
    cumulative_return = (cumulative - 1.0) * 100.0
    return len(trade_returns), win_rate, cumulative_return


def render_backtest_tab(profile_name: str, adjustments: StrategyAdjustments) -> None:
    st.markdown("#### 백테스팅")
    st.caption("yfinance 기반 `30분봉/일봉/4시간봉`으로 SRC 신호를 계산하고 long/short open·close를 표시합니다.")

    today = pd.Timestamp.now().date()
    default_end = today
    default_start = today - pd.Timedelta(days=30)
    col_a, col_b = st.columns([1.2, 1.0], vertical_alignment="bottom")
    with col_a:
        symbol_input = st.text_input(
            "종목",
            value="에이비엘바이오",
            key="backtest-symbol-input",
            help="예: 122630, 252670, 005930, BTC-USD, 삼성전자",
        )
        timeframe = st.selectbox(
            "타임프레임",
            options=["30분봉", "일봉", "4시간봉"],
            index=0,
            key="backtest-timeframe-input",
        )
    with col_b:
        start_date = st.date_input("시작일", value=default_start, key="backtest-start-date-input")
        end_date = st.date_input("종료일", value=default_end, key="backtest-end-date-input")

    run_clicked = st.button("신호 계산", type="primary", key="backtest-run-button")
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
                "timeframe": timeframe,
                "job_id": job_id,
            }
        except Exception as exc:
            st.session_state[BACKTEST_RESULT_STATE_KEY] = {"error": str(exc)}
            st.session_state[BACKTEST_JOB_ID_STATE_KEY] = ""

    result = st.session_state.get(BACKTEST_RESULT_STATE_KEY)
    if not isinstance(result, dict):
        st.info("입력값을 정하고 `시뮬레이션 실행`을 눌러주세요.")
        return
    if result.get("error"):
        st.error(f"백테스트 실패: {result['error']}")
        return

    current_start = pd.Timestamp(start_date).date()
    current_end = pd.Timestamp(end_date).date()
    if current_start > current_end:
        current_start, current_end = current_end, current_start
    st.caption(
        f"{result['name']} ({result['symbol']}) · {result['timeframe']} · {current_start.isoformat()} ~ {current_end.isoformat()}"
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
    trade_count, win_rate_pct, cumulative_return_pct = _backtest_long_performance(frame)
    metric_cols = st.columns(4)
    metric_cols[0].metric("Long Open", f"{int(frame['long_open'].sum())}")
    metric_cols[1].metric("Long Close", f"{int(frame['long_close'].sum())}")
    metric_cols[2].metric("Short Open", f"{int(frame['short_open'].sum())}")
    metric_cols[3].metric("Short Close", f"{int(frame['short_close'].sum())}")
    perf_cols = st.columns(3)
    perf_cols[0].metric("거래 수", f"{trade_count}")
    perf_cols[1].metric("승률", f"{win_rate_pct:.1f}%")
    perf_cols[2].metric("누적 수익률", f"{cumulative_return_pct:.2f}%")

    price_fig = go.Figure()
    price_fig.add_trace(
        go.Scatter(
            x=frame.index,
            y=frame["Close"],
            mode="lines",
            name="종가",
            line={"color": "#60a5fa", "width": 1.8},
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
                x=long_open.index,
                y=long_open_y.loc[long_open.index],
                mode="markers",
                name="long_open",
                marker={"color": "#16a34a", "symbol": "circle", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not long_close.empty:
        price_fig.add_trace(
            go.Scatter(
                x=long_close.index,
                y=long_close_y.loc[long_close.index],
                mode="markers",
                name="long_close",
                marker={"color": "#16a34a", "symbol": "circle-open", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not short_open.empty:
        price_fig.add_trace(
            go.Scatter(
                x=short_open.index,
                y=short_open_y.loc[short_open.index],
                mode="markers",
                name="short_open",
                marker={"color": "#dc2626", "symbol": "star", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    if not short_close.empty:
        price_fig.add_trace(
            go.Scatter(
                x=short_close.index,
                y=short_close_y.loc[short_close.index],
                mode="markers",
                name="short_close",
                marker={"color": "#dc2626", "symbol": "star-open", "size": 13, "line": {"color": "#ffffff", "width": 1.2}},
            )
        )
    price_fig.update_layout(
        height=420,
        margin={"l": 24, "r": 24, "t": 24, "b": 24},
        template="plotly_dark",
        legend={"orientation": "h", "y": 1.02, "x": 0},
    )
    st.plotly_chart(price_fig, use_container_width=True)

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
