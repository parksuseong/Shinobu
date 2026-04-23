from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from shinobu import data as market_data
from shinobu.strategy import StrategyAdjustments, calculate_strategy, normalize_strategy_name


@dataclass(frozen=True)
class BacktestTimeframeSpec:
    label: str
    interval: str
    max_days: int | None
    resample_rule: str | None = None


BACKTEST_TIMEFRAME_SPECS: dict[str, BacktestTimeframeSpec] = {
    "5m": BacktestTimeframeSpec("5m", "5m", 60),
    "15m": BacktestTimeframeSpec("15m", "15m", 60),
    "30m": BacktestTimeframeSpec("30m", "30m", 60),
    "1h": BacktestTimeframeSpec("1h", "1h", 60),
    "4h": BacktestTimeframeSpec("4h", "60m", 60, resample_rule="4h"),
    "1d": BacktestTimeframeSpec("1d", "1d", None),
    "1w": BacktestTimeframeSpec("1w", "1wk", None),
}
_BACKTEST_EXPECTED_STEP_MINUTES: dict[str, int] = {
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "4h": 240,
}

_BACKTEST_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="shinobu-backtest")
_BACKTEST_LOCK = threading.RLock()
_BACKTEST_JOBS: dict[str, dict[str, Any]] = {}
BACKTEST_FETCH_TIMEOUT_SECONDS = 25.0
BACKTEST_CALC_TIMEOUT_SECONDS = 25.0
_KST = ZoneInfo("Asia/Seoul")


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def get_backtest_timeframe_labels() -> list[str]:
    return list(BACKTEST_TIMEFRAME_SPECS.keys())


def get_backtest_timeframe_max_days(timeframe_label: str) -> int | None:
    spec = BACKTEST_TIMEFRAME_SPECS.get(timeframe_label)
    return None if spec is None else spec.max_days


def _timeframe_period(spec: BacktestTimeframeSpec) -> str:
    if spec.max_days is None:
        return "max"
    return f"{int(spec.max_days)}d"


def _download_ohlcv(symbol: str, *, interval: str, period: str) -> pd.DataFrame:
    raw = yf.download(
        symbol,
        interval=interval,
        period=period,
        auto_adjust=False,
        progress=False,
        prepost=False,
        threads=False,
    )
    return _normalize_ohlcv_frame(raw)


def _normalize_ohlcv_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    normalized = frame.copy()
    if isinstance(normalized.columns, pd.MultiIndex):
        normalized.columns = normalized.columns.get_level_values(0)

    rename_map = {str(column): str(column).title() for column in normalized.columns}
    normalized = normalized.rename(columns=rename_map)

    required_columns = ["Open", "High", "Low", "Close", "Volume"]
    missing = [column for column in required_columns if column not in normalized.columns]
    if missing:
        return pd.DataFrame(columns=required_columns)

    out = normalized.loc[:, required_columns].copy()
    out.index = pd.to_datetime(out.index, errors="coerce")
    out = out[~out.index.isna()]
    idx = pd.DatetimeIndex(out.index)
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    idx = idx.tz_convert(_KST).tz_localize(None)
    out.index = idx
    out = out.sort_index()
    return out.dropna(subset=["Open", "High", "Low", "Close"])


def _resample_ohlcv(frame: pd.DataFrame, rule: str) -> pd.DataFrame:
    if frame.empty:
        return frame
    # Align intraday buckets to KRX market-open clock (09:00 KST).
    resampled = frame.resample(rule, origin="start_day", offset="9h").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
            "Volume": "sum",
        }
    )
    return resampled.dropna(subset=["Open", "High", "Low", "Close"])


def _matches_timeframe_granularity(frame: pd.DataFrame, timeframe_label: str) -> bool:
    expected_minutes = _BACKTEST_EXPECTED_STEP_MINUTES.get(str(timeframe_label))
    if expected_minutes is None:
        return True
    if frame.empty or len(frame.index) < 3:
        return False

    index = pd.DatetimeIndex(frame.index).sort_values()
    deltas = index.to_series().diff().dropna()
    intraday_deltas = deltas[deltas < pd.Timedelta(days=1)]
    if intraday_deltas.empty:
        return False

    median_step = intraday_deltas.median()
    expected_step = pd.Timedelta(minutes=int(expected_minutes))
    min_allowed = expected_step * 0.5
    max_allowed = expected_step * 1.5
    return bool(min_allowed <= median_step <= max_allowed)


def _load_backtest_frame_from_yfinance(symbol: str, timeframe_label: str) -> pd.DataFrame:
    spec = BACKTEST_TIMEFRAME_SPECS.get(timeframe_label)
    if spec is None:
        allowed = ", ".join(get_backtest_timeframe_labels())
        raise ValueError(f"Unsupported backtest timeframe: {timeframe_label} (allowed: {allowed})")

    period = _timeframe_period(spec)
    frame = _download_ohlcv(symbol, interval=spec.interval, period=period)
    if spec.resample_rule:
        frame = _resample_ohlcv(frame, spec.resample_rule)
    if _matches_timeframe_granularity(frame, timeframe_label):
        return frame

    fallback_specs: dict[str, list[tuple[str, str | None]]] = {
        "15m": [("5m", "15min")],
        "30m": [("15m", "30min")],
        "1h": [("30m", "1h"), ("15m", "1h")],
        "4h": [("30m", "4h"), ("15m", "4h"), ("5m", "4h")],
    }
    for fallback_interval, fallback_rule in fallback_specs.get(timeframe_label, []):
        fallback_frame = _download_ohlcv(symbol, interval=fallback_interval, period=period)
        if fallback_rule:
            fallback_frame = _resample_ohlcv(fallback_frame, fallback_rule)
        if _matches_timeframe_granularity(fallback_frame, timeframe_label):
            return fallback_frame

    raise ValueError(
        f"yfinance returned mismatched granularity for {symbol} ({timeframe_label}); "
        "requested intraday bars but received non-intraday spacing."
    )


def _backtest_symbol_candidates(symbol: str) -> list[str]:
    text = str(symbol or "").strip().upper()
    candidates: list[str] = []
    if text:
        candidates.append(text)
    if text.endswith(".KS"):
        candidates.append(text[:-3] + ".KQ")
    elif text.endswith(".KQ"):
        candidates.append(text[:-3] + ".KS")
    elif text.isdigit() and len(text) == 6:
        candidates.append(f"{text}.KS")
        candidates.append(f"{text}.KQ")
    unique: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if item and item not in seen:
            unique.append(item)
            seen.add(item)
    return unique


def _load_backtest_frame_with_fallback(symbol: str, timeframe_label: str) -> tuple[pd.DataFrame, str]:
    last_error: Exception | None = None
    for candidate in _backtest_symbol_candidates(symbol):
        try:
            frame = _load_backtest_frame_from_yfinance(candidate, timeframe_label)
        except Exception as exc:  # pragma: no cover - defensive path
            last_error = exc
            continue
        if not frame.empty:
            return frame, candidate
    if last_error is not None:
        raise last_error
    return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"]), str(symbol)


def build_long_short_signals(strategy_frame: pd.DataFrame) -> pd.DataFrame:
    frame = strategy_frame.copy()
    frame["long_open"] = frame["buy_open"].astype(bool)
    frame["long_close"] = frame["buy_close"].astype(bool)
    frame["short_open"] = frame["buy_close"].astype(bool)
    frame["short_close"] = frame["buy_open"].astype(bool)
    return frame


def _run_with_timeout(func: Any, *args: Any, timeout_seconds: float) -> Any:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(func, *args)
    try:
        return future.result(timeout=float(timeout_seconds))
    except FuturesTimeoutError as exc:
        raise TimeoutError(f"Backtest task timed out ({timeout_seconds:.0f}s)") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _set_job(job_id: str, **updates: Any) -> None:
    with _BACKTEST_LOCK:
        job = _BACKTEST_JOBS.get(job_id, {})
        job.update(updates)
        job["updated_at"] = _now_iso()
        _BACKTEST_JOBS[job_id] = job


def submit_backtest_job(
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    adjustments: StrategyAdjustments,
) -> str:
    job_id = uuid.uuid4().hex
    normalized_strategy = normalize_strategy_name(strategy_name)
    _set_job(
        job_id,
        id=job_id,
        status="queued",
        error="",
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=normalized_strategy,
        adjustments=asdict(adjustments),
        strategy_frame=None,
        created_at=_now_iso(),
    )

    def _runner() -> None:
        _set_job(job_id, status="running", started_at=_now_iso())
        try:
            source_frame, used_symbol = _run_with_timeout(
                _load_backtest_frame_with_fallback,
                symbol,
                timeframe,
                timeout_seconds=BACKTEST_FETCH_TIMEOUT_SECONDS,
            )
            if source_frame.empty:
                raise ValueError("Failed to fetch data from yfinance.")
            strategy_frame = _run_with_timeout(
                calculate_strategy,
                source_frame,
                adjustments,
                timeframe,
                normalized_strategy,
                timeout_seconds=BACKTEST_CALC_TIMEOUT_SECONDS,
            )
            _set_job(
                job_id,
                status="succeeded",
                strategy_frame=strategy_frame,
                data_symbol=used_symbol,
                finished_at=_now_iso(),
            )
        except Exception as exc:  # pragma: no cover - defensive path
            _set_job(job_id, status="failed", error=str(exc), strategy_frame=None, finished_at=_now_iso())

    _BACKTEST_EXECUTOR.submit(_runner)
    return job_id


def get_backtest_job(job_id: str) -> dict[str, Any] | None:
    with _BACKTEST_LOCK:
        job = _BACKTEST_JOBS.get(job_id)
        if job is None:
            return None
        return dict(job)
