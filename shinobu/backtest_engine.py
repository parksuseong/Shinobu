from __future__ import annotations

import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import asdict
from datetime import datetime
from typing import Any

import pandas as pd

from shinobu import data as market_data
from shinobu.strategy import StrategyAdjustments, calculate_strategy, normalize_strategy_name


_ALLOWED_TIMEFRAMES = {"30분봉", "일봉", "4시간봉"}
_BACKTEST_EXECUTOR = ThreadPoolExecutor(max_workers=1, thread_name_prefix="shinobu-backtest")
_BACKTEST_LOCK = threading.RLock()
_BACKTEST_JOBS: dict[str, dict[str, Any]] = {}
BACKTEST_FETCH_TIMEOUT_SECONDS = 25.0
BACKTEST_CALC_TIMEOUT_SECONDS = 25.0


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _load_backtest_frame_from_yfinance(symbol: str, timeframe_label: str) -> pd.DataFrame:
    if timeframe_label not in _ALLOWED_TIMEFRAMES:
        raise ValueError("백테스팅 타임프레임은 30분봉/일봉/4시간봉만 지원합니다.")
    frame = market_data._load_yfinance_data(symbol, timeframe_label)  # noqa: SLF001
    if frame.empty:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])
    return frame[["Open", "High", "Low", "Close", "Volume"]].dropna()


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
        raise TimeoutError(f"백테스트 작업 시간 초과({timeout_seconds:.0f}초)") from exc
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
                raise ValueError("yfinance에서 데이터를 받지 못했습니다.")
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
