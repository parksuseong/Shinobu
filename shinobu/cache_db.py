from __future__ import annotations

import json
import sqlite3
import threading
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd


DB_PATH = Path(__file__).resolve().parent.parent / ".streamlit" / "shinobu_cache.db"
_DB_LOCK = threading.RLock()
_INITIALIZED = False


def _connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    connection.execute("PRAGMA journal_mode=WAL;")
    connection.execute("PRAGMA synchronous=NORMAL;")
    return connection


def _initialize() -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return
    with _DB_LOCK:
        if _INITIALIZED:
            return
        with _connect() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS raw_market_data (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    open REAL,
                    high REAL,
                    low REAL,
                    close REAL,
                    volume REAL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY(symbol, timeframe, ts)
                );

                CREATE TABLE IF NOT EXISTS indicator_data (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    adjustment_key TEXT NOT NULL,
                    ts TEXT NOT NULL,
                    row_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY(symbol, timeframe, strategy_name, adjustment_key, ts)
                );

                CREATE TABLE IF NOT EXISTS strategy_state (
                    symbol TEXT NOT NULL,
                    timeframe TEXT NOT NULL,
                    strategy_name TEXT NOT NULL,
                    adjustment_key TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    signature_json TEXT NOT NULL,
                    source_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY(symbol, timeframe, strategy_name, adjustment_key)
                );

                CREATE TABLE IF NOT EXISTS payload_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS execution_cache (
                    cache_key TEXT PRIMARY KEY,
                    frame_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS app_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
                );
                """
            )
        _INITIALIZED = True


def _frame_to_json(frame: pd.DataFrame) -> str:
    normalized = frame.copy()
    if not normalized.empty:
        normalized = normalized.sort_index()
    return normalized.to_json(orient="split", date_format="iso")


def _frame_from_json(payload: str) -> pd.DataFrame:
    frame = pd.read_json(StringIO(payload), orient="split")
    frame.index = pd.to_datetime(frame.index, errors="coerce")
    frame = frame[~frame.index.isna()]
    return frame.sort_index()


def load_raw_intraday(symbol: str, timeframe: str, start_ts: pd.Timestamp) -> pd.DataFrame:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            rows = connection.execute(
                """
                SELECT ts, open, high, low, close, volume
                FROM raw_market_data
                WHERE symbol = ? AND timeframe = ? AND ts >= ?
                ORDER BY ts
                """,
                (symbol, timeframe, pd.Timestamp(start_ts).isoformat()),
            ).fetchall()
    if not rows:
        return pd.DataFrame(columns=["Open", "High", "Low", "Close", "Volume"])

    frame = pd.DataFrame(rows, columns=["ts", "Open", "High", "Low", "Close", "Volume"])
    frame["ts"] = pd.to_datetime(frame["ts"], errors="coerce")
    frame = frame.dropna(subset=["ts"])
    return frame.set_index("ts").sort_index()


def upsert_raw_intraday(symbol: str, timeframe: str, frame: pd.DataFrame) -> None:
    _initialize()
    if frame.empty:
        return
    ordered = frame.sort_index()
    payload = []
    for timestamp, row in ordered.iterrows():
        payload.append(
            (
                symbol,
                timeframe,
                pd.Timestamp(timestamp).isoformat(),
                float(row.get("Open", 0) or 0),
                float(row.get("High", 0) or 0),
                float(row.get("Low", 0) or 0),
                float(row.get("Close", 0) or 0),
                float(row.get("Volume", 0) or 0),
            )
        )
    with _DB_LOCK:
        with _connect() as connection:
            connection.executemany(
                """
                INSERT INTO raw_market_data(symbol, timeframe, ts, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, ts) DO UPDATE SET
                    open = excluded.open,
                    high = excluded.high,
                    low = excluded.low,
                    close = excluded.close,
                    volume = excluded.volume,
                    updated_at = datetime('now')
                """,
                payload,
            )


def _normalize_json_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, (pd.Timestamp,)):
        return value.isoformat()
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            return value
    return value


def save_strategy_cache_payload(
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    adjustment_key: str,
    version: int,
    signature: dict[str, Any],
    source_frame: pd.DataFrame,
    frame: pd.DataFrame,
) -> None:
    _initialize()
    ordered = frame.sort_index()
    rows_payload: list[tuple[str, str, str, str, str, str]] = []
    for timestamp, row in ordered.iterrows():
        row_dict = {key: _normalize_json_value(value) for key, value in row.to_dict().items()}
        rows_payload.append(
            (
                symbol,
                timeframe,
                strategy_name,
                adjustment_key,
                pd.Timestamp(timestamp).isoformat(),
                json.dumps(row_dict, ensure_ascii=False),
            )
        )

    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                """
                DELETE FROM indicator_data
                WHERE symbol = ? AND timeframe = ? AND strategy_name = ? AND adjustment_key = ?
                """,
                (symbol, timeframe, strategy_name, adjustment_key),
            )
            if rows_payload:
                connection.executemany(
                    """
                    INSERT INTO indicator_data(symbol, timeframe, strategy_name, adjustment_key, ts, row_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows_payload,
                )
            connection.execute(
                """
                INSERT INTO strategy_state(symbol, timeframe, strategy_name, adjustment_key, version, signature_json, source_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timeframe, strategy_name, adjustment_key) DO UPDATE SET
                    version = excluded.version,
                    signature_json = excluded.signature_json,
                    source_json = excluded.source_json,
                    updated_at = datetime('now')
                """,
                (
                    symbol,
                    timeframe,
                    strategy_name,
                    adjustment_key,
                    int(version),
                    json.dumps(signature, ensure_ascii=False),
                    _frame_to_json(source_frame),
                ),
            )


def load_strategy_cache_payload(
    *,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    adjustment_key: str,
) -> dict[str, Any] | None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            state_row = connection.execute(
                """
                SELECT version, signature_json, source_json
                FROM strategy_state
                WHERE symbol = ? AND timeframe = ? AND strategy_name = ? AND adjustment_key = ?
                """,
                (symbol, timeframe, strategy_name, adjustment_key),
            ).fetchone()
            indicator_rows = connection.execute(
                """
                SELECT ts, row_json
                FROM indicator_data
                WHERE symbol = ? AND timeframe = ? AND strategy_name = ? AND adjustment_key = ?
                ORDER BY ts
                """,
                (symbol, timeframe, strategy_name, adjustment_key),
            ).fetchall()

    if state_row is None:
        return None
    version, signature_json, source_json = state_row
    try:
        signature = json.loads(signature_json)
        source_frame = _frame_from_json(source_json)
        if not indicator_rows:
            frame = pd.DataFrame()
        else:
            idx: list[pd.Timestamp] = []
            rows: list[dict[str, Any]] = []
            for ts_text, row_json in indicator_rows:
                idx.append(pd.Timestamp(ts_text))
                row_payload = json.loads(str(row_json))
                if isinstance(row_payload, dict):
                    rows.append(row_payload)
                else:
                    rows.append({})
            frame = pd.DataFrame(rows, index=pd.DatetimeIndex(idx)).sort_index()
    except Exception:
        return None
    return {
        "version": int(version),
        "signature": signature,
        "source_frame": source_frame,
        "frame": frame,
    }


def save_payload_cache(cache_key: str, payload: dict[str, Any]) -> None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO payload_cache(cache_key, payload_json)
                VALUES (?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    updated_at = datetime('now')
                """,
                (cache_key, json.dumps(payload, ensure_ascii=False)),
            )


def load_payload_cache(cache_key: str) -> dict[str, Any] | None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM payload_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(str(row[0]))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def save_execution_cache(cache_key: str, frame: pd.DataFrame) -> None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO execution_cache(cache_key, frame_json)
                VALUES (?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    frame_json = excluded.frame_json,
                    updated_at = datetime('now')
                """,
                (cache_key, _frame_to_json(frame)),
            )


def load_execution_cache(cache_key: str) -> pd.DataFrame | None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            row = connection.execute(
                "SELECT frame_json FROM execution_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
    if row is None:
        return None
    try:
        return _frame_from_json(str(row[0]))
    except Exception:
        return None


def load_execution_cache_with_updated_at(cache_key: str) -> tuple[pd.DataFrame | None, pd.Timestamp | None]:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            row = connection.execute(
                "SELECT frame_json, updated_at FROM execution_cache WHERE cache_key = ?",
                (cache_key,),
            ).fetchone()
    if row is None:
        return None, None
    frame_json, updated_at = row
    try:
        frame = _frame_from_json(str(frame_json))
    except Exception:
        frame = None
    try:
        updated = pd.to_datetime(updated_at, errors="coerce")
        if pd.isna(updated):
            updated = None
    except Exception:
        updated = None
    return frame, updated


def get_meta_value(key: str) -> str | None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            row = connection.execute("SELECT value FROM app_meta WHERE key = ?", (key,)).fetchone()
    if row is None:
        return None
    return str(row[0])


def set_meta_value(key: str, value: str) -> None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO app_meta(key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = datetime('now')
                """,
                (key, value),
            )


def is_startup_initialized() -> bool:
    return get_meta_value("startup_initialized") == "1"


def mark_startup_initialized(done: bool) -> None:
    set_meta_value("startup_initialized", "1" if done else "0")


def clear_all_cache_data() -> None:
    _initialize()
    with _DB_LOCK:
        with _connect() as connection:
            connection.executescript(
                """
                DELETE FROM raw_market_data;
                DELETE FROM indicator_data;
                DELETE FROM strategy_state;
                DELETE FROM payload_cache;
                DELETE FROM execution_cache;
                DELETE FROM app_meta;
                """
            )
