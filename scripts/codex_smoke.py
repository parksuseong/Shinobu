"""Fast smoke checks for core Shinobu strategy behavior."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shinobu.cache_db import DB_PATH, load_payload_cache  # noqa: E402
from shinobu.chart_controller import build_chart_payload_controlled  # noqa: E402
from shinobu.chart_worker import ChartFrameBundle  # noqa: E402
from shinobu.strategy_cache import calculate_strategy_cached  # noqa: E402
from shinobu.strategy import (  # noqa: E402
    DEFAULT_STRATEGY_NAME,
    StrategyAdjustments,
    get_strategy_label,
    list_strategy_options,
    normalize_strategy_name,
)


def _check(condition: bool, title: str) -> int:
    if condition:
        print(f"[PASS] {title}")
        return 0
    print(f"[FAIL] {title}")
    return 1


def _check_sqlite_tables() -> bool:
    expected = {"raw_market_data", "indicator_data", "strategy_state"}
    load_payload_cache("__harness_init__")
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    found = {str(name) for (name,) in rows}
    return expected.issubset(found)


def _seed_indicator_cache() -> bool:
    test_symbol = "__HARNESS__.KS"
    index = pd.date_range("2026-04-01 09:00", periods=20, freq="5min")
    frame = pd.DataFrame(
        {
            "Open": [100 + i * 0.1 for i in range(20)],
            "High": [100.5 + i * 0.1 for i in range(20)],
            "Low": [99.5 + i * 0.1 for i in range(20)],
            "Close": [100.2 + i * 0.1 for i in range(20)],
            "Volume": [1000 + i * 5 for i in range(20)],
        },
        index=index,
    )
    result = calculate_strategy_cached(
        frame,
        adjustments=StrategyAdjustments(),
        timeframe_label="5분봉",
        strategy_name="src_v2_adx",
        symbol=test_symbol,
    )
    if result.empty:
        return False
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT COUNT(*)
            FROM indicator_data
            WHERE symbol = ? AND timeframe = ? AND strategy_name = ?
            """,
            (test_symbol, "5분봉", "src_v2_adx"),
        ).fetchone()
        is_persisted = int(rows[0] if rows else 0) > 0
        connection.execute("DELETE FROM indicator_data WHERE symbol = ?", (test_symbol,))
        connection.execute("DELETE FROM strategy_state WHERE symbol = ?", (test_symbol,))
        connection.commit()
    return is_persisted


def run_smoke() -> int:
    failed = 0
    failed += _check(
        normalize_strategy_name(None) == DEFAULT_STRATEGY_NAME,
        "default strategy normalization",
    )
    failed += _check(
        normalize_strategy_name("v2") == "src_v2_normal",
        "strategy alias normalization",
    )
    failed += _check(
        get_strategy_label("src_v2_adx") == "SRC V2 ADX",
        "strategy label lookup",
    )
    failed += _check(
        len(list_strategy_options()) >= 4,
        "strategy options are populated",
    )
    failed += _check(
        callable(build_chart_payload_controlled),
        "controller module import",
    )
    failed += _check(
        hasattr(ChartFrameBundle, "__dataclass_fields__"),
        "worker module import",
    )
    failed += _check(
        _check_sqlite_tables(),
        "sqlite core tables initialized",
    )
    failed += _check(
        _seed_indicator_cache(),
        "indicator data persisted in sqlite",
    )

    if failed:
        print(f"Smoke failed: {failed} check(s).")
        return 1

    print("Smoke passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_smoke())
