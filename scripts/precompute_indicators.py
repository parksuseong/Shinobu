"""Manual worker entry to precompute real symbol indicators into sqlite."""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shinobu.cache_db import DB_PATH, load_payload_cache  # noqa: E402
from shinobu.indicator_worker import precompute_indicator_data  # noqa: E402
from shinobu.strategy import StrategyAdjustments, list_strategy_options  # noqa: E402


def run() -> int:
    strategies = [option.key for option in list_strategy_options()]
    symbols = ["122630.KS", "252670.KS"]
    precompute_indicator_data(
        symbols=symbols,
        strategy_names=strategies,
        adjustments=StrategyAdjustments(),
        timeframe_label="5분봉",
    )

    load_payload_cache("__harness_init__")
    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT symbol, timeframe, strategy_name, COUNT(*) AS row_count
            FROM indicator_data
            WHERE symbol IN ('122630.KS', '252670.KS')
            GROUP BY symbol, timeframe, strategy_name
            ORDER BY symbol, strategy_name
            """
        ).fetchall()

    print("indicator_data summary")
    for symbol, timeframe, strategy_name, row_count in rows:
        print(f"- {symbol} {timeframe} {strategy_name}: {int(row_count)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
