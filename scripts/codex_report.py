"""Deterministic report for strategy metadata sanity checks."""

from __future__ import annotations

import sys
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shinobu.cache_db import DB_PATH, load_payload_cache  # noqa: E402
from shinobu.strategy import (  # noqa: E402
    DEFAULT_STRATEGY_NAME,
    get_strategy_history_business_days,
    list_strategy_options,
)


def run_report() -> int:
    load_payload_cache("__harness_init__")
    options = list_strategy_options()
    with sqlite3.connect(DB_PATH) as connection:
        table_counts = {}
        for table in ["raw_market_data", "indicator_data", "strategy_state"]:
            count = connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            table_counts[table] = int(count[0] if count else 0)

    print("# Shinobu Codex Harness Report")
    print(f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}")
    print(f"default_strategy: {DEFAULT_STRATEGY_NAME}")
    print("strategy_options:")
    for option in options:
        days = get_strategy_history_business_days(option.key)
        print(f"- key={option.key!r} label={option.label!r} history_days={days}")
    print("sqlite_tables:")
    for table, count in table_counts.items():
        print(f"- table={table!r} rows={count}")
    print("status: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_report())
