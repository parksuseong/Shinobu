from __future__ import annotations

import sqlite3

import pandas as pd

from shinobu import data as market_data
from shinobu.cache_db import DB_PATH, clear_chart_payload_caches
from shinobu.strategy import DEFAULT_STRATEGY_NAME, StrategyAdjustments
from shinobu.strategy_cache import calculate_strategy_cached


def main() -> None:
    start_ts = pd.Timestamp("2026-01-01 00:00:00")
    now_ts = pd.Timestamp.now(tz=None)
    lookback_days = max(int((now_ts - start_ts).days) + 3, 5)
    timeframe = next((key for key in market_data.INTRADAY_RESAMPLE_MINUTES if "5" in str(key)), list(market_data.INTRADAY_RESAMPLE_MINUTES.keys())[0])
    symbols = ["122630.KS", "252670.KS"]

    with sqlite3.connect(DB_PATH) as connection:
        connection.execute("DELETE FROM indicator_data WHERE strategy_name IN (?, ?)", (DEFAULT_STRATEGY_NAME, "src_blog_scr"))
        connection.execute("DELETE FROM strategy_state WHERE strategy_name IN (?, ?)", (DEFAULT_STRATEGY_NAME, "src_blog_scr"))
        connection.commit()

    for symbol in symbols:
        frame = market_data.load_live_chart_data_cached_only(symbol, timeframe, lookback_days=lookback_days)
        frame = frame.loc[frame.index >= start_ts].copy()
        calculate_strategy_cached(
            frame,
            adjustments=StrategyAdjustments(),
            timeframe_label=timeframe,
            strategy_name=DEFAULT_STRATEGY_NAME,
            symbol=symbol,
        )

    clear_chart_payload_caches()

    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT symbol, timeframe, strategy_name, COUNT(*) AS row_count, MIN(ts), MAX(ts)
            FROM indicator_data
            WHERE strategy_name = ?
            GROUP BY symbol, timeframe, strategy_name
            ORDER BY symbol, strategy_name
            """,
            (DEFAULT_STRATEGY_NAME,),
        ).fetchall()

    print("strategy indicator_data rebuilt")
    for symbol, tf, strategy_name, count, min_ts, max_ts in rows:
        print(f"- {symbol} {tf} {strategy_name}: {int(count)} rows ({min_ts} ~ {max_ts})")


if __name__ == "__main__":
    main()
