from __future__ import annotations

import sqlite3

from shinobu.cache_db import DB_PATH, invalidate_strategy_cache_for_symbols
from shinobu.indicator_worker import precompute_indicator_data
from shinobu.strategy import StrategyAdjustments


def main() -> None:
    symbols = ["122630.KS", "252670.KS"]
    timeframe = "5분봉"
    strategy = "src_v2_adx"

    invalidate_strategy_cache_for_symbols(symbols, timeframe)
    precompute_indicator_data(
        symbols=symbols,
        strategy_names=[strategy],
        adjustments=StrategyAdjustments(),
        timeframe_label=timeframe,
    )

    with sqlite3.connect(DB_PATH) as connection:
        rows = connection.execute(
            """
            SELECT symbol, timeframe, strategy_name, COUNT(*) AS row_count, MIN(ts), MAX(ts)
            FROM indicator_data
            WHERE strategy_name = 'src_v2_adx'
            GROUP BY symbol, timeframe, strategy_name
            ORDER BY symbol
            """
        ).fetchall()

    print("src_v2_adx indicator_data rebuilt")
    for symbol, tf, st, count, min_ts, max_ts in rows:
        print(f"- {symbol} {tf} {st}: {int(count)} rows ({min_ts} ~ {max_ts})")


if __name__ == "__main__":
    main()
