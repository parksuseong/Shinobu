# MEMORY

## Project Identity

- Project name: Shinobu Project
- Purpose: 5-minute auto-trading operations for a leverage/inverse pair using KIS APIs.
- Main runtime: Streamlit UI + Signal API + local chart server + SQLite cache.

## Core Runtime Components

- UI: `app.py`
- Trading loop: `shinobu/live_trading.py` (5-second cycle)
- Chart data/payload: `shinobu/chart_payload.py`, `shinobu/chart_worker.py`, `shinobu/chart_controller.py`
- API: `scripts/run_signal_api.py`, `shinobu/signal_api.py`
- Cache DB: `shinobu/cache_db.py` (`.streamlit/shinobu_cache.db`)

## Stable Operational Facts

- Timeframe is 5-minute candles.
- Strategy key is unified to `src`.
- Startup initialization can clear/rebuild data and prewarm chart caches.
- Pair recovery runs periodically and realigns candle timestamps between leverage/inverse symbols.

## What Success Looks Like

- New contributor can bootstrap, run, and verify quickly.
- Commands are explicit and copy/paste friendly.
- Harness checks pass before merge/deploy.
