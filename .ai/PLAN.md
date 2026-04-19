# PLAN

## Current Objective

- Keep the SCR blog strategy stable and keep sqlite precompute/cache flow reliable.
- Keep pair candle recovery resilient at market close (include 15:30 bars promptly).

## Current Hotspots

- `shinobu/strategy.py`
- `shinobu/strategy_src.py`
- `app.py` (UI composition)
- `scripts/codex_smoke.py`
- `scripts/codex_report.py`

## Next Steps

1. Expand smoke checks for the single SCR blog strategy path.
2. Add formal tests when function boundaries stabilize.
3. Keep report output useful for manual sanity checks.
