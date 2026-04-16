# Harness Guide

This repository uses a lightweight Codex harness workflow.

## Context Files

- `AGENTS.md`
- `.ai/MEMORY.md`
- `.ai/PLAN.md`
- `.ai/RULES.md`

## Commands

```bash
python scripts/codex_smoke.py
python scripts/codex_report.py
python harness.py
python scripts/precompute_indicators.py
```

## Roles

- `scripts/codex_smoke.py`
  - Fast pass/fail checks for strategy behavior plus controller/worker/sqlite wiring.
- `scripts/codex_report.py`
  - Prints strategy metadata and sqlite table row counts.
- `harness.py`
  - Runs smoke then report in a fixed loop.
- `scripts/precompute_indicators.py`
  - Precomputes real-symbol indicator rows (`122630.KS`, `252670.KS`) into sqlite.

## Runtime Split

- Control: `shinobu/chart_controller.py`
- Worker: `shinobu/chart_worker.py`

## SQLite Tables

- `raw_market_data` (original OHLCV data)
- `indicator_data` (calculated indicator rows)
- `strategy_state` (strategy cache signature/state)
