# RULES

## Edit Scope

- Prefer editing `main.py`, `harness.py`, `scripts/`, and docs in `.ai/`.
- Avoid changing `venv/` and IDE metadata.

## Compatibility Rules

- Keep public behavior of existing functions unless task requires change.
- When behavior changes, update harness checks and docs in same change.

## Verification Priority

1. `python scripts/codex_smoke.py`
2. `python scripts/codex_report.py`
3. `python harness.py`

## Documentation Rule

- Reflect meaningful workflow or behavior changes in:
  - `.ai/PLAN.md`
  - `HARNESS.md`
  - `README.md` (if user-facing usage changes)
