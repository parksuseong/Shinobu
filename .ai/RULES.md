# RULES

## Edit Scope

- Prefer edits in `shinobu/`, `app.py`, `scripts/`, and docs.
- Do not edit `.streamlit/secrets.toml` or runtime cache artifacts.

## Compatibility Rules

- Keep current strategy names and normalization behavior compatible.
- If behavior changes, update harness scripts and docs in the same change.

## Verification Priority

1. `python scripts/codex_smoke.py`
2. `python scripts/codex_report.py`
3. `python harness.py`

## Documentation Rules

- Keep `HARNESS.md` in sync with command changes.
- Keep `.ai/PLAN.md` updated when the work direction changes.
