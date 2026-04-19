# RULES

## Editing Rules

- Prefer minimal, reversible changes.
- Do not modify secrets/runtime state files:
  - `.streamlit/secrets.toml`
  - `.streamlit/kis_token.json`
  - `.streamlit/live_state.json`
- Keep strategy naming compatibility (`src`).

## Verification Rules

Run in this order after code changes:

1. `python scripts/codex_smoke.py`
2. `python scripts/codex_report.py`
3. `python harness.py`

## Deployment Rules

- For EC2 operational reset, use only `bash scripts/ec2_service.sh reset`.
- Confirm both processes after reset:
  - Streamlit (8501)
  - Signal API (8766)

## Documentation Rules

- Update `README.md` for command or workflow changes.
- Update `HARNESS.md` when harness flow or operational commands change.
- Update `docs/harness/PLAN.md` with current priority work.
