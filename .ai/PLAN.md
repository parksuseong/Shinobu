# PLAN

## Current Objective

- Keep the project runnable while preparing for future trading logic.

## Current Hotspots

- `main.py`: seed function behavior
- `app.py`: Streamlit tabs and chart view entrypoint
- `shinobu/chart_component.py`: shared chart renderer for live/backtesting tabs
- `app.py`: `ai신호탐색기` tab mirrors backtesting content using shared chart component
- `scripts/codex_smoke.py`: quick import and core behavior check
- `scripts/codex_report.py`: deterministic output report for review
- `harness.py`: combined verification entry point
- `scripts/release.py`: check/commit/push/deploy automation entry point
- `scripts/setup_aws_cli.ps1`: AWS CLI bootstrap helper
- `scripts/deploy_ec2.ps1`: EC2 SSH deployment runner

## Next Steps

1. Add first domain functions (signal or order decision skeleton).
2. Expand smoke checks to include new core functions.
3. Add formal tests (`pytest`) when module boundaries stabilize.
4. Finalize production `DEPLOY_COMMAND` in `.env.release`.
