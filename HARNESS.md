# Harness Guide

This project uses a small Codex harness inspired by the document workflow:

- `AGENTS.md`
- `.ai/MEMORY.md`
- `.ai/PLAN.md`
- `.ai/RULES.md`

## Commands

```bash
python scripts/codex_smoke.py
python scripts/codex_report.py
python harness.py
python scripts/release.py -m "chore: release"
```

## Command Roles

- `scripts/codex_smoke.py`
  - Fast pass/fail checks for current core behavior.
- `scripts/codex_report.py`
  - Prints deterministic outputs for quick manual inspection.
- `harness.py`
  - Runs smoke then report in a fixed loop.

## Exit Codes

- `0`: success
- non-zero: failure

## One-Command Release

1. Copy `.env.release.example` to `.env.release`.
2. Fill `GIT_REMOTE_URL`, `AWS_PROFILE`, `AWS_REGION`, and EC2 fields:
   - `EC2_HOST`
   - `EC2_SSH_KEY_PATH`
   - `EC2_APP_DIR`
   - `EC2_DEPLOY_COMMAND`
3. (One-time) run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup_aws_cli.ps1 -Profile default -Region ap-northeast-2
```

4. Release:

```bash
python scripts/release.py -m "chore: release"
```

`scripts/deploy_ec2.ps1` is used by default from `DEPLOY_COMMAND`.
