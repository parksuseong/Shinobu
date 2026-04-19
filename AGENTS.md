# AGENTS

Codex entry guide for the Shinobu project.

## Project Targets

- App entry: `app.py`
- Core package: `shinobu/`
- Harness runner: `harness.py`
- Harness scripts: `scripts/codex_smoke.py`, `scripts/codex_report.py`
- Harness docs: `HARNESS.md`, `docs/harness/MEMORY.md`, `docs/harness/RULES.md`, `docs/harness/PLAN.md`

## Read Order

1. `AGENTS.md`
2. `docs/harness/MEMORY.md`
3. `docs/harness/RULES.md`
4. `docs/harness/PLAN.md`
5. `HARNESS.md`

## Default Work Loop

1. Read context docs in the order above.
2. Make minimal, reversible edits in focused files.
3. Run `python scripts/codex_smoke.py`.
4. Run `python scripts/codex_report.py`.
5. Run `python harness.py`.
6. If behavior changed, update `HARNESS.md` and `docs/harness/*.md`.
