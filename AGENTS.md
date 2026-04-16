# AGENTS

Codex entry guide for this repository.

## Project Targets

- App entry: `app.py`
- Core package: `shinobu/`
- Harness runner: `harness.py`
- Harness scripts: `scripts/codex_smoke.py`, `scripts/codex_report.py`
- AI context docs: `.ai/MEMORY.md`, `.ai/RULES.md`, `.ai/PLAN.md`

## Read Order

1. `AGENTS.md`
2. `.ai/MEMORY.md`
3. `.ai/RULES.md`
4. `.ai/PLAN.md`

## Default Work Loop

1. Read context docs in the order above.
2. Make minimal, reversible edits.
3. Run `python scripts/codex_smoke.py`
4. Run `python scripts/codex_report.py`
5. Run `python harness.py`
6. Update docs if behavior or workflow changed.
