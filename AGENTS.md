# AGENTS

This file is the entry point for Codex work in this repository.

## Project Target

- Main code: `main.py`
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
2. Make minimal code changes for the task.
3. Run smoke check: `python scripts/codex_smoke.py`
4. Run report check: `python scripts/codex_report.py`
5. Run full harness: `python harness.py`
6. Update docs (`.ai/PLAN.md` and harness docs) when behavior changes.

## Notes

- Keep edits focused and reversible.
- Prefer small verifiable steps.
