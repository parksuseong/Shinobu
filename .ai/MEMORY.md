# MEMORY

## Stable Facts

- This project is a Streamlit-based trading app (`app.py`) backed by
  the `shinobu/` package.
- Strategy profile handling is centralized in `shinobu/strategy.py`.
- Live trading and KIS integration exist, but harness checks should avoid
  network/account-dependent paths.

## Environment Constraints

- Run commands with `python ...`.
- Secrets and runtime state are under `.streamlit/`; do not modify them.
- Keep harness deterministic and fast.

## Core Goal

- Maintain a reliable Codex harness loop for safe local changes:
  smoke -> report -> integrated harness.
