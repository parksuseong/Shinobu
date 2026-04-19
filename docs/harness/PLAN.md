# PLAN

## Current Goal

Make Shinobu maintainable for handoff development with a clear Codex harness workflow and executable command guide.

## Current Priorities

1. Keep startup reset/rebuild behavior predictable.
2. Keep chart payload and marker rendering responsive.
3. Keep Signal API availability stable on EC2 (`8766` port conflict-safe startup).

## Next Tasks

1. Add a small diagnostics command set for recovery/strategy ranges.
2. Expand smoke checks to include date-range chart payload behavior.
3. Add runbook section for common EC2 incidents (port conflict, token expiry, empty markers).
