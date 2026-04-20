"""Deterministic report output for quick manual inspection."""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from main import print_hi


def _capture_output(name: str) -> str:
    buffer = io.StringIO()
    with redirect_stdout(buffer):
        print_hi(name)
    return buffer.getvalue().strip()


def run_report() -> int:
    names = ["PyCharm", "Shinobu", "Codex"]
    outputs = [(name, _capture_output(name)) for name in names]

    print("# Codex Harness Report")
    print(f"generated_at_utc: {datetime.now(timezone.utc).isoformat()}")
    print("module: main.print_hi")
    print("results:")
    for name, out in outputs:
        print(f"- input={name!r} output={out!r}")

    print("status: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_report())
