"""Fast smoke checks for the current project."""

from __future__ import annotations

import io
import sys
from contextlib import redirect_stdout
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


def run_smoke() -> int:
    cases = [
        ("PyCharm", "Hi, PyCharm"),
        ("Shinobu", "Hi, Shinobu"),
    ]

    failed = 0
    for name, expected in cases:
        actual = _capture_output(name)
        if actual == expected:
            print(f"[PASS] {name!r} -> {actual!r}")
        else:
            failed += 1
            print(f"[FAIL] {name!r} expected={expected!r} actual={actual!r}")

    if failed:
        print(f"Smoke failed: {failed} case(s).")
        return 1

    print("Smoke passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_smoke())
