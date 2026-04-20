"""Project harness runner.

Runs smoke and report checks in a fixed order.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def _run_step(label: str, script_relpath: str) -> int:
    script_path = ROOT / script_relpath
    print(f"\n== {label} ==")
    result = subprocess.run([sys.executable, str(script_path)], check=False)
    if result.returncode != 0:
        print(f"[FAIL] {label} exited with code {result.returncode}")
    else:
        print(f"[PASS] {label}")
    return result.returncode


def run_harness() -> int:
    steps = [
        ("harness:smoke", "scripts/codex_smoke.py"),
        ("harness:report", "scripts/codex_report.py"),
    ]

    for label, script in steps:
        if _run_step(label, script) != 0:
            print("\nHarness finished with failures.")
            return 1

    print("\nHarness finished successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(run_harness())
