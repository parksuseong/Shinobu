from __future__ import annotations

import sys
from pathlib import Path

import uvicorn


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
    uvicorn.run(
        "shinobu.signal_api:app",
        host="0.0.0.0",
        port=8766,
        reload=False,
        log_level="info",
    )


if __name__ == "__main__":
    main()
