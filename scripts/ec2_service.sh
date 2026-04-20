#!/usr/bin/env bash
set -euo pipefail

# Lightweight service wrapper for EC2 deploy hooks.
# Default behavior:
# - stop: terminate background `python main.py`
# - start: run `python main.py` in background and write to main.log

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_PATTERN="python main.py"
LOG_FILE="$APP_ROOT/main.log"
PYTHON_BIN="${PYTHON_BIN:-}"

detect_python() {
  if [[ -n "$PYTHON_BIN" ]] && command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    PYTHON_BIN="python"
    return 0
  fi
  echo "python interpreter not found (tried: python3, python)"
  exit 1
}

start_app() {
  cd "$APP_ROOT"
  detect_python
  nohup "$PYTHON_BIN" main.py >>"$LOG_FILE" 2>&1 &
  echo "started: $PYTHON_BIN main.py (log: $LOG_FILE)"
}

stop_app() {
  pkill -f "$PID_PATTERN" || true
  echo "stopped matching process: $PID_PATTERN"
}

status_app() {
  if pgrep -f "$PID_PATTERN" >/dev/null 2>&1; then
    echo "status: running"
  else
    echo "status: stopped"
  fi
}

cmd="${1:-restart}"
case "$cmd" in
  start)
    start_app
    ;;
  stop)
    stop_app
    ;;
  restart)
    stop_app
    sleep 1
    start_app
    ;;
  status)
    status_app
    ;;
  *)
    echo "usage: $0 {start|stop|restart|status}"
    exit 2
    ;;
esac
