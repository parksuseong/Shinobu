#!/usr/bin/env bash
set -euo pipefail

# Lightweight service wrapper for EC2 deploy hooks.
# Default behavior:
# - stop: terminate background `streamlit run app.py`
# - start: run Streamlit app in background and write to app.log

APP_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_PATTERN="streamlit run .*app.py"
LOG_FILE="$APP_ROOT/app.log"
PYTHON_BIN="${PYTHON_BIN:-}"
PORT="${PORT:-8501}"

detect_python() {
  if [[ -x "$APP_ROOT/.venv/bin/python" ]]; then
    PYTHON_BIN="$APP_ROOT/.venv/bin/python"
    return 0
  fi
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
  nohup "$PYTHON_BIN" -m streamlit run app.py --server.address 0.0.0.0 --server.port "$PORT" >>"$LOG_FILE" 2>&1 &
  echo "started: $PYTHON_BIN -m streamlit run app.py (log: $LOG_FILE)"
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
