#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
LOG_DIR="$ROOT_DIR/.streamlit"

STREAMLIT_HOST="${STREAMLIT_HOST:-0.0.0.0}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
SIGNAL_API_PORT="${SIGNAL_API_PORT:-8766}"

STREAMLIT_PID_FILE="$LOG_DIR/streamlit.pid"
SIGNAL_PID_FILE="$LOG_DIR/signal_api.pid"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ec2_service.sh bootstrap   # install deps + create venv + pip install
  bash scripts/ec2_service.sh start       # start streamlit + signal api
  bash scripts/ec2_service.sh stop        # stop both processes
  bash scripts/ec2_service.sh restart     # stop then start
  bash scripts/ec2_service.sh reset       # stop -> clear sqlite caches -> force startup re-init -> start
  bash scripts/ec2_service.sh status      # show process status
EOF
}

ensure_log_dir() {
  mkdir -p "$LOG_DIR"
}

is_pid_running() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

find_listen_pids_by_port() {
  local port="$1"

  if command -v lsof >/dev/null 2>&1; then
    lsof -t -iTCP:"$port" -sTCP:LISTEN 2>/dev/null | sort -u
    return 0
  fi

  if command -v ss >/dev/null 2>&1; then
    ss -ltnp 2>/dev/null \
      | awk -v p=":$port" '$4 ~ p"$" { if (match($0, /pid=[0-9]+/)) { print substr($0, RSTART+4, RLENGTH-4) } }' \
      | sort -u
    return 0
  fi

  return 0
}

read_pid() {
  local file="$1"
  [[ -f "$file" ]] || return 1
  tr -d '[:space:]' <"$file"
}

clear_port_conflicts() {
  local port="$1"
  local name="$2"
  local pid_file="$3"
  local tracked_pid=""

  tracked_pid="$(read_pid "$pid_file" 2>/dev/null || true)"

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if [[ -n "$tracked_pid" && "$pid" == "$tracked_pid" ]]; then
      continue
    fi
    if ! is_pid_running "$pid"; then
      continue
    fi
    echo "$name port conflict detected on :$port (pid=$pid). Stopping stale process."
    kill "$pid" 2>/dev/null || true
    sleep 1
    if is_pid_running "$pid"; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done < <(find_listen_pids_by_port "$port")
}

install_system_deps() {
  if command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y python3 python3-pip sqlite sqlite-devel
  elif command -v apt >/dev/null 2>&1; then
    sudo apt update
    sudo apt install -y python3 python3-pip python3-venv sqlite3 libsqlite3-dev
  else
    echo "Unsupported package manager. Install python3/sqlite manually."
  fi
}

bootstrap() {
  ensure_log_dir
  install_system_deps

  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Python not found at $PYTHON_BIN"
    exit 1
  fi

  "$PYTHON_BIN" -m venv "$VENV_DIR"
  "$VENV_DIR/bin/python" -m pip install --upgrade pip
  "$VENV_DIR/bin/pip" install -r "$ROOT_DIR/requirements.txt"

  "$VENV_DIR/bin/python" -c "import sqlite3; print('sqlite ok:', sqlite3.sqlite_version)"
  echo "Bootstrap complete."
}

start_streamlit() {
  ensure_log_dir
  local current_pid=""
  if current_pid="$(read_pid "$STREAMLIT_PID_FILE" 2>/dev/null)" && is_pid_running "$current_pid"; then
    echo "Streamlit already running (pid=$current_pid)"
    return
  fi

  clear_port_conflicts "$STREAMLIT_PORT" "Streamlit" "$STREAMLIT_PID_FILE"

  nohup "$VENV_DIR/bin/python" -m streamlit run "$ROOT_DIR/app.py" \
    --server.address "$STREAMLIT_HOST" \
    --server.port "$STREAMLIT_PORT" \
    >/dev/null 2>"$LOG_DIR/streamlit.err.log" &
  echo $! >"$STREAMLIT_PID_FILE"
  echo "Streamlit started (pid=$(cat "$STREAMLIT_PID_FILE"))"
}

start_signal_api() {
  ensure_log_dir
  local current_pid=""
  if current_pid="$(read_pid "$SIGNAL_PID_FILE" 2>/dev/null)" && is_pid_running "$current_pid"; then
    echo "Signal API already running (pid=$current_pid)"
    return
  fi

  clear_port_conflicts "$SIGNAL_API_PORT" "Signal API" "$SIGNAL_PID_FILE"

  if [[ "$SIGNAL_API_PORT" != "8766" ]]; then
    echo "Warning: scripts/run_signal_api.py currently binds fixed port 8766."
  fi

  nohup "$VENV_DIR/bin/python" "$ROOT_DIR/scripts/run_signal_api.py" \
    >/dev/null 2>"$LOG_DIR/signal_api.err.log" &
  echo $! >"$SIGNAL_PID_FILE"
  echo "Signal API started (pid=$(cat "$SIGNAL_PID_FILE"))"
}

stop_one() {
  local name="$1"
  local pid_file="$2"
  local pid=""

  if ! pid="$(read_pid "$pid_file" 2>/dev/null)"; then
    echo "$name not running (pid file missing)"
    return
  fi

  if ! is_pid_running "$pid"; then
    rm -f "$pid_file"
    echo "$name not running (stale pid file removed)"
    return
  fi

  kill "$pid" 2>/dev/null || true
  sleep 1
  if is_pid_running "$pid"; then
    kill -9 "$pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
  echo "$name stopped"
}

status_one() {
  local name="$1"
  local pid_file="$2"
  local pid=""
  if pid="$(read_pid "$pid_file" 2>/dev/null)" && is_pid_running "$pid"; then
    echo "$name: running (pid=$pid)"
  else
    echo "$name: stopped"
  fi
}

start_all() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "Virtualenv not found at $VENV_DIR. Run bootstrap first."
    exit 1
  fi
  start_streamlit
  start_signal_api
}

stop_all() {
  stop_one "Signal API" "$SIGNAL_PID_FILE"
  stop_one "Streamlit" "$STREAMLIT_PID_FILE"
}

status_all() {
  status_one "Streamlit" "$STREAMLIT_PID_FILE"
  status_one "Signal API" "$SIGNAL_PID_FILE"
  echo "Logs:"
  echo "  $LOG_DIR/streamlit.err.log"
  echo "  $LOG_DIR/signal_api.err.log"
}

reset_data() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "Virtualenv not found at $VENV_DIR. Run bootstrap first."
    exit 1
  fi

  echo "Stopping services before reset..."
  stop_all

  echo "Clearing sqlite cache tables and startup flags..."
  "$VENV_DIR/bin/python" - <<'PY'
from shinobu.cache_db import (
    clear_all_cache_data,
    mark_startup_initialized,
    release_startup_init_lock,
    set_meta_value,
)

clear_all_cache_data()
mark_startup_initialized(False)
release_startup_init_lock()
set_meta_value("startup_init_lock", "0")
print("reset complete: cache cleared, startup initialization forced")
PY

  echo "Starting services..."
  start_all
  echo "Reset flow complete. Streamlit will recollect and recalculate on startup."
}

main() {
  local cmd="${1:-}"
  case "$cmd" in
    bootstrap) bootstrap ;;
    start) start_all ;;
    stop) stop_all ;;
    restart) stop_all; start_all ;;
    reset) reset_data ;;
    status) status_all ;;
    *) usage; exit 1 ;;
  esac
}

main "${1:-}"
