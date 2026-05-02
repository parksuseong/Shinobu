#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$ROOT_DIR/.venv}"
PYTHON_BIN="${PYTHON_BIN:-/usr/bin/python3}"
LOG_DIR="$ROOT_DIR/.streamlit"

STREAMLIT_HOST="${STREAMLIT_HOST:-0.0.0.0}"
STREAMLIT_PORT="${STREAMLIT_PORT:-8501}"
SIGNAL_API_PORT="${SIGNAL_API_PORT:-8766}"
NGINX_DOMAIN="${NGINX_DOMAIN:-shinobu.ukin.dev}"
NGINX_CONF_PATH="${NGINX_CONF_PATH:-/etc/nginx/conf.d/shinobu.conf}"
NGINX_CERT_PATH="${NGINX_CERT_PATH:-/etc/letsencrypt/live/$NGINX_DOMAIN/fullchain.pem}"
NGINX_CERT_KEY_PATH="${NGINX_CERT_KEY_PATH:-/etc/letsencrypt/live/$NGINX_DOMAIN/privkey.pem}"

STREAMLIT_PID_FILE="$LOG_DIR/streamlit.pid"
SIGNAL_PID_FILE="$LOG_DIR/signal_api.pid"
LIVE_ENGINE_PID_FILE="$LOG_DIR/live_engine.pid"
DAILY_RECO_CRON_TAG="# shinobu-daily-recommendation"
DAILY_RECO_CRON_SCHEDULE="${DAILY_RECO_CRON_SCHEDULE:-0 6 * * 1-5}"
DAILY_RECO_CRON_TZ="${DAILY_RECO_CRON_TZ:-Asia/Seoul}"

usage() {
  cat <<'EOF'
Usage:
  bash scripts/ec2_service.sh bootstrap   # install deps + create venv + pip install
  bash scripts/ec2_service.sh start       # start streamlit + signal api + live engine
  bash scripts/ec2_service.sh stop        # stop both processes
  bash scripts/ec2_service.sh restart     # stop then start
  bash scripts/ec2_service.sh reset       # stop -> clear sqlite caches -> force startup re-init -> start
  bash scripts/ec2_service.sh status      # show process status
  bash scripts/ec2_service.sh reco-cron-install # install/update daily recommendation cron (06:00 KST weekdays)
  bash scripts/ec2_service.sh reco-cron-status  # show daily recommendation cron status
  bash scripts/ec2_service.sh reco-run-now      # run daily recommendation job immediately
  bash scripts/ec2_service.sh nginx-apply # write nginx conf (/ + /chart) and reload nginx
  bash scripts/ec2_service.sh nginx-check # verify /chart and /v1/chart return JSON
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

  SHINOBU_EXTERNAL_ENGINE=1 nohup "$VENV_DIR/bin/python" -m streamlit run "$ROOT_DIR/app.py" \
    --server.address "$STREAMLIT_HOST" \
    --server.port "$STREAMLIT_PORT" \
    >/dev/null 2>"$LOG_DIR/streamlit.err.log" &
  echo $! >"$STREAMLIT_PID_FILE"
  echo "Streamlit started (pid=$(cat "$STREAMLIT_PID_FILE"))"
}

start_live_engine() {
  ensure_log_dir
  local current_pid=""
  if current_pid="$(read_pid "$LIVE_ENGINE_PID_FILE" 2>/dev/null)" && is_pid_running "$current_pid"; then
    echo "Live engine already running (pid=$current_pid)"
    return
  fi

  PYTHONPATH="$ROOT_DIR${PYTHONPATH:+:$PYTHONPATH}" \
    nohup "$VENV_DIR/bin/python" "$ROOT_DIR/scripts/run_live_engine.py" \
    >/dev/null 2>"$LOG_DIR/live_engine.err.log" &
  echo $! >"$LIVE_ENGINE_PID_FILE"
  echo "Live engine started (pid=$(cat "$LIVE_ENGINE_PID_FILE"))"
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
  start_live_engine
}

stop_all() {
  stop_one "Live engine" "$LIVE_ENGINE_PID_FILE"
  stop_one "Signal API" "$SIGNAL_PID_FILE"
  stop_one "Streamlit" "$STREAMLIT_PID_FILE"
}

status_all() {
  status_one "Streamlit" "$STREAMLIT_PID_FILE"
  status_one "Signal API" "$SIGNAL_PID_FILE"
  status_one "Live engine" "$LIVE_ENGINE_PID_FILE"
  echo "Logs:"
  echo "  $LOG_DIR/streamlit.err.log"
  echo "  $LOG_DIR/signal_api.err.log"
  echo "  $LOG_DIR/live_engine.err.log"
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
import sqlite3
from shinobu.cache_db import (
    DB_PATH,
    clear_all_cache_data,
    mark_startup_initialized,
    release_startup_init_lock,
    get_meta_value,
    set_meta_value,
)

clear_all_cache_data()
mark_startup_initialized(False)
release_startup_init_lock()
set_meta_value("startup_init_lock", "0")
with sqlite3.connect(DB_PATH) as connection:
    cursor = connection.cursor()
    table_counts = {}
    for table_name in ("raw_market_data", "indicator_data", "strategy_state", "payload_cache", "execution_cache"):
        table_counts[table_name] = int(cursor.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

print("reset complete: cache cleared, startup initialization forced")
print(f"db_path={DB_PATH}")
for table_name, count in table_counts.items():
    print(f"{table_name}: {count}")
print(f"startup_initialized={get_meta_value('startup_initialized')}")
print(f"startup_init_lock={get_meta_value('startup_init_lock')}")
PY

  echo "Starting services..."
  start_all
  echo "Reset flow complete. Streamlit will recollect and recalculate on startup."
}

apply_nginx_conf() {
  local tmp_conf
  tmp_conf="$(mktemp)"

  cat >"$tmp_conf" <<EOF
server {
  listen 80;
  server_name $NGINX_DOMAIN;
  return 301 https://\$host\$request_uri;
}

server {
  listen 443 ssl http2;
  server_name $NGINX_DOMAIN;

  ssl_certificate $NGINX_CERT_PATH;
  ssl_certificate_key $NGINX_CERT_KEY_PATH;

  location ^~ /chart {
    proxy_pass http://127.0.0.1:$SIGNAL_API_PORT;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_read_timeout 60s;
  }

  location ^~ /v1/ {
    proxy_pass http://127.0.0.1:$SIGNAL_API_PORT/v1/;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Real-IP \$remote_addr;
    proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_read_timeout 60s;
  }

  location = /docs {
    proxy_pass http://127.0.0.1:$SIGNAL_API_PORT/docs;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Proto \$scheme;
  }

  location = /openapi.json {
    proxy_pass http://127.0.0.1:$SIGNAL_API_PORT/openapi.json;
    proxy_http_version 1.1;
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Proto \$scheme;
  }

  location / {
    proxy_pass http://127.0.0.1:$STREAMLIT_PORT;
    proxy_http_version 1.1;
    proxy_set_header Upgrade \$http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host \$host;
    proxy_set_header X-Forwarded-Proto \$scheme;
    proxy_read_timeout 86400;
  }
}
EOF

  echo "Applying nginx config to $NGINX_CONF_PATH"
  sudo cp "$tmp_conf" "$NGINX_CONF_PATH"
  rm -f "$tmp_conf"

  echo "Validating nginx config..."
  sudo nginx -t
  echo "Reloading nginx..."
  sudo systemctl reload nginx
  echo "nginx apply complete: https://$NGINX_DOMAIN -> / (streamlit:$STREAMLIT_PORT), /chart|/v1/* (api:$SIGNAL_API_PORT)"
  check_nginx_routes
}

check_nginx_routes() {
  local chart_local chart_v1_local chart_public chart_v1_public
  local query="kind=overlay&symbol=122630.KS&pair_symbol=252670.KS&strategy_name=src&start_date=2026-04-01&end_date=2026-04-17&include_markers=1"

  chart_local="$(curl -sS -o /dev/null -w '%{http_code} %{content_type}' "http://127.0.0.1:$SIGNAL_API_PORT/chart?$query" || true)"
  chart_v1_local="$(curl -sS -o /dev/null -w '%{http_code} %{content_type}' "http://127.0.0.1:$SIGNAL_API_PORT/v1/chart?$query" || true)"
  chart_public="$(curl -k -sS -o /dev/null -w '%{http_code} %{content_type}' "https://$NGINX_DOMAIN/chart?$query" || true)"
  chart_v1_public="$(curl -k -sS -o /dev/null -w '%{http_code} %{content_type}' "https://$NGINX_DOMAIN/v1/chart?$query" || true)"

  echo "local  /chart    -> $chart_local"
  echo "local  /v1/chart -> $chart_v1_local"
  echo "public /chart    -> $chart_public"
  echo "public /v1/chart -> $chart_v1_public"

  if [[ "$chart_public" != *"application/json"* && "$chart_v1_public" != *"application/json"* ]]; then
    echo "ERROR: public routes are not returning JSON. Check nginx loaded config and domain origin settings."
    echo "Hint: sudo nginx -T | sed -n '/server_name $NGINX_DOMAIN/,/}/p'"
    return 1
  fi
}

daily_reco_cron_entry() {
  local py="$VENV_DIR/bin/python"
  local script="$ROOT_DIR/scripts/run_daily_recommendation.py"
  local log_file="$LOG_DIR/recommendation_cron.log"
  echo "CRON_TZ=$DAILY_RECO_CRON_TZ $DAILY_RECO_CRON_SCHEDULE cd $ROOT_DIR && $py $script >> $log_file 2>&1 $DAILY_RECO_CRON_TAG"
}

install_daily_reco_cron() {
  local current filtered entry tmp
  current="$(crontab -l 2>/dev/null || true)"
  filtered="$(printf '%s\n' "$current" | grep -vF "$DAILY_RECO_CRON_TAG" || true)"
  entry="$(daily_reco_cron_entry)"
  tmp="$(mktemp)"
  {
    printf '%s\n' "$filtered"
    printf '%s\n' "$entry"
  } | awk 'NF{print}' >"$tmp"
  crontab "$tmp"
  rm -f "$tmp"
  echo "Installed cron entry:"
  echo "  $entry"
  reco_cron_status
}

reco_cron_status() {
  local current line
  current="$(crontab -l 2>/dev/null || true)"
  line="$(printf '%s\n' "$current" | grep -F "$DAILY_RECO_CRON_TAG" || true)"
  if [[ -z "$line" ]]; then
    echo "Daily recommendation cron: not installed"
  else
    echo "Daily recommendation cron: installed"
    echo "  $line"
  fi
}

run_daily_reco_now() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "Virtualenv not found at $VENV_DIR. Run bootstrap first."
    exit 1
  fi
  ensure_log_dir
  echo "Running daily recommendation job now..."
  cd "$ROOT_DIR"
  "$VENV_DIR/bin/python" "$ROOT_DIR/scripts/run_daily_recommendation.py" | tee -a "$LOG_DIR/recommendation_cron.log"
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
    reco-cron-install) install_daily_reco_cron ;;
    reco-cron-status) reco_cron_status ;;
    reco-run-now) run_daily_reco_now ;;
    nginx-apply) apply_nginx_conf ;;
    nginx-check) check_nginx_routes ;;
    *) usage; exit 1 ;;
  esac
}

main "${1:-}"
