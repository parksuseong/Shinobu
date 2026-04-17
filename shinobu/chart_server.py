from __future__ import annotations

import errno
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import URLError
from urllib.parse import parse_qs, urlparse
from urllib.request import urlopen

from shinobu.chart_controller import build_chart_payload_controlled
from shinobu.strategy import StrategyAdjustments


CHART_SERVER_HOST = "0.0.0.0"
CHART_SERVER_PORT = 8765
_SERVER_LOCK = threading.Lock()
_SERVER_STARTED = False


class _ChartHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self._send_json({"ok": True})
            return

        if parsed.path != "/chart":
            self.send_error(404)
            return

        query = parse_qs(parsed.query)
        kind = query.get("kind", ["raw"])[0]
        symbol = query.get("symbol", ["122630.KS"])[0]
        pair_symbol = query.get("pair_symbol", [""])[0] or None
        adjustments = StrategyAdjustments(
            stoch_pct=int(query.get("stoch_pct", ["0"])[0]),
            cci_pct=int(query.get("cci_pct", ["0"])[0]),
            rsi_pct=int(query.get("rsi_pct", ["0"])[0]),
        )
        strategy_name = query.get("strategy_name", query.get("profile_name", ["src_v2_adx"]))[0]
        visible_business_days = int(query.get("visible_business_days", ["5"])[0])
        include_markers = query.get("include_markers", ["1"])[0].strip() not in {"0", "false", "False"}
        payload = build_chart_payload_controlled(
            kind=kind,
            symbol=symbol,
            pair_symbol=pair_symbol,
            adjustments=adjustments,
            strategy_name=strategy_name,
            visible_business_days=visible_business_days,
            include_markers=include_markers,
        )
        self._send_json(payload)

    def log_message(self, format: str, *args) -> None:
        return

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def _send_json(self, payload: dict) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def ensure_chart_server() -> str:
    global _SERVER_STARTED
    with _SERVER_LOCK:
        if _SERVER_STARTED:
            return f"http://{CHART_SERVER_HOST}:{CHART_SERVER_PORT}"

        try:
            server = ThreadingHTTPServer((CHART_SERVER_HOST, CHART_SERVER_PORT), _ChartHandler)
        except OSError as exc:
            # Another process/session may already own the chart port.
            if exc.errno == errno.EADDRINUSE and _is_chart_server_alive():
                _SERVER_STARTED = True
                return f"http://{CHART_SERVER_HOST}:{CHART_SERVER_PORT}"
            raise
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        _SERVER_STARTED = True
    return f"http://{CHART_SERVER_HOST}:{CHART_SERVER_PORT}"


def _is_chart_server_alive(timeout_seconds: float = 0.5) -> bool:
    try:
        with urlopen(f"http://127.0.0.1:{CHART_SERVER_PORT}/health", timeout=timeout_seconds) as response:
            return response.status == 200
    except (URLError, OSError):
        return False
