"""Small HTTP probe server for long-running processor workers."""

from __future__ import annotations

import os
import threading
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

METRICS_BODY = b"""# HELP s2p_process_up Process-level liveness.
# TYPE s2p_process_up gauge
s2p_process_up 1
"""

MetricsProvider = Callable[[], bytes]


class ProbeServer(ThreadingHTTPServer):
    metrics_provider: MetricsProvider | None = None


class _ProbeHandler(BaseHTTPRequestHandler):
    server_version = "Stream2PretrainProbe/1.0"

    def do_GET(self) -> None:
        if self.path in {"/healthz", "/readyz"}:
            self._write(200, b"ok\n", "text/plain; charset=utf-8")
            return
        if self.path == "/metrics":
            provider = getattr(self.server, "metrics_provider", None)
            body = provider() if callable(provider) else METRICS_BODY
            self._write(200, body, "text/plain; version=0.0.4")
            return
        self._write(404, b"not found\n", "text/plain; charset=utf-8")

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _write(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_probe_server(
    host: str = "0.0.0.0",
    port: int | None = None,
    *,
    metrics_provider: MetricsProvider | None = None,
) -> ProbeServer:
    """Start `/healthz`, `/readyz`, and `/metrics` in a daemon thread."""
    resolved_port = port if port is not None else int(os.environ.get("S2P_PROBE_PORT", "9090"))
    server = ProbeServer((host, resolved_port), _ProbeHandler)
    server.metrics_provider = metrics_provider
    thread = threading.Thread(target=server.serve_forever, name="s2p-probes", daemon=True)
    thread.start()
    return server
