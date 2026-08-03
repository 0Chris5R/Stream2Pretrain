"""Tests for ingest probe endpoints."""

from __future__ import annotations

from urllib.request import urlopen

from ingest.common.probes import start_probe_server


def _get(port: int, path: str) -> bytes:
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=2) as resp:
        assert resp.status == 200
        return resp.read()


def test_probe_server_serves_health_ready_and_metrics() -> None:
    server = start_probe_server(host="127.0.0.1", port=0)
    try:
        port = int(server.server_port)
        assert _get(port, "/healthz") == b"ok\n"
        assert _get(port, "/readyz") == b"ok\n"
        assert b"s2p_process_up 1" in _get(port, "/metrics")
    finally:
        server.shutdown()
        server.server_close()


def test_probe_server_serves_custom_metrics() -> None:
    server = start_probe_server(
        host="127.0.0.1",
        port=0,
        metrics_provider=lambda: b"custom_metric 3\n",
    )
    try:
        assert _get(int(server.server_port), "/metrics") == b"custom_metric 3\n"
    finally:
        server.shutdown()
        server.server_close()
