"""HTTP API for Decon-Gate attestations stored by the Iceberg writer."""

from __future__ import annotations

import json
import os
import socket
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

from processor import common
from schemas.decon import DeconAttestation

METRICS_BODY = b"""# HELP s2p_process_up Process-level liveness.
# TYPE s2p_process_up gauge
s2p_process_up 1
"""


class AttestationStore:
    """Read signed attestations from the dedicated MinIO decon bucket."""

    def __init__(
        self,
        *,
        s3_client: object,
        bucket: str,
        benchmark_set_version: str,
        prefix: str = "decon",
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._version = benchmark_set_version
        self._prefix = prefix.strip("/")

    def key_for_snapshot(self, snapshot_id: int) -> str:
        """Return the canonical object key for one snapshot id."""
        return f"{self._prefix}/{self._version}/{snapshot_id:020d}.json"

    def get(self, snapshot_id: int) -> DeconAttestation | None:
        """Load one attestation, returning ``None`` if the object is absent."""
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=self.key_for_snapshot(snapshot_id))  # type: ignore[attr-defined]
        except Exception:
            return None
        body = resp["Body"].read()
        return common.decon_loads(body)

    def list(self, limit: int) -> list[DeconAttestation]:
        """Load the newest attestations by object key order."""
        prefix = f"{self._prefix}/{self._version}/"
        try:
            resp = self._s3.list_objects_v2(Bucket=self._bucket, Prefix=prefix)  # type: ignore[attr-defined]
        except Exception:
            return []
        contents = resp.get("Contents", [])
        keys = sorted((obj["Key"] for obj in contents if "Key" in obj), reverse=True)
        out: list[DeconAttestation] = []
        for key in keys[:limit]:
            try:
                item = self._s3.get_object(Bucket=self._bucket, Key=key)  # type: ignore[attr-defined]
                out.append(common.decon_loads(item["Body"].read()))
            except Exception:
                continue
        return out


class _Handler(BaseHTTPRequestHandler):
    server_version = "Stream2PretrainDeconAPI/1.0"

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/healthz", "/readyz"}:
            self._write(200, b"ok\n", "text/plain; charset=utf-8")
            return
        if parsed.path == "/metrics":
            self._write(200, METRICS_BODY, "text/plain; version=0.0.4")
            return
        if parsed.path == "/attestations":
            query = parse_qs(parsed.query)
            limit = _bounded_int(query.get("limit", ["20"])[0], default=20, low=1, high=200)
            records = self.server.store.list(limit)  # type: ignore[attr-defined]
            payload = [r.model_dump(mode="json") for r in records]
            self._write_json(200, payload)
            return
        prefix = "/attestations/"
        if parsed.path.startswith(prefix):
            raw = parsed.path[len(prefix) :]
            snapshot_id = _bounded_int(raw, default=-1, low=0, high=2**63 - 1)
            if snapshot_id < 0:
                self._write_json(400, {"detail": "invalid snapshot id"})
                return
            record = self.server.store.get(snapshot_id)  # type: ignore[attr-defined]
            if record is None:
                self._write_json(404, {"detail": "attestation not found"})
                return
            self._write_json(200, record.model_dump(mode="json"))
            return
        self._write_json(404, {"detail": "not found"})

    def log_message(self, _format: str, *_args: object) -> None:
        return

    def _write_json(self, status: int, payload: Any) -> None:
        self._write(status, json.dumps(payload, separators=(",", ":")).encode("utf-8"), "application/json")

    def _write(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class DeconAPIServer(ThreadingHTTPServer):
    """HTTPServer carrying the attestation store for handlers."""

    def __init__(self, addr: tuple[str, int], store: AttestationStore) -> None:
        bind_addr: tuple[str, int] | tuple[str, int, int, int] = addr
        if ":" in addr[0]:
            self.address_family = socket.AF_INET6
            bind_addr = (addr[0], addr[1], 0, 0)
        super().__init__(bind_addr, _Handler)
        self.store = store


def _bounded_int(raw: str, *, default: int, low: int, high: int) -> int:
    try:
        value = int(raw)
    except ValueError:
        return default
    return min(high, max(low, value))


def build_store(cfg: common.ProcessorConfig) -> AttestationStore:
    """Create an S3-backed attestation store from runtime config."""
    import boto3

    s3_client = boto3.client(
        "s3",
        endpoint_url=cfg.minio_endpoint,
        aws_access_key_id=cfg.minio_access_key,
        aws_secret_access_key=cfg.minio_secret_key,
        region_name="us-east-1",
    )
    return AttestationStore(
        s3_client=s3_client,
        bucket=cfg.decon_bucket,
        benchmark_set_version=cfg.benchmark_set_version,
    )


def serve(store: AttestationStore, *, host: str = "::", port: int = 8081) -> None:
    """Run the blocking Decon API server."""
    DeconAPIServer((host, port), store).serve_forever()


def main() -> None:
    """Entrypoint for the ``s2p-decon-api`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.decon_api")
    port = int(os.environ.get("S2P_DECON_API_PORT", "8081"))
    log.info("starting decon api", bucket=cfg.decon_bucket, port=port)
    serve(build_store(cfg), host=os.environ.get("S2P_BIND_HOST", "::"), port=port)


__all__ = ["AttestationStore", "DeconAPIServer", "build_store", "serve"]
