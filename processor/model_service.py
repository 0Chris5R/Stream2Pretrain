"""Stateless HTTP inference service for the curator's large CPU models."""

from __future__ import annotations

import os
import socket
import threading
import time
from collections.abc import Callable, Sequence
from contextlib import suppress
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal, TypeVar, cast

import orjson
from prometheus_client import Counter, Gauge, Histogram, generate_latest

from processor import common
from processor.operators.kenlm_score import KenLMScorer
from processor.operators.quality import QualityClassifier

MAX_REQUEST_BYTES = 2 * 1024 * 1024
ModelProfile = Literal["finepdfs", "fineweb", "quality", "kenlm", "all"]
MODEL_PROFILES: frozenset[str] = frozenset({"finepdfs", "fineweb", "quality", "kenlm", "all"})
_Result = TypeVar("_Result")

MODEL_REQUESTS = Counter(
    "s2p_model_requests_total",
    "Completed curator model-service requests.",
    ["profile", "operation", "model_family", "status"],
)
MODEL_BATCH_ITEMS = Histogram(
    "s2p_model_batch_items",
    "Items submitted in one bounded curator model-service request.",
    ["profile", "operation", "model_family"],
    buckets=(1, 2, 4, 8, 16, 32),
)
MODEL_QUEUE_SECONDS = Histogram(
    "s2p_model_queue_seconds",
    "Time spent waiting for the model-family inference lock.",
    ["profile", "operation", "model_family"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60),
)
MODEL_INFERENCE_SECONDS = Histogram(
    "s2p_model_inference_seconds",
    "Wall time spent in curator model inference.",
    ["profile", "operation", "model_family"],
    buckets=(0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 15, 30, 60, 180),
)
MODEL_ACTIVE = Gauge(
    "s2p_model_active_requests",
    "Model requests currently executing in this Pod.",
    ["profile", "operation", "model_family"],
)


class CuratorModelRuntime:
    """Eagerly loaded, strict model bundle shared by HTTP handlers."""

    def __init__(self, models_dir: str | Path, *, profile: ModelProfile = "all") -> None:
        root = Path(models_dir)
        self.profile = profile
        self.finepdfs: QualityClassifier | None = None
        self.fineweb: QualityClassifier | None = None
        self.kenlm: KenLMScorer | None = None
        if profile in {"finepdfs", "quality", "all"}:
            self.finepdfs = QualityClassifier(
                root / "finepdfs-edu-v2",
                revision=os.environ.get("S2P_FINEPDFS_EDU_REVISION"),
                model_family="finepdfs-edu-v2",
                allow_fallback=False,
            )
        if profile in {"fineweb", "quality", "all"}:
            self.fineweb = QualityClassifier(
                root / "fineweb-edu",
                revision=os.environ.get("S2P_FINEWEB_EDU_REVISION"),
                model_family="fineweb-edu",
                allow_fallback=False,
            )
        if profile in {"kenlm", "all"}:
            self.kenlm = KenLMScorer(
                root / "kenlm" / "en.arpa.bin",
                root / "kenlm" / "en.sp.model",
                allow_fallback=False,
            )
        # Each model instance has an independent lock.  FinePDFs and FineWeb
        # are separate immutable runtimes in production, while the ``all``
        # profile used by local tests can still execute distinct models in
        # parallel without entering the same global critical section.
        self.locks = {
            "finepdfs-edu-v2": threading.Lock(),
            "fineweb-edu": threading.Lock(),
            "kenlm": threading.Lock(),
        }
        self.max_batch_items = _positive_int_env("S2P_MODEL_SERVICE_MAX_BATCH_ITEMS", 8)

    def metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {"ready": True, "profile": self.profile}
        quality: dict[str, dict[str, str]] = {}
        if self.finepdfs is not None:
            quality["finepdfs-edu-v2"] = {
                "backend": self.finepdfs.backend,
                "revision": self.finepdfs.revision,
            }
        if self.fineweb is not None:
            quality["fineweb-edu"] = {
                "backend": self.fineweb.backend,
                "revision": self.fineweb.revision,
            }
        if quality:
            metadata["quality"] = quality
        if self.kenlm is not None:
            metadata["kenlm"] = {
                "backend": "kenlm-sentencepiece",
                "scorer": self.kenlm.scorer,
            }
        return metadata

    def quality_many(self, family: str, texts: Sequence[str]) -> list[dict[str, Any]]:
        classifiers = {
            "finepdfs-edu-v2": self.finepdfs,
            "fineweb-edu": self.fineweb,
        }
        classifier = classifiers.get(family)
        if classifier is None:
            raise ValueError("unsupported model_family")
        if not texts or len(texts) > self.max_batch_items:
            raise ValueError(f"texts must contain between 1 and {self.max_batch_items} items")
        return self._run_locked(
            operation="quality",
            model_family=family,
            item_count=len(texts),
            lock=self.locks[family],
            callback=lambda: [
                {
                    "edu_score": result.edu_score,
                    "revision": result.revision,
                }
                for result in (classifier.score(text) for text in texts)
            ],
        )

    def perplexity(self, text: str) -> Any:
        if self.kenlm is None:
            raise ValueError("perplexity is unavailable in this model profile")
        return self._run_locked(
            operation="perplexity",
            model_family="kenlm",
            item_count=1,
            lock=self.locks["kenlm"],
            callback=lambda: self.kenlm.score(text),
        )

    def _run_locked(
        self,
        *,
        operation: str,
        model_family: str,
        item_count: int,
        lock: threading.Lock,
        callback: Callable[[], _Result],
    ) -> _Result:
        labels = (self.profile, operation, model_family)
        MODEL_BATCH_ITEMS.labels(*labels).observe(item_count)
        queued_at = time.monotonic()
        lock.acquire()
        MODEL_QUEUE_SECONDS.labels(*labels).observe(time.monotonic() - queued_at)
        MODEL_ACTIVE.labels(*labels).inc()
        started_at = time.monotonic()
        try:
            result = callback()
        except Exception:
            MODEL_REQUESTS.labels(*labels, "error").inc()
            raise
        else:
            MODEL_REQUESTS.labels(*labels, "success").inc()
            return result
        finally:
            MODEL_INFERENCE_SECONDS.labels(*labels).observe(time.monotonic() - started_at)
            MODEL_ACTIVE.labels(*labels).dec()
            lock.release()


class CuratorModelServer(ThreadingHTTPServer):
    runtime: CuratorModelRuntime


class IPv6CuratorModelServer(CuratorModelServer):
    address_family = socket.AF_INET6

    def server_bind(self) -> None:
        with suppress(OSError):
            self.socket.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        super().server_bind()


class _Handler(BaseHTTPRequestHandler):
    server_version = "Stream2PretrainModelService/1.0"

    @property
    def runtime(self) -> CuratorModelRuntime:
        return self.server.runtime  # type: ignore[attr-defined,no-any-return]

    def do_GET(self) -> None:
        if self.path in {"/healthz", "/readyz"}:
            self._write(HTTPStatus.OK, {"ready": True})
            return
        if self.path == "/v1/metadata":
            self._write(HTTPStatus.OK, self.runtime.metadata())
            return
        if self.path == "/metrics":
            self._write_bytes(
                HTTPStatus.OK,
                generate_latest(),
                content_type="text/plain; version=0.0.4; charset=utf-8",
            )
            return
        self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            payload = self._read_payload()
            if self.path in {"/v1/quality", "/v1/quality:batch"}:
                family = payload.get("model_family")
                if not isinstance(family, str):
                    raise ValueError("model_family must be a string")
                raw_texts = (
                    [payload.get("text")] if self.path == "/v1/quality" else payload.get("texts")
                )
                if not isinstance(raw_texts, list) or not all(
                    isinstance(text, str) for text in raw_texts
                ):
                    raise ValueError("texts must be a list of strings")
                results = self.runtime.quality_many(family, cast(list[str], raw_texts))
                if self.path == "/v1/quality":
                    self._write(HTTPStatus.OK, results[0])
                else:
                    self._write(HTTPStatus.OK, {"results": results})
                return
            text = payload.get("text")
            if not isinstance(text, str):
                raise ValueError("text must be a string")
            if self.path == "/v1/perplexity":
                perplexity_result = self.runtime.perplexity(text)
                self._write(
                    HTTPStatus.OK,
                    {
                        "perplexity": perplexity_result.perplexity,
                        "bucket": perplexity_result.bucket,
                        "scorer": perplexity_result.scorer,
                    },
                )
                return
            self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})
        except (ValueError, orjson.JSONDecodeError) as exc:
            self._write(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
        except Exception as exc:
            common.get_logger("s2p.model-service").exception(
                "model inference failed", path=self.path, error=str(exc)
            )
            self._write(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "inference failed"})

    def _read_payload(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ValueError("invalid Content-Length") from exc
        if length < 1 or length > MAX_REQUEST_BYTES:
            raise ValueError("request body is empty or too large")
        value = orjson.loads(self.rfile.read(length))
        if not isinstance(value, dict):
            raise ValueError("request body must be a JSON object")
        return value

    def _write(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        self._write_bytes(status, orjson.dumps(payload), content_type="application/json")

    def _write_bytes(self, status: HTTPStatus, body: bytes, *, content_type: str) -> None:
        self.send_response(int(status))
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-S2P-Model-Backend", socket.gethostname())
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: object) -> None:
        return


def serve(runtime: CuratorModelRuntime, *, host: str = "::", port: int = 8094) -> None:
    """Serve the strict model runtime until the process receives a signal."""
    server_class = IPv6CuratorModelServer if ":" in host else CuratorModelServer
    server = server_class((host, port), _Handler)
    server.runtime = runtime
    server.serve_forever()


def _positive_int_env(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value < 1:
        raise RuntimeError(f"{name} must be positive")
    return value


def main() -> None:
    """Load every pinned model before making the readiness endpoint available."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.model-service")
    profile_value = os.environ.get("S2P_MODEL_SERVICE_PROFILE", "all").strip().lower()
    if profile_value not in MODEL_PROFILES:
        raise RuntimeError(f"unsupported S2P_MODEL_SERVICE_PROFILE={profile_value!r}")
    profile = cast(ModelProfile, profile_value)
    log.info("loading curator model service", models_dir=cfg.models_dir, profile=profile)
    runtime = CuratorModelRuntime(cfg.models_dir, profile=profile)
    log.info("curator model service ready", metadata=runtime.metadata())
    serve(runtime, port=int(os.environ.get("S2P_MODEL_SERVICE_PORT", "8094")))


if __name__ == "__main__":
    main()
