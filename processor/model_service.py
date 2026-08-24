"""Stateless HTTP inference service for the curator's large CPU models."""

from __future__ import annotations

import os
import socket
import threading
from contextlib import suppress
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Literal, cast

import orjson

from processor import common
from processor.decon_gate import _EmbeddingSketch  # type: ignore[attr-defined]
from processor.operators.kenlm_score import KenLMScorer
from processor.operators.pii import PiiScanner
from processor.operators.quality import QualityClassifier

MAX_REQUEST_BYTES = 2 * 1024 * 1024
ModelProfile = Literal["quality", "kenlm", "embedding", "privacy", "all"]
MODEL_PROFILES: frozenset[str] = frozenset(
    {"quality", "kenlm", "embedding", "privacy", "all"}
)


class CuratorModelRuntime:
    """Eagerly loaded, strict model bundle shared by HTTP handlers."""

    def __init__(self, models_dir: str | Path, *, profile: ModelProfile = "all") -> None:
        root = Path(models_dir)
        self.profile = profile
        self.finepdfs: QualityClassifier | None = None
        self.fineweb: QualityClassifier | None = None
        self.kenlm: KenLMScorer | None = None
        self.embedding: _EmbeddingSketch | None = None
        self.privacy: PiiScanner | None = None
        if profile in {"quality", "all"}:
            self.finepdfs = QualityClassifier(
                root / "finepdfs-edu-v2",
                revision=os.environ.get("S2P_FINEPDFS_EDU_REVISION"),
                model_family="finepdfs-edu-v2",
                allow_fallback=False,
            )
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
        if profile in {"embedding", "all"}:
            self.embedding = _EmbeddingSketch(
                root / "e5-small",
                revision=os.environ.get("E5_SMALL_REVISION"),
                allow_fallback=False,
            )
        if profile == "privacy":
            self.privacy = PiiScanner(use_presidio=True, allow_fallback=False)
        self.lock = threading.Lock()

    def metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {"ready": True, "profile": self.profile}
        if self.finepdfs is not None and self.fineweb is not None:
            metadata["quality"] = {
                "finepdfs-edu-v2": {
                    "backend": self.finepdfs.backend,
                    "revision": self.finepdfs.revision,
                },
                "fineweb-edu": {
                    "backend": self.fineweb.backend,
                    "revision": self.fineweb.revision,
                },
            }
        if self.kenlm is not None:
            metadata["kenlm"] = {
                "backend": "kenlm-sentencepiece",
                "scorer": self.kenlm.scorer,
            }
        if self.embedding is not None:
            metadata["embedding"] = {
                "backend": self.embedding.backend,
                "revision": self.embedding.revision,
            }
        if self.privacy is not None:
            metadata["privacy"] = {
                "backend": "presidio-spacy",
                "revision": self.privacy.revision,
            }
        return metadata


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
        self._write(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:
        try:
            payload = self._read_payload()
            text = payload.get("text")
            if not isinstance(text, str):
                raise ValueError("text must be a string")
            with self.runtime.lock:
                if self.path == "/v1/quality":
                    family = payload.get("model_family")
                    classifiers = {
                        "finepdfs-edu-v2": self.runtime.finepdfs,
                        "fineweb-edu": self.runtime.fineweb,
                    }
                    if (
                        not isinstance(family, str)
                        or family not in classifiers
                        or classifiers[family] is None
                    ):
                        raise ValueError("unsupported model_family")
                    quality_result = classifiers[family].score(text)  # type: ignore[union-attr]
                    self._write(
                        HTTPStatus.OK,
                        {
                            "edu_score": quality_result.edu_score,
                            "revision": quality_result.revision,
                        },
                    )
                    return
                if self.path == "/v1/perplexity":
                    if self.runtime.kenlm is None:
                        raise ValueError("perplexity is unavailable in this model profile")
                    perplexity_result = self.runtime.kenlm.score(text)
                    self._write(
                        HTTPStatus.OK,
                        {
                            "perplexity": perplexity_result.perplexity,
                            "bucket": perplexity_result.bucket,
                            "scorer": perplexity_result.scorer,
                        },
                    )
                    return
                if self.path == "/v1/embed":
                    if self.runtime.embedding is None:
                        raise ValueError("embedding is unavailable in this model profile")
                    self._write(
                        HTTPStatus.OK,
                        {"embedding": self.runtime.embedding.embed(text)},
                    )
                    return
                if self.path == "/v1/pii":
                    if self.runtime.privacy is None:
                        raise ValueError("privacy scanning is unavailable in this model profile")
                    hits = self.runtime.privacy.scan(text)
                    self._write(
                        HTTPStatus.OK,
                        {
                            "revision": self.runtime.privacy.revision,
                            "hits": [
                                {"flag": hit.flag, "snippet": hit.snippet}
                                for hit in hits
                            ],
                            "blocking_flags": self.runtime.privacy.blocking_flags(text),
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
        body = orjson.dumps(payload)
        self.send_response(int(status))
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
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
