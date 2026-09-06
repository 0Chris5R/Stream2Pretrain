"""Hard process boundary for the native CPU PDF extraction stack.

Docling coordinates Python threads, Tesseract subprocesses, and PDFium native
handles.  A timed-out thread cannot be cancelled safely inside the long-lived
Bytewax process, so one persistent child owns that complete stack.  Successful
requests reuse its loaded models.  Any timeout, crash, or processing error
destroys the whole process group before another PDF is admitted.
"""

from __future__ import annotations

import atexit
import math
import multiprocessing
import os
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from multiprocessing.connection import Connection
from typing import Any, Protocol

import boto3
import orjson

from processor.metrics import ProcessorMetrics
from processor.scientific import (
    DoclingDocumentConversionError,
    PdfExceedsDoclingLimitError,
    ScientificProcessingResult,
    ScientificProcessor,
)
from schemas.scientific import ScientificDocument

# Temporary operational bound. The 2026-08-31 rollout returned PDF conversions
# at 196.26s and 203.95s, proving the previous 180s bound was too short. Replace
# this with the measured high-percentile latency plus operating margin after a
# representative cloud sample is recorded by ``s2p_pdf_processing_seconds``.
TEMPORARY_PDF_HARD_TIMEOUT_SECONDS = 240.0
# Control-plane cleanup bound, not a throughput claim. Its cloud exit latency
# is needs-measurement; tune only from observed native-grandchild shutdowns.
DEFAULT_TERMINATION_GRACE_SECONDS = 5.0


class PdfProcessingTimeoutError(ValueError):
    """One PDF exceeded the parent-enforced hard processing deadline."""


class PdfWorkerCrashedError(ValueError):
    """The isolated native PDF worker exited without a complete response."""


class PdfWorkerDocumentError(ValueError):
    """The isolated worker conclusively rejected one document."""


class PdfWorkerProcessingError(RuntimeError):
    """A retryable processing or storage error occurred inside the worker."""


@dataclass(frozen=True, slots=True)
class PdfWorkerConfig:
    """Serializable construction inputs for the spawned PDF worker."""

    minio_endpoint: str
    minio_access_key: str = field(repr=False)
    minio_secret_key: str = field(repr=False)
    silver_bucket: str
    models_dir: str
    user_agent: str
    require_real_models: bool


@dataclass(frozen=True, slots=True)
class PdfWorkerRequest:
    """One trusted parent-to-child PDF conversion request."""

    doc_id: str
    source_url: str
    pdf: bytes
    extraction_pipeline: str


class PdfRequestHandler(Protocol):
    def __call__(self, request: PdfWorkerRequest) -> ScientificProcessingResult: ...


PdfHandlerFactory = Callable[[PdfWorkerConfig], PdfRequestHandler]


class _ScientificPdfHandler:
    """Child-owned, reusable production Docling processor."""

    def __init__(self, config: PdfWorkerConfig) -> None:
        s3 = boto3.client(
            "s3",
            endpoint_url=config.minio_endpoint,
            aws_access_key_id=config.minio_access_key,
            aws_secret_access_key=config.minio_secret_key,
            region_name="us-east-1",
        )
        self._processor = ScientificProcessor(
            s3_client=s3,
            bucket=config.silver_bucket,
            models_dir=config.models_dir,
            user_agent=config.user_agent,
            require_real_models=config.require_real_models,
            disable_docling_document_timeout=True,
        )

    def __call__(self, request: PdfWorkerRequest) -> ScientificProcessingResult:
        return self._processor.process_pdf(
            doc_id=request.doc_id,
            source_url=request.source_url,
            pdf=request.pdf,
            extraction_pipeline=request.extraction_pipeline,
        )


def _production_handler_factory(config: PdfWorkerConfig) -> PdfRequestHandler:
    return _ScientificPdfHandler(config)


def _result_payload(result: ScientificProcessingResult) -> dict[str, object]:
    return {
        "text": result.text,
        "model_text": result.model_text,
        "source_metadata_text": result.source_metadata_text,
        "structured_text": result.structured_text,
        "artifact_s3_uri": result.artifact_s3_uri,
        "document": result.document.model_dump(mode="json"),
    }


def _decode_result(payload: object) -> ScientificProcessingResult:
    if not isinstance(payload, dict):
        raise ValueError("PDF worker returned an invalid result payload")
    return ScientificProcessingResult(
        text=str(payload["text"]),
        model_text=str(payload["model_text"]),
        source_metadata_text=str(payload["source_metadata_text"]),
        structured_text=str(payload["structured_text"]),
        artifact_s3_uri=str(payload["artifact_s3_uri"]),
        document=ScientificDocument.model_validate(payload["document"]),
    )


def _send_response(connection: Connection, payload: dict[str, object]) -> None:
    connection.send_bytes(orjson.dumps(payload))


def _pdf_worker_main(
    connection: Connection,
    config: PdfWorkerConfig,
    handler_factory: PdfHandlerFactory,
) -> None:
    """Own all native PDF resources inside a dedicated POSIX process group."""
    try:
        if os.name == "posix":
            os.setsid()
        handler = handler_factory(config)
        _send_response(connection, {"status": "ready"})
    except BaseException as exc:
        try:
            _send_response(
                connection,
                {
                    "status": "startup_error",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                },
            )
        finally:
            connection.close()
        return

    try:
        while True:
            try:
                request = connection.recv()
            except EOFError:
                return
            if request is None:
                return
            if not isinstance(request, PdfWorkerRequest):
                _send_response(
                    connection,
                    {
                        "status": "processing_error",
                        "error_type": "InvalidPdfWorkerRequest",
                        "message": "PDF worker received an invalid request",
                    },
                )
                continue
            try:
                result = handler(request)
            except ValueError as exc:
                response: dict[str, object] = {
                    "status": "document_error",
                    "error_type": type(exc).__name__,
                    "message": str(exc),
                }
                if isinstance(exc, PdfExceedsDoclingLimitError):
                    response["actual_bytes"] = exc.actual_bytes
                    response["limit_bytes"] = exc.limit_bytes
                _send_response(connection, response)
            except Exception as exc:
                _send_response(
                    connection,
                    {
                        "status": "processing_error",
                        "error_type": type(exc).__name__,
                        "message": str(exc),
                    },
                )
            else:
                _send_response(
                    connection,
                    {"status": "success", "result": _result_payload(result)},
                )
    finally:
        connection.close()


class PdfProcessWorker:
    """Parent-side controller for one persistent isolated PDF worker."""

    def __init__(
        self,
        config: PdfWorkerConfig,
        *,
        hard_timeout_seconds: float = TEMPORARY_PDF_HARD_TIMEOUT_SECONDS,
        startup_timeout_seconds: float | None = None,
        termination_grace_seconds: float = DEFAULT_TERMINATION_GRACE_SECONDS,
        handler_factory: PdfHandlerFactory = _production_handler_factory,
        multiprocessing_context: str = "spawn",
    ) -> None:
        if not math.isfinite(hard_timeout_seconds) or hard_timeout_seconds <= 0:
            raise RuntimeError("PDF hard timeout must be positive")
        if not math.isfinite(termination_grace_seconds) or termination_grace_seconds <= 0:
            raise RuntimeError("PDF worker termination grace must be positive")
        self._config = config
        self._hard_timeout_seconds = hard_timeout_seconds
        self._startup_timeout_seconds = (
            hard_timeout_seconds if startup_timeout_seconds is None else startup_timeout_seconds
        )
        if not math.isfinite(self._startup_timeout_seconds) or self._startup_timeout_seconds <= 0:
            raise RuntimeError("PDF worker startup timeout must be positive")
        self._termination_grace_seconds = termination_grace_seconds
        self._handler_factory = handler_factory
        # Typeshed exposes only the abstract context here even though the
        # concrete spawn context supplies Pipe and Process at runtime.
        self._context: Any = multiprocessing.get_context(multiprocessing_context)
        self._process: multiprocessing.Process | None = None
        self._connection: Connection | None = None
        self._lock = threading.Lock()
        atexit.register(self.close)

    @property
    def pid(self) -> int | None:
        process = self._process
        return process.pid if process is not None else None

    def start(self) -> None:
        """Eagerly validate that the child can construct the production stack."""
        with self._lock:
            self._ensure_started()

    def process(
        self,
        *,
        doc_id: str,
        source_url: str,
        pdf: bytes,
        extraction_pipeline: str,
        metrics: ProcessorMetrics | None = None,
    ) -> ScientificProcessingResult:
        with self._lock:
            started = time.monotonic()
            try:
                self._ensure_started()
                connection = self._connection
                process = self._process
                if connection is None or process is None:
                    raise PdfWorkerCrashedError("PDF worker did not initialize")
                connection.send(
                    PdfWorkerRequest(
                        doc_id=doc_id,
                        source_url=source_url,
                        pdf=pdf,
                        extraction_pipeline=extraction_pipeline,
                    )
                )
                if not connection.poll(self._hard_timeout_seconds):
                    self._record_duration(metrics, "timeout", started)
                    self._recycle("timeout", metrics)
                    raise PdfProcessingTimeoutError(
                        f"PDF processing exceeded the temporary "
                        f"{self._hard_timeout_seconds:g}s hard deadline"
                    )
                response = self._receive_response()
            except PdfProcessingTimeoutError:
                raise
            except PdfWorkerCrashedError:
                self._record_duration(metrics, "worker_crash", started)
                self._recycle("worker_crash", metrics)
                raise
            except (BrokenPipeError, EOFError, OSError) as exc:
                self._record_duration(metrics, "worker_crash", started)
                self._recycle("worker_crash", metrics)
                raise PdfWorkerCrashedError(
                    f"PDF worker exited before completing {doc_id}"
                ) from exc

            status = response.get("status")
            if status == "success":
                try:
                    result = _decode_result(response.get("result"))
                except (KeyError, TypeError, ValueError) as exc:
                    self._record_duration(metrics, "worker_crash", started)
                    self._recycle("invalid_response", metrics)
                    raise PdfWorkerCrashedError("PDF worker returned invalid output") from exc
                self._record_duration(metrics, "success", started)
                return result

            error_type = str(response.get("error_type") or "UnknownError")
            message = str(response.get("message") or "PDF worker failed")
            if status == "document_error":
                self._record_duration(metrics, "document_error", started)
                self._recycle("document_error", metrics)
                if error_type == "DoclingDocumentConversionError":
                    raise DoclingDocumentConversionError(message)
                if error_type == "PdfExceedsDoclingLimitError":
                    try:
                        actual_bytes = int(response["actual_bytes"])
                        limit_bytes = int(response["limit_bytes"])
                    except (KeyError, TypeError, ValueError) as exc:
                        raise PdfWorkerCrashedError(
                            "PDF worker omitted the exact size-limit evidence"
                        ) from exc
                    raise PdfExceedsDoclingLimitError(
                        actual_bytes=actual_bytes,
                        limit_bytes=limit_bytes,
                    )
                raise PdfWorkerDocumentError(f"{error_type}: {message}")
            if status == "processing_error":
                self._record_duration(metrics, "processing_error", started)
                self._recycle("processing_error", metrics)
                raise PdfWorkerProcessingError(f"{error_type}: {message}")

            self._record_duration(metrics, "worker_crash", started)
            self._recycle("invalid_response", metrics)
            raise PdfWorkerCrashedError("PDF worker returned an unknown response")

    def close(self) -> None:
        with self._lock:
            self._terminate_current()

    def _ensure_started(self) -> None:
        if self._process is not None and self._process.is_alive() and self._connection is not None:
            return
        self._terminate_current()
        parent_connection, child_connection = self._context.Pipe(duplex=True)
        process = self._context.Process(
            target=_pdf_worker_main,
            args=(child_connection, self._config, self._handler_factory),
            name="s2p-pdf-worker",
            # Explicit process-group teardown handles lifecycle. Keeping this
            # non-daemonic does not constrain libraries that may themselves
            # need child processes in a future pinned Docling release.
            daemon=False,
        )
        try:
            process.start()
        except Exception as exc:
            parent_connection.close()
            child_connection.close()
            raise PdfWorkerCrashedError("PDF worker process could not start") from exc
        child_connection.close()
        self._process = process
        self._connection = parent_connection
        try:
            if not parent_connection.poll(self._startup_timeout_seconds):
                self._terminate_current()
                raise PdfWorkerCrashedError("PDF worker startup exceeded its deadline")
            response = self._receive_response()
        except (BrokenPipeError, EOFError, OSError) as exc:
            self._terminate_current()
            raise PdfWorkerCrashedError("PDF worker exited during startup") from exc
        if response.get("status") != "ready":
            error_type = str(response.get("error_type") or "UnknownError")
            message = str(response.get("message") or "PDF worker failed to start")
            self._terminate_current()
            raise PdfWorkerCrashedError(f"{error_type}: {message}")

    def _receive_response(self) -> dict[str, Any]:
        connection = self._connection
        if connection is None:
            raise PdfWorkerCrashedError("PDF worker connection is unavailable")
        try:
            payload = orjson.loads(connection.recv_bytes())
        except (orjson.JSONDecodeError, TypeError) as exc:
            raise PdfWorkerCrashedError("PDF worker response is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise PdfWorkerCrashedError("PDF worker response is not an object")
        return payload

    def _record_duration(
        self,
        metrics: ProcessorMetrics | None,
        outcome: str,
        started: float,
    ) -> None:
        if metrics is not None:
            metrics.record_pdf_processing(outcome=outcome, seconds=time.monotonic() - started)

    def _recycle(self, reason: str, metrics: ProcessorMetrics | None) -> None:
        self._terminate_current()
        if metrics is not None:
            metrics.record_pdf_worker_restart(reason=reason)

    def _terminate_current(self) -> None:
        connection = self._connection
        process = self._process
        self._connection = None
        self._process = None
        if connection is not None:
            connection.close()
        if process is None:
            return
        if process.is_alive():
            self._signal_process(process, signal.SIGTERM)
            process.join(self._termination_grace_seconds)
        if process.is_alive():
            self._signal_process(process, signal.SIGKILL)
            process.join(self._termination_grace_seconds)
        else:
            process.join(timeout=0)

    @staticmethod
    def _signal_process(process: multiprocessing.Process, sig: signal.Signals) -> None:
        pid = process.pid
        if pid is None:
            return
        if os.name == "posix":
            try:
                if os.getpgid(pid) == pid:
                    os.killpg(pid, sig)
                    return
            except ProcessLookupError:
                return
            except OSError:
                pass
        if sig == signal.SIGTERM:
            process.terminate()
        else:
            process.kill()
