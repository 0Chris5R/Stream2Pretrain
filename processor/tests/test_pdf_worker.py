"""Process-boundary tests without loading the optional Docling runtime."""

from __future__ import annotations

import os
import signal
import time

import pytest

from processor.metrics import ProcessorMetrics
from processor.pdf_worker import (
    PdfProcessingTimeoutError,
    PdfProcessWorker,
    PdfWorkerConfig,
    PdfWorkerCrashedError,
    PdfWorkerProcessingError,
    PdfWorkerRequest,
)
from processor.scientific import (
    DoclingDocumentConversionError,
    PdfExceedsDoclingLimitError,
    ScientificProcessingResult,
)
from schemas.scientific import ScientificDocument, ScientificSection


def _result(request: PdfWorkerRequest, *, suffix: str) -> ScientificProcessingResult:
    text = f"body:{request.pdf.decode()}:{suffix}"
    document = ScientificDocument(
        doc_id=request.doc_id,
        source_url=request.source_url,
        title="Isolated PDF",
        text_sha256="a" * 64,
        extraction_pipeline=request.extraction_pipeline,
        sections=[
            ScientificSection(
                section_id="section-1",
                level=1,
                title="Body",
                text=text,
                word_count=1,
            )
        ],
    )
    return ScientificProcessingResult(
        text=text,
        model_text=text,
        source_metadata_text="Isolated PDF",
        structured_text="",
        artifact_s3_uri=f"s3://silver/{request.doc_id}/document.json",
        document=document,
    )


class _StatefulHandler:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, request: PdfWorkerRequest) -> ScientificProcessingResult:
        self.calls += 1
        return _result(request, suffix=str(self.calls))


def _stateful_factory(_config: PdfWorkerConfig) -> _StatefulHandler:
    return _StatefulHandler()


class _SlowTermIgnoringHandler:
    def __call__(self, _request: PdfWorkerRequest) -> ScientificProcessingResult:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        time.sleep(30)
        raise AssertionError("hard timeout did not terminate the worker")


def _slow_factory(_config: PdfWorkerConfig) -> _SlowTermIgnoringHandler:
    return _SlowTermIgnoringHandler()


class _CrashHandler:
    def __call__(self, _request: PdfWorkerRequest) -> ScientificProcessingResult:
        os._exit(23)


def _crash_factory(_config: PdfWorkerConfig) -> _CrashHandler:
    return _CrashHandler()


class _ErrorHandler:
    def __call__(self, request: PdfWorkerRequest) -> ScientificProcessingResult:
        if request.pdf == b"document":
            raise DoclingDocumentConversionError("conclusive conversion failure")
        if request.pdf == b"oversized":
            raise PdfExceedsDoclingLimitError(actual_bytes=12, limit_bytes=8)
        raise RuntimeError("temporary object-store failure")


def _error_factory(_config: PdfWorkerConfig) -> _ErrorHandler:
    return _ErrorHandler()


@pytest.fixture
def worker_config() -> PdfWorkerConfig:
    return PdfWorkerConfig(
        minio_endpoint="http://minio.invalid",
        minio_access_key="access",
        minio_secret_key="secret",
        silver_bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )


def _process(
    worker: PdfProcessWorker, payload: bytes, **kwargs: object
) -> ScientificProcessingResult:
    return worker.process(
        doc_id="sha256:" + "a" * 64,
        source_url="https://example.invalid/paper.pdf",
        pdf=payload,
        extraction_pipeline="docling-test",
        **kwargs,
    )


def test_success_reuses_worker_and_preserves_exact_structured_result(
    worker_config: PdfWorkerConfig,
) -> None:
    worker = PdfProcessWorker(
        worker_config,
        hard_timeout_seconds=2,
        startup_timeout_seconds=5,
        handler_factory=_stateful_factory,
    )
    try:
        worker.start()
        first_pid = worker.pid
        first = _process(worker, b"first")
        second = _process(worker, b"second")

        expected = _result(
            PdfWorkerRequest(
                doc_id="sha256:" + "a" * 64,
                source_url="https://example.invalid/paper.pdf",
                pdf=b"first",
                extraction_pipeline="docling-test",
            ),
            suffix="1",
        )
        assert first == expected
        assert second.text == "body:second:2"
        assert worker.pid == first_pid
    finally:
        worker.close()


@pytest.mark.skipif(os.name != "posix", reason="process-group signals require POSIX")
def test_timeout_uses_term_then_kill_and_records_typed_outcome(
    worker_config: PdfWorkerConfig,
) -> None:
    metrics = ProcessorMetrics(namespace="pdf-test")
    worker = PdfProcessWorker(
        worker_config,
        hard_timeout_seconds=0.1,
        startup_timeout_seconds=5,
        termination_grace_seconds=0.1,
        handler_factory=_slow_factory,
    )
    try:
        worker.start()
        pid = worker.pid
        with pytest.raises(PdfProcessingTimeoutError):
            _process(worker, b"slow", metrics=metrics)

        assert pid is not None
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        body = metrics.render_prometheus().decode()
        assert (
            's2p_pdf_processing_seconds_count{namespace="pdf-test",outcome="timeout"} 1.0' in body
        )
        assert 's2p_pdf_worker_restarts_total{namespace="pdf-test",reason="timeout"} 1.0' in body
    finally:
        worker.close()


def test_child_exit_is_a_typed_record_local_crash(worker_config: PdfWorkerConfig) -> None:
    worker = PdfProcessWorker(
        worker_config,
        hard_timeout_seconds=2,
        startup_timeout_seconds=5,
        handler_factory=_crash_factory,
    )
    try:
        worker.start()
        with pytest.raises(PdfWorkerCrashedError):
            _process(worker, b"crash")
        assert worker.pid is None
    finally:
        worker.close()


def test_document_and_retryable_errors_keep_distinct_semantics(
    worker_config: PdfWorkerConfig,
) -> None:
    worker = PdfProcessWorker(
        worker_config,
        hard_timeout_seconds=2,
        startup_timeout_seconds=5,
        handler_factory=_error_factory,
    )
    try:
        with pytest.raises(DoclingDocumentConversionError):
            _process(worker, b"document")
        with pytest.raises(PdfExceedsDoclingLimitError) as oversized:
            _process(worker, b"oversized")
        assert oversized.value.actual_bytes == 12
        assert oversized.value.limit_bytes == 8
        with pytest.raises(PdfWorkerProcessingError):
            _process(worker, b"storage")
    finally:
        worker.close()
