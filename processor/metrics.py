"""Prometheus metrics for processor dataflows.

Metric names are shared with the Grafana dashboard and the Next.js BFF. Keep
this module as the canonical producer-side contract when dashboards evolve.
"""

from __future__ import annotations

import os
import threading
from collections.abc import Iterable

from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest

QUALITY_BUCKETS = (0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0)
FLUSH_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)
PDF_PROCESSING_BUCKETS = (1.0, 5.0, 15.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0, 600.0)


class ProcessorMetrics:
    """Process-local metric registry used by Bytewax worker probes."""

    def __init__(self, *, namespace: str | None = None) -> None:
        self._namespace = namespace or os.environ.get("S2P_NAMESPACE", "default")
        self.registry = CollectorRegistry()
        self._lock = threading.Lock()
        self._process_up = Counter(
            "s2p_process_starts_total",
            "Process starts for this worker.",
            ["namespace"],
            registry=self.registry,
        )
        self._documents_emitted = Counter(
            "s2p_documents_emitted_total",
            "Documents emitted by processor stage.",
            ["namespace", "stage"],
            registry=self.registry,
        )
        self._processor_received = Counter(
            "s2p_processor_received_total",
            "Content-bearing Bronze documents selected for normalization.",
            ["namespace", "source"],
            registry=self.registry,
        )
        self._processor_ingested = Counter(
            "s2p_processor_ingested_total",
            "Bronze documents accepted by the processor fetcher.",
            ["namespace", "source"],
            registry=self.registry,
        )
        self._processor_curated = Counter(
            "s2p_processor_curated_total",
            "Trainable Gold documents emitted by the curator.",
            ["namespace", "source"],
            registry=self.registry,
        )
        self._processor_dropped = Counter(
            "s2p_processor_dropped_total",
            "Documents scored but rejected before Gold publication.",
            ["namespace", "reason"],
            registry=self.registry,
        )
        self._processor_failures = Counter(
            "s2p_processor_failures_total",
            "Records that failed processing or bounded transport validation.",
            ["namespace", "stage", "reason"],
            registry=self.registry,
        )
        self._processor_routed = Counter(
            "s2p_processor_routed_total",
            "Documents assigned to each final corpus route.",
            ["namespace", "route"],
            registry=self.registry,
        )
        self._quality_score = Histogram(
            "s2p_quality_score",
            "Explainable composite corpus-quality score distribution.",
            ["namespace"],
            buckets=QUALITY_BUCKETS,
            registry=self.registry,
        )
        self._edu_score = Histogram(
            "s2p_source_quality_score",
            "Source-specific ModernBERT diagnostic quality distribution.",
            ["namespace"],
            buckets=QUALITY_BUCKETS,
            registry=self.registry,
        )
        self._iceberg_flush_seconds = Histogram(
            "s2p_iceberg_flush_seconds",
            "Iceberg micro-batch flush duration.",
            ["namespace"],
            buckets=FLUSH_BUCKETS,
            registry=self.registry,
        )
        self._pdf_processing_seconds = Histogram(
            "s2p_pdf_processing_seconds",
            "End-to-end isolated PDF processing duration by outcome.",
            ["namespace", "outcome"],
            buckets=PDF_PROCESSING_BUCKETS,
            registry=self.registry,
        )
        self._pdf_worker_restarts = Counter(
            "s2p_pdf_worker_restarts_total",
            "Isolated PDF worker replacements after an unsafe outcome.",
            ["namespace", "reason"],
            registry=self.registry,
        )
        self._process_up.labels(self._namespace).inc()

    @property
    def namespace(self) -> str:
        return self._namespace

    def record_normalized(self, *, source_feed: str) -> None:
        with self._lock:
            self._processor_ingested.labels(self._namespace, source_feed).inc()
            self._documents_emitted.labels(self._namespace, "normalize").inc()

    def record_received(self, *, source_feed: str) -> None:
        with self._lock:
            self._processor_received.labels(self._namespace, source_feed).inc()

    def record_curated(
        self, *, source_feed: str, quality_score: float, edu_score: float | None = None
    ) -> None:
        with self._lock:
            self._processor_curated.labels(self._namespace, source_feed).inc()
            self._documents_emitted.labels(self._namespace, "curate").inc()
            self._quality_score.labels(self._namespace).observe(quality_score)
            if edu_score is not None:
                self._edu_score.labels(self._namespace).observe(edu_score)

    def record_dropped(
        self, *, reasons: Iterable[str], quality_score: float, edu_score: float | None = None
    ) -> None:
        reasons_list = list(reasons) or ["unknown"]
        with self._lock:
            for reason in reasons_list:
                self._processor_dropped.labels(self._namespace, reason).inc()
            self._quality_score.labels(self._namespace).observe(quality_score)
            if edu_score is not None:
                self._edu_score.labels(self._namespace).observe(edu_score)

    def record_route(self, *, route: str) -> None:
        with self._lock:
            self._processor_routed.labels(self._namespace, route).inc()

    def record_failure(self, *, stage: str, reason: str) -> None:
        with self._lock:
            self._processor_failures.labels(self._namespace, stage, reason).inc()

    def record_pdf_processing(self, *, outcome: str, seconds: float) -> None:
        with self._lock:
            self._pdf_processing_seconds.labels(self._namespace, outcome).observe(max(0.0, seconds))

    def record_pdf_worker_restart(self, *, reason: str) -> None:
        with self._lock:
            self._pdf_worker_restarts.labels(self._namespace, reason).inc()

    def record_iceberg_flush(
        self,
        *,
        rows: int,
        seconds: float,
        decisions: int | None = None,
    ) -> None:
        with self._lock:
            self._documents_emitted.labels(self._namespace, "iceberg").inc(max(0, rows))
            self._documents_emitted.labels(self._namespace, "decision").inc(
                max(0, rows if decisions is None else decisions)
            )
            self._iceberg_flush_seconds.labels(self._namespace).observe(max(0.0, seconds))

    def render_prometheus(self) -> bytes:
        body = generate_latest(self.registry)
        return (
            b"# HELP s2p_process_up Process-level liveness.\n"
            b"# TYPE s2p_process_up gauge\n"
            b"s2p_process_up 1\n"
        ) + body


PROCESSOR_METRICS = ProcessorMetrics()
