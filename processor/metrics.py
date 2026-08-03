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
        self._quality_score = Histogram(
            "s2p_quality_score",
            "FineWeb-Edu quality score distribution.",
            ["namespace"],
            buckets=QUALITY_BUCKETS,
            registry=self.registry,
        )
        self._decon_checked = Counter(
            "s2p_decon_checked_total",
            "Documents checked by Decon-Gate.",
            ["namespace"],
            registry=self.registry,
        )
        self._decon_flagged = Counter(
            "s2p_decon_flagged_total",
            "Decon-Gate benchmark hits.",
            ["namespace", "benchmark"],
            registry=self.registry,
        )
        self._iceberg_flush_seconds = Histogram(
            "s2p_iceberg_flush_seconds",
            "Iceberg micro-batch flush duration.",
            ["namespace"],
            buckets=FLUSH_BUCKETS,
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

    def record_curated(self, *, source_feed: str, quality_score: float) -> None:
        with self._lock:
            self._processor_curated.labels(self._namespace, source_feed).inc()
            self._documents_emitted.labels(self._namespace, "curate").inc()
            self._quality_score.labels(self._namespace).observe(quality_score)

    def record_dropped(self, *, reasons: Iterable[str], quality_score: float) -> None:
        reasons_list = list(reasons) or ["unknown"]
        with self._lock:
            for reason in reasons_list:
                self._processor_dropped.labels(self._namespace, reason).inc()
            self._quality_score.labels(self._namespace).observe(quality_score)

    def record_decon_scan(self, *, benchmarks: Iterable[str]) -> None:
        hits = list(benchmarks)
        with self._lock:
            self._decon_checked.labels(self._namespace).inc()
            for benchmark in hits:
                self._decon_flagged.labels(self._namespace, benchmark).inc()

    def record_iceberg_flush(self, *, rows: int, seconds: float) -> None:
        with self._lock:
            self._documents_emitted.labels(self._namespace, "iceberg").inc(max(0, rows))
            self._iceberg_flush_seconds.labels(self._namespace).observe(max(0.0, seconds))

    def render_prometheus(self) -> bytes:
        body = generate_latest(self.registry)
        return (
            b"# HELP s2p_process_up Process-level liveness.\n"
            b"# TYPE s2p_process_up gauge\n"
            b"s2p_process_up 1\n"
        ) + body


PROCESSOR_METRICS = ProcessorMetrics()
