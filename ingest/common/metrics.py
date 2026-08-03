"""Small Prometheus text metrics for ingest workers."""

from __future__ import annotations

import os
import threading
from collections import defaultdict


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


class IngestMetrics:
    """Dependency-light metrics registry for pollers and fetchers."""

    def __init__(self, *, namespace: str | None = None) -> None:
        self._namespace = namespace or os.environ.get("S2P_NAMESPACE", "default")
        self._lock = threading.Lock()
        self._feed_poll_total: defaultdict[tuple[str, str], int] = defaultdict(int)

    def record_feed_poll(self, *, source_feed: str, outcome: str) -> None:
        with self._lock:
            self._feed_poll_total[(source_feed, outcome)] += 1

    def render_prometheus(self) -> bytes:
        lines = [
            "# HELP s2p_process_up Process-level liveness.",
            "# TYPE s2p_process_up gauge",
            "s2p_process_up 1",
            "# HELP s2p_feed_poll_total SourceFeed poll outcomes.",
            "# TYPE s2p_feed_poll_total counter",
        ]
        with self._lock:
            for (source_feed, outcome), value in sorted(self._feed_poll_total.items()):
                lines.append(
                    "s2p_feed_poll_total{"
                    f'namespace="{_escape(self._namespace)}",'
                    f'source_feed="{_escape(source_feed)}",'
                    f'outcome="{_escape(outcome)}"'
                    f"}} {value}"
                )
        return ("\n".join(lines) + "\n").encode("utf-8")


INGEST_METRICS = IngestMetrics()
