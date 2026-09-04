"""Read-only counters and broker frontiers for matched cloud snapshots."""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from datetime import UTC, datetime

from confluent_kafka import Consumer, TopicPartition


def main() -> None:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    report: dict[str, object] = {"measured_at": datetime.now(UTC).isoformat()}

    def fetch(url: str) -> object:
        try:
            with opener.open(url, timeout=60) as response:
                return json.load(response)
        except Exception as exc:
            return {"error": str(exc)}

    report["overview"] = fetch("http://[::1]:8090/corpus-overview")
    report["source_activity"] = fetch("http://[::1]:8090/source-activity?window_hours=24")
    prometheus = os.environ.get(
        "PROMETHEUS_URL", "http://kps-prometheus.monitoring.svc.cluster.local:9090"
    ).rstrip("/")
    queries = {
        "counters": '{__name__=~"s2p_.*(total|seconds_sum|seconds_count)",namespace="stream2pretrain"}',
        "cpu_5m": 'sum by(pod,container)(rate(container_cpu_usage_seconds_total{namespace="stream2pretrain",container!="",container!="POD"}[5m]))',
        "memory": 'container_memory_working_set_bytes{namespace="stream2pretrain",container!="",container!="POD"}',
        "throttling_5m": 'sum by(pod,container)(rate(container_cpu_cfs_throttled_seconds_total{namespace="stream2pretrain"}[5m]))',
        "restarts": 'kube_pod_container_status_restarts_total{namespace="stream2pretrain"}',
        "head_seconds_1h": "sum by(task)(increase(s2p_classifier_head_seconds_sum[1h]))",
        "head_windows_1h": "sum by(task)(increase(s2p_classifier_head_windows_total[1h]))",
        "head_tokens_1h": "sum by(task)(increase(s2p_classifier_head_tokens_total[1h]))",
    }
    report["prometheus"] = {
        name: fetch(prometheus + "/api/v1/query?" + urllib.parse.urlencode({"query": query}))
        for name, query in queries.items()
    }
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
            "group.id": "submission-read-only-frontiers",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    frontiers = {}
    try:
        for topic in (
            "raw.fetched",
            "docs.normalized",
            "curation.decisions",
            "docs.curated",
        ):
            metadata = consumer.list_topics(topic, timeout=15).topics[topic]
            frontiers[topic] = {
                str(partition): consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=15
                )
                for partition in metadata.partitions
            }
    finally:
        consumer.close()
    report["broker_frontiers"] = frontiers
    report["completed_at"] = datetime.now(UTC).isoformat()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
