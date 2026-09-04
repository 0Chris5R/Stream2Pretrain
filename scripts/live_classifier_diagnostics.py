"""Read a bounded broker tail without committing offsets or invoking classifiers.

Run in the cloud curator image. The examples are live production decisions,
not canaries; the bounded tail is not claimed as a representative corpus sample.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition


def main() -> None:
    topic = os.environ.get("S2P_DECISIONS_TOPIC", "curation.decisions")
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["REDPANDA_BROKERS"],
            "group.id": f"classifier-read-only-audit-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    rows = {}
    seen = 0
    try:
        metadata = consumer.list_topics(topic, timeout=10).topics[topic]
        ends = {}
        assignments = []
        for partition in metadata.partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high > low:
                ends[partition] = high
                assignments.append(TopicPartition(topic, partition, max(low, high - 40)))
        consumer.assign(assignments)
        deadline = time.monotonic() + 45
        while ends and time.monotonic() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            partition = message.partition()
            if partition not in ends:
                continue
            if message.offset() + 1 >= ends[partition]:
                ends.pop(partition)
                consumer.pause([TopicPartition(topic, partition)])
            seen += 1
            row = json.loads(message.value())
            diagnostics = row.get("quality_diagnostics") or {}
            if not str(diagnostics.get("bundle_revision", "")).startswith(
                "source-modernbert-2026-09-04@"
            ):
                continue
            rows[row["doc_id"]] = row
    finally:
        consumer.close()
    selected = []
    counts = Counter()
    for row in rows.values():
        source = row["source_feed"]
        counts[source] += 1
        if sum(item["source_feed"] == source for item in selected) < 3:
            selected.append(
                {
                    key: row.get(key)
                    for key in (
                        "doc_id",
                        "source_feed",
                        "valid_from",
                        "text",
                        "route",
                        "eligible_routes",
                        "reject_reasons",
                        "quality_diagnostics",
                        "scientific_artifact_s3_uri",
                    )
                }
            )
    print(
        json.dumps(
            {
                "observed_at": time.time(),
                "tail_messages": seen,
                "unfinished_partitions": sorted(ends),
                "unique_four_head_decisions": dict(counts),
                "examples": selected,
            }
        )
    )


if __name__ == "__main__":
    main()
