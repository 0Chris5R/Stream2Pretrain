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

from schemas.gold import GoldRecord


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
    validation_errors = Counter()
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
            try:
                GoldRecord.model_validate(row)
            except ValueError as exc:
                for error in exc.errors(include_input=False, include_url=False):
                    validation_errors[str(error["loc"]) + ":" + error["type"]] += 1
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
    ordered = sorted(
        rows.values(),
        key=lambda row: (
            (row.get("quality_diagnostics") or {}).get("mode") == "active",
            row.get("trace_id", ""),
        ),
        reverse=True,
    )
    active = []
    for row in ordered:
        source = row["source_feed"]
        counts[source] += 1
        report = row.get("quality_diagnostics") or {}
        if report.get("mode") == "active":
            active.append(
                {
                    "doc_id": row["doc_id"],
                    "source": source,
                    "score": report.get("score"),
                    "cutoff": report.get("cutoff"),
                    "passed": report.get("passed"),
                    "route": row.get("route"),
                    "eligible_routes": row.get("eligible_routes"),
                    "reject_reasons": row.get("reject_reasons"),
                    "evidence": row.get("scientific_artifact_s3_uri"),
                    "sections": len(report.get("sections", [])),
                    "heads": list(report.get("classifiers", {})),
                }
            )
        if sum(item["source_feed"] == source for item in selected) < 3:
            selected.append(
                {"title": str(row.get("text", "")).splitlines()[0][:200] if row.get("text") else ""}
                | {
                    key: row.get(key)
                    for key in (
                        "doc_id",
                        "source_feed",
                        "valid_from",
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
                "validation_errors": dict(validation_errors),
                "active_decisions": active,
                "examples": selected,
            }
        )
    )


if __name__ == "__main__":
    main()
