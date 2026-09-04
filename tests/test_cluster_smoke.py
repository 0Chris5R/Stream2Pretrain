"""Regression tests for the live deployment canary document."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from processor.operators.minhash import shingle
from scripts import cluster_smoke
from scripts.cluster_smoke import assert_document_absent, canary_body, prepare_curator_offsets


def test_canary_body_is_natural_varied_prose() -> None:
    first = canary_body("first-probe")
    second = canary_body("second-probe")

    assert first != second
    assert 180 <= len(first.split()) <= 320
    assert "first-probe" not in first

    first_shingles = set(shingle(first))
    second_shingles = set(shingle(second))
    similarity = len(first_shingles & second_shingles) / len(first_shingles | second_shingles)
    assert similarity < 0.85


class _Message:
    def __init__(self, doc_id: str) -> None:
        self._value = json.dumps({"doc_id": doc_id}).encode()

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return self._value


class _Consumer:
    def __init__(self, doc_id: str | None = None) -> None:
        self._message = _Message(doc_id) if doc_id is not None else None
        self.closed = False

    def poll(self, _timeout: float) -> _Message | None:
        message, self._message = self._message, None
        return message

    def close(self) -> None:
        self.closed = True


def test_production_leak_check_matches_exact_document() -> None:
    decision_consumer = _Consumer("sha256:target")
    curated_consumer = _Consumer("sha256:other")

    with pytest.raises(RuntimeError, match=r"leaked into production topic curation\.decisions"):
        assert_document_absent(
            {
                "curation.decisions": decision_consumer,  # type: ignore[dict-item]
                "docs.curated": curated_consumer,  # type: ignore[dict-item]
            },
            "sha256:target",
            0.01,
        )

    assert decision_consumer.closed
    assert curated_consumer.closed


def test_canary_captures_each_partition_before_worker_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    class FrontierConsumer:
        def __init__(self, config: dict) -> None:
            captured["config"] = config

        def list_topics(self, topic: str, **_kwargs: object) -> object:
            return SimpleNamespace(
                topics={topic: SimpleNamespace(error=None, partitions={0: {}, 1: {}})}
            )

        def get_watermark_offsets(self, partition: object, **_kwargs: object) -> tuple[int, int]:
            return (0, {0: 137, 1: 19}[partition.partition])

        def assign(self, offsets: list) -> None:
            captured["assigned"] = [(p.topic, p.partition, p.offset) for p in offsets]

        def commit(self, *, offsets: list, asynchronous: bool) -> list:
            assert asynchronous is False
            captured["committed"] = [(p.topic, p.partition, p.offset) for p in offsets]
            return offsets

        def close(self) -> None:
            captured["closed"] = True

    monkeypatch.setenv("REDPANDA_BROKERS", "test:9092")
    monkeypatch.setenv("S2P_SMOKE_NORMALIZED_TOPIC", "docs.normalized.smoke")
    monkeypatch.setattr(cluster_smoke, "Consumer", FrontierConsumer)
    assert prepare_curator_offsets("s2p-curate-smoke-123-2") == {"0": 137, "1": 19}
    assert (
        captured["assigned"]
        == captured["committed"]
        == [("docs.normalized.smoke", 0, 137), ("docs.normalized.smoke", 1, 19)]
    )
    assert captured["config"]["enable.auto.commit"] is False
    assert captured["closed"] is True


def test_canary_frontiers_never_modify_production_group_or_topic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(ValueError, match="canary group"):
        prepare_curator_offsets("s2p-curate")
    monkeypatch.setenv("S2P_SMOKE_NORMALIZED_TOPIC", "docs.normalized")
    with pytest.raises(ValueError, match="isolated smoke topic"):
        prepare_curator_offsets("s2p-curate-smoke-123")
