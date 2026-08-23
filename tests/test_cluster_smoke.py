"""Regression tests for the live deployment canary document."""

from __future__ import annotations

import json

import pytest

from processor.operators.minhash import shingle
from scripts.cluster_smoke import assert_document_absent, canary_body


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

    with pytest.raises(RuntimeError, match="leaked into production topic curation.decisions"):
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
