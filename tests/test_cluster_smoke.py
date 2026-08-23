"""Regression tests for the live deployment canary document."""

from __future__ import annotations

from processor.operators.minhash import shingle
from scripts.cluster_smoke import canary_body


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
