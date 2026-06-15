"""Tests for :mod:`processor.operators.minhash`."""

from __future__ import annotations

from processor.operators.minhash import MinHasher, shingle


def test_shingle_basic() -> None:
    out = shingle("the quick brown fox jumps over", n=3)
    assert "the quick brown" in out
    assert "quick brown fox" in out


def test_signature_is_deterministic() -> None:
    h = MinHasher(num_perms=64)
    a = h.signature("the quick brown fox jumps over the lazy dog")
    b = h.signature("the quick brown fox jumps over the lazy dog")
    assert a.digest == b.digest
    assert a.num_perms == 64
    assert len(a.digest) == 64 * 4


def test_signature_diverges_for_different_text() -> None:
    h = MinHasher(num_perms=64)
    a = h.signature("the quick brown fox jumps over the lazy dog")
    b = h.signature("entirely different text about apples and oranges")
    assert a.digest != b.digest


def test_band_keys_partition_evenly() -> None:
    h = MinHasher(num_perms=112)
    sig = h.signature("alpha beta gamma delta epsilon zeta eta theta iota")
    bands = sig.band_keys(28)
    assert len(bands) == 28
    assert all(len(b) == (112 // 28) * 4 for b in bands)
