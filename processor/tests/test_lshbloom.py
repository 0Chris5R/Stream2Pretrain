"""Tests for :mod:`processor.operators.lshbloom`."""

from __future__ import annotations

from processor.operators.lshbloom import LSHBloomIndex
from processor.operators.minhash import MinHasher


def test_first_observation_is_not_dup() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature("alpha beta gamma delta")
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    res = idx.observe("sha256:" + "a" * 64, sig)
    assert res.is_near_duplicate is False
    assert res.cluster_id is not None


def test_identical_text_is_near_dup() -> None:
    h = MinHasher(num_perms=64)
    sig = h.signature(
        "the streaming pipeline curates documents into training shards "
        "deterministically without duplicates"
    )
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    first = idx.observe("sha256:" + "a" * 64, sig)
    second = idx.observe("sha256:" + "b" * 64, sig)
    assert first.is_near_duplicate is False
    assert second.is_near_duplicate is True
    assert second.cluster_id == first.cluster_id


def test_different_text_not_near_dup() -> None:
    h = MinHasher(num_perms=64)
    a = h.signature("alpha beta gamma delta epsilon zeta eta theta iota")
    b = h.signature("totally different vocabulary here apple orange banana mango")
    idx = LSHBloomIndex(num_bands=16, bits_per_band=1 << 14)
    res_a = idx.observe("sha256:" + "1" * 64, a)
    res_b = idx.observe("sha256:" + "2" * 64, b)
    assert res_a.is_near_duplicate is False
    assert res_b.is_near_duplicate is False
    assert res_a.cluster_id != res_b.cluster_id
