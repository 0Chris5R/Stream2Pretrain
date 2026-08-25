"""Tests for the durable one-time Bytewax cutover marker."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.bytewax_cutover_marker import ensure_marker, expected_marker, marker_path


def _environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "stored")
    monkeypatch.setenv("S2P_CUTOVER_COMPONENT", "fetcher")
    monkeypatch.setenv("S2P_CUTOVER_TOPIC", "raw.fetched")
    monkeypatch.setenv("S2P_CONSUMER_GROUP", "s2p-fetcher")
    monkeypatch.setenv("S2P_BYTEWAX_FLOW_NAME", "s2p-fetcher-live-v5")
    monkeypatch.setenv("S2P_BYTEWAX_RECOVERY_NAME", "fetcher-live-v5")
    monkeypatch.setenv("S2P_BYTEWAX_RECOVERY_PARTITIONS", "4")


def test_marker_is_idempotent_and_identity_bound(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _environment(monkeypatch)
    expected = expected_marker()
    path = marker_path(tmp_path, "fetcher")

    first = ensure_marker(path, expected)
    second = ensure_marker(path, expected)

    assert first == second
    assert json.loads(path.read_text())["starting_offset"] == "stored"


def test_marker_fails_closed_on_identity_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _environment(monkeypatch)
    expected = expected_marker()
    path = marker_path(tmp_path, "fetcher")
    ensure_marker(path, expected)

    changed = {**expected, "flow_name": "s2p-fetcher-live-v6"}
    with pytest.raises(RuntimeError, match="does not match"):
        ensure_marker(path, changed)


def test_marker_rejects_non_stored_cutover(monkeypatch: pytest.MonkeyPatch) -> None:
    _environment(monkeypatch)
    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "beginning")

    with pytest.raises(RuntimeError, match="requires S2P_KAFKA_START_OFFSET=stored"):
        expected_marker()
