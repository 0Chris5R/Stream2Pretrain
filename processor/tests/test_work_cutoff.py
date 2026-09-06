"""Rolling intake-window tests independent of source publication time."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from processor.metrics import ProcessorMetrics
from processor.work_cutoff import WorkCutoff

NOW = datetime(2026, 9, 4, 12, tzinfo=UTC)


def test_work_is_eligible_until_exact_twenty_four_hour_boundary() -> None:
    cutoff = WorkCutoff(clock=lambda: NOW)
    assert not cutoff.expired(
        NOW - timedelta(hours=24) + timedelta(microseconds=1),
        stage="normalize",
        source_feed="arxiv-html-fetcher",
    )
    assert cutoff.expired(
        NOW - timedelta(hours=24),
        stage="normalize",
        source_feed="arxiv-html-fetcher",
    )


def test_missing_timestamp_expires_closed_and_is_observable() -> None:
    metrics = ProcessorMetrics(namespace="test")
    cutoff = WorkCutoff(clock=lambda: NOW)
    assert cutoff.expired(None, stage="curate", source_feed="legacy", metrics=metrics)
    body = metrics.render_prometheus().decode()
    assert (
        's2p_processor_work_expired_total{namespace="test",reason="missing_intake_timestamp",source="legacy",stage="curate"} 1.0'
        in body
    )


def test_work_cutoff_requires_positive_duration() -> None:
    with pytest.raises(ValueError, match="must be positive"):
        WorkCutoff(max_age_seconds=0)


def test_work_cutoff_reads_deployment_setting(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2P_PRETRAIN_MAX_WORK_AGE_SECONDS", "3600")
    assert WorkCutoff.from_env().max_age_seconds == 3600
