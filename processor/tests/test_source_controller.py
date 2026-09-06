from __future__ import annotations

from processor.source_controller import (
    _ARXIV_DISCOVERY_SOURCE_ORDER,
    _BUILTIN_SOURCES,
    _cron_schedule,
    _sourcefeed_status,
)


def test_sourcefeed_status_maps_kubernetes_crd_shape() -> None:
    status = _sourcefeed_status(
        {
            "metadata": {"name": "rss-arxiv-cs-cl"},
            "spec": {
                "protocol": "rss",
                "endpoint": "https://rss.arxiv.org/rss/cs.CL",
                "pollIntervalSeconds": 7200,
                "rateLimit": {"requestsPerSecond": 1.0, "burst": 4},
                "licenseDefault": "per-record",
                "enabled": True,
            },
            "status": {
                "phase": "Active",
                "lastPolledAt": "2026-06-17T10:00:00Z",
                "lastSuccessAt": "2026-06-17T10:00:01Z",
                "docsEmittedTotal": 12,
            },
        }
    )

    assert status["name"] == "rss-arxiv-cs-cl"
    assert status["spec"]["poll_interval_seconds"] == 7200
    assert status["documents_24h"] == 12
    assert status["poll_state"] == "idle"


def test_sourcefeed_intervals_and_arxiv_staggering() -> None:
    assert _cron_schedule(60) == "* * * * *"
    assert _cron_schedule(900) == "*/15 * * * *"
    assert _cron_schedule(86400) == "0 0 * * *"
    schedules = {
        name: _cron_schedule(7200, source_name=name) for name in _ARXIV_DISCOVERY_SOURCE_ORDER
    }
    assert len(set(schedules.values())) == len(_ARXIV_DISCOVERY_SOURCE_ORDER)


def test_builtin_inventory_is_only_training_content_sources() -> None:
    assert {source["name"] for source in _BUILTIN_SOURCES} == {
        "arxiv-html-fetcher",
        "hf-models",
        "hf-datasets",
    }
