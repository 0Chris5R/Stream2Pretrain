from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

from processor.common import ProcessorConfig
from processor.mixture_controller.controller import (
    _BUILTIN_SOURCES,
    _builtin_source_status,
    _cron_schedule,
    _source_job_runtime,
    _sourcefeed_status,
    MixtureController,
)
from schemas.sourcefeed import MixtureRecipeSpec


def test_sourcefeed_status_maps_kubernetes_crd_shape() -> None:
    item = {
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

    status = _sourcefeed_status(item)

    assert status["name"] == "rss-arxiv-cs-cl"
    assert status["spec"]["poll_interval_seconds"] == 7200
    assert status["spec"]["rate_limit"]["requests_per_second"] == 1.0
    assert status["documents_24h"] == 12
    assert status["poll_state"] == "idle"


def test_sourcefeed_intervals_map_to_valid_cron_schedules() -> None:
    assert _cron_schedule(60) == "* * * * *"
    assert _cron_schedule(900) == "*/15 * * * *"
    assert _cron_schedule(7200) == "0 */2 * * *"
    assert _cron_schedule(86400) == "0 0 * * *"


def test_source_job_runtime_prefers_latest_attempt_and_retains_last_success() -> None:
    def job(
        *,
        started: datetime,
        active: int = 0,
        failed: int = 0,
        succeeded: int = 0,
        completed: datetime | None = None,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            metadata=SimpleNamespace(
                labels={"stream2pretrain.io/source-feed": "rss-bair-blog"},
                creation_timestamp=started,
            ),
            status=SimpleNamespace(
                start_time=started,
                completion_time=completed,
                active=active,
                failed=failed,
                succeeded=succeeded,
                conditions=[],
            ),
        )

    first = datetime(2026, 8, 22, 10, tzinfo=UTC)
    latest = datetime(2026, 8, 23, 10, tzinfo=UTC)
    runtime = _source_job_runtime(
        [
            job(started=first, succeeded=1, completed=first),
            job(started=latest, active=1),
        ]
    )["rss-bair-blog"]

    assert runtime["phase"] == "Polling"
    assert runtime["last_attempt_at"] == latest.isoformat()
    assert runtime["last_success_at"] == first.isoformat()


def test_sourcefeed_status_uses_observed_job_runtime() -> None:
    item = {
        "metadata": {"name": "rss-bair-blog"},
        "spec": {
            "protocol": "rss",
            "endpoint": "https://bair.berkeley.edu/blog/feed.xml",
            "pollIntervalSeconds": 86400,
            "rateLimit": {"requestsPerSecond": 1.0, "burst": 2},
            "licenseDefault": "per-record",
            "enabled": True,
        },
        "status": {"phase": "Active"},
    }

    status = _sourcefeed_status(
        item,
        runtime={
            "phase": "Failed",
            "last_attempt_at": "2026-08-23T10:00:00+00:00",
            "last_success_at": "2026-08-22T10:00:00+00:00",
            "last_error": "DeadlineExceeded",
        },
    )

    assert status["poll_state"] == "error"
    assert status["last_attempt_at"] == "2026-08-23T10:00:00+00:00"
    assert status["last_success_at"] == "2026-08-22T10:00:00+00:00"
    assert status["last_error"] == "DeadlineExceeded"


def test_builtin_inventory_includes_every_seed_component() -> None:
    names = {str(item["name"]) for item in _BUILTIN_SOURCES}

    assert {
        "seed:allenai/peS2o",
        "seed:togethercomputer/RedPajama-Data-1T",
        "seed:HuggingFaceFW/fineweb-edu",
        "seed:HuggingFaceTB/stack-edu",
        "seed:wayback",
    } <= names


def test_seed_builtin_reports_only_components_selected_by_the_job() -> None:
    job = SimpleNamespace(
        metadata=SimpleNamespace(
            labels={"app.kubernetes.io/component": "seed-loader"},
            annotations={"stream2pretrain.io/seed-components": "pes2o,stack-edu"},
            creation_timestamp=datetime(2026, 8, 23, 10, tzinfo=UTC),
        ),
        status=SimpleNamespace(active=0, succeeded=1, failed=0, completion_time=None),
    )
    pes2o = next(item for item in _BUILTIN_SOURCES if item["name"] == "seed:allenai/peS2o")
    fineweb = next(
        item for item in _BUILTIN_SOURCES if item["name"] == "seed:HuggingFaceFW/fineweb-edu"
    )

    assert _builtin_source_status(pes2o, deployments={}, cronjobs={}, jobs=[job])["spec"][
        "enabled"
    ]
    assert not _builtin_source_status(fineweb, deployments={}, cronjobs={}, jobs=[job])["spec"][
        "enabled"
    ]


def test_mixture_compare_returns_ui_payload(cfg: ProcessorConfig) -> None:
    controller = MixtureController(cfg)
    controller.upsert_recipe(
        MixtureRecipeSpec.model_validate(
            {
                "name": "main",
                "branch": "main",
                "sources": [{"sourceFeed": "rss-arxiv-cs-cl", "weight": 1.0}],
            }
        )
    )

    payload = controller.compare("main", "shadow")

    assert payload == {
        "recipe_a": "main",
        "recipe_b": "shadow",
        "perplexity_delta": [],
        "tokens_per_hour_a": 0.0,
        "tokens_per_hour_b": 0.0,
    }
