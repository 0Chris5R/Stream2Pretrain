from __future__ import annotations

from processor.common import ProcessorConfig
from processor.mixture_controller.controller import MixtureController, _sourcefeed_status
from schemas.sourcefeed import MixtureRecipeSpec


def test_sourcefeed_status_maps_kubernetes_crd_shape() -> None:
    item = {
        "metadata": {"name": "rss-arxiv-cs-cl"},
        "spec": {
            "protocol": "rss",
            "endpoint": "https://rss.arxiv.org/rss/cs.CL",
            "pollIntervalSeconds": 7200,
            "rateLimit": {"requestsPerSecond": 1.0, "burst": 4},
            "licenseDefault": "arxiv-non-exclusive-distribution",
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
