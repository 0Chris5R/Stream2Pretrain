"""Cross-environment contracts for broker-visible control topics."""

from __future__ import annotations

from schemas.topics import (
    ALL_TOPICS,
    CURATION_DECISIONS,
    CURATION_DECISIONS_SMOKE,
    DOCS_CURATED,
    DOCS_CURATED_SMOKE,
    DOCS_NORMALIZED,
    DOCS_NORMALIZED_SMOKE,
    GITHUB_RELEASE_JOBS,
    LICENSE_ADMISSIONS,
    LICENSE_ADMISSIONS_SMOKE,
    RAW_FETCHED,
    RAW_SMOKE,
    dev_topic_configs,
    prod_topic_configs,
)


def test_github_release_jobs_topic_is_provisioned_in_every_profile() -> None:
    assert GITHUB_RELEASE_JOBS in ALL_TOPICS
    for configs in (dev_topic_configs(), prod_topic_configs()):
        matching = [config for config in configs if config.name == GITHUB_RELEASE_JOBS]
        assert len(matching) == 1
        assert matching[0].partitions > 1


def test_smoke_lane_is_complete_short_lived_and_distinct() -> None:
    pairs = (
        (RAW_FETCHED, RAW_SMOKE),
        (DOCS_NORMALIZED, DOCS_NORMALIZED_SMOKE),
        (CURATION_DECISIONS, CURATION_DECISIONS_SMOKE),
        (DOCS_CURATED, DOCS_CURATED_SMOKE),
        (LICENSE_ADMISSIONS, LICENSE_ADMISSIONS_SMOKE),
    )
    for production, smoke in pairs:
        assert production in ALL_TOPICS
        assert smoke in ALL_TOPICS
        assert production != smoke
    for configs in (dev_topic_configs(), prod_topic_configs()):
        by_name = {config.name: config for config in configs}
        smoke_retentions = {by_name[smoke].retention_ms for _, smoke in pairs}
        assert len(smoke_retentions) == 1
        for production, smoke in pairs:
            assert by_name[smoke].retention_ms < by_name[production].retention_ms
