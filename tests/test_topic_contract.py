"""Cross-environment contracts for broker-visible control topics."""

from __future__ import annotations

from schemas.topics import (
    ALL_TOPICS,
    GITHUB_RELEASE_JOBS,
    dev_topic_configs,
    prod_topic_configs,
)


def test_github_release_jobs_topic_is_provisioned_in_every_profile() -> None:
    assert GITHUB_RELEASE_JOBS in ALL_TOPICS
    for configs in (dev_topic_configs(), prod_topic_configs()):
        matching = [config for config in configs if config.name == GITHUB_RELEASE_JOBS]
        assert len(matching) == 1
        assert matching[0].partitions > 1
