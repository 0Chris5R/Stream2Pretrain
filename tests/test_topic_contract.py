"""Cross-environment contracts for broker-visible control topics."""

from __future__ import annotations

from pathlib import Path

from schemas.topics import (
    ALL_TOPICS,
    ARXIV_DISCOVERY,
    CURATION_DECISIONS,
    CURATION_DECISIONS_SMOKE,
    DOCS_CURATED,
    DOCS_CURATED_SMOKE,
    DOCS_NORMALIZED,
    DOCS_NORMALIZED_SMOKE,
    FOUNDRY_ARTIFACTS,
    FOUNDRY_EVENTS,
    FOUNDRY_JOBS,
    LICENSE_ADMISSIONS,
    LICENSE_ADMISSIONS_SMOKE,
    RAW_FETCHED,
    RAW_SMOKE,
    dev_topic_configs,
    prod_topic_configs,
)


def test_arxiv_discovery_has_parallel_partitions_in_each_profile() -> None:
    assert ARXIV_DISCOVERY in ALL_TOPICS
    for configs in (dev_topic_configs(), prod_topic_configs()):
        by_name = {config.name: config for config in configs}
        assert by_name[ARXIV_DISCOVERY].partitions > 1


def test_every_distributed_writer_input_has_parallel_partitions() -> None:
    for configs in (dev_topic_configs(), prod_topic_configs()):
        by_name = {config.name: config for config in configs}
        for topic in (CURATION_DECISIONS, LICENSE_ADMISSIONS):
            assert by_name[topic].partitions > 1


def test_topic_reconciliation_fails_closed_on_replication_mismatch() -> None:
    script = (
        Path(__file__).resolve().parents[1] / "scripts" / "reconcile_topic_partitions.sh"
    ).read_text(encoding="utf-8")

    assert 'replication_factor="${S2P_TOPIC_REPLICATION_FACTOR:-3}"' in script
    assert "NR > 1 && $1 == topic {print $3; exit}" in script
    assert '[[ "$current_replication" -ne "$replication_factor" ]]' in script
    assert "No automatic live reassignment is attempted." in script
    for topic in (FOUNDRY_JOBS, FOUNDRY_EVENTS, FOUNDRY_ARTIFACTS):
        assert topic in script
    assert 'managed_topics=("${topics[@]}" "${foundry_topics[@]}")' in script
    assert 'rpk topic create "${missing_foundry[@]}"' in script
    assert "--partitions 1" in script


def test_foundry_topics_keep_one_partition_in_the_development_contract() -> None:
    by_name = {config.name: config for config in dev_topic_configs()}

    for topic in (FOUNDRY_JOBS, FOUNDRY_EVENTS, FOUNDRY_ARTIFACTS):
        assert by_name[topic].partitions == 1


def test_manual_cluster_topic_creation_preserves_parallel_streams() -> None:
    script = (Path(__file__).resolve().parents[1] / "scripts" / "setup_dhbw_demo.sh").read_text(
        encoding="utf-8"
    )

    assert 'core_partitions="${S2P_CORE_TOPIC_PARTITIONS:-4}"' in script
    assert "arxiv.discovery | raw.fetched | raw.smoke" in script
    assert "license.admissions | license.admissions.smoke" in script
    assert '--partitions "$partitions"' in script


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
