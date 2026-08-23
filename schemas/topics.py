"""Redpanda topic catalogue.

These are the canonical topics every Stream2Pretrain component reads or
writes. The constants are imported across ingest, processor, and decon-gate so
typos surface at import time, not at runtime in the field.

Partition / replication factors are split between dev and prod profiles.
The dev profile (1/1) is what the local docker-compose stack and a single-
broker k3s cluster can actually serve; the prod profile (12/3) is the target
for a full 3-broker Redpanda cluster on DHBWCloud (needs-measurement: pick
the partition count after the Week 5 throughput benchmark).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

# Topic name constants. Kept as module-level finals so they appear verbatim in
# k8s manifests, rpk scripts, and OpenTelemetry span attributes.
RAW_FETCHED: Final[str] = "raw.fetched"
RAW_SMOKE: Final[str] = "raw.smoke"
DOCS_NORMALIZED: Final[str] = "docs.normalized"
DOCS_NORMALIZED_SMOKE: Final[str] = "docs.normalized.smoke"
DOCS_CURATED: Final[str] = "docs.curated"
DECON_ATTEST: Final[str] = "decon.attest"
CURATION_DECISIONS: Final[str] = "curation.decisions"
LICENSE_ADMISSIONS: Final[str] = "license.admissions"
FOUNDRY_JOBS: Final[str] = "foundry.jobs"
FOUNDRY_EVENTS: Final[str] = "foundry.events"
FOUNDRY_ARTIFACTS: Final[str] = "foundry.artifacts"

# v0.2.0 deliberately does NOT add a ``docs.code`` topic. Per-file code
# records produced by ``ingest/github_release_tarball_fetcher`` ride the same
# ``raw.fetched`` topic and carry ``source_format='code'`` on the BronzeRecord;
# Silver/Gold equivalents likewise ride ``docs.normalized`` / ``docs.curated``.
# Downstream operators dispatch on the ``source_format`` column. This keeps
# the shared document topics stable. ``curation.decisions`` is an audit stream,
# not another document-format stream: every accepted or rejected score result
# is published there so attestations and quarantine storage are complete.
CODE_SOURCE_FORMAT: Final[str] = "code"

ALL_TOPICS: Final[tuple[str, ...]] = (
    RAW_FETCHED,
    RAW_SMOKE,
    DOCS_NORMALIZED,
    DOCS_NORMALIZED_SMOKE,
    DOCS_CURATED,
    CURATION_DECISIONS,
    LICENSE_ADMISSIONS,
    DECON_ATTEST,
    FOUNDRY_JOBS,
    FOUNDRY_EVENTS,
    FOUNDRY_ARTIFACTS,
)


@dataclass(frozen=True, slots=True)
class TopicConfig:
    """rpk-friendly topic provisioning record."""

    name: str
    partitions: int
    replication_factor: int
    retention_ms: int
    cleanup_policy: str = "delete"

    def rpk_args(self) -> list[str]:
        """Render the rpk topic create flags for this topic."""
        return [
            "topic",
            "create",
            self.name,
            "--partitions",
            str(self.partitions),
            "--replicas",
            str(self.replication_factor),
            "--config",
            f"retention.ms={self.retention_ms}",
            "--config",
            f"cleanup.policy={self.cleanup_policy}",
        ]


# Dev profile: single-broker Redpanda, light retention, easy to wipe.
# 7-day retention is enough to replay a full demo cycle.
_DEV_RETENTION_MS = 7 * 24 * 60 * 60 * 1000
_SMOKE_RETENTION_MS = 24 * 60 * 60 * 1000

# Prod profile: 3-broker target, longer retention so contamination bisect can
# replay weeks of history. The decon.attest topic is "compact + tombstone-free"
# in spirit; we keep delete so old certificates can age out alongside their
# Iceberg snapshots, but with a long retention.
_PROD_RETENTION_MS = 30 * 24 * 60 * 60 * 1000


def dev_topic_configs() -> list[TopicConfig]:
    """Topic configs for the local dev stack and small k3s clusters."""
    return [
        TopicConfig(
            RAW_FETCHED, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            RAW_SMOKE, partitions=1, replication_factor=1, retention_ms=_SMOKE_RETENTION_MS
        ),
        TopicConfig(
            DOCS_NORMALIZED, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            DOCS_NORMALIZED_SMOKE,
            partitions=1,
            replication_factor=1,
            retention_ms=_SMOKE_RETENTION_MS,
        ),
        TopicConfig(
            DOCS_CURATED, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            CURATION_DECISIONS, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            LICENSE_ADMISSIONS, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            DECON_ATTEST, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_JOBS, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_EVENTS, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_ARTIFACTS, partitions=1, replication_factor=1, retention_ms=_DEV_RETENTION_MS
        ),
    ]


def prod_topic_configs() -> list[TopicConfig]:
    """Topic configs for a 3-broker prod Redpanda cluster.

    Partition counts are conservative defaults; revisit after the Week 5
    throughput benchmark (needs-measurement).
    """
    return [
        TopicConfig(
            RAW_FETCHED, partitions=12, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            RAW_SMOKE, partitions=3, replication_factor=3, retention_ms=_SMOKE_RETENTION_MS
        ),
        TopicConfig(
            DOCS_NORMALIZED, partitions=12, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            DOCS_NORMALIZED_SMOKE,
            partitions=3,
            replication_factor=3,
            retention_ms=_SMOKE_RETENTION_MS,
        ),
        TopicConfig(
            DOCS_CURATED, partitions=12, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            CURATION_DECISIONS, partitions=12, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            LICENSE_ADMISSIONS, partitions=6, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            DECON_ATTEST, partitions=3, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_JOBS, partitions=6, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_EVENTS, partitions=6, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
        TopicConfig(
            FOUNDRY_ARTIFACTS, partitions=6, replication_factor=3, retention_ms=_PROD_RETENTION_MS
        ),
    ]
