from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from processor.common import ProcessorConfig
from processor.mixture_controller.controller import (
    _ARXIV_DISCOVERY_SOURCE_ORDER,
    _BUILTIN_SOURCES,
    _SOURCE_TEMPLATE_HASH_ANNOTATION,
    MixtureController,
    _cleanup_obsolete_active_source_jobs,
    _cron_schedule,
    _reconcile_source_schedule,
    _source_egress_class,
    _source_job_runtime,
    _sourcefeed_status,
)
from schemas.sourcefeed import MixtureRecipeSpec, SourceFeedSpec


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


def test_arxiv_discovery_schedules_are_evenly_staggered() -> None:
    assert {
        name: _cron_schedule(7200, source_name=name) for name in _ARXIV_DISCOVERY_SOURCE_ORDER
    } == {
        "oai-arxiv-cs": "13 */2 * * *",
        "rss-arxiv-cs-cl": "37 */2 * * *",
        "rss-arxiv-cs-lg": "1 1-23/2 * * *",
        "rss-arxiv-cs-ai": "25 1-23/2 * * *",
        "rss-arxiv-cs-cv": "49 1-23/2 * * *",
    }


def test_source_schedule_replacement_clears_obsolete_pod_fields_and_keeps_s3_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from kubernetes import client
    from kubernetes.client import ApiException

    base_job_template = client.V1JobTemplateSpec(
        spec=client.V1JobSpec(
            template=client.V1PodTemplateSpec(
                metadata=client.V1ObjectMeta(labels={"app": "rss"}),
                spec=client.V1PodSpec(
                    restart_policy="OnFailure",
                    containers=[
                        client.V1Container(
                            name="rss",
                            args=["--config", "/etc/s2p/feeds/arxiv.json"],
                            env=[
                                client.V1EnvVar(name="S2P_COMPONENT", value="ingest-rss"),
                                client.V1EnvVar(name="S2P_STATE_BACKEND", value="s3"),
                                client.V1EnvVar(name="S2P_STATE_BUCKET", value="state"),
                                client.V1EnvVar(name="S2P_STATE_PREFIX", value="ingest-cursors"),
                            ],
                            volume_mounts=[
                                client.V1VolumeMount(name="feeds", mount_path="/etc/s2p/feeds")
                            ],
                        )
                    ],
                    volumes=[
                        client.V1Volume(
                            name="feeds",
                            config_map=client.V1ConfigMapVolumeSource(name="base-feeds"),
                        )
                    ],
                ),
            )
        )
    )
    base = client.V1CronJob(
        metadata=client.V1ObjectMeta(name="base-rss"),
        spec=client.V1CronJobSpec(
            schedule="27 */2 * * *", job_template=base_job_template, suspend=True
        ),
    )
    obsolete = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name="s2p-feed-rss-arxiv-cs-cl",
            resource_version="42",
            uid="cron-uid",
        ),
        spec=client.V1CronJobSpec(
            schedule="0 */2 * * *",
            job_template=client.V1JobTemplateSpec(
                spec=client.V1JobSpec(
                    template=client.V1PodTemplateSpec(
                        spec=client.V1PodSpec(
                            restart_policy="OnFailure",
                            node_selector={"kubernetes.io/hostname": "obsolete-worker"},
                            affinity=client.V1Affinity(node_affinity=client.V1NodeAffinity()),
                            containers=[
                                client.V1Container(
                                    name="rss",
                                    volume_mounts=[
                                        client.V1VolumeMount(
                                            name="state", mount_path="/var/lib/s2p-state"
                                        )
                                    ],
                                )
                            ],
                            volumes=[
                                client.V1Volume(
                                    name="state",
                                    persistent_volume_claim=(
                                        client.V1PersistentVolumeClaimVolumeSource(
                                            claim_name="obsolete-state"
                                        )
                                    ),
                                )
                            ],
                        )
                    )
                )
            ),
        ),
    )

    class FakeBatchApi:
        replaced: client.V1CronJob | None = None
        cleanup_listed = False

        def read_namespaced_cron_job(self, name: str, namespace: str) -> client.V1CronJob:
            assert namespace == "stream2pretrain"
            return base if name == "base-rss" else obsolete

        def create_namespaced_cron_job(self, namespace: str, body: client.V1CronJob) -> None:
            raise ApiException(status=409)

        def replace_namespaced_cron_job(
            self, name: str, namespace: str, body: client.V1CronJob
        ) -> None:
            assert name == "s2p-feed-rss-arxiv-cs-cl"
            assert namespace == "stream2pretrain"
            self.replaced = body

        def list_namespaced_job(self, namespace: str) -> SimpleNamespace:
            assert namespace == "stream2pretrain"
            self.cleanup_listed = True
            return SimpleNamespace(items=[])

    class FakeCoreApi:
        def create_namespaced_config_map(self, namespace: str, body: client.V1ConfigMap) -> None:
            assert namespace == "stream2pretrain"

    batch_api = FakeBatchApi()
    monkeypatch.setattr(client, "BatchV1Api", lambda: batch_api)
    monkeypatch.setattr(client, "CoreV1Api", FakeCoreApi)
    monkeypatch.setenv("S2P_RSS_CRONJOB", "base-rss")

    source = SourceFeedSpec.model_validate(
        {
            "name": "rss-arxiv-cs-cl",
            "protocol": "rss",
            "endpoint": "https://rss.arxiv.org/rss/cs.CL",
            "pollIntervalSeconds": 7200,
            "rateLimit": {"requestsPerSecond": 1.0, "burst": 4},
            "licenseDefault": "per-record",
            "enabled": True,
        }
    )
    _reconcile_source_schedule(source, namespace="stream2pretrain", owner_uid="source-uid")

    assert batch_api.replaced is not None
    assert batch_api.replaced.metadata.resource_version == "42"
    assert batch_api.replaced.spec.schedule == "37 */2 * * *"
    template_hash = batch_api.replaced.metadata.annotations[_SOURCE_TEMPLATE_HASH_ANNOTATION]
    assert len(template_hash) == 64
    assert batch_api.cleanup_listed is True
    assert (
        batch_api.replaced.spec.job_template.metadata.annotations[_SOURCE_TEMPLATE_HASH_ANNOTATION]
        == template_hash
    )
    pod_spec = batch_api.replaced.spec.job_template.spec.template.spec
    assert (
        batch_api.replaced.spec.job_template.spec.template.metadata.annotations[
            _SOURCE_TEMPLATE_HASH_ANNOTATION
        ]
        == template_hash
    )
    assert pod_spec.node_selector is None
    assert pod_spec.affinity is None
    assert [volume.name for volume in pod_spec.volumes] == ["feeds"]
    assert [mount.name for mount in pod_spec.containers[0].volume_mounts] == ["feeds"]
    env = {item.name: item.value for item in pod_spec.containers[0].env}
    assert env["S2P_COMPONENT"] == "ingest-rss"
    assert env["S2P_STATE_BACKEND"] == "s3"
    assert env["S2P_STATE_BUCKET"] == "state"
    assert env["S2P_STATE_PREFIX"] == "ingest-cursors"
    assert env["S2P_FEED_CONFIG"] == "/etc/s2p/feeds/source.json"


def _owned_job(
    name: str,
    *,
    owner_name: str = "s2p-feed-rss-arxiv-cs-cl",
    owner_uid: str = "cron-uid",
    active: int = 1,
    template_hash: str | None = None,
    completed: bool = False,
) -> SimpleNamespace:
    annotations = {_SOURCE_TEMPLATE_HASH_ANNOTATION: template_hash} if template_hash else {}
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            annotations=annotations,
            owner_references=[
                SimpleNamespace(
                    api_version="batch/v1",
                    kind="CronJob",
                    name=owner_name,
                    uid=owner_uid,
                )
            ],
        ),
        status=SimpleNamespace(
            active=active,
            completion_time=(datetime(2026, 8, 31, tzinfo=UTC) if completed else None),
            conditions=([SimpleNamespace(type="Complete", status="True")] if completed else []),
        ),
    )


class _FakeJobsApi:
    def __init__(self, jobs: list[SimpleNamespace]) -> None:
        self.jobs = jobs
        self.deleted: list[tuple[str, str, str]] = []

    def list_namespaced_job(self, namespace: str) -> SimpleNamespace:
        return SimpleNamespace(items=self.jobs)

    def delete_namespaced_job(self, name: str, namespace: str, *, propagation_policy: str) -> None:
        self.deleted.append((name, namespace, propagation_policy))


def test_cleanup_deletes_legacy_active_job_but_preserves_completed_history() -> None:
    api = _FakeJobsApi(
        [
            _owned_job("legacy-pending"),
            _owned_job("stale-pending", template_hash="stale-hash"),
            _owned_job("legacy-completed", active=1, completed=True),
        ]
    )

    _cleanup_obsolete_active_source_jobs(
        api,
        namespace="stream2pretrain",
        cronjob_name="s2p-feed-rss-arxiv-cs-cl",
        cronjob_uid="cron-uid",
        desired_template_hash="current-hash",
    )

    assert api.deleted == [
        ("legacy-pending", "stream2pretrain", "Background"),
        ("stale-pending", "stream2pretrain", "Background"),
    ]


def test_cleanup_preserves_matching_active_job() -> None:
    api = _FakeJobsApi([_owned_job("current", template_hash="current-hash")])

    _cleanup_obsolete_active_source_jobs(
        api,
        namespace="stream2pretrain",
        cronjob_name="s2p-feed-rss-arxiv-cs-cl",
        cronjob_uid="cron-uid",
        desired_template_hash="current-hash",
    )

    assert api.deleted == []


def test_cleanup_never_deletes_unrelated_active_jobs() -> None:
    api = _FakeJobsApi(
        [
            _owned_job("other-schedule", owner_name="unrelated-cronjob"),
            _owned_job("recreated-schedule", owner_uid="replacement-uid"),
        ]
    )

    _cleanup_obsolete_active_source_jobs(
        api,
        namespace="stream2pretrain",
        cronjob_name="s2p-feed-rss-arxiv-cs-cl",
        cronjob_uid="cron-uid",
        desired_template_hash="current-hash",
    )

    assert api.deleted == []


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
                labels={"stream2pretrain.io/source-feed": "rss-arxiv-cs-lg"},
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
    )["rss-arxiv-cs-lg"]

    assert runtime["phase"] == "Polling"
    assert runtime["last_attempt_at"] == latest.isoformat()
    assert runtime["last_success_at"] == first.isoformat()


def test_sourcefeed_status_uses_observed_job_runtime() -> None:
    item = {
        "metadata": {"name": "rss-arxiv-cs-lg"},
        "spec": {
            "protocol": "rss",
            "endpoint": "https://rss.arxiv.org/rss/cs.LG",
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


def test_sourcefeed_egress_rejects_unreviewed_hosts() -> None:
    source = SourceFeedSpec.model_validate(
        {
            "name": "unreviewed-feed",
            "protocol": "rss",
            "endpoint": "https://example.com/feed.xml",
            "pollIntervalSeconds": 3600,
            "rateLimit": {"requestsPerSecond": 1.0, "burst": 1},
            "licenseDefault": "per-record",
            "enabled": True,
        }
    )

    with pytest.raises(ValueError, match="unsupported SourceFeed endpoint host"):
        _source_egress_class(source)


def test_builtin_inventory_excludes_removed_discovery_and_backfill_sources() -> None:
    names = {str(item["name"]) for item in _BUILTIN_SOURCES}

    assert names == {
        "arxiv-html-fetcher",
        "hf-models",
        "hf-datasets",
    }


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
