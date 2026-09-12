"""SourceFeed scheduling and read-only Kubernetes source monitoring."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import os
from datetime import UTC, datetime
from typing import Any

from ingest.common.state import cursor_lease
from schemas.sourcefeed import SourceFeedSpec

_ARXIV_DISCOVERY_SOURCE_ORDER = (
    "oai-arxiv-cs",
    "rss-arxiv-cs-cl",
    "rss-arxiv-cs-lg",
    "rss-arxiv-cs-ai",
    "rss-arxiv-cs-cv",
)
_ARXIV_DISCOVERY_BASE_PHASE_MINUTES = 13
_SOURCE_TEMPLATE_HASH_ANNOTATION = "stream2pretrain.io/template-hash"
_SOURCE_FEED_LABEL = "stream2pretrain.io/source-feed"


def make_kopf_handlers() -> Any:
    """Bind SourceFeed scheduling to a lazily imported kopf module.

    Returns the kopf module itself so the caller can run it. Production runs
    the controller as a Deployment; the chart supplies its in-cluster RBAC.
    """
    import kopf  # type: ignore[import-untyped]

    group = "stream2pretrain.io"
    version = "v1alpha1"

    @kopf.on.create(group, version, "sourcefeeds")  # type: ignore[misc]
    @kopf.on.update(group, version, "sourcefeeds")  # type: ignore[misc]
    @kopf.on.resume(group, version, "sourcefeeds")  # type: ignore[misc]
    def _on_source_upsert(
        spec: dict[str, Any], name: str, namespace: str, body: dict[str, Any], **_: Any
    ) -> dict[str, Any]:
        source = SourceFeedSpec.model_validate({**spec, "name": name})
        uid = str(body.get("metadata", {}).get("uid", ""))
        _reconcile_source_schedule(source, namespace=namespace, owner_uid=uid)
        return {"phase": "Active" if source.enabled else "Disabled"}

    @kopf.on.delete(group, version, "sourcefeeds")  # type: ignore[misc]
    def _on_source_delete(name: str, namespace: str, **_: Any) -> None:
        _delete_source_schedule(name, namespace=namespace)

    return kopf


def _kube_custom_objects_api() -> Any:
    """Build a Kubernetes CustomObjectsApi using in-cluster config first."""
    from kubernetes import client, config  # type: ignore[import-untyped]

    config.load_incluster_config()
    return client.CustomObjectsApi()


def _poller_cronjob_name(protocol: str) -> str:
    key = {
        "rss": "S2P_RSS_CRONJOB",
        "atom": "S2P_RSS_CRONJOB",
        "oai-pmh": "S2P_OAI_CRONJOB",
    }.get(protocol)
    if key is None or not os.environ.get(key):
        raise ValueError(f"No poller is configured for {protocol} sources")
    return os.environ[key]


def _source_schedule_name(name: str) -> str:
    return f"s2p-feed-{name}"[:63].rstrip("-")


def _source_config_name(name: str) -> str:
    return f"{_source_schedule_name(name)}-config"[:63].rstrip("-")


def _cron_schedule(interval_seconds: int, *, source_name: str | None = None) -> str:
    """Map a SourceFeed interval onto a conservative five-field Cron schedule."""
    minutes = max(1, round(interval_seconds / 60))
    if minutes < 60:
        return "* * * * *" if minutes == 1 else f"*/{minutes} * * * *"
    hours = max(1, round(minutes / 60))
    if hours < 24:
        phase_minutes = 0
        if source_name in _ARXIV_DISCOVERY_SOURCE_ORDER:
            source_index = _ARXIV_DISCOVERY_SOURCE_ORDER.index(source_name)
            phase_minutes = (
                _ARXIV_DISCOVERY_BASE_PHASE_MINUTES
                + round(minutes * source_index / len(_ARXIV_DISCOVERY_SOURCE_ORDER))
            ) % minutes
        minute = phase_minutes % 60
        hour_phase = phase_minutes // 60
        if hours == 1:
            return f"{minute} * * * *"
        hour_field = f"*/{hours}" if hour_phase == 0 else f"{hour_phase}-23/{hours}"
        return f"{minute} {hour_field} * * *"
    days = max(1, round(hours / 24))
    return "0 0 * * *" if days == 1 else f"0 0 */{min(days, 31)} * *"


def _bind_source_config(
    job_spec: Any, *, config_name: str, source_name: str, egress_class: str
) -> None:
    """Point a cloned poller job at one generated SourceFeed config."""
    from kubernetes import client  # type: ignore[import-untyped]

    config_path = "/etc/s2p/feeds/source.json"
    template = job_spec.template
    template.metadata.labels["stream2pretrain.io/source-feed"] = source_name
    template.metadata.labels["stream2pretrain.io/egress-class"] = egress_class
    for volume in template.spec.volumes or []:
        if volume.name == "feeds":
            volume.config_map.name = config_name
    for container in template.spec.containers:
        container.args = [
            config_path if isinstance(arg, str) and arg.startswith("/etc/s2p/feeds/") else arg
            for arg in (container.args or [])
        ]
        container.env = [env for env in (container.env or []) if env.name != "S2P_FEED_CONFIG"]
        container.env.append(client.V1EnvVar(name="S2P_FEED_CONFIG", value=config_path))


def _source_template_hash(job_template: Any) -> str:
    """Return a stable identity for the complete desired Job template."""
    serialized = job_template.to_dict() if hasattr(job_template, "to_dict") else job_template
    payload = json.dumps(
        serialized,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _stamp_source_template(job_template: Any, *, source_name: str, template_hash: str) -> None:
    """Put the desired-template identity on Jobs and Pods created by the CronJob."""
    from kubernetes import client  # type: ignore[import-untyped]

    if job_template.metadata is None:
        job_template.metadata = client.V1ObjectMeta()
    job_template.metadata.labels = dict(job_template.metadata.labels or {})
    job_template.metadata.labels[_SOURCE_FEED_LABEL] = source_name
    job_template.metadata.annotations = dict(job_template.metadata.annotations or {})
    job_template.metadata.annotations[_SOURCE_TEMPLATE_HASH_ANNOTATION] = template_hash

    pod_metadata = job_template.spec.template.metadata
    if pod_metadata is None:
        pod_metadata = client.V1ObjectMeta()
        job_template.spec.template.metadata = pod_metadata
    pod_metadata.annotations = dict(pod_metadata.annotations or {})
    pod_metadata.annotations[_SOURCE_TEMPLATE_HASH_ANNOTATION] = template_hash


def _cleanup_obsolete_active_source_jobs(
    batch_api: Any,
    *,
    namespace: str,
    cronjob_name: str,
    cronjob_uid: str,
    desired_template_hash: str,
) -> None:
    """Delete only active Jobs owned by this CronJob with an obsolete template."""
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    if not cronjob_uid:
        return
    jobs = batch_api.list_namespaced_job(namespace).items
    for job in jobs:
        metadata = getattr(job, "metadata", None)
        status = getattr(job, "status", None)
        if metadata is None or status is None or int(getattr(status, "active", 0) or 0) < 1:
            continue
        conditions = getattr(status, "conditions", None) or []
        if getattr(status, "completion_time", None) is not None or any(
            getattr(condition, "type", "") in {"Complete", "Failed"}
            and str(getattr(condition, "status", "")).lower() == "true"
            for condition in conditions
        ):
            continue
        owner_references = getattr(metadata, "owner_references", None) or []
        owned_by_schedule = any(
            getattr(owner, "api_version", "") == "batch/v1"
            and getattr(owner, "kind", "") == "CronJob"
            and getattr(owner, "name", "") == cronjob_name
            and str(getattr(owner, "uid", "")) == cronjob_uid
            for owner in owner_references
        )
        if not owned_by_schedule:
            continue
        annotations = getattr(metadata, "annotations", None) or {}
        if annotations.get(_SOURCE_TEMPLATE_HASH_ANNOTATION) == desired_template_hash:
            continue
        try:
            batch_api.delete_namespaced_job(
                metadata.name,
                namespace,
                propagation_policy="Background",
            )
        except ApiException as exc:
            if exc.status != 404:
                raise


def _source_egress_class(source: SourceFeedSpec) -> str:
    """Allow only the audited arXiv discovery endpoints in SourceFeed CRDs."""
    endpoint_host = str(source.endpoint.host or "").lower()
    if endpoint_host.endswith("arxiv.org"):
        return "arxiv"
    raise ValueError(f"unsupported SourceFeed endpoint host: {endpoint_host}")


def _reconcile_source_schedule(source: SourceFeedSpec, *, namespace: str, owner_uid: str) -> None:
    """Materialize one SourceFeed CRD as a suspended or active CronJob."""
    from kubernetes import client  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    batch_api = client.BatchV1Api()
    core_api = client.CoreV1Api()
    schedule_name = _source_schedule_name(source.name)
    config_name = _source_config_name(source.name)
    owner_references = (
        [
            client.V1OwnerReference(
                api_version="stream2pretrain.io/v1alpha1",
                kind="SourceFeed",
                name=source.name,
                uid=owner_uid,
                controller=True,
                block_owner_deletion=True,
            )
        ]
        if owner_uid
        else None
    )
    config = client.V1ConfigMap(
        metadata=client.V1ObjectMeta(
            name=config_name, namespace=namespace, owner_references=owner_references
        ),
        data={
            "source.json": json.dumps({"feeds": [source.model_dump(mode="json", by_alias=True)]})
        },
    )
    try:
        core_api.create_namespaced_config_map(namespace, config)
    except ApiException as exc:
        if exc.status != 409:
            raise
        core_api.patch_namespaced_config_map(config_name, namespace, config)

    base = batch_api.read_namespaced_cron_job(_poller_cronjob_name(source.protocol), namespace)
    job_template = copy.deepcopy(base.spec.job_template)
    _bind_source_config(
        job_template.spec,
        config_name=config_name,
        source_name=source.name,
        egress_class=_source_egress_class(source),
    )
    template_hash = _source_template_hash(job_template)
    _stamp_source_template(
        job_template,
        source_name=source.name,
        template_hash=template_hash,
    )
    cron = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name=schedule_name,
            namespace=namespace,
            labels={_SOURCE_FEED_LABEL: source.name},
            annotations={_SOURCE_TEMPLATE_HASH_ANNOTATION: template_hash},
            owner_references=owner_references,
        ),
        spec=client.V1CronJobSpec(
            schedule=_cron_schedule(
                source.poll_interval_seconds,
                source_name=source.name,
            ),
            suspend=not source.enabled,
            concurrency_policy="Forbid",
            successful_jobs_history_limit=2,
            failed_jobs_history_limit=2,
            job_template=job_template,
        ),
    )
    try:
        batch_api.create_namespaced_cron_job(namespace, cron)
    except ApiException as exc:
        if exc.status != 409:
            raise
        existing = batch_api.read_namespaced_cron_job(schedule_name, namespace)
        cron.metadata.resource_version = existing.metadata.resource_version
        # This controller owns the complete generated CronJob. Replace it so
        # fields removed from the source template (for example an obsolete
        # nodeSelector, affinity, or state PVC) are removed from the live
        # object instead of surviving a strategic-merge patch. Feed cursors
        # remain in MinIO under the unchanged component and SourceFeed names.
        batch_api.replace_namespaced_cron_job(schedule_name, namespace, cron)
        _cleanup_obsolete_active_source_jobs(
            batch_api,
            namespace=namespace,
            cronjob_name=schedule_name,
            cronjob_uid=str(existing.metadata.uid or ""),
            desired_template_hash=template_hash,
        )


def _delete_source_schedule(name: str, *, namespace: str) -> None:
    from kubernetes import client  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    for operation, resource_name in (
        (client.BatchV1Api().delete_namespaced_cron_job, _source_schedule_name(name)),
        (client.CoreV1Api().delete_namespaced_config_map, _source_config_name(name)),
    ):
        try:
            operation(resource_name, namespace)
        except ApiException as exc:
            if exc.status != 404:
                raise


def _as_utc_iso(value: Any) -> str | None:
    if not isinstance(value, datetime):
        return None
    normalized = value if value.tzinfo else value.replace(tzinfo=UTC)
    return normalized.astimezone(UTC).isoformat()


def _source_job_runtime(jobs: list[Any]) -> dict[str, dict[str, Any]]:
    """Summarize the latest real Kubernetes Job for each SourceFeed."""
    grouped: dict[str, list[Any]] = {}
    for job in jobs:
        metadata = getattr(job, "metadata", None)
        labels = getattr(metadata, "labels", None) or {}
        source_name = labels.get("stream2pretrain.io/source-feed")
        if isinstance(source_name, str) and source_name:
            grouped.setdefault(source_name, []).append(job)

    observations: dict[str, dict[str, Any]] = {}
    for source_name, source_jobs in grouped.items():

        def started_at(job: Any) -> datetime:
            metadata = getattr(job, "metadata", None)
            status = getattr(job, "status", None)
            return (
                getattr(status, "start_time", None)
                or getattr(metadata, "creation_timestamp", None)
                or datetime.min.replace(tzinfo=UTC)
            )

        latest = max(source_jobs, key=started_at)
        latest_status = getattr(latest, "status", None)
        successes = [
            job
            for job in source_jobs
            if int(getattr(getattr(job, "status", None), "succeeded", None) or 0) > 0
        ]
        last_success = None
        if successes:
            successful = max(
                successes,
                key=lambda job: (
                    getattr(getattr(job, "status", None), "completion_time", None)
                    or started_at(job)
                ),
            )
            successful_status = getattr(successful, "status", None)
            last_success = _as_utc_iso(
                getattr(successful_status, "completion_time", None) or started_at(successful)
            )

        active = int(getattr(latest_status, "active", None) or 0) > 0
        failed = int(getattr(latest_status, "failed", None) or 0) > 0
        succeeded = int(getattr(latest_status, "succeeded", None) or 0) > 0
        phase = (
            "Polling" if active else "Failed" if failed else "Active" if succeeded else "Pending"
        )
        error = None
        if failed:
            conditions = getattr(latest_status, "conditions", None) or []
            failure = next(
                (
                    condition
                    for condition in conditions
                    if str(getattr(condition, "type", "")).lower() == "failed"
                ),
                None,
            )
            error = (
                getattr(failure, "message", None)
                or getattr(failure, "reason", None)
                or "Scheduled ingest job failed"
            )
        observations[source_name] = {
            "phase": phase,
            "last_attempt_at": _as_utc_iso(started_at(latest)),
            "last_success_at": last_success,
            "last_error": error,
        }
    return observations


def _sourcefeed_status(
    item: dict[str, Any], runtime: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Map a SourceFeed CRD item to the UI status payload."""
    metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
    spec_raw = item.get("spec", {}) if isinstance(item.get("spec"), dict) else {}
    spec_raw.setdefault("name", metadata.get("name", "unnamed"))
    spec = SourceFeedSpec.model_validate(spec_raw)
    status = item.get("status", {}) if isinstance(item.get("status"), dict) else {}
    runtime = runtime or {}
    phase = str(runtime.get("phase") or status.get("phase", "Pending"))
    poll_state = {
        "Active": "idle",
        "Polling": "polling",
        "Throttled": "cooldown",
        "Failed": "error",
        "Disabled": "idle",
    }.get(phase, "idle")
    is_arxiv = "arxiv" in spec.name.lower() or "arxiv.org" in str(spec.endpoint).lower()
    quality_policy = (
        "source-specific ModernBERT on scheduled full text"
        if is_arxiv
        else "source-specific ModernBERT on page body"
    )
    license_resolver = (
        "arXiv item rights" if is_arxiv else "RSS item or page-level licence metadata"
    )
    stages = (
        ["discover", "license", "dispatch"]
        if is_arxiv
        else ["discover", "license", "fetch", "extract", "classify", "route"]
    )
    return {
        "name": spec.name,
        "spec": spec.model_dump(mode="json"),
        "last_success_at": runtime.get("last_success_at") or status.get("lastSuccessAt"),
        "last_attempt_at": runtime.get("last_attempt_at") or status.get("lastPolledAt"),
        "last_error": (
            runtime.get("last_error") if "last_error" in runtime else status.get("lastErrorMessage")
        ),
        "documents_24h": int(status.get("docsEmitted24h") or status.get("docsEmittedTotal") or 0),
        "error_rate_24h": float(status.get("errorRate24h") or 0.0),
        "poll_state": poll_state,
        "management": "sourcefeed",
        "quality_policy": quality_policy,
        "license_resolver": license_resolver,
        "stages": stages,
        "supports_run": spec.protocol in {"rss", "atom", "oai-pmh"},
    }


_BUILTIN_SOURCES: tuple[dict[str, Any], ...] = (
    {
        "name": "arxiv-html-fetcher",
        "component": "ingest-arxiv-html",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://arxiv.org/html",
        "quality": "source-specific ModernBERT on structured full text",
        "license": "arXiv item rights before full-text fetch",
        "stages": ["discover", "license", "fetch", "extract", "classify", "route"],
    },
    {
        "name": "hf-models",
        "component": "ingest-hf-cards",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://huggingface.co/api/models",
        "quality": "HF ModernBERT quality on versioned model cards",
        "license": "Versioned public Hub repository terms for README prose",
        "stages": ["discover", "license", "fetch", "classify", "route"],
    },
    {
        "name": "hf-datasets",
        "component": "ingest-hf-cards",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://huggingface.co/api/datasets",
        "quality": "HF ModernBERT quality on versioned dataset cards",
        "license": "Versioned public Hub repository terms for README prose",
        "stages": ["discover", "license", "fetch", "classify", "route"],
    },
)


def _builtin_source_status(
    descriptor: dict[str, Any],
    *,
    deployments: dict[str, Any],
    cronjobs: dict[str, Any],
    jobs: list[Any],
) -> dict[str, Any]:
    """Describe a chart-managed source from its real Kubernetes workload."""
    component = str(descriptor["component"])
    component_jobs = [
        job
        for job in jobs
        if (getattr(getattr(job, "metadata", None), "labels", None) or {}).get(
            "app.kubernetes.io/component"
        )
        == component
    ]
    if descriptor["kind"] == "deployment":
        workload = deployments.get(component)
    elif descriptor["kind"] == "cronjob":
        workload = cronjobs.get(component)
    else:
        workload = max(
            component_jobs,
            key=lambda job: str(getattr(getattr(job, "metadata", None), "creation_timestamp", "")),
            default=None,
        )
    seed_component = descriptor.get("seed_component")
    if workload is not None and seed_component:
        annotations = getattr(getattr(workload, "metadata", None), "annotations", None) or {}
        configured_components = {
            item.strip()
            for item in str(annotations.get("stream2pretrain.io/seed-components", "")).split(",")
            if item.strip()
        }
        if seed_component not in configured_components:
            workload = None
    enabled = workload is not None
    poll_state = "idle"
    last_attempt = None
    last_success = None
    last_error = None
    if workload is not None and descriptor["kind"] == "deployment":
        status = getattr(workload, "status", None)
        desired = int(getattr(getattr(workload, "spec", None), "replicas", None) or 0)
        ready = int(getattr(status, "ready_replicas", None) or 0)
        if desired > 0 and ready < desired:
            poll_state = "error"
            last_error = "Deployment is not ready"
    elif workload is not None and descriptor["kind"] == "cronjob":
        status = getattr(workload, "status", None)
        active = list(getattr(status, "active", None) or [])
        poll_state = "polling" if active else "idle"
        last_attempt = _as_utc_iso(getattr(status, "last_schedule_time", None))
        succeeded = [
            job
            for job in component_jobs
            if int(getattr(getattr(job, "status", None), "succeeded", None) or 0) > 0
        ]
        failed = [
            job
            for job in component_jobs
            if int(getattr(getattr(job, "status", None), "failed", None) or 0) > 0
        ]
        if succeeded:
            latest = max(
                succeeded,
                key=lambda job: str(
                    getattr(getattr(job, "metadata", None), "creation_timestamp", "")
                ),
            )
            last_success = _as_utc_iso(
                getattr(getattr(latest, "status", None), "completion_time", None)
            )
        if failed and not active:
            latest_failed = max(
                failed,
                key=lambda job: str(
                    getattr(getattr(job, "metadata", None), "creation_timestamp", "")
                ),
            )
            latest_failed_key = str(
                getattr(getattr(latest_failed, "metadata", None), "creation_timestamp", "")
            )
            latest_success_key = max(
                (
                    str(getattr(getattr(job, "metadata", None), "creation_timestamp", ""))
                    for job in succeeded
                ),
                default="",
            )
            if latest_failed_key > latest_success_key:
                poll_state = "error"
                last_error = "Latest scheduled ingest job failed"
    elif workload is not None:
        status = getattr(workload, "status", None)
        last_attempt = _as_utc_iso(
            getattr(getattr(workload, "metadata", None), "creation_timestamp", None)
        )
        if int(getattr(status, "active", None) or 0) > 0:
            poll_state = "polling"
        elif int(getattr(status, "succeeded", None) or 0) > 0:
            last_success = _as_utc_iso(getattr(status, "completion_time", None))
        elif int(getattr(status, "failed", None) or 0) > 0:
            poll_state = "error"
            last_error = "Backfill ingest job failed"
    spec = SourceFeedSpec(
        name=str(descriptor["name"]),
        protocol=str(descriptor["protocol"]),  # type: ignore[arg-type]
        endpoint=str(descriptor["endpoint"]),  # type: ignore[arg-type]
        enabled=enabled,
        poll_interval_seconds=60,
        rate_limit={"requests_per_second": 1.0, "burst": 1},
        license_default="per-record",
    )
    return {
        "name": descriptor["name"],
        "spec": spec.model_dump(mode="json"),
        "last_success_at": last_success,
        "last_attempt_at": last_attempt,
        "last_error": last_error,
        "documents_24h": 0,
        "error_rate_24h": 0.0,
        "poll_state": poll_state,
        "management": "builtin",
        "quality_policy": descriptor["quality"],
        "license_resolver": descriptor["license"],
        "stages": descriptor["stages"],
        "supports_run": False,
    }


async def serve_rest_api(port: int = 8080) -> None:
    """Serve the source catalogue and workload state used by the cockpit."""
    from aiohttp import web  # type: ignore[import-untyped]
    from kubernetes import client  # type: ignore[import-untyped]

    namespace = os.environ.get("S2P_NAMESPACE", "stream2pretrain")
    api = _kube_custom_objects_api()
    batch_api = client.BatchV1Api()
    apps_api = client.AppsV1Api()

    async def list_sources(_: web.Request) -> web.Response:
        resp = api.list_namespaced_custom_object(
            group="stream2pretrain.io",
            version="v1alpha1",
            namespace=namespace,
            plural="sourcefeeds",
        )
        jobs = batch_api.list_namespaced_job(namespace)
        runtime = _source_job_runtime(list(jobs.items or []))
        crd_sources = [
            _sourcefeed_status(
                item,
                runtime=runtime.get(str(item.get("metadata", {}).get("name", ""))),
            )
            for item in resp.get("items", [])
            # arXiv RSS/OAI are internal scheduling lanes for the one logical
            # arXiv full-text source. Discovery lanes never appear as corpus
            # sources or contribute accepted/quarantined counts.
            if not str(item.get("metadata", {}).get("name", "")).startswith("rss-arxiv-")
            and str(item.get("metadata", {}).get("name", "")) != "oai-arxiv-cs"
        ]
        known = {str(source["name"]) for source in crd_sources}
        deployments = {
            str((deployment.metadata.labels or {}).get("app.kubernetes.io/component")): deployment
            for deployment in apps_api.list_namespaced_deployment(namespace).items
        }
        cronjobs = {
            str((cron.metadata.labels or {}).get("app.kubernetes.io/component")): cron
            for cron in batch_api.list_namespaced_cron_job(namespace).items
        }
        builtins = [
            _builtin_source_status(
                descriptor,
                deployments=deployments,
                cronjobs=cronjobs,
                jobs=list(jobs.items or []),
            )
            for descriptor in _BUILTIN_SOURCES
            if descriptor["name"] not in known
        ]
        builtins = [source for source in builtins if source["spec"]["enabled"]]
        return web.json_response(sorted([*crd_sources, *builtins], key=lambda row: row["name"]))

    async def probe(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/healthz", probe)
    app.router.add_get("/readyz", probe)
    app.router.add_get("/v1/sources", list_sources)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, os.environ.get("S2P_BIND_HOST", "::"), port)
    await site.start()
    while True:
        await asyncio.sleep(3600)


async def run_controller_services(kopf: Any, namespace: str) -> None:
    """Run an active-passive reconciler beside the replicated status API."""
    settings = kopf.OperatorSettings()
    settings.scanning.disabled = True

    async def run_elected_operator() -> None:
        if os.environ.get("S2P_CURSOR_LEASE_BACKEND", "none").strip().lower() == "none":
            await kopf.operator(namespace=namespace, standalone=True, settings=settings)
            return
        raw_duration = os.environ.get("S2P_CURSOR_LEASE_DURATION_SECONDS", "")
        duration_seconds = int(raw_duration)
        retry_seconds = max(1.0, duration_seconds / 3)
        while True:
            try:
                async with cursor_lease("source-controller") as is_leader:
                    if not is_leader:
                        await asyncio.sleep(retry_seconds)
                        continue
                    await kopf.operator(namespace=namespace, standalone=True, settings=settings)
            except asyncio.CancelledError as exc:
                if not exc.args or not str(exc.args[0]).startswith("cursor lease lost:"):
                    raise
                current = asyncio.current_task()
                if current is not None:
                    current.uncancel()

    await asyncio.gather(
        serve_rest_api(port=int(os.environ.get("S2P_CONTROL_API_PORT", "8080"))),
        run_elected_operator(),
    )


def main() -> None:
    """Run the SourceFeed controller in the configured namespace."""
    import logging

    logging.basicConfig(level=os.environ.get("S2P_LOG_LEVEL", "INFO"))
    namespace = os.environ.get("S2P_NAMESPACE", "stream2pretrain")
    logging.getLogger("s2p.sources").info("starting source controller in %s", namespace)
    kopf = make_kopf_handlers()
    asyncio.run(run_controller_services(kopf, namespace))
