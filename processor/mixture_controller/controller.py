"""kopf operator for ``MixtureRecipe`` CRDs (shadow-mode A/B mixture).

Reconciliation behaviour
------------------------
On create / update of a ``MixtureRecipe`` resource, the controller spawns
two Bytewax-style consumers (one per branch) reading the same
``SourceFeed`` set with their respective per-branch weights. A
:class:`processor.mixture_controller.proxy_lm.ProxyLM` is trained on each
branch over rolling windows. Per-branch perplexity is exported via
:class:`processor.mixture_controller.metrics.MixtureMetrics`.

If branch B's perplexity is more than ``promotion_threshold`` percent
better than branch A for ``promotion_required_windows`` consecutive
windows, the controller updates the CRD's status with a ``promote``
decision; otherwise it records ``hold`` or ``rollback`` decisions
respectively.

Stub note
---------
The Bytewax sub-pipeline is wired up via the standard curation dataflow;
what the controller adds is the per-branch proxy-LM training driver. The
proxy LM itself is the ``proxy-bigram-0.1`` stub documented in
:mod:`processor.mixture_controller.proxy_lm`. The promotion mechanism is
real - it just acts on a stub LM signal until a real LM is wired in.
"""

from __future__ import annotations

import asyncio
import copy
import json
import os
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from processor import common
from processor.mixture_controller.metrics import MixtureMetrics, PromotionDecision
from processor.mixture_controller.proxy_lm import ProxyLM
from schemas.sourcefeed import MixtureRecipeSpec, SourceFeedSpec


@dataclass(slots=True)
class _BranchState:
    """Internal per-branch training state."""

    name: str
    proxy: ProxyLM = field(default_factory=ProxyLM)
    last_window_perplexity: float | None = None
    tokens_in_window: int = 0


@dataclass(slots=True)
class _RecipeState:
    """Aggregate state for one MixtureRecipe under reconciliation."""

    spec: MixtureRecipeSpec
    branch_a: _BranchState
    branch_b: _BranchState
    consecutive_wins_b: int = 0
    last_decision: PromotionDecision = "hold"
    last_window_at: float = field(default_factory=time.time)


class MixtureController:
    """In-process controller core; the kopf bindings call into this."""

    def __init__(
        self,
        cfg: common.ProcessorConfig,
        *,
        metrics: MixtureMetrics | None = None,
    ) -> None:
        self._cfg = cfg
        self._states: dict[str, _RecipeState] = {}
        self._metrics = metrics or MixtureMetrics()
        self._delta_history: dict[tuple[str, str], list[dict[str, float | int]]] = {}

    @property
    def metrics(self) -> MixtureMetrics:
        return self._metrics

    def upsert_recipe(self, spec: MixtureRecipeSpec) -> None:
        """Register or replace a recipe state."""
        self._states[spec.name] = _RecipeState(
            spec=spec,
            branch_a=_BranchState(name=f"{spec.name}-a"),
            branch_b=_BranchState(name=f"{spec.name}-b"),
        )

    def remove_recipe(self, name: str) -> None:
        """Drop a recipe state on CRD delete."""
        self._states.pop(name, None)

    def observe_document(
        self,
        recipe_name: str,
        branch: str,
        text: str,
    ) -> None:
        """Train the relevant branch's proxy LM on a curated document."""
        st = self._states.get(recipe_name)
        if st is None or not text:
            return
        bs = st.branch_a if branch == "a" else st.branch_b if branch == "b" else None
        if bs is None:
            return
        bs.proxy.train(text)
        bs.tokens_in_window += len(text)

    def close_window(self, recipe_name: str, eval_text: str) -> PromotionDecision:
        """End the current rolling window; emit a promotion decision."""
        st = self._states.get(recipe_name)
        if st is None:
            return "hold"
        ppl_a = st.branch_a.proxy.perplexity(eval_text)
        ppl_b = st.branch_b.proxy.perplexity(eval_text)
        st.branch_a.last_window_perplexity = ppl_a
        st.branch_b.last_window_perplexity = ppl_b
        delta = (ppl_a - ppl_b) / ppl_a if ppl_a > 0 else 0.0
        self._metrics.observe_branch(recipe_name, "a", ppl_a, st.branch_a.tokens_in_window)
        self._metrics.observe_branch(recipe_name, "b", ppl_b, st.branch_b.tokens_in_window)
        self._metrics.observe_delta(recipe_name, delta)
        self._delta_history.setdefault((st.branch_a.name, st.branch_b.name), []).append(
            {
                "step": len(self._delta_history.get((st.branch_a.name, st.branch_b.name), [])),
                "delta": delta,
            }
        )
        st.branch_a.tokens_in_window = 0
        st.branch_b.tokens_in_window = 0
        st.last_window_at = time.time()
        decision = self._decide(st, delta)
        st.last_decision = decision
        self._metrics.record_decision(recipe_name, decision)
        return decision

    def _decide(self, st: _RecipeState, delta: float) -> PromotionDecision:
        """Translate the delta + history into a promotion decision."""
        if delta >= self._cfg.promotion_threshold:
            st.consecutive_wins_b += 1
            if st.consecutive_wins_b >= self._cfg.promotion_required_windows:
                st.consecutive_wins_b = 0
                return "promote"
            return "hold"
        if delta <= -self._cfg.promotion_threshold:
            st.consecutive_wins_b = 0
            return "rollback"
        st.consecutive_wins_b = 0
        return "hold"

    def status_for(self, recipe_name: str) -> dict[str, Any]:
        """Return a serialisable view used by kopf to update CRD status."""
        st = self._states.get(recipe_name)
        if st is None:
            return {}
        from dataclasses import asdict

        return {
            "branchA": {
                "perplexity": st.branch_a.last_window_perplexity,
                "snapshot": asdict(st.branch_a.proxy.snapshot()),
            },
            "branchB": {
                "perplexity": st.branch_b.last_window_perplexity,
                "snapshot": asdict(st.branch_b.proxy.snapshot()),
            },
            "lastDecision": st.last_decision,
            "consecutiveWinsB": st.consecutive_wins_b,
            "lastWindowAt": st.last_window_at,
        }

    def compare(self, recipe_a: str, recipe_b: str) -> dict[str, Any]:
        """Return the REST payload consumed by the cockpit mixture page."""
        state_a = self._states.get(recipe_a)
        state_b = self._states.get(recipe_b)
        return {
            "recipe_a": recipe_a,
            "recipe_b": recipe_b,
            "perplexity_delta": self._delta_history.get((recipe_a, recipe_b), []),
            "tokens_per_hour_a": float(state_a.branch_a.tokens_in_window if state_a else 0),
            "tokens_per_hour_b": float(state_b.branch_b.tokens_in_window if state_b else 0),
        }


def make_kopf_handlers(controller: MixtureController) -> Any:
    """Bind the controller to a kopf module - imported lazily.

    Returns the kopf module itself so the caller can ``kopf.run`` it. We
    register one create handler, one update handler, and one delete
    handler. Production runs the controller as a Deployment; the in-cluster
    RBAC is generated by the chart.
    """
    import kopf  # type: ignore[import-untyped]

    group = "stream2pretrain.io"
    version = "v1alpha1"
    plural = "mixturerecipes"

    @kopf.on.create(group, version, plural)  # type: ignore[misc]
    def _on_create(spec: dict[str, Any], name: str, **_: Any) -> dict[str, Any]:
        recipe = MixtureRecipeSpec.model_validate({**spec, "name": name})
        controller.upsert_recipe(recipe)
        return {"phase": "Running", "decision": "hold"}

    @kopf.on.update(group, version, plural)  # type: ignore[misc]
    def _on_update(spec: dict[str, Any], name: str, **_: Any) -> dict[str, Any]:
        recipe = MixtureRecipeSpec.model_validate({**spec, "name": name})
        controller.upsert_recipe(recipe)
        return {
            "phase": "Running",
            "decision": controller.status_for(name).get("lastDecision", "hold"),
        }

    @kopf.on.delete(group, version, plural)  # type: ignore[misc]
    def _on_delete(name: str, **_: Any) -> None:
        controller.remove_recipe(name)

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


async def serve_metrics(metrics: MixtureMetrics, port: int = 9090) -> None:
    """Tiny aiohttp probe and Prometheus exporter."""
    from aiohttp import web  # type: ignore[import-untyped]
    from prometheus_client import generate_latest

    async def metrics_handler(_: web.Request) -> web.Response:
        body = generate_latest(metrics.registry)  # type: ignore[arg-type]
        return web.Response(body=body, content_type="text/plain")

    async def probe_handler(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/healthz", probe_handler)
    app.router.add_get("/readyz", probe_handler)
    app.router.add_get("/metrics", metrics_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, os.environ.get("S2P_BIND_HOST", "::"), port)
    await site.start()
    while True:
        await asyncio.sleep(3600)


def _kube_custom_objects_api() -> Any:
    """Build a Kubernetes CustomObjectsApi using in-cluster config first."""
    from kubernetes import client, config  # type: ignore[import-untyped]

    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
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


def _cron_schedule(interval_seconds: int) -> str:
    """Map a SourceFeed interval onto a conservative five-field Cron schedule."""
    minutes = max(1, round(interval_seconds / 60))
    if minutes < 60:
        return "* * * * *" if minutes == 1 else f"*/{minutes} * * * *"
    hours = max(1, round(minutes / 60))
    if hours < 24:
        return "0 * * * *" if hours == 1 else f"0 */{hours} * * *"
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


def _source_egress_class(source: SourceFeedSpec) -> str:
    """Select the chart's broad external-egress policy for a dynamic source."""
    endpoint_host = str(source.endpoint.host or "").lower()
    return "arxiv" if endpoint_host.endswith("arxiv.org") else "blogs"


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
    cron = client.V1CronJob(
        metadata=client.V1ObjectMeta(
            name=schedule_name,
            namespace=namespace,
            labels={"stream2pretrain.io/source-feed": source.name},
            owner_references=owner_references,
        ),
        spec=client.V1CronJobSpec(
            schedule=_cron_schedule(source.poll_interval_seconds),
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
        batch_api.patch_namespaced_cron_job(schedule_name, namespace, cron)


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
        "FinePDFs Edu v2 on scheduled full text" if is_arxiv else "FineWeb-Edu on page body"
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
        "quality": "FinePDFs Edu v2 on structured full text",
        "license": "arXiv item rights before full-text fetch",
        "stages": ["discover", "license", "fetch", "extract", "classify", "route"],
    },
    {
        "name": "github-release-tarballs",
        "component": "ingest-github-tarball-fetcher",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://api.github.com/repos",
        "quality": "Stack v2 / Dolma code rules; FineWeb-Edu audit for docs",
        "license": "SPDX file header, then tagged repository ref",
        "stages": ["license", "fetch", "extract", "classify", "route"],
    },
    {
        "name": "hf-models",
        "component": "ingest-hf-cards",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://huggingface.co/api/models",
        "quality": "FineWeb-Edu audit on versioned model cards",
        "license": "Versioned public Hub repository terms for README prose",
        "stages": ["discover", "license", "fetch", "classify", "route"],
    },
    {
        "name": "hf-datasets",
        "component": "ingest-hf-cards",
        "kind": "deployment",
        "protocol": "rest-json",
        "endpoint": "https://huggingface.co/api/datasets",
        "quality": "FineWeb-Edu audit on versioned dataset cards",
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
                key=lambda job: getattr(getattr(job, "metadata", None), "creation_timestamp", None),
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


async def serve_rest_api(controller: MixtureController, port: int = 8080) -> None:
    """REST surface used by the Next.js BFF for SourceFeeds and mixtures."""
    from aiohttp import web  # type: ignore[import-untyped]
    from kubernetes import client  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    namespace = os.environ.get("S2P_NAMESPACE", "stream2pretrain")
    api = _kube_custom_objects_api()
    batch_api = client.BatchV1Api()
    apps_api = client.AppsV1Api()
    core_api = client.CoreV1Api()
    background_tasks: set[asyncio.Task[None]] = set()

    async def _watch_source_job(name: str, job_name: str) -> None:
        for _ in range(720):
            await asyncio.sleep(5)
            job = batch_api.read_namespaced_job(job_name, namespace)
            status = job.status
            if status.succeeded:
                phase = "Active"
                error = None
            elif status.failed:
                phase = "Failed"
                error = "Run-once ingest job failed"
            else:
                continue
            stamp = datetime.now(tz=UTC).isoformat()
            patch: dict[str, Any] = {
                "status": {
                    "phase": phase,
                    "lastPolledAt": stamp,
                    "lastErrorMessage": error,
                }
            }
            if phase == "Active":
                patch["status"]["lastSuccessAt"] = stamp
            api.patch_namespaced_custom_object_status(
                group="stream2pretrain.io",
                version="v1alpha1",
                namespace=namespace,
                plural="sourcefeeds",
                name=name,
                body=patch,
            )
            return

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

    async def create_source(request: web.Request) -> web.Response:
        body = await request.json()
        spec = SourceFeedSpec.model_validate(body)
        manifest = {
            "apiVersion": "stream2pretrain.io/v1alpha1",
            "kind": "SourceFeed",
            "metadata": {"name": spec.name},
            "spec": spec.model_dump(mode="json", by_alias=True, exclude_none=True),
        }
        try:
            item = api.create_namespaced_custom_object(
                group="stream2pretrain.io",
                version="v1alpha1",
                namespace=namespace,
                plural="sourcefeeds",
                body=manifest,
            )
        except ApiException as exc:
            if exc.status == 409:
                item = api.patch_namespaced_custom_object(
                    group="stream2pretrain.io",
                    version="v1alpha1",
                    namespace=namespace,
                    plural="sourcefeeds",
                    name=spec.name,
                    body=manifest,
                )
            else:
                raise
        return web.json_response(_sourcefeed_status(item), status=201)

    async def delete_source(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        try:
            api.delete_namespaced_custom_object(
                group="stream2pretrain.io",
                version="v1alpha1",
                namespace=namespace,
                plural="sourcefeeds",
                name=name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return web.json_response({"detail": "source not found"}, status=404)
            raise
        return web.json_response({"deleted": True})

    async def patch_source(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        body = await request.json()
        enabled = body.get("enabled")
        if not isinstance(enabled, bool):
            return web.json_response({"detail": "enabled must be a boolean"}, status=400)
        try:
            item = api.patch_namespaced_custom_object(
                group="stream2pretrain.io",
                version="v1alpha1",
                namespace=namespace,
                plural="sourcefeeds",
                name=name,
                body={"spec": {"enabled": enabled}},
            )
        except ApiException as exc:
            if exc.status == 404:
                return web.json_response({"detail": "source not found"}, status=404)
            raise
        return web.json_response(_sourcefeed_status(item))

    async def run_source(request: web.Request) -> web.Response:
        name = request.match_info["name"]
        try:
            item = api.get_namespaced_custom_object(
                group="stream2pretrain.io",
                version="v1alpha1",
                namespace=namespace,
                plural="sourcefeeds",
                name=name,
            )
        except ApiException as exc:
            if exc.status == 404:
                return web.json_response({"detail": "source not found"}, status=404)
            raise
        source = SourceFeedSpec.model_validate({**item["spec"], "name": name})
        if not source.enabled:
            return web.json_response({"detail": "source is disabled"}, status=409)
        try:
            cron = batch_api.read_namespaced_cron_job(
                _poller_cronjob_name(source.protocol), namespace
            )
        except (ApiException, ValueError) as exc:
            return web.json_response({"detail": str(exc)}, status=409)

        suffix = f"{int(time.time()):x}"[-8:]
        run_name = f"s2p-source-{name[:38]}-{suffix}".rstrip("-")
        config_name = f"{run_name}-feed"
        config = client.V1ConfigMap(
            metadata=client.V1ObjectMeta(name=config_name, namespace=namespace),
            data={
                "source.json": json.dumps(
                    {"feeds": [source.model_dump(mode="json", by_alias=True)]}
                )
            },
        )
        core_api.create_namespaced_config_map(namespace, config)

        job_spec = cron.spec.job_template.spec
        job_spec.ttl_seconds_after_finished = 3600
        _bind_source_config(
            job_spec,
            config_name=config_name,
            source_name=name,
            egress_class=_source_egress_class(source),
        )
        job = batch_api.create_namespaced_job(
            namespace,
            client.V1Job(
                metadata=client.V1ObjectMeta(name=run_name, namespace=namespace),
                spec=job_spec,
            ),
        )
        core_api.patch_namespaced_config_map(
            config_name,
            namespace,
            {
                "metadata": {
                    "ownerReferences": [
                        {
                            "apiVersion": "batch/v1",
                            "kind": "Job",
                            "name": run_name,
                            "uid": job.metadata.uid,
                            "controller": True,
                            "blockOwnerDeletion": True,
                        }
                    ]
                }
            },
        )
        item = api.patch_namespaced_custom_object_status(
            group="stream2pretrain.io",
            version="v1alpha1",
            namespace=namespace,
            plural="sourcefeeds",
            name=name,
            body={
                "status": {
                    "phase": "Polling",
                    "lastPolledAt": datetime.now(tz=UTC).isoformat(),
                    "lastErrorMessage": None,
                }
            },
        )
        task = asyncio.create_task(_watch_source_job(name, run_name))
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        return web.json_response(_sourcefeed_status(item), status=202)

    async def compare(request: web.Request) -> web.Response:
        recipe_a = request.query.get("a")
        recipe_b = request.query.get("b")
        if not recipe_a or not recipe_b:
            return web.json_response({"detail": "missing a or b"}, status=400)
        return web.json_response(controller.compare(recipe_a, recipe_b))

    async def probe(_: web.Request) -> web.Response:
        return web.Response(text="ok\n", content_type="text/plain")

    app = web.Application()
    app.router.add_get("/healthz", probe)
    app.router.add_get("/readyz", probe)
    app.router.add_get("/v1/sources", list_sources)
    app.router.add_post("/v1/sources", create_source)
    app.router.add_patch("/v1/sources/{name}", patch_source)
    app.router.add_delete("/v1/sources/{name}", delete_source)
    app.router.add_post("/v1/sources/{name}/run", run_source)
    app.router.add_get("/v1/compare", compare)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, os.environ.get("S2P_BIND_HOST", "::"), port)
    await site.start()
    while True:
        await asyncio.sleep(3600)


async def run_controller_services(kopf: Any, controller: MixtureController, namespace: str) -> None:
    """Run kopf reconciliation plus HTTP/metrics surfaces in one event loop."""
    settings = kopf.OperatorSettings()
    settings.scanning.disabled = True
    await asyncio.gather(
        serve_metrics(controller.metrics),
        serve_rest_api(controller, port=int(os.environ.get("S2P_CONTROL_API_PORT", "8080"))),
        kopf.operator(namespace=namespace, standalone=True, settings=settings),
    )


def main() -> None:
    """Entrypoint for the ``s2p-mixture-controller`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.mixture")
    namespace = os.environ.get("S2P_NAMESPACE", "stream2pretrain")
    log.info(
        "starting mixture controller", namespace=namespace, recipe_threshold=cfg.promotion_threshold
    )
    controller = MixtureController(cfg)
    kopf = make_kopf_handlers(controller)
    asyncio.run(run_controller_services(kopf, controller, namespace))
