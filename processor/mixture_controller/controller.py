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
import os
import time
from dataclasses import dataclass, field
from typing import Any

from processor import common
from processor.mixture_controller.metrics import MixtureMetrics, PromotionDecision
from processor.mixture_controller.proxy_lm import ProxyLM
from schemas.sourcefeed import MixtureRecipeSpec


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
            {"step": len(self._delta_history.get((st.branch_a.name, st.branch_b.name), [])), "delta": delta}
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
        return {"phase": "Running", "decision": controller.status_for(name).get("lastDecision", "hold")}

    @kopf.on.delete(group, version, plural)  # type: ignore[misc]
    def _on_delete(name: str, **_: Any) -> None:
        controller.remove_recipe(name)

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


def _sourcefeed_status(item: dict[str, Any]) -> dict[str, Any]:
    """Map a SourceFeed CRD item to the UI status payload."""
    from schemas.sourcefeed import SourceFeedSpec

    metadata = item.get("metadata", {}) if isinstance(item.get("metadata"), dict) else {}
    spec_raw = item.get("spec", {}) if isinstance(item.get("spec"), dict) else {}
    spec_raw.setdefault("name", metadata.get("name", "unnamed"))
    spec = SourceFeedSpec.model_validate(spec_raw)
    status = item.get("status", {}) if isinstance(item.get("status"), dict) else {}
    phase = str(status.get("phase", "Pending"))
    poll_state = {
        "Active": "idle",
        "Throttled": "cooldown",
        "Failed": "error",
        "Disabled": "idle",
    }.get(phase, "idle")
    return {
        "name": spec.name,
        "spec": spec.model_dump(mode="json"),
        "last_success_at": status.get("lastSuccessAt"),
        "last_attempt_at": status.get("lastPolledAt"),
        "last_error": status.get("lastErrorMessage"),
        "documents_24h": int(status.get("docsEmitted24h") or status.get("docsEmittedTotal") or 0),
        "error_rate_24h": float(status.get("errorRate24h") or 0.0),
        "poll_state": poll_state,
    }


async def serve_rest_api(controller: MixtureController, port: int = 8080) -> None:
    """REST surface used by the Next.js BFF for SourceFeeds and mixtures."""
    from aiohttp import web  # type: ignore[import-untyped]
    from kubernetes.client import ApiException  # type: ignore[import-untyped]

    from schemas.sourcefeed import SourceFeedSpec

    namespace = os.environ.get("S2P_NAMESPACE", "stream2pretrain")
    api = _kube_custom_objects_api()

    async def list_sources(_: web.Request) -> web.Response:
        resp = api.list_namespaced_custom_object(
            group="stream2pretrain.io",
            version="v1alpha1",
            namespace=namespace,
            plural="sourcefeeds",
        )
        return web.json_response([_sourcefeed_status(item) for item in resp.get("items", [])])

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
    app.router.add_delete("/v1/sources/{name}", delete_source)
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
    log.info("starting mixture controller", namespace=namespace, recipe_threshold=cfg.promotion_threshold)
    controller = MixtureController(cfg)
    kopf = make_kopf_handlers(controller)
    asyncio.run(run_controller_services(kopf, controller, namespace))
