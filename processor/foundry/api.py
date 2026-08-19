"""HTTP API for the post-training foundry cockpit and audited control actions."""

from __future__ import annotations

import os
import re
import secrets

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from processor.foundry.config import provider_configs
from processor.foundry.inspection import ArtifactInspector
from processor.foundry.metrics import HUMAN_AUDITS
from processor.foundry.quota import QuotaLedger
from processor.foundry.store import FoundryStore


def build_app(
    store: FoundryStore | None = None,
    quota: QuotaLedger | None = None,
    s3_client: object | None = None,
) -> web.Application:
    state_dir = os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry")
    providers = provider_configs()
    control_store = store or FoundryStore(os.path.join(state_dir, "control.sqlite3"))
    quota_ledger = quota or QuotaLedger(
        os.path.join(state_dir, "quota.sqlite3"),
        providers,
    )
    package_client = s3_client or _s3_client()
    inspector = ArtifactInspector(store=control_store, s3_client=package_client)
    app = web.Application(client_max_size=1024 * 1024)

    async def health(_: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def dashboard(_: web.Request) -> web.Response:
        payload = control_store.dashboard()
        payload["quotas"] = [value.model_dump(mode="json") for value in quota_ledger.states()]
        payload["models"] = control_store.model_snapshots()
        payload["daily_run_hour_utc"] = int(os.environ.get("S2P_FOUNDRY_DAILY_RUN_HOUR_UTC", "0"))
        return web.json_response(payload)

    async def activity(request: web.Request) -> web.Response:
        try:
            payload = control_store.activity(request.query.get("window", "5m"))
        except ValueError as exc:
            return web.json_response({"detail": str(exc)}, status=400)
        return web.json_response(payload)

    async def jobs(request: web.Request) -> web.Response:
        try:
            limit = max(1, min(int(request.query.get("limit", "100")), 500))
        except ValueError:
            return web.json_response({"detail": "limit must be an integer"}, status=400)
        return web.json_response(
            {"items": control_store.jobs(limit=limit, state=request.query.get("state"))}
        )

    async def job(request: web.Request) -> web.Response:
        value = control_store.job(request.match_info["job_id"])
        if value is None:
            return web.json_response({"detail": "job not found"}, status=404)
        return web.json_response(value)

    async def artifacts(request: web.Request) -> web.Response:
        try:
            limit = max(1, min(int(request.query.get("limit", "100")), 500))
        except ValueError:
            return web.json_response({"detail": "limit must be an integer"}, status=400)
        values = control_store.artifacts(limit=limit, job_id=request.query.get("job_id"))
        status = request.query.get("status")
        kind = request.query.get("kind")
        family = request.query.get("family")
        pool = request.query.get("pool")
        dataset_split = request.query.get("dataset_split")
        if status:
            values = [value for value in values if value["status"] == status]
        if kind:
            values = [value for value in values if value["kind"] == kind]
        if family:
            values = [value for value in values if value["family"] == family]
        if pool:
            values = [value for value in values if value["pool"] == pool]
        if dataset_split:
            values = [value for value in values if value["dataset_split"] == dataset_split]
        return web.json_response({"items": values})

    async def quotas(_: web.Request) -> web.Response:
        return web.json_response(
            {"items": [value.model_dump(mode="json") for value in quota_ledger.states()]}
        )

    async def models(_: web.Request) -> web.Response:
        return web.json_response({"items": control_store.model_snapshots()})

    def authorized(request: web.Request) -> web.Response | None:
        expected = os.environ.get("S2P_FOUNDRY_CONTROL_TOKEN", "")
        supplied = request.headers.get("Authorization", "").removeprefix("Bearer ")
        if not expected:
            return web.json_response(
                {"detail": "foundry control token is not configured"}, status=503
            )
        if not supplied or not secrets.compare_digest(supplied, expected):
            return web.json_response({"detail": "unauthorized"}, status=401)
        return None

    async def manual_run(request: web.Request) -> web.Response:
        denied = authorized(request)
        if denied is not None:
            return denied
        max_candidates: int | None = None
        if request.can_read_body:
            try:
                payload = await request.json()
            except Exception:
                return web.json_response({"detail": "request body must be JSON"}, status=400)
            if not isinstance(payload, dict):
                return web.json_response({"detail": "request body must be an object"}, status=400)
            raw_limit = payload.get("max_candidates")
            if raw_limit is not None:
                if isinstance(raw_limit, bool) or not isinstance(raw_limit, int) or raw_limit < 1:
                    return web.json_response(
                        {"detail": "max_candidates must be a positive integer"}, status=400
                    )
                max_candidates = raw_limit
        run, created = control_store.request_manual_run(max_candidates=max_candidates)
        return web.json_response(
            {"run": run, "created": created},
            status=202 if run["state"] in {"pending", "running"} else 200,
        )

    async def audit_artifact(request: web.Request) -> web.Response:
        denied = authorized(request)
        if denied is not None:
            return denied
        try:
            payload = await request.json()
        except Exception:
            return web.json_response({"detail": "request body must be JSON"}, status=400)
        if not isinstance(payload, dict):
            return web.json_response({"detail": "request body must be an object"}, status=400)
        decision = payload.get("decision")
        reviewer = payload.get("reviewer")
        reason = payload.get("reason")
        if not isinstance(decision, str) or not isinstance(reviewer, str):
            return web.json_response(
                {"detail": "decision and reviewer must be strings"}, status=400
            )
        if reason is not None and not isinstance(reason, str):
            return web.json_response({"detail": "reason must be a string"}, status=400)
        try:
            audit = control_store.audit_artifact(
                artifact_id=request.match_info["artifact_id"],
                decision=decision,
                reviewer=reviewer,
                reason=reason,
            )
        except KeyError:
            return web.json_response({"detail": "artifact not found"}, status=404)
        except ValueError as exc:
            return web.json_response({"detail": str(exc)}, status=400)
        HUMAN_AUDITS.labels(decision=audit.decision).inc()
        return web.json_response({"audit": audit.model_dump(mode="json")}, status=201)

    async def inspect_artifact(request: web.Request) -> web.Response:
        denied = authorized(request)
        if denied is not None:
            return denied
        value = inspector.inspect(request.match_info["artifact_id"])
        if value is None:
            return web.json_response({"detail": "artifact not found"}, status=404)
        return web.json_response(value)

    async def download_artifact(request: web.Request) -> web.Response:
        denied = authorized(request)
        if denied is not None:
            return denied
        artifact = control_store.artifact(request.match_info["artifact_id"])
        if artifact is None:
            return web.json_response({"detail": "artifact not found"}, status=404)
        package_uri = artifact.get("package_uri")
        if not package_uri:
            return web.json_response(
                {"detail": "rejected artifact has no immutable package"}, status=404
            )
        try:
            content = inspector.package_bytes(str(package_uri))
        except Exception:
            return web.json_response({"detail": "artifact package is unavailable"}, status=502)
        safe_id = re.sub(r"[^A-Za-z0-9._-]+", "_", str(artifact["artifact_id"]))
        filename = f"{safe_id}.tar.gz"
        return web.Response(
            body=content,
            content_type="application/gzip",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Content-Length": str(len(content)),
            },
        )

    async def metrics(_: web.Request) -> web.Response:
        return web.Response(body=generate_latest(), headers={"Content-Type": CONTENT_TYPE_LATEST})

    app.add_routes(
        [
            web.get("/healthz", health),
            web.get("/readyz", health),
            web.get("/metrics", metrics),
            web.get("/api/foundry/dashboard", dashboard),
            web.get("/api/foundry/activity", activity),
            web.get("/api/foundry/jobs", jobs),
            web.get("/api/foundry/jobs/{job_id}", job),
            web.get("/api/foundry/artifacts", artifacts),
            web.get("/api/foundry/artifacts/{artifact_id}/inspect", inspect_artifact),
            web.get("/api/foundry/artifacts/{artifact_id}/package", download_artifact),
            web.get("/api/foundry/quotas", quotas),
            web.get("/api/foundry/models", models),
            web.post("/api/foundry/runs/manual", manual_run),
            web.post("/api/foundry/artifacts/{artifact_id}/audit", audit_artifact),
        ]
    )
    return app


def _s3_client() -> object:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", ""),
        aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", ""),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
    )


def main() -> None:
    web.run_app(
        build_app(),
        host=os.environ.get("S2P_BIND_HOST", "0.0.0.0"),
        port=int(os.environ.get("S2P_FOUNDRY_API_PORT", "8092")),
        access_log=None,
    )


if __name__ == "__main__":
    main()
