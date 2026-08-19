"""Deterministic, network-disabled official-artifact oracles."""

from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

from processor.foundry.util import canonical_json, normalize_identifier, sha256
from schemas.foundry import OfficialArtifact, OracleRecipe, OracleResult


class OracleRuntimeError(RuntimeError):
    pass


class OracleRunner(Protocol):
    def run(self, artifact: OfficialArtifact, recipe: OracleRecipe) -> OracleResult: ...


class PodmanOracleRunner:
    """Run an immutable oracle image with no network, capabilities, or writable root."""

    def __init__(self, binary: str = "podman") -> None:
        resolved = shutil.which(binary)
        if not resolved:
            raise OracleRuntimeError(f"{binary} is required for local oracle execution")
        self.binary = resolved

    def run(self, artifact: OfficialArtifact, recipe: OracleRecipe) -> OracleResult:
        started = time.perf_counter()
        with tempfile.TemporaryDirectory(prefix="s2p-oracle-") as scratch:
            command = [
                self.binary,
                "run",
                "--rm",
                "--network=none",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--pids-limit=128",
                f"--cpus={recipe.cpu_millis / 1000}",
                f"--memory={recipe.memory_mib}m",
                "--tmpfs=/tmp:rw,noexec,nosuid,size=64m",
                f"--volume={scratch}:/output:rw,Z",
                recipe.image,
                *recipe.command,
            ]
            try:
                completed = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    timeout=recipe.timeout_seconds,
                    text=True,
                )
            except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                raise OracleRuntimeError(f"oracle {recipe.oracle_id} failed") from exc
        return _result(
            artifact=artifact,
            recipe=recipe,
            runner="podman",
            stdout=completed.stdout,
            started=started,
        )


class KubernetesOracleRunner:
    """Create one resource-bounded Kubernetes Job and read its JSON stdout."""

    def __init__(
        self,
        *,
        namespace: str | None = None,
        runtime_class: str | None = None,
    ) -> None:
        host = os.environ.get("KUBERNETES_SERVICE_HOST")
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        if not host:
            raise OracleRuntimeError("KUBERNETES_SERVICE_HOST is unavailable")
        self.base_url = f"https://{host}:{port}"
        self.namespace = namespace or _service_account_namespace()
        self.token = _read_required("/var/run/secrets/kubernetes.io/serviceaccount/token")
        ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        self.ssl_context = ssl.create_default_context(cafile=ca_path)
        self.runtime_class = runtime_class

    def run(self, artifact: OfficialArtifact, recipe: OracleRecipe) -> OracleResult:
        started = time.perf_counter()
        name = (
            normalize_identifier(f"s2p-oracle-{recipe.oracle_id}-{uuid.uuid4().hex[:8]}")
            .lower()[:63]
            .rstrip("-")
        )
        manifest = kubernetes_job_manifest(
            name=name,
            namespace=self.namespace,
            recipe=recipe,
            runtime_class=self.runtime_class,
        )
        self._request(
            "POST",
            f"/apis/batch/v1/namespaces/{self.namespace}/jobs",
            manifest,
        )
        try:
            deadline = time.monotonic() + recipe.timeout_seconds
            while time.monotonic() < deadline:
                job = self._request(
                    "GET",
                    f"/apis/batch/v1/namespaces/{self.namespace}/jobs/{name}",
                )
                status = job.get("status", {})
                if int(status.get("succeeded", 0)) >= 1:
                    break
                if int(status.get("failed", 0)) >= 1:
                    raise OracleRuntimeError(f"Kubernetes oracle job {name} failed")
                time.sleep(2)
            else:
                raise OracleRuntimeError(f"Kubernetes oracle job {name} timed out")
            pods = self._request(
                "GET",
                f"/api/v1/namespaces/{self.namespace}/pods?labelSelector="
                + urllib.parse.quote(f"job-name={name}"),
            )
            items = pods.get("items", [])
            if not items:
                raise OracleRuntimeError(f"Kubernetes oracle job {name} has no Pod")
            pod_name = items[0]["metadata"]["name"]
            stdout = self._request_text(
                "GET",
                f"/api/v1/namespaces/{self.namespace}/pods/{pod_name}/log?container=oracle",
            )
        finally:
            with suppress(Exception):
                self._request(
                    "DELETE",
                    f"/apis/batch/v1/namespaces/{self.namespace}/jobs/{name}",
                    {"propagationPolicy": "Background"},
                )
        return _result(
            artifact=artifact,
            recipe=recipe,
            runner="kubernetes",
            stdout=stdout,
            started=started,
        )

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = self._request_text(method, path, body)
        return json.loads(value) if value else {}

    def _request_text(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> str:
        request = urllib.request.Request(
            self.base_url + path,
            method=method,
            data=canonical_json(body) if body is not None else None,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(
                request,
                context=self.ssl_context,
                timeout=30,
            ) as response:
                payload = bytes(response.read())
                return payload.decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise OracleRuntimeError(
                f"Kubernetes API {method} {path} failed: {exc.code} {detail[:500]}"
            ) from exc


class OracleCoordinator:
    def __init__(self, runner: OracleRunner) -> None:
        self.runner = runner

    def run_all(self, artifacts: list[OfficialArtifact]) -> list[OracleResult]:
        results: list[OracleResult] = []
        for artifact in artifacts:
            if artifact.oracle_recipe is not None:
                if artifact.oracle_recipe.embedded_artifact_hash != artifact.content_hash:
                    raise ValueError(
                        f"official artifact {artifact.artifact_id} does not match its oracle image recipe"
                    )
                if not artifact.build_recipe:
                    raise ValueError(
                        f"official artifact {artifact.artifact_id} has no audited build recipe"
                    )
                results.append(self.runner.run(artifact, artifact.oracle_recipe))
        return results


class S3OracleRegistry:
    """Optional audited manifests keyed by paper family ID in the posttrain bucket."""

    def __init__(self, *, s3_client: object, bucket: str) -> None:
        self.s3 = s3_client
        self.bucket = bucket

    def load(self, paper_id: str) -> list[OfficialArtifact]:
        key = f"oracle-manifests/{normalize_identifier(paper_id)}.json"
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)  # type: ignore[attr-defined]
        except Exception as exc:
            response_data = getattr(exc, "response", {})
            code = str(response_data.get("Error", {}).get("Code", ""))
            if code in {"NoSuchKey", "404", "NotFound"}:
                return []
            raise
        payload = json.loads(response["Body"].read())
        if not isinstance(payload, list):
            raise ValueError(f"oracle manifest {key} must contain a list")
        return [OfficialArtifact.model_validate(value) for value in payload]


def kubernetes_job_manifest(
    *,
    name: str,
    namespace: str,
    recipe: OracleRecipe,
    runtime_class: str | None,
) -> dict[str, Any]:
    pod_spec: dict[str, Any] = {
        "automountServiceAccountToken": False,
        "restartPolicy": "Never",
        "securityContext": {
            "runAsNonRoot": True,
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [
            {
                "name": "oracle",
                "image": recipe.image,
                "command": recipe.command,
                "securityContext": {
                    "allowPrivilegeEscalation": False,
                    "readOnlyRootFilesystem": True,
                    "capabilities": {"drop": ["ALL"]},
                },
                "resources": {
                    "requests": {
                        "cpu": f"{recipe.cpu_millis}m",
                        "memory": f"{recipe.memory_mib}Mi",
                    },
                    "limits": {
                        "cpu": f"{recipe.cpu_millis}m",
                        "memory": f"{recipe.memory_mib}Mi",
                    },
                },
                "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
            }
        ],
        "volumes": [{"name": "tmp", "emptyDir": {"medium": "Memory"}}],
    }
    if runtime_class:
        pod_spec["runtimeClassName"] = runtime_class
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/name": "stream2pretrain",
                "stream2pretrain.io/component": "foundry-oracle",
                "stream2pretrain.io/network": "deny-all",
            },
        },
        "spec": {
            "backoffLimit": 0,
            "activeDeadlineSeconds": recipe.timeout_seconds,
            "ttlSecondsAfterFinished": 300,
            "template": {
                "metadata": {
                    "labels": {
                        "app.kubernetes.io/name": "stream2pretrain",
                        "stream2pretrain.io/component": "foundry-oracle",
                        "stream2pretrain.io/network": "deny-all",
                    }
                },
                "spec": pod_spec,
            },
        },
    }


def build_oracle_coordinator() -> OracleCoordinator:
    runtime = os.environ.get("S2P_FOUNDRY_ORACLE_RUNTIME", "auto")
    if runtime == "auto":
        runtime = "kubernetes" if os.environ.get("KUBERNETES_SERVICE_HOST") else "podman"
    if runtime == "kubernetes":
        runner: OracleRunner = KubernetesOracleRunner(
            runtime_class=os.environ.get("S2P_FOUNDRY_ORACLE_RUNTIME_CLASS") or None
        )
    elif runtime == "podman":
        runner = PodmanOracleRunner()
    else:
        raise OracleRuntimeError("S2P_FOUNDRY_ORACLE_RUNTIME must be auto, podman, or kubernetes")
    return OracleCoordinator(runner)


def _result(
    *,
    artifact: OfficialArtifact,
    recipe: OracleRecipe,
    runner: Literal["podman", "kubernetes"],
    stdout: str,
    started: float,
) -> OracleResult:
    try:
        output = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise OracleRuntimeError(f"oracle {recipe.oracle_id} did not emit JSON") from exc
    if not isinstance(output, (dict, list)):
        raise OracleRuntimeError(f"oracle {recipe.oracle_id} JSON root is not structured")
    output_hash = sha256(output)
    if recipe.expected_output_hash and recipe.expected_output_hash != output_hash:
        raise OracleRuntimeError(f"oracle {recipe.oracle_id} output hash changed")
    return OracleResult(
        oracle_id=recipe.oracle_id,
        artifact_id=artifact.artifact_id,
        runner=runner,
        output=output,
        output_hash=output_hash,
        stdout_hash=sha256(stdout),
        duration_ms=int((time.perf_counter() - started) * 1000),
        completed_at=datetime.now(UTC),
    )


def _service_account_namespace() -> str:
    path = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
    return _read_required(path).strip()


def _read_required(path: str) -> str:
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise OracleRuntimeError(f"required Kubernetes service-account file is empty: {path}")
    return value


__all__ = [
    "KubernetesOracleRunner",
    "OracleCoordinator",
    "OracleRuntimeError",
    "PodmanOracleRunner",
    "S3OracleRegistry",
    "build_oracle_coordinator",
    "kubernetes_job_manifest",
]
