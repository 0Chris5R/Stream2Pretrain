"""Human-audit views over accepted packages and rejected generation attempts."""

from __future__ import annotations

import io
import json
import tarfile
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse

from processor.foundry.store import FoundryStore
from processor.foundry.tasking import _deterministic_corruption_task, _normalize_task
from schemas.foundry import PaperBundle, PaperEvidenceGraph, ProviderTrace, TaskSpec

_ROOT = "paper_environment/"


class ArtifactInspector:
    """Resolve the exact training artifact without exposing provider credentials."""

    def __init__(self, *, store: FoundryStore, s3_client: object) -> None:
        self._store = store
        self._s3 = s3_client

    def inspect(self, artifact_id: str) -> dict[str, Any] | None:
        artifact = self._store.artifact(artifact_id)
        if artifact is None:
            return None
        bundle = _model_from_bytes(PaperBundle, self._store.load_bundle(artifact["job_id"]))
        graph = _model_from_bytes(PaperEvidenceGraph, self._store.load_graph(artifact["job_id"]))
        results = self._store.provider_results(job_id=artifact["job_id"])
        attempts = _task_attempts(str(artifact["task_id"]), results)

        package_error: str | None = None
        package_view: dict[str, Any] | None = None
        if artifact.get("package_uri"):
            try:
                package_view = inspect_package(self.package_bytes(str(artifact["package_uri"])))
            except Exception as exc:
                package_error = f"package could not be read: {type(exc).__name__}"

        if package_view is None:
            task = _recover_task(artifact, bundle, graph, results)
            package_view = _rejected_view(artifact, bundle, graph, task)

        package_view.update(
            {
                "artifact": artifact,
                "source": "package"
                if artifact.get("package_uri") and not package_error
                else "durable_cache",
                "package_available": bool(artifact.get("package_uri")),
                "package_error": package_error,
                "generation_attempts": attempts,
            }
        )
        return package_view

    def package_bytes(self, uri: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
            raise ValueError("invalid artifact package URI")
        response = self._s3.get_object(  # type: ignore[attr-defined]
            Bucket=parsed.netloc,
            Key=parsed.path.lstrip("/"),
        )
        body = response["Body"]
        try:
            return bytes(body.read())
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()


def inspect_package(content: bytes) -> dict[str, Any]:
    """Read only the files produced by EnvironmentPackager, without extraction."""
    with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
        members = {
            member.name: member
            for member in archive.getmembers()
            if member.isfile() and member.name.startswith(_ROOT)
        }

        def raw(path: str) -> bytes | None:
            member = members.get(f"{_ROOT}{path}")
            if member is None:
                return None
            handle = archive.extractfile(member)
            return handle.read() if handle is not None else None

        def parsed(path: str) -> Any:
            value = raw(path)
            return json.loads(value) if value else None

        def jsonl(path: str) -> list[Any]:
            value = raw(path)
            if not value:
                return []
            return [json.loads(line) for line in value.splitlines() if line.strip()]

        reference = parsed("hidden/reference_state.json") or {}
        files = [
            {
                "path": member.name.removeprefix(_ROOT),
                "size": member.size,
                "category": PurePosixPath(member.name.removeprefix(_ROOT)).parts[0],
            }
            for member in sorted(members.values(), key=lambda value: value.name)
        ]
        return {
            "task": reference.get("task"),
            "prompt": parsed("prompt.json"),
            "public_context": {
                "paper_text": (raw("public_context/paper.txt") or b"").decode(
                    "utf-8", errors="replace"
                ),
                "spans": list((parsed("public_context/span_index.json") or {}).values()),
                "equations": parsed("public_context/equations.json") or [],
                "tables": parsed("public_context/tables/tables.json") or [],
                "figures": parsed("public_context/figures/index.json") or [],
            },
            "evidence_graph": parsed("hidden/evidence_graph.json"),
            "trajectories": [
                *jsonl("trajectories/accepted.jsonl"),
                *jsonl("trajectories/rejected.jsonl"),
            ],
            "verifier": parsed("hidden/verifier_spec.json"),
            "validation": {
                "report": parsed("validation/replay_report.json"),
                "valid": jsonl("validation/valid_solutions.jsonl"),
                "equivalent": jsonl("validation/equivalent_solutions.jsonl"),
                "adversarial": jsonl("validation/adversarial_solutions.jsonl"),
                "mutations": jsonl("validation/mutations.jsonl"),
                "metamorphic": jsonl("validation/metamorphic_tests.jsonl"),
            },
            "manifest": parsed("manifest.json"),
            "provenance": parsed("provenance/model_provider_audit.json") or [],
            "files": files,
        }


def _rejected_view(
    artifact: dict[str, Any],
    bundle: PaperBundle | None,
    graph: PaperEvidenceGraph | None,
    task: TaskSpec | None,
) -> dict[str, Any]:
    included = set(task.public_context_policy.included_spans if task else [])
    included.update(task.public_context_policy.same_paper_distractors if task else [])
    spans = [
        span.model_dump(mode="json")
        for span in (bundle.stable_spans if bundle else [])
        if span.span_id in included
    ]
    return {
        "task": task.model_dump(mode="json") if task else None,
        "prompt": (
            {
                "instruction": task.public_instruction,
                "answer_contract": task.answer_contract,
                "allowed_tools": task.public_context_policy.tool_access,
            }
            if task
            else None
        ),
        "public_context": {
            "paper_text": "\n\n".join(f"[{span['span_id']}]\n{span['text']}" for span in spans),
            "spans": spans,
            "equations": [value.model_dump(mode="json") for value in bundle.equations]
            if bundle
            else [],
            "tables": [value.model_dump(mode="json") for value in bundle.tables] if bundle else [],
            "figures": [value.model_dump(mode="json") for value in bundle.figures]
            if bundle
            else [],
        },
        "evidence_graph": graph.model_dump(mode="json") if graph else None,
        "trajectories": [],
        "verifier": None,
        "validation": {
            "report": artifact["validation"],
            "valid": [],
            "equivalent": [],
            "adversarial": [],
            "mutations": [],
            "metamorphic": [],
        },
        "manifest": None,
        "provenance": [],
        "files": [],
    }


def _recover_task(
    artifact: dict[str, Any],
    bundle: PaperBundle | None,
    graph: PaperEvidenceGraph | None,
    results: list[dict[str, Any]],
) -> TaskSpec | None:
    if bundle is None or graph is None:
        return None
    designer_trace: ProviderTrace | None = None
    candidates: list[TaskSpec] = []
    for result in results:
        if result["call_key"] == "task_designer":
            designer_trace = ProviderTrace.model_validate(result["trace"])
        if result["call_key"] not in {"task_designer", "schema_repair:task_designer"}:
            continue
        for raw in _task_payloads(result["response"]):
            try:
                candidates.append(TaskSpec.model_validate(raw))
            except ValueError:
                continue
    fallback_trace = designer_trace
    if fallback_trace is None and results:
        fallback_trace = ProviderTrace.model_validate(results[0]["trace"])
    if fallback_trace is None:
        return None
    for candidate in candidates:
        normalized = _normalize_task(candidate, bundle, graph, fallback_trace, set())
        if normalized.task_id == artifact["task_id"]:
            return normalized.model_copy(update={"route": artifact["pool"]})
    corruption = _deterministic_corruption_task(
        bundle=bundle,
        graph=graph,
        designer_trace=fallback_trace,
    )
    if corruption is not None and corruption.task_id == artifact["task_id"]:
        return corruption.model_copy(update={"route": artifact["pool"]})
    return None


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, dict):
        return []
    tasks = value.get("tasks")
    if isinstance(tasks, list):
        return [task for task in tasks if isinstance(task, dict)]
    if isinstance(value.get("family"), str) and isinstance(value.get("public_instruction"), str):
        return [value]
    return []


def _task_attempts(task_id: str, results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for result in results:
        call_key = str(result["call_key"])
        if task_id not in call_key:
            continue
        attempts.append(
            {
                "call_key": call_key,
                "stage": _attempt_stage(call_key),
                "response": result["response"],
                "trace": result["trace"],
                "created_at": result["created_at"],
            }
        )
    return attempts


def _attempt_stage(call_key: str) -> str:
    if "solver_" in call_key or "solution_contract" in call_key:
        return "solution"
    if "grounding_critic" in call_key:
        return "grounding_review"
    if "verifier" in call_key:
        return "verifier"
    return "repair"


def _model_from_bytes(model: type[Any], payload: bytes | None) -> Any | None:
    if payload is None:
        return None
    try:
        return model.model_validate_json(payload)
    except ValueError:
        return None


__all__ = ["ArtifactInspector", "inspect_package"]
