"""Immutable public/hidden SFT and RL package construction plus MinIO sink."""

from __future__ import annotations

import gzip
import io
import os
import tarfile
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from processor.foundry.util import canonical_json, normalize_identifier, sha256, stable_id
from processor.sign import AttestationSigner
from schemas.foundry import (
    DatasetSplit,
    EnvironmentManifest,
    FoundryAnswer,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    PosttrainPool,
    ProviderTrace,
    TaskSpec,
    Trajectory,
    ValidationReport,
    VerifierSpec,
)


@dataclass(frozen=True, slots=True)
class PackageResult:
    content: bytes
    package_hash: str
    environment_hash: str
    manifest: EnvironmentManifest
    signature_b64: str
    signer_cert_pem: str
    signature_backend: str


class EnvironmentPackager:
    def __init__(
        self,
        asset_loader: Callable[[str], bytes] | None = None,
        signer: AttestationSigner | None = None,
    ) -> None:
        self._asset_loader = asset_loader
        self._signer = signer or AttestationSigner(
            key_path=os.environ.get("S2P_FOUNDRY_SIGNING_KEY"),
            cert_path=os.environ.get("S2P_FOUNDRY_SIGNING_CERT"),
        )

    def build(
        self,
        *,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        task: TaskSpec,
        trajectories: list[Trajectory],
        validation: ValidationReport,
        traces: list[ProviderTrace],
        verifier: VerifierSpec | None,
        pool: PosttrainPool,
        dataset_split: DatasetSplit,
        validation_cases: dict[str, list[FoundryAnswer]] | None = None,
        oracle_results: list[OracleResult] | None = None,
    ) -> PackageResult:
        with tempfile.TemporaryDirectory(prefix="s2p-foundry-") as temp:
            root = Path(temp) / "paper_environment"
            self._write_tree(
                root=root,
                bundle=bundle,
                graph=graph,
                task=task,
                trajectories=trajectories,
                validation=validation,
                traces=traces,
                verifier=verifier,
                validation_cases=validation_cases,
                oracle_results=oracle_results or [],
            )
            requirements_hash = sha256((root / "lock" / "requirements.lock").read_bytes())
            files = _file_hashes(root, exclude={"manifest.json", "lock/environment_hash.json"})
            environment_hash = sha256(files)
            public_files = sorted(
                path.relative_to(root).as_posix()
                for path in (root / "public_context").rglob("*")
                if path.is_file()
            ) + sorted(
                path.relative_to(root).as_posix()
                for path in (root / "public_tools").rglob("*")
                if path.is_file()
            )
            hidden_files = sorted(
                path.relative_to(root).as_posix()
                for path in (root / "hidden").rglob("*")
                if path.is_file()
            )
            manifest = EnvironmentManifest(
                environment_id=stable_id("paper-environment", task.task_id, environment_hash),
                task_id=task.task_id,
                content_policy_revision=task.content_policy_revision,
                paper_id=bundle.paper_id,
                family=task.family,
                pool=pool,
                dataset_split=dataset_split,
                quality_label=("verified_adversarial" if verifier else "verified_automatic"),
                public_files=public_files,
                hidden_files=hidden_files,
                verifier_version=verifier.version if verifier else 0,
                determinism_seed=verifier.determinism_seed if verifier else 7342,
                created_at=datetime.fromisoformat(str(bundle.metadata["valid_from"])),
                construction_trace_ids=[trace.trace_id for trace in traces],
                requirements_lock_hash=requirements_hash,
            )
            _write_json(root / "manifest.json", manifest)
            _write_json(
                root / "lock" / "environment_hash.json",
                {"environment_hash": environment_hash, "files": files},
            )
            content = _deterministic_tar(root)
            signature = self._signer.sign(content)
            return PackageResult(
                content=content,
                package_hash=sha256(content),
                environment_hash=environment_hash,
                manifest=manifest,
                signature_b64=signature.signature_b64,
                signer_cert_pem=signature.cert_pem,
                signature_backend=signature.backend,
            )

    def _write_tree(
        self,
        *,
        root: Path,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        task: TaskSpec,
        trajectories: list[Trajectory],
        validation: ValidationReport,
        traces: list[ProviderTrace],
        verifier: VerifierSpec | None,
        validation_cases: dict[str, list[FoundryAnswer]] | None,
        oracle_results: list[OracleResult],
    ) -> None:
        for directory in (
            "public_context/figures",
            "public_tools",
            "hidden",
            "validation",
            "trajectories",
            "provenance",
            "lock",
            "prime_verifiers/paper_foundry",
        ):
            (root / directory).mkdir(parents=True, exist_ok=True)
        spans = {
            span.span_id: span
            for span in bundle.stable_spans
            if span.span_id
            in {
                *task.public_context_policy.included_spans,
                *task.public_context_policy.same_paper_distractors,
            }
        }
        paper_text = "\n\n".join(f"[{span_id}]\n{span.text}" for span_id, span in spans.items())
        _write_text(root / "public_context" / "paper.txt", paper_text)
        _write_json(
            root / "public_context" / "span_index.json",
            {
                span_id: {
                    "span_id": span.span_id,
                    "section_id": span.section_id,
                    "section_role": span.section_role,
                    "ordinal": span.ordinal,
                    "text": span.text,
                    "text_hash": span.text_hash,
                }
                for span_id, span in spans.items()
            },
        )
        _write_json(
            root / "public_context" / "equations.json",
            [value for value in bundle.equations if set(value.source_span_ids) & spans.keys()],
        )
        _write_json(
            root / "public_context" / "tables" / "tables.json",
            [value for value in bundle.tables if set(value.source_span_ids) & spans.keys()],
        )
        figure_index: list[dict[str, Any]] = []
        for value in bundle.figures:
            if not set(value.source_span_ids) & spans.keys():
                continue
            item = value.model_dump(mode="json", exclude={"asset_uri"})
            if value.asset_uri and self._asset_loader is not None:
                content = self._asset_loader(value.asset_uri)
                if value.image_hash and sha256(content) != value.image_hash:
                    raise ValueError(f"figure hash mismatch for {value.figure_id}")
                suffix = Path(urlparse(value.asset_uri).path).suffix.lower()
                if suffix not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}:
                    suffix = ".bin"
                name = f"{normalize_identifier(value.figure_id)}{suffix}"
                (root / "public_context" / "figures" / name).write_bytes(content)
                item["packaged_path"] = f"public_context/figures/{name}"
                item["image_hash"] = sha256(content)
            figure_index.append(item)
        _write_json(root / "public_context" / "figures" / "index.json", figure_index)
        allowed_node_index = [
            {"id": node.id, "type": node.type}
            for node in graph.nodes
            if set(node.supporting_spans) & spans.keys()
        ]
        _write_json(
            root / "prompt.json",
            {
                "instruction": task.public_instruction,
                "answer_contract": task.answer_contract,
                "allowed_manifest_nodes": allowed_node_index,
                "allowed_tools": task.public_context_policy.tool_access,
                "answer_schema": {
                    "report": "string",
                    "answer_manifest": {
                        "claims": ["node_id"],
                        **(
                            {"evidence": ["span_id"]}
                            if task.family != "derivation_completion"
                            else {}
                        ),
                        "equations": [{"id": "node_id", "latex": "string"}],
                        "method_nodes": ["node_id"],
                        "faults": ["node_id"],
                        "numeric_results": [{"id": "target_id", "value": 0.0, "unit": None}],
                        "relations": [
                            {"source": "node_id", "relation": "supports", "target": "node_id"}
                        ],
                        "qualifications": ["node_id"],
                        "configuration": {"parameter": "value"},
                    },
                },
            },
        )
        self._write_public_tools(root)
        _write_json(root / "hidden" / "evidence_graph.json", graph)
        _write_json(root / "hidden" / "oracle_results.json", oracle_results)
        _write_json(
            root / "hidden" / "reference_state.json",
            {"task": task.model_dump(mode="json"), "paper_hash": bundle.paper_hash},
        )
        _write_json(
            root / "hidden" / "accepted_equivalences.json",
            {
                node.id: node.canonical_symbolic_form
                for node in graph.nodes
                if node.canonical_symbolic_form
            },
        )
        _write_json(
            root / "hidden" / "tolerances.json",
            {
                predicate.id: predicate.tolerance
                for predicate in (verifier.predicates if verifier else [])
                if predicate.tolerance is not None
            },
        )
        if verifier is not None:
            _write_json(root / "hidden" / "verifier_spec.json", verifier)
            _write_text(root / "hidden" / "__init__.py", "")
            standalone = (
                Path(__file__).with_name("standalone_verifier.py").read_text(encoding="utf-8")
            )
            _write_text(root / "hidden" / "verifier.py", standalone)
        self._write_validation(root, validation, trajectories, validation_cases)
        _write_jsonl(
            root / "trajectories" / "accepted.jsonl",
            [value for value in trajectories if value.accepted],
        )
        _write_jsonl(
            root / "trajectories" / "rejected.jsonl",
            [value for value in trajectories if not value.accepted],
        )
        _write_json(
            root / "provenance" / "model_provider_audit.json",
            [trace.model_dump(mode="json") for trace in traces],
        )
        _write_text(
            root / "lock" / "requirements.lock",
            "lark>=1.1,<2\npydantic==2.12.5\nsympy==1.13.3\nverifiers==0.3.1\n",
        )
        if verifier is not None:
            self._write_prime_export(root)

    @staticmethod
    def _write_public_tools(root: Path) -> None:
        runtime = Path(__file__).with_name("tools.py").read_text(encoding="utf-8")
        _write_text(root / "public_tools" / "runtime.py", runtime)
        _write_text(
            root / "public_tools" / "__init__.py", "from .runtime import PaperRuntime, ToolError\n"
        )
        for name in ("search", "open", "find", "calculator", "symbolic"):
            _write_text(
                root / "public_tools" / f"{name}.py",
                f'"""Frozen {name} tool. See runtime.PaperRuntime.{name}."""\nfrom .runtime import PaperRuntime\n',
            )

    @staticmethod
    def _write_validation(
        root: Path,
        report: ValidationReport,
        trajectories: list[Trajectory],
        cases: dict[str, list[FoundryAnswer]] | None,
    ) -> None:
        accepted = [value.answer for value in trajectories if value.accepted]
        rejected = [value.answer for value in trajectories if not value.accepted]
        _write_jsonl(root / "validation" / "valid_solutions.jsonl", accepted[:1])
        _write_jsonl(
            root / "validation" / "equivalent_solutions.jsonl",
            (cases or {}).get("equivalent", accepted[1:]),
        )
        _write_jsonl(
            root / "validation" / "adversarial_solutions.jsonl",
            (cases or {}).get("adversarial", rejected),
        )
        _write_jsonl(
            root / "validation" / "mutations.jsonl",
            (cases or {}).get("mutations", []),
        )
        _write_jsonl(
            root / "validation" / "metamorphic_tests.jsonl",
            (cases or {}).get("metamorphic", []),
        )
        _write_json(root / "validation" / "replay_report.json", report)

    @staticmethod
    def _write_prime_export(root: Path) -> None:
        _write_text(root / "prime_verifiers" / "paper_foundry" / "__init__.py", _PRIME_INIT)
        _write_text(root / "prime_verifiers" / "paper_foundry" / "taskset.py", _PRIME_TASKSET)
        _write_text(root / "prime_verifiers" / "pyproject.toml", _PRIME_PYPROJECT)
        _write_text(root / "prime_verifiers" / "README.md", _PRIME_README)


class MinioPackageSink:
    def __init__(self, *, s3_client: object, bucket: str) -> None:
        self._s3 = s3_client
        self._bucket = bucket

    def write(
        self,
        package: PackageResult,
        *,
        paper_id: str,
        task_id: str,
    ) -> str:
        key = (
            f"{package.manifest.pool}/{package.manifest.dataset_split}/"
            f"revisions/{normalize_identifier(package.manifest.content_policy_revision)}/"
            f"environments/{normalize_identifier(paper_id)}/"
            f"{normalize_identifier(task_id)}/{package.package_hash.removeprefix('sha256:')}.tar.gz"
        )
        self._s3.put_object(  # type: ignore[attr-defined]
            Bucket=self._bucket,
            Key=key,
            Body=package.content,
            ContentType="application/gzip",
            Metadata={
                "package-sha256": package.package_hash.removeprefix("sha256:"),
                "environment-sha256": package.environment_hash.removeprefix("sha256:"),
            },
        )
        signature_key = f"{key}.sig.json"
        self._s3.put_object(  # type: ignore[attr-defined]
            Bucket=self._bucket,
            Key=signature_key,
            Body=canonical_json(
                {
                    "package_hash": package.package_hash,
                    "signature_b64": package.signature_b64,
                    "signer_cert_pem": package.signer_cert_pem,
                    "backend": package.signature_backend,
                }
            ),
            ContentType="application/json",
        )
        return f"s3://{self._bucket}/{key}"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json(value) + b"\n")


def _write_jsonl(path: Path, values: list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"".join(canonical_json(value) + b"\n" for value in values))


def _write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8", newline="\n")


def _file_hashes(root: Path, *, exclude: set[str]) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): sha256(path.read_bytes())
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.relative_to(root).as_posix() not in exclude
    }


def _deterministic_tar(root: Path) -> bytes:
    target = io.BytesIO()
    with (
        gzip.GzipFile(fileobj=target, mode="wb", mtime=0, filename="") as compressed,
        tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as archive,
    ):
        for path in sorted(root.rglob("*")):
            relative = Path(root.name) / path.relative_to(root)
            info = archive.gettarinfo(str(path), arcname=relative.as_posix())
            info.uid = 0
            info.gid = 0
            info.uname = "root"
            info.gname = "root"
            info.mtime = 0
            if path.is_file():
                with path.open("rb") as handle:
                    archive.addfile(info, handle)
            else:
                archive.addfile(info)
    return target.getvalue()


_PRIME_INIT = (
    """from .taskset import PaperFoundryTaskset\n\n__all__ = [\"PaperFoundryTaskset\"]\n"""
)

_PRIME_TASKSET = r'''from __future__ import annotations

import json
import sys
from pathlib import Path

import verifiers.v1 as vf


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _runtime():
    from public_tools.runtime import PaperRuntime

    spans = json.loads((ROOT / "public_context" / "span_index.json").read_text(encoding="utf-8"))
    equations = json.loads((ROOT / "public_context" / "equations.json").read_text(encoding="utf-8"))
    tables = json.loads((ROOT / "public_context" / "tables" / "tables.json").read_text(encoding="utf-8"))
    return PaperRuntime(
        spans={key: value["text"] for key, value in spans.items()},
        equations={value["equation_id"]: value for value in equations},
        tables={value["table_id"]: value for value in tables},
    )


class PaperFoundryTools(vf.Toolset[vf.SharedToolsetConfig]):
    TOOL_PREFIX = "paper"

    @vf.tool
    async def search(self, query: str, limit: int = 8):
        """Search the frozen public paper context."""
        return _runtime().search(query, limit=limit)

    @vf.tool
    async def open(self, object_id: str):
        """Open one frozen span, equation, or table by ID."""
        return _runtime().open(object_id)

    @vf.tool
    async def find(self, needle: str, object_id: str | None = None):
        """Find literal text within the frozen public paper context."""
        return _runtime().find(needle, object_id=object_id)

    @vf.tool
    async def calculator(self, expression: str):
        """Evaluate a bounded arithmetic expression without Python execution."""
        return _runtime().calculator(expression)

    @vf.tool
    async def symbolic(self, operation: str, expression: str, other: str | None = None):
        """Run an allowlisted deterministic SymPy operation."""
        return _runtime().symbolic(operation, expression, other=other)


class PaperFoundryData(vf.TaskData):
    root: str


class PaperFoundryTask(vf.Task[PaperFoundryData]):
    @vf.reward
    async def scientific_reward(self, trace: vf.Trace) -> float:
        import sys
        root = Path(self.data.root)
        sys.path.insert(0, str(root))
        from hidden.verifier import score_response
        return float(
            score_response(
                trace.last_reply,
                root,
                tool_call_count=len(trace.tool_messages),
            )
        )


class PaperFoundryConfig(vf.TasksetConfig):
    tools: vf.SharedToolsetConfig = vf.SharedToolsetConfig()


class PaperFoundryTaskset(vf.Taskset[PaperFoundryTask, PaperFoundryConfig]):
    @classmethod
    def toolsets(cls, config: PaperFoundryConfig) -> list[vf.Toolset]:
        return [PaperFoundryTools(config.tools)]

    def load(self) -> list[PaperFoundryTask]:
        prompt = json.loads((ROOT / "prompt.json").read_text(encoding="utf-8"))
        instruction = (
            prompt["instruction"]
            + "\n\nReturn one JSON object matching answer_schema: "
            + json.dumps(prompt["answer_schema"], sort_keys=True)
        )
        return [
            PaperFoundryTask(
                PaperFoundryData(
                    idx=0,
                    prompt=instruction,
                    root=str(ROOT),
                    network_allow=[],
                ),
                self.config.task,
            )
        ]
'''

_PRIME_PYPROJECT = """[project]\nname = \"stream2train-paper-foundry\"\nversion = \"0.1.0\"\nrequires-python = \">=3.11,<3.14\"\ndependencies = [\"verifiers==0.3.1\", \"sympy==1.13.3\", \"lark>=1.1,<2\"]\n\n[build-system]\nrequires = [\"hatchling\"]\nbuild-backend = \"hatchling.build\"\n\n[tool.hatch.build.targets.wheel]\npackages = [\"paper_foundry\"]\n"""

_PRIME_README = """# Paper Foundry environment\n\nThis export targets the current Verifiers v1 Taskset API. The reward reads only the frozen hidden state and runs without model APIs or network access. Run it in a network-disabled Docker runtime and mount hidden files read-only.\n"""


__all__ = ["EnvironmentPackager", "MinioPackageSink", "PackageResult"]
