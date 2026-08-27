"""Contract, control-plane, verifier, package, and sandbox tests for the foundry."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import sys
import tarfile
import threading
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from processor.foundry.config import FoundryConfig, ProviderConfig
from processor.foundry.control import ProviderControlPlane
from processor.foundry.graph import BoundedGraphPatch, EvidenceGraphCompiler
from processor.foundry.inspection import ArtifactInspector, inspect_package
from processor.foundry.lakehouse import _schema_column_names
from processor.foundry.oracle_build import tree_hash
from processor.foundry.oracles import kubernetes_job_manifest
from processor.foundry.packaging import EnvironmentPackager
from processor.foundry.paper_adapter import bundle_json, bundle_prompt_json
from processor.foundry.pipeline import PipelineResult, _validate_sft
from processor.foundry.providers import (
    OpenAICompatibleProvider,
    ProviderBudgetExhaustedError,
    ProviderError,
    StructuredGeneration,
)
from processor.foundry.quota import QuotaExceededError, QuotaLedger
from processor.foundry.routing import ROLE_PROVIDER
from processor.foundry.store import FoundryStore
from processor.foundry.symbolic import symbolically_equivalent
from processor.foundry.tasking import (
    GroundingCritique,
    SolvedTask,
    SolverTurn,
    TaskFactory,
    TaskOutputError,
    _machine_verifiable,
    _normalize_solver_turn_data,
    _solution_contract_violations,
    _solver_prompt,
    _validate_or_repair,
)
from processor.foundry.tools import PaperRuntime, ToolError
from processor.foundry.util import sha256
from processor.foundry.validation import run_acceptance_suite, suite_passes
from processor.foundry.verifier import (
    VerifierCompiler,
    deterministic_verifier,
    evaluate,
    normalize_spec,
)
from processor.foundry.worker import WorkerRuntime
from processor.sign import AttestationSigner, verify_signature
from schemas.foundry import (
    AnswerManifest,
    BundleEquation,
    BundleFigure,
    BundleTable,
    BundleTableCell,
    Difficulty,
    EvidenceEdge,
    EvidenceNode,
    FoundryAnswer,
    FoundryArtifactRecord,
    HiddenTargets,
    NumericResult,
    OracleRecipe,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderModelSnapshot,
    ProviderTrace,
    PublicContextPolicy,
    StableSpan,
    SubmittedEquation,
    TaskSpec,
    Trajectory,
    ValidationReport,
    VerifierPredicate,
    VerifierSpec,
)
from schemas.gold import GoldRecord
from schemas.scientific import ScientificDocument, ScientificParagraph, ScientificSection

FIXED_TIME = datetime(2026, 8, 19, tzinfo=UTC)


def _provider_config() -> ProviderConfig:
    return ProviderConfig(
        name="hetzner",
        base_url="https://provider.test/v1",
        api_key_env="HETZNER_INFERENCE_API_KEY",
        credential_label="test",
        preferred_models=("Qwen3.8-27B",),
        allowed_model_families=("qwen",),
        terms_url="https://provider.test/terms",
        terms_audit_date="2026-08-19",
        minute_requests=10,
        minute_input_tokens=1_000_000,
        minute_output_tokens=1_000_000,
        daily_input_tokens=1_000_000,
        daily_output_tokens=1_000_000,
    )


def _bundle() -> PaperBundle:
    source = " ".join(f"sourceword{index}" for index in range(80))
    return PaperBundle(
        paper_id="2608.00001v1",
        paper_family_id="2608.00001",
        paper_hash=sha256("paper"),
        source_uri="https://arxiv.org/html/2608.00001",
        metadata={"valid_from": FIXED_TIME.isoformat()},
        stable_spans=[
            StableSpan(
                span_id="section-1.span1",
                section_id="section-1",
                section_role="results",
                ordinal=0,
                text=source,
                text_hash=sha256(source),
            )
        ],
        source_gold_hash=sha256("gold"),
        scientific_artifact_hash=sha256("scientific"),
    )


def _gold_candidate() -> GoldRecord:
    doc_id = f"sha256:{'a' * 64}"
    return GoldRecord(
        doc_id=doc_id,
        text="A retained scientific result with enough supporting body text.",
        lang="en",
        tokens=10,
        quality_score=4.0,
        edu_score=4.0,
        reasoning_score=0.9,
        route="posttrain_candidate",
        eligible_routes=["posttrain_candidate"],
        license="CC-BY-4.0",
        license_source="manual",
        risk_tier=1,
        valid_from=FIXED_TIME,
        scoring_version="test-v1",
        classifier_revision="test-v1",
        policy_revision="git:test",
        trace_id="a" * 32,
        source_feed="arxiv-html-fetcher",
        source_format="html",
        extraction_pipeline="arxiv-html-test",
        training_word_count=10,
        included_section_count=1,
        scientific_artifact_s3_uri=(
            f"s3://silver/scientific/{doc_id.removeprefix('sha256:')}/document.json"
        ),
    )


def _scientific_document() -> ScientificDocument:
    gold = _gold_candidate()
    paragraph = ScientificParagraph(
        paragraph_id="results.p1",
        text="A retained result is supported by the measured scientific evidence.",
    )
    return ScientificDocument(
        doc_id=gold.doc_id,
        source_url="https://arxiv.org/html/2608.00001",
        source_identifier="2608.00001v1",
        text_sha256="a" * 64,
        extraction_pipeline="arxiv-html-test",
        training_word_count=10,
        included_section_count=1,
        sections=[
            ScientificSection(
                section_id="results",
                level=2,
                title="Results",
                text=paragraph.text,
                role="results",
                word_count=10,
                paragraphs=[paragraph],
            )
        ],
    )


def _task() -> TaskSpec:
    return TaskSpec(
        task_id="task:claim",
        paper_id="2608.00001v1",
        family="claim_evidence",
        public_instruction="Reconstruct the supported claim and cite its exact stable span.",
        public_context_policy=PublicContextPolicy(
            included_spans=["section-1.span1"],
            tool_access=["search", "open"],
        ),
        hidden_targets=HiddenTargets(
            required_nodes=["claim:1"],
            accepted_evidence_sets=[["section-1.span1"]],
        ),
        verifier_class="claim_evidence_v1",
        difficulty=Difficulty(estimated=2),
        route="rl",
    )


def _graph() -> PaperEvidenceGraph:
    return PaperEvidenceGraph(
        graph_id="graph:1",
        paper_id="2608.00001v1",
        nodes=[
            EvidenceNode(
                id="claim:1",
                type="claim",
                canonical_text="The reported result is supported.",
                supporting_spans=["section-1.span1"],
                confidence=1.0,
            )
        ],
        edges=[],
    )


def _answer() -> FoundryAnswer:
    report = " ".join(f"analysisword{index}" for index in range(240))
    return FoundryAnswer(
        report=report,
        answer_manifest=AnswerManifest(
            claims=["claim:1"],
            evidence=["section-1.span1"],
        ),
    )


def _trace(trace_id: str = "trace:1") -> ProviderTrace:
    return ProviderTrace(
        trace_id=trace_id,
        provider="replay",
        credential_label="fixture",
        role="solver_a",
        base_url="replay://local",
        requested_model="replay/qwen",
        returned_model="replay/qwen",
        model_family="qwen",
        prompt_version="test-v1",
        request_hash=sha256("request"),
        response_hash=sha256("response"),
        input_tokens=0,
        output_tokens=0,
        latency_ms=0,
        terms_snapshot_hash=sha256("terms"),
        started_at=FIXED_TIME,
        completed_at=FIXED_TIME,
    )


def _spec() -> VerifierSpec:
    raw = VerifierSpec(
        verifier_id="candidate",
        task_id="task:claim",
        version=1,
        predicates=[
            VerifierPredicate(
                id="outcome:claim",
                type="required_nodes",
                targets=["claim:1"],
                weight=0.5,
            ),
            VerifierPredicate(
                id="outcome:evidence",
                type="evidence_membership",
                allowed_spans=["section-1.span1"],
                weight=0.5,
            ),
        ],
        determinism_seed=1,
    )
    return normalize_spec(raw, _task(), _bundle(), _graph())


def test_prompt_bundle_removes_only_duplicate_model_context() -> None:
    bundle = _bundle().model_copy(
        update={
            "equations": [
                BundleEquation(
                    equation_id="equation-1",
                    latex="x = 1",
                    mathml="<math><mi>x</mi><mo>=</mo><mn>1</mn></math>",
                    source_span_ids=["section-1.span1"],
                )
            ],
            "tables": [
                BundleTable(
                    table_id="table-1",
                    caption="Result",
                    rows=[["method", "score"], ["ours", "1.0"]],
                    cells=[
                        BundleTableCell(
                            cell_id="table-1.cell:r1c1", row=0, column=0, value="method"
                        )
                    ],
                    source_span_ids=["section-1.span1"],
                )
            ],
            "figures": [
                BundleFigure(
                    figure_id="figure-1",
                    caption="Result curve",
                    ocr_text="score 1.0",
                    asset_uri="s3://scientific/figure.png",
                    image_hash=sha256("figure"),
                    source_span_ids=["section-1.span1"],
                )
            ],
            "captions": [{"object_id": "figure-1", "text": "Result curve"}],
        }
    )

    prompt = json.loads(bundle_prompt_json(bundle))

    assert "captions" not in prompt
    assert prompt["equations"][0]["representation"] == "x = 1"
    assert "mathml" not in prompt["equations"][0]
    assert "cells" not in prompt["tables"][0]
    assert prompt["tables"][0]["rows"][1] == ["ours", "1.0"]
    assert "asset_uri" not in prompt["figures"][0]
    assert prompt["stable_spans"] == bundle.model_dump(mode="json")["stable_spans"]
    assert len(bundle_prompt_json(bundle)) < len(bundle_json(bundle))


def test_graph_pass_rejects_more_than_24_incremental_nodes() -> None:
    nodes = [
        EvidenceNode(
            id=f"claim:{index}",
            type="claim",
            canonical_text=f"Claim {index}",
            supporting_spans=["section-1.span1"],
        )
        for index in range(25)
    ]

    with pytest.raises(ValueError, match="at most 24 items"):
        BoundedGraphPatch(nodes=nodes)


def test_bounded_graph_patch_keeps_hard_edge_contract() -> None:
    edges = [
        EvidenceEdge(source="entity:1", relation="supports", target=f"entity:{index}")
        for index in range(41)
    ]

    with pytest.raises(ValueError, match="at most 40 items"):
        BoundedGraphPatch(edges=edges)


def test_graph_compiler_bounds_provider_edges_in_declared_priority_order() -> None:
    nodes = [
        {
            "id": f"entity:{index}",
            "type": "artifact",
            "canonical_text": f"Entity {index}",
        }
        for index in range(7)
    ]
    edges = [
        {"source": source, "relation": "supports", "target": target}
        for source in (node["id"] for node in nodes)
        for target in (node["id"] for node in nodes)
        if source != target
    ]
    assert len(edges) == 42

    class OverlongEdgeControl:
        config = SimpleNamespace(prompt_version="test-v1")

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def call(self, **kwargs: Any) -> tuple[dict[str, Any], ProviderTrace]:
            self.calls.append(kwargs)
            role = kwargs["role"]
            if role == "structure_compiler":
                data: dict[str, Any] = {"nodes": nodes}
            elif role == "dependency_compiler":
                data = {"edges": edges}
            elif role == "graph_critic":
                data = {"accepted": True}
            else:
                data = {}
            return data, _trace(f"trace:{len(self.calls)}")

    graph, _ = EvidenceGraphCompiler(OverlongEdgeControl()).compile(  # type: ignore[arg-type]
        job_id="job:overlong-edges",
        bundle=_bundle(),
    )

    assert len(graph.edges) == 40
    assert graph.edges == [EvidenceEdge.model_validate(edge) for edge in edges[:40]]
    dependency_run = next(run for run in graph.compiler_runs if run.pass_name == "dependency")
    assert dependency_run.findings == [
        "deterministically bounded provider patch edges: retained 40 of 42 returned entries"
    ]


def test_graph_repair_is_a_bounded_delta_that_preserves_valid_nodes() -> None:
    class RepairControl:
        config = SimpleNamespace(prompt_version="test-v1")

        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []
            self.critic_calls = 0

        def call(self, **kwargs: Any) -> tuple[dict[str, Any], ProviderTrace]:
            self.calls.append(kwargs)
            role = kwargs["role"]
            if role == "structure_compiler":
                data: dict[str, Any] = {
                    "nodes": [
                        {
                            "id": "claim:1",
                            "type": "claim",
                            "canonical_text": "The reported result is supported.",
                            "supporting_spans": ["section-1.span1"],
                        }
                    ]
                }
            elif role == "graph_critic":
                self.critic_calls += 1
                data = (
                    {
                        "accepted": False,
                        "findings": ["Record the qualification."],
                        "repair_instructions": ["Add the uncertainty."],
                    }
                    if self.critic_calls == 1
                    else {"accepted": True}
                )
            elif role == "graph_repair":
                data = {"uncertainties": ["The result is limited to the reported setting."]}
            else:
                data = {}
            return data, _trace(f"trace:{len(self.calls)}")

    control = RepairControl()
    graph, _ = EvidenceGraphCompiler(control).compile(  # type: ignore[arg-type]
        job_id="job:1",
        bundle=_bundle(),
    )

    assert [node.id for node in graph.nodes] == ["claim:1"]
    assert graph.uncertainties == ["The result is limited to the reported setting."]
    repair_call = next(call for call in control.calls if call["role"] == "graph_repair")
    assert repair_call["max_output_tokens"] == 6_000
    assert "prioritized incremental JSON patch" in repair_call["system"]
    assert "do not restate unchanged graph content" in repair_call["user"]


def test_solver_turn_drops_symbolic_null_from_numeric_results() -> None:
    raw = {
        "status": "final",
        "report": "The symbolic limit depends on lambda; the measured exponent is one third.",
        "answer_manifest": {
            "claims": ["claim:1"],
            "numeric_results": [
                {"id": "measured_exponent", "value": "0.3333333333", "unit": None},
                {"id": "symbolic_lambda_limit", "value": None, "unit": None},
            ],
        },
        "tool_calls": [],
    }

    turn = SolverTurn.model_validate(_normalize_solver_turn_data(raw))

    assert turn.answer_manifest is not None
    assert [value.id for value in turn.answer_manifest.numeric_results] == ["measured_exponent"]
    assert turn.answer_manifest.numeric_results[0].value == pytest.approx(1 / 3)


def test_invalid_solver_turn_gets_one_bounded_schema_repair() -> None:
    class RepairControl:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def call(self, **kwargs: Any) -> tuple[dict[str, Any], ProviderTrace]:
            self.calls.append(kwargs)
            return (
                {
                    "status": "final",
                    "report": "Supported answer.",
                    "answer_manifest": {"claims": ["claim:1"]},
                    "tool_calls": [],
                },
                _trace("trace:repair"),
            )

    control = RepairControl()
    turn, repair_trace = _validate_or_repair(
        control=control,  # type: ignore[arg-type]
        model=SolverTurn,
        data={"status": "final", "report": None, "answer_manifest": None},
        job_id="job",
        paper_id="paper",
        call_key="solver_a:task:turn:0",
        context="Repair only schema violations.",
        normalizer=_normalize_solver_turn_data,
    )

    assert turn.report == "Supported answer."
    assert repair_trace is not None and repair_trace.trace_id == "trace:repair"
    assert len(control.calls) == 1
    assert control.calls[0]["role"] == "final_repair"
    assert control.calls[0]["call_key"] == "schema_repair:solver_a:task:turn:0"


def test_solution_contract_requires_labeled_numeric_and_symbolic_results() -> None:
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "hidden_targets": HiddenTargets(
                required_nodes=["claim:1"],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"closed_form": "x + 1", "score": 1.5},
            ),
        }
    )
    complete = AnswerManifest(
        claims=["A readable statement is allowed here."],
        method_nodes=["claim:1"],
        evidence=["section-1.span1"],
        equations=[SubmittedEquation(id="closed_form", latex="x + 1")],
        numeric_results=[NumericResult(id="score", value=1.5)],
    )
    assert _solution_contract_violations(complete, task, _graph()) == []
    violations = _solution_contract_violations(
        complete.model_copy(update={"numeric_results": []}),
        task,
        _graph(),
    )
    assert violations == ["missing or incorrect numeric result score"]


def test_sft_validation_accepts_grounded_ids_alongside_readable_claim_text() -> None:
    trajectory = Trajectory(
        trajectory_id="trajectory:1",
        task_id=_task().task_id,
        provider_trace_id="trace:1",
        answer=FoundryAnswer(
            report="A grounded explanation of the paper result.",
            answer_manifest=AnswerManifest(
                claims=["The readable report can restate the scientific result."],
                method_nodes=["claim:1"],
                evidence=["section-1.span1"],
            ),
        ),
        accepted=False,
        reward=0,
    )
    solved = SolvedTask(
        task=_task().model_copy(update={"route": "sft"}),
        trajectories=[trajectory],
        traces=[],
        critic=GroundingCritique(accepted=True),
    )
    report, validated, cases = _validate_sft(solved, _bundle(), _graph())
    assert report.positive_pass
    assert report.equivalent_pass
    assert report.adversarial_pass
    assert report.mutation_total > 0
    assert report.mutation_killed == report.mutation_total
    assert report.metamorphic_pass
    assert report.replay_pass
    assert report.security_pass
    assert cases["adversarial"]
    assert validated[0].accepted


def test_verifier_normalizes_compiler_field_placement_and_expected_targets() -> None:
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "hidden_targets": HiddenTargets(
                required_nodes=["claim:1"],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"closed_form": "x + 1", "score": 1.5},
            ),
        }
    )
    normalized = normalize_spec(
        VerifierSpec(
            verifier_id="compiler-output",
            task_id=task.task_id,
            version=1,
            determinism_seed=1,
            predicates=[
                VerifierPredicate(
                    id="evidence",
                    type="evidence_membership",
                    target="manifest.evidence",
                    targets=["section-1.span1"],
                    weight=0.25,
                    config={"allowed_spans": ["section-1.span1"]},
                ),
                VerifierPredicate(
                    id="coverage",
                    type="evidence_coverage",
                    target="manifest.evidence",
                    weight=0.25,
                    config={"required_spans": ["section-1.span1"]},
                ),
                VerifierPredicate(
                    id="symbolic",
                    type="symbolic_equivalence",
                    target="closed_form",
                    expected="wrong compiler value",
                    weight=0.25,
                ),
                VerifierPredicate(
                    id="numeric",
                    type="numeric_tolerance",
                    target="score",
                    expected=999,
                    tolerance=0.01,
                    weight=0.25,
                ),
                VerifierPredicate(
                    id="report",
                    type="nonempty_report",
                    target="report",
                    targets=["report"],
                    weight=0,
                ),
            ],
        ),
        task,
        _bundle(),
        _graph(),
    )
    by_id = {predicate.id: predicate for predicate in normalized.predicates}
    assert "evidence" not in by_id
    assert "coverage" not in by_id
    assert by_id["symbolic"].expected == "x + 1"
    assert by_id["numeric"].expected == 1.5
    assert by_id["report"].target is None
    assert by_id["report"].targets == []


def test_derivation_solver_prompt_is_public_and_does_not_leak_hidden_targets() -> None:
    graph = PaperEvidenceGraph(
        graph_id="graph:derivation",
        paper_id=_bundle().paper_id,
        nodes=[
            EvidenceNode(
                id="equation:result",
                type="equation",
                canonical_text="The result is x plus one.",
                latex="y=x+1",
                canonical_symbolic_form="y=x+1",
                supporting_spans=["section-1.span1"],
            )
        ],
        edges=[],
    )
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "public_instruction": "Derive the final expression step by step.",
            "hidden_targets": HiddenTargets(
                required_nodes=["equation:result"],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"equation:result": "y=x+1"},
            ),
        }
    )

    prompt = _solver_prompt(_bundle(), graph, task, "independent derivation")

    assert "hidden_targets" not in prompt
    assert "CONSTRUCTION_TARGETS_FOR_REFERENCE_SOLVER_ONLY" not in prompt
    assert '"y=x+1"' not in prompt
    assert '"evidence_policy":"optional_internal_provenance"' in prompt
    assert '"output_target_ids":["equation:result"]' in prompt


def test_derivation_requires_checkable_expected_outcome_for_rl() -> None:
    graph = PaperEvidenceGraph(
        graph_id="graph:derivation",
        paper_id=_bundle().paper_id,
        nodes=[
            EvidenceNode(
                id="equation:result",
                type="equation",
                canonical_text="The result is x plus one.",
                latex="y=x+1",
                canonical_symbolic_form="y=x+1",
                supporting_spans=["section-1.span1"],
            )
        ],
        edges=[],
    )
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "hidden_targets": HiddenTargets(
                required_nodes=["equation:result"],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"equation:result": "y=x+1"},
            ),
        }
    )

    assert _machine_verifiable(task, graph, _bundle())
    assert not _machine_verifiable(
        task.model_copy(
            update={
                "hidden_targets": task.hidden_targets.model_copy(update={"expected_values": {}})
            }
        ),
        graph,
        _bundle(),
    )
    assert not _machine_verifiable(
        task.model_copy(
            update={
                "hidden_targets": task.hidden_targets.model_copy(
                    update={"expected_values": {"equation:result": "x if condition else y"}}
                )
            }
        ),
        graph,
        _bundle(),
    )


def test_derivation_manifest_does_not_require_public_span_ids() -> None:
    graph = PaperEvidenceGraph(
        graph_id="graph:derivation",
        paper_id=_bundle().paper_id,
        nodes=[
            EvidenceNode(
                id="equation:result",
                type="equation",
                canonical_text="The result is x plus one.",
                latex="y=x+1",
                canonical_symbolic_form="y=x+1",
                supporting_spans=["section-1.span1"],
            )
        ],
        edges=[],
    )
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "hidden_targets": HiddenTargets(
                required_nodes=["equation:result"],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"equation:result": "y=x+1"},
            ),
        }
    )
    manifest = AnswerManifest(equations=[SubmittedEquation(id="equation:result", latex="y=x+1")])

    assert _solution_contract_violations(manifest, task, graph) == []
    assert symbolically_equivalent("y=x+1", "x+1=y")


def test_derivation_verifier_checks_equations_and_step_order() -> None:
    edge = EvidenceEdge(source="equation:start", relation="derives", target="equation:result")
    graph = PaperEvidenceGraph(
        graph_id="graph:ordered-derivation",
        paper_id=_bundle().paper_id,
        nodes=[
            EvidenceNode(
                id="equation:start",
                type="equation",
                canonical_text="Start from y minus one equals x.",
                latex="y-1=x",
                canonical_symbolic_form="y-1=x",
                supporting_spans=["section-1.span1"],
            ),
            EvidenceNode(
                id="equation:result",
                type="equation",
                canonical_text="Rearrange to y equals x plus one.",
                latex="y=x+1",
                canonical_symbolic_form="y=x+1",
                supporting_spans=["section-1.span1"],
            ),
        ],
        edges=[edge],
    )
    task = _task().model_copy(
        update={
            "family": "derivation_completion",
            "hidden_targets": HiddenTargets(
                required_nodes=["equation:start", "equation:result"],
                required_relations=[edge],
                accepted_evidence_sets=[["section-1.span1"]],
                expected_values={"final": "y=x+1"},
            ),
        }
    )
    spec = deterministic_verifier(task, _bundle(), graph)
    correct = FoundryAnswer(
        report="Starting from the first equality, add one to both sides to obtain the result.",
        answer_manifest=AnswerManifest(
            equations=[
                SubmittedEquation(id="equation:start", latex="x=y-1"),
                SubmittedEquation(id="equation:result", latex="x+1=y"),
                SubmittedEquation(id="final", latex="y=x+1"),
            ],
            relations=[edge],
        ),
    )

    assert evaluate(spec, correct, task=task, graph=graph, bundle=_bundle()).passed
    claims_only = correct.model_copy(
        update={
            "answer_manifest": correct.answer_manifest.model_copy(
                update={
                    "claims": ["equation:start", "equation:result"],
                    "equations": [SubmittedEquation(id="final", latex="y=x+1")],
                }
            )
        }
    )
    reversed_steps = correct.model_copy(
        update={
            "answer_manifest": correct.answer_manifest.model_copy(
                update={"equations": list(reversed(correct.answer_manifest.equations))}
            )
        }
    )

    assert not evaluate(spec, claims_only, task=task, graph=graph, bundle=_bundle()).passed
    assert not evaluate(spec, reversed_steps, task=task, graph=graph, bundle=_bundle()).passed


def test_irrelevant_numeric_output_does_not_create_a_false_adversary() -> None:
    answer = _answer().model_copy(
        update={
            "answer_manifest": _answer().answer_manifest.model_copy(
                update={"numeric_results": [NumericResult(id="incidental", value=3.0)]}
            )
        }
    )
    report, _validated, _cases = run_acceptance_suite(
        task=_task(),
        spec=_spec(),
        bundle=_bundle(),
        graph=_graph(),
        trajectories=[
            Trajectory(
                trajectory_id="trajectory:incidental-numeric",
                task_id=_task().task_id,
                provider_trace_id="trace:incidental-numeric",
                answer=answer,
                accepted=False,
                reward=0.0,
            )
        ],
    )

    assert report.adversarial_pass
    assert report.false_positive_count == 0
    assert suite_passes(report)


def test_verifier_critic_can_accept_with_documented_residual_risks() -> None:
    class RepairControl:
        def __init__(self) -> None:
            self.responses = [
                _spec().model_dump(mode="json"),
                {
                    "accepted": False,
                    "findings": ["repair is required"],
                    "false_positive_risks": ["blocking risk"],
                    "false_negative_risks": [],
                    "repair_instructions": ["repair the verifier"],
                },
                _spec().model_dump(mode="json"),
                {
                    "accepted": True,
                    "findings": ["the repair is safe"],
                    "false_positive_risks": ["documented residual risk"],
                    "false_negative_risks": ["documented residual risk"],
                    "repair_instructions": [],
                },
            ]

        def call(self, **_: Any) -> tuple[dict[str, Any], ProviderTrace]:
            return self.responses.pop(0), _trace()

    verifier, traces = VerifierCompiler(RepairControl()).compile(  # type: ignore[arg-type]
        job_id="job:1",
        bundle=_bundle(),
        graph=_graph(),
        task=_task(),
    )

    assert verifier.task_id == _task().task_id
    assert len(traces) == 4


def test_pyiceberg_schema_names_supports_current_and_legacy_shapes() -> None:
    class CurrentSchema:
        column_names = ("pool", "dataset_split")

    class Field:
        def __init__(self, name: str) -> None:
            self.name = name

    class LegacySchema:
        fields = (Field("pool"), Field("dataset_split"))

    assert _schema_column_names(CurrentSchema()) == {"pool", "dataset_split"}
    assert _schema_column_names(LegacySchema()) == {"pool", "dataset_split"}


def test_replayed_idempotent_transition_restores_job_state(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    job_id, _ = store.start_job(
        paper_id="paper",
        paper_hash=sha256("paper"),
        doc_id="doc",
        policy_version="v1",
    )
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="REJECTED",
        reason="original terminal result",
        idempotency_suffix="REJECTED",
    )
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="CALL_STARTED",
        attempt=2,
        idempotency_suffix="retry",
    )
    assert store.job(job_id)["state"] == "CALL_STARTED"  # type: ignore[index]
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="REJECTED",
        reason="ignored because the event already exists",
        idempotency_suffix="REJECTED",
    )
    replayed = store.job(job_id)
    assert replayed is not None
    assert replayed["state"] == "REJECTED"
    assert replayed["reason"] == "original terminal result"


def test_openai_provider_streams_and_records_exact_licensed_route(monkeypatch: Any) -> None:
    monkeypatch.setenv("HETZNER_INFERENCE_API_KEY", "test-key")
    model = "Qwen3.8-27B"

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [{"id": model}]})
        body = (
            f'data: {{"model":"{model}","choices":[{{"delta":{{"content":"{{\\"ok\\":"}}}}]}}\n\n'
            f'data: {{"model":"{model}","choices":[{{"delta":{{"content":"true}}"}}}}],'
            '"usage":{"prompt_tokens":3,"completion_tokens":2}}\n\ndata: [DONE]\n\n'
        )
        return httpx.Response(200, text=body, headers={"content-type": "text/event-stream"})

    checkpoints: list[str] = []
    provider = OpenAICompatibleProvider(
        _provider_config(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
    )
    result = provider.generate_json(
        role="solver_a",
        system="Return JSON.",
        user="Return ok.",
        prompt_version="test-v1",
        max_output_tokens=32,
        checkpoint=lambda partial, _digest: checkpoints.append(partial),
    )

    assert result.data == {"ok": True}
    assert checkpoints[-1] == '{"ok":true}'
    assert result.trace.returned_model == model
    assert result.trace.model_license == "Apache-2.0"
    assert result.trace.request_attempts == 1
    assert result.trace.time_to_first_token_ms is not None


def test_openai_provider_treats_payment_required_as_exhausted_budget(monkeypatch: Any) -> None:
    monkeypatch.setenv("HETZNER_INFERENCE_API_KEY", "test-key")
    requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [{"id": "Qwen3.8-27B"}]})
        return httpx.Response(
            402,
            json={"error": {"message": "organization balance is too low"}},
        )

    provider = OpenAICompatibleProvider(
        _provider_config(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        max_retries=2,
    )

    with pytest.raises(
        ProviderBudgetExhaustedError,
        match="hetzner budget exhausted: organization balance is too low",
    ):
        provider.generate_json(
            role="graph_critic",
            system="Return JSON.",
            user="Return ok.",
            prompt_version="test-v1",
            max_output_tokens=32,
        )

    assert requests == 2


def test_openai_provider_reports_bounded_streaming_http_error(monkeypatch: Any) -> None:
    monkeypatch.setenv("HETZNER_INFERENCE_API_KEY", "test-key")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/models"):
            return httpx.Response(200, json={"data": [{"id": "Qwen3.8-27B"}]})
        return httpx.Response(
            400,
            json={"error": {"message": "prompt exceeds model context"}},
        )

    provider = OpenAICompatibleProvider(
        _provider_config(),
        client=httpx.Client(transport=httpx.MockTransport(handler)),
        max_retries=0,
    )

    with pytest.raises(
        ProviderError,
        match="hetzner streaming request failed: prompt exceeds model context",
    ):
        provider.generate_json(
            role="structure_compiler",
            system="Return JSON.",
            user="Return ok.",
            prompt_version="test-v1",
            max_output_tokens=32,
        )


class _CountingProvider:
    name = "hetzner"

    def __init__(self) -> None:
        self.calls = 0

    def discover_models(self) -> Any:
        raise AssertionError("not used")

    def generate_json(self, **kwargs: Any) -> StructuredGeneration:
        self.calls += 1
        checkpoint = kwargs.get("checkpoint")
        if checkpoint:
            checkpoint('{"ok":true}', sha256('{"ok":true}'))
        trace = _trace(f"trace:{self.calls}").model_copy(
            update={
                "provider": "hetzner",
                "base_url": "https://provider.test/v1",
                "requested_model": "Qwen3.8-27B",
                "returned_model": "Qwen3.8-27B",
            }
        )
        return StructuredGeneration(data={"ok": True}, trace=trace)


class _FailingProvider:
    name = "hetzner"

    def discover_models(self) -> Any:
        raise AssertionError("not used")

    def generate_json(self, **_kwargs: Any) -> StructuredGeneration:
        raise ProviderError("upstream response failed")


def test_control_plane_reuses_completed_call_without_spending_quota(tmp_path: Path) -> None:
    configs = {"hetzner": _provider_config()}
    config = FoundryConfig(providers=configs, state_dir=str(tmp_path))
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    quota = QuotaLedger(str(tmp_path / "quota.sqlite3"), configs)
    provider = _CountingProvider()
    control = ProviderControlPlane(
        config=config,
        providers={"hetzner": provider},
        quota=quota,
        store=store,
    )
    job_id, _ = store.start_job(
        paper_id="paper",
        paper_hash=sha256("paper"),
        doc_id="doc",
        policy_version="v1",
    )

    first = control.call(
        job_id=job_id,
        paper_id="paper",
        role="solver_a",
        system="system",
        user="user",
        max_output_tokens=32,
        call_key="stable-call",
    )
    second = control.call(
        job_id=job_id,
        paper_id="paper",
        role="solver_a",
        system="system",
        user="user",
        max_output_tokens=32,
        call_key="stable-call",
    )

    assert first == second
    assert provider.calls == 1
    day = next(
        value for value in quota.states() if value.provider == "hetzner" and value.window == "day"
    )
    assert day.observed_requests_used == 1


def test_quota_reconciliation_is_audit_only_and_retries_remain_visible(tmp_path: Path) -> None:
    configs = {"hetzner": _provider_config()}
    config = FoundryConfig(providers=configs, state_dir=str(tmp_path))
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    quota = QuotaLedger(str(tmp_path / "quota.sqlite3"), configs)
    provider = _FailingProvider()
    control = ProviderControlPlane(
        config=config,
        providers={"hetzner": provider},
        quota=quota,
        store=store,
    )
    job_id, _ = store.start_job(
        paper_id="paper",
        paper_hash=sha256("paper"),
        doc_id="doc",
        policy_version="v1",
    )

    for _ in range(2):
        with pytest.raises(ProviderError, match="upstream response failed"):
            control.call(
                job_id=job_id,
                paper_id="paper",
                role="solver_a",
                system="system",
                user="user",
                max_output_tokens=32,
                call_key="retryable-call",
            )

    job = store.job(job_id)
    assert job is not None
    assert job["state"] == "CALL_FAILED"
    assert [event["attempt"] for event in job["events"] if event["state"] == "CALL_STARTED"] == [
        1,
        2,
    ]
    assert job["events"][-1]["state"] == "QUOTA_RECONCILED"
    provider_status = store.dashboard()["provider_statuses"]["hetzner"]
    assert provider_status["state"] == "CALL_FAILED"
    assert provider_status["reason"] == "upstream response failed"


def test_foundry_activity_reports_active_stream_and_exact_completed_tokens(
    tmp_path: Path,
) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    job_id, _ = store.start_job(
        paper_id="2608.00001",
        paper_hash=sha256("paper"),
        doc_id="doc",
        policy_version="v1",
    )
    store.append_event(
        job_id=job_id,
        paper_id="2608.00001",
        state="RECEIVED",
        idempotency_suffix="received",
    )
    store.append_event(
        job_id=job_id,
        paper_id="2608.00001",
        state="CALL_PLANNED",
        metadata={
            "provider": "hetzner",
            "role": "structure_compiler",
            "estimated_input_tokens": 1_200,
            "max_output_tokens": 800,
        },
        attempt=1,
        idempotency_suffix="structure",
    )
    started = store.append_event(
        job_id=job_id,
        paper_id="2608.00001",
        state="CALL_STARTED",
        metadata={"provider": "hetzner", "role": "structure_compiler"},
        attempt=1,
        idempotency_suffix="structure",
    )
    store.save_stream_checkpoint(
        job_id=job_id,
        call_key="structure",
        attempt=1,
        partial_text="streamed response",
        partial_hash=sha256("streamed response"),
    )

    active = store.activity("5m", now=started.occurred_at + timedelta(seconds=1))
    assert active["totals"]["calls"]["started"] == 1
    assert active["totals"]["stages"]["received"] == 1
    assert active["active_calls"] == [
        {
            "job_id": job_id,
            "paper_id": "2608.00001",
            "call_key": "structure",
            "role": "structure_compiler",
            "provider": "hetzner",
            "attempt": 1,
            "started_at": started.occurred_at.isoformat(),
            "checkpoint_at": active["active_calls"][0]["checkpoint_at"],
            "partial_characters": 17,
            "estimated_input_tokens": 1_200,
            "max_output_tokens": 800,
        }
    ]

    completed_at = datetime.now(UTC)
    trace = _trace("trace:activity").model_copy(
        update={
            "provider": "hetzner",
            "input_tokens": 1_111,
            "output_tokens": 222,
            "started_at": completed_at - timedelta(seconds=2),
            "completed_at": completed_at,
        }
    )
    store.record_trace(job_id, trace)
    store.append_event(
        job_id=job_id,
        paper_id="2608.00001",
        state="CALL_SUCCEEDED",
        metadata={"provider": "hetzner", "role": "structure_compiler"},
        provider_trace_id=trace.trace_id,
        attempt=1,
        idempotency_suffix="structure",
    )

    completed = store.activity("1h", now=datetime.now(UTC) + timedelta(seconds=1))
    assert completed["active_calls"] == []
    assert completed["totals"]["calls"]["succeeded"] == 1
    assert completed["totals"]["tokens"] == {"input": 1_111, "output": 222}


def test_foundry_activity_rejects_unknown_window(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    with pytest.raises(ValueError, match="window must be one of"):
        store.activity("week")


def test_quota_stops_at_provider_limit(tmp_path: Path) -> None:
    tiny = replace(_provider_config(), daily_output_tokens=10)
    quota = QuotaLedger(str(tmp_path / "quota.sqlite3"), {"hetzner": tiny})
    with pytest.raises(QuotaExceededError) as caught:
        quota.reserve("hetzner", input_tokens=1, output_tokens=11)
    assert caught.value.provider == "hetzner"
    assert caught.value.window == "day"
    assert caught.value.resource == "output"


def test_failed_call_reconciles_conservative_reserved_usage(tmp_path: Path) -> None:
    config = _provider_config()
    quota = QuotaLedger(str(tmp_path / "quota.sqlite3"), {"hetzner": config})
    reservation = quota.reserve(
        "hetzner",
        requests=3,
        input_tokens=25,
        output_tokens=50,
    )
    quota.reconcile(reservation, None)
    day = next(value for value in quota.states() if value.window == "day")
    assert day.observed_requests_used == 3
    assert day.observed_input_used == 25
    assert day.observed_output_used == 50


def test_abandoned_reservation_is_conservatively_reconciled_after_restart(
    tmp_path: Path,
) -> None:
    config = _provider_config()
    path = tmp_path / "quota.sqlite3"
    first = QuotaLedger(str(path), {"hetzner": config})
    first.reserve(
        "hetzner",
        requests=3,
        input_tokens=25,
        output_tokens=50,
        now=FIXED_TIME,
    )
    first.close()

    recovered = QuotaLedger(str(path), {"hetzner": config})
    assert recovered.reconcile_abandoned_reservations() == 1
    state = next(
        value
        for value in recovered.states(now=FIXED_TIME)
        if value.provider == "hetzner" and value.window == "day"
    )
    assert state.locally_reserved_requests == 0
    assert state.observed_requests_used == 3
    assert recovered.reconcile_abandoned_reservations() == 0


def test_candidate_queue_ranks_snapshot_by_composite_score(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    for doc_id, reasoning, quality, ranking in (
        ("low-reasoning", 0.7, 5.0, 0.1),
        ("high-reasoning", 0.9, 3.0, 0.2),
        ("quality-tie-break", 0.9, 4.0, 0.3),
    ):
        store.enqueue_candidate(
            doc_id=doc_id,
            payload=doc_id.encode(),
            reasoning_score=reasoning,
            quality_score=quality,
            ranking_score=ranking,
            valid_from=FIXED_TIME,
        )

    cutoff = datetime.now(UTC) + timedelta(seconds=1)
    claimed = [store.claim_candidate(cutoff_at=cutoff) for _ in range(3)]
    assert [value[0] if value else None for value in claimed] == [
        "quality-tie-break",
        "high-reasoning",
        "low-reasoning",
    ]


def test_candidate_queue_updates_changed_payload_and_removes_only_waiting_rows(
    tmp_path: Path,
) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    store.enqueue_candidate(
        doc_id="paper",
        payload=b"old",
        reasoning_score=0.1,
        quality_score=1.0,
        valid_from=FIXED_TIME,
    )
    store.enqueue_candidate(
        doc_id="paper",
        payload=b"new",
        reasoning_score=0.9,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    assert store.claim_candidate(cutoff_at=datetime.now(UTC) + timedelta(seconds=1)) == (
        "paper",
        b"new",
    )
    store.remove_queued_candidate("paper")
    assert store.queued_candidates() == 0
    store.release_candidate("paper")
    assert store.queued_candidates() == 1
    store.remove_queued_candidate("paper")
    assert store.queued_candidates() == 0


def test_candidate_queue_retains_validated_scientific_projection(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    scientific_payload = _scientific_document().model_dump_json().encode()
    store.enqueue_candidate(
        doc_id="paper",
        payload=b"gold",
        scientific_payload=scientific_payload,
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )

    assert store.claim_candidate(cutoff_at=datetime.now(UTC) + timedelta(seconds=1)) == (
        "paper",
        b"gold",
    )
    assert store.candidate_scientific_payload("paper") == scientific_payload


def test_missing_legacy_artifact_is_audited_and_does_not_pin_manual_run(
    tmp_path: Path,
) -> None:
    class ObjectMissingError(Exception):
        def __init__(self) -> None:
            self.response = {"Error": {"Code": "NoSuchKey"}}

    class MissingS3:
        def get_object(self, **_: Any) -> None:
            raise ObjectMissingError

    class KafkaSink:
        def __init__(self) -> None:
            self.jobs: list[dict[str, Any]] = []

        def event(self, _event: Any) -> None:
            pass

        def artifact(self, _artifact: Any) -> None:
            pass

        def job(self, value: dict[str, Any]) -> None:
            self.jobs.append(value)

        def flush(self) -> None:
            pass

    class LakehouseSink:
        def add_event(self, _event: Any) -> None:
            pass

        def add_artifact(self, _artifact: Any) -> None:
            pass

        def flush(self) -> None:
            pass

    gold = _gold_candidate()
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    store.enqueue_candidate(
        doc_id=gold.doc_id,
        payload=gold.model_dump_json().encode(),
        reasoning_score=gold.reasoning_score,
        quality_score=gold.quality_score,
        valid_from=gold.valid_from,
    )
    requested, _ = store.request_manual_run()
    claimed_run = store.claim_manual_run()
    assert claimed_run is not None

    runtime = object.__new__(WorkerRuntime)
    runtime.config = FoundryConfig(providers={}, queue_poll_seconds=5)
    runtime.store = store
    runtime.s3 = MissingS3()
    runtime.kafka = KafkaSink()
    runtime.lakehouse = LakehouseSink()
    runtime.oracle_registry = SimpleNamespace(load=lambda _paper_id: [])
    runtime.pipeline = SimpleNamespace(
        process=lambda *_args, **_kwargs: PipelineResult(
            job_id="unexpected",
            paper_id="unexpected",
            final_state="REJECTED",
            artifacts=[],
        )
    )
    runtime._drain_lock = threading.Lock()
    runtime._drain_stop = threading.Event()
    log = SimpleNamespace(info=lambda *_args, **_kwargs: None)

    runtime._run_manual_snapshot(claimed_run, log)

    run = next(item for item in store.manual_runs() if item["run_id"] == requested["run_id"])
    assert run["state"] == "completed"
    assert run["processed_count"] == 1
    assert store.queued_candidates() == 0
    assert runtime.kafka.jobs[-1]["status"] == "posttrain_preflight_rejected"
    assert runtime.kafka.jobs[-1]["scientific_artifact_bucket"] == "silver"
    assert runtime.kafka.jobs[-1]["scientific_artifact_key"].endswith("/document.json")
    rejected = store.jobs(state="REJECTED")
    assert len(rejected) == 1
    assert rejected[0]["reason"].startswith("scientific artifact object is missing")


def test_candidate_without_structured_uri_is_an_auditable_rejection(tmp_path: Path) -> None:
    gold = _gold_candidate().model_copy(update={"scientific_artifact_s3_uri": None})
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    published_jobs: list[dict[str, Any]] = []
    runtime = object.__new__(WorkerRuntime)
    runtime.config = FoundryConfig(providers={})
    runtime.store = store
    runtime.kafka = SimpleNamespace(
        event=lambda _event: None,
        artifact=lambda _artifact: None,
        job=published_jobs.append,
        flush=lambda: None,
    )
    runtime.lakehouse = SimpleNamespace(
        add_event=lambda _event: None,
        add_artifact=lambda _artifact: None,
        flush=lambda: None,
    )

    result = runtime.process(gold.model_dump_json().encode())

    assert result["status"] == "posttrain_preflight_rejected"
    assert result["state"] == "REJECTED"
    assert result["rejection_reason"].startswith("scientific artifact URI is absent")
    assert len(store.jobs(state="REJECTED")) == 1
    assert len(published_jobs) == 1
    assert published_jobs[0]["job_id"] == result["job_id"]
    assert published_jobs[0]["status"] == "posttrain_preflight_rejected"


def test_nonpaper_posttrain_pool_row_is_not_sent_to_paper_foundry(tmp_path: Path) -> None:
    gold = _gold_candidate().model_copy(
        update={
            "source_feed": "hf-models",
            "source_format": "web",
            "extraction_pipeline": "hf-model-card-markdown-v1",
            "scientific_artifact_s3_uri": None,
            "training_usage": "posttrain_transform_only",
        }
    )
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    published_jobs: list[dict[str, Any]] = []
    runtime = object.__new__(WorkerRuntime)
    runtime.config = FoundryConfig(providers={})
    runtime.store = store
    runtime.kafka = SimpleNamespace(
        event=lambda _event: None,
        artifact=lambda _artifact: None,
        job=published_jobs.append,
        flush=lambda: None,
    )

    result = runtime.process(gold.model_dump_json().encode())

    assert result == {
        "doc_id": gold.doc_id,
        "status": "unsupported_posttrain_source",
    }
    assert store.queued_candidates() == 0
    assert published_jobs == []


def test_interrupted_provider_calls_are_identified_until_terminal(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    job_id, _ = store.start_job(
        paper_id="paper",
        paper_hash=sha256("paper"),
        doc_id=f"sha256:{'b' * 64}",
        policy_version="v1",
    )
    metadata = {"provider": "hetzner", "role": "solver_a"}
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="CALL_PLANNED",
        metadata=metadata,
        attempt=1,
        idempotency_suffix="solver_a:CALL_PLANNED",
    )
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="CALL_STARTED",
        metadata=metadata,
        attempt=1,
        idempotency_suffix="solver_a:CALL_STARTED",
    )

    assert store.interrupted_provider_calls() == [
        {
            "job_id": job_id,
            "paper_id": "paper",
            "attempt": 1,
            "role": "solver_a",
            "provider": "hetzner",
            "was_started": True,
        }
    ]
    store.append_event(
        job_id=job_id,
        paper_id="paper",
        state="CALL_FAILED",
        metadata=metadata,
        attempt=1,
        idempotency_suffix="restart-recovery:solver_a",
    )
    assert store.interrupted_provider_calls() == []


def test_only_worker_startup_recovers_processing_candidates(tmp_path: Path) -> None:
    path = tmp_path / "control.sqlite3"
    worker = FoundryStore(str(path))
    worker.enqueue_candidate(
        doc_id="paper",
        payload=b"paper",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    cutoff = datetime.now(UTC) + timedelta(seconds=1)
    assert worker.claim_candidate(cutoff_at=cutoff) == ("paper", b"paper")
    worker.close()

    api = FoundryStore(str(path))
    assert api.queued_candidates() == 0
    assert api.claim_candidate(cutoff_at=cutoff) is None
    api.close()

    restarted_worker = FoundryStore(str(path), recover_processing=True)
    assert restarted_worker.queued_candidates() == 1
    assert restarted_worker.claim_candidate(cutoff_at=cutoff) == ("paper", b"paper")


def test_daily_run_freezes_even_an_empty_24_hour_cohort(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    run_day = date(2026, 8, 19)
    boundary = datetime.now(UTC)
    empty = store.start_daily_run(run_day, boundary_at=boundary)
    assert empty["state"] == "completed"
    assert empty["candidate_count"] == 0
    store.enqueue_candidate(
        doc_id="first",
        payload=b"first",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    assert store.start_daily_run(run_day, boundary_at=boundary)["state"] == "completed"

    next_day = run_day + timedelta(days=1)
    first = store.start_daily_run(next_day, boundary_at=boundary + timedelta(days=1))
    assert first["state"] == "running"
    assert first["candidate_count"] == 1
    cutoff = datetime.fromisoformat(str(first["cutoff_at"]))
    cutoff_ordinal = int(first["cutoff_ordinal"])
    store.enqueue_candidate(
        doc_id="after-cutoff",
        payload=b"after-cutoff",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    assert store.claim_candidate(cutoff_at=cutoff, cutoff_ordinal=cutoff_ordinal) == (
        "first",
        b"first",
    )
    assert store.claim_candidate(cutoff_at=cutoff, cutoff_ordinal=cutoff_ordinal) is None
    store.finish_candidate("first")
    store.finish_daily_run(next_day, state="completed", reason="test")
    store.enqueue_candidate(
        doc_id="later",
        payload=b"later",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    assert (
        store.start_daily_run(next_day, boundary_at=boundary + timedelta(days=1))["state"]
        == "completed"
    )


def test_daily_run_keeps_only_ranked_limit_and_clears_frozen_queue(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    boundary = datetime.now(UTC) + timedelta(seconds=1)
    for index in range(25):
        store.enqueue_candidate(
            doc_id=f"paper-{index:02d}",
            payload=f"paper-{index:02d}".encode(),
            reasoning_score=index / 25,
            quality_score=5.0,
            ranking_score=index / 25,
            valid_from=FIXED_TIME,
        )

    run = store.start_daily_run(boundary.date(), boundary_at=boundary, candidate_limit=20)

    assert run["candidate_count"] == 20
    retained = {
        str(row["doc_id"])
        for row in store._conn.execute(
            "SELECT doc_id FROM candidate_queue ORDER BY doc_id"
        ).fetchall()
    }
    assert retained == {f"paper-{index:02d}" for index in range(5, 25)}
    first = store.claim_candidate(
        cutoff_at=boundary,
        cutoff_ordinal=int(run["cutoff_ordinal"]),
        daily_run_date=boundary.date(),
    )
    assert first == ("paper-24", b"paper-24")


def test_changed_daily_boundary_replaces_same_day_snapshot(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    midnight = datetime(2026, 8, 27, 0, tzinfo=UTC)
    assert store.start_daily_run(midnight.date(), boundary_at=midnight)["candidate_count"] == 0
    store.enqueue_candidate(
        doc_id="afternoon-paper",
        payload=b"paper",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )

    moved = store.start_daily_run(
        midnight.date(),
        boundary_at=midnight.replace(hour=14),
        candidate_limit=20,
    )

    assert moved["candidate_count"] == 1
    assert moved["cutoff_at"] == midnight.replace(hour=14).isoformat()


def test_foundry_config_parses_daily_not_before_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("S2P_FOUNDRY_DAILY_NOT_BEFORE_UTC", "2026-08-27T14:00:00Z")
    config = FoundryConfig.from_env()
    assert config.daily_not_before_utc == datetime(2026, 8, 27, 14, tzinfo=UTC)


def test_manual_run_snapshots_queue_and_coalesces_clicks(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    empty, created = store.request_manual_run()
    assert created
    assert empty["state"] == "completed"

    store.enqueue_candidate(
        doc_id="manual-paper",
        payload=b"paper",
        reasoning_score=1.0,
        quality_score=5.0,
        valid_from=FIXED_TIME,
    )
    requested, created = store.request_manual_run()
    assert created
    assert requested["state"] == "pending"
    duplicate, duplicate_created = store.request_manual_run()
    assert not duplicate_created
    assert duplicate["run_id"] == requested["run_id"]

    claimed = store.claim_manual_run()
    assert claimed is not None and claimed["state"] == "running"
    store.record_manual_processed(str(claimed["run_id"]))
    store.finish_manual_run(
        str(claimed["run_id"]), state="completed", reason="ranked snapshot exhausted"
    )
    completed = store.manual_runs()[0]
    assert completed["processed_count"] == 1
    assert completed["state"] == "completed"


def test_manual_run_discards_stale_candidates_and_freezes_fresh_cohort(
    tmp_path: Path,
) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    for doc_id in ("stale", "fresh"):
        store.enqueue_candidate(
            doc_id=doc_id,
            payload=doc_id.encode(),
            reasoning_score=1.0,
            quality_score=5.0,
            valid_from=FIXED_TIME,
        )
    store._conn.execute(
        "UPDATE candidate_queue SET enqueued_at=? WHERE doc_id='stale'",
        ((datetime.now(UTC) - timedelta(hours=25)).isoformat(),),
    )
    store._conn.commit()

    requested, created = store.request_manual_run()

    assert created
    assert requested["candidate_count"] == 1
    assert (
        store._conn.execute("SELECT COUNT(*) FROM candidate_queue WHERE doc_id='stale'").fetchone()[
            0
        ]
        == 0
    )
    claimed = store.claim_candidate(
        cutoff_at=datetime.fromisoformat(str(requested["cutoff_at"])),
        cutoff_ordinal=int(requested["cutoff_ordinal"]),
    )
    assert claimed == ("fresh", b"fresh")


def test_manual_run_can_bound_the_validation_snapshot(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    for index in range(3):
        store.enqueue_candidate(
            doc_id=f"manual-paper-{index}",
            payload=b"paper",
            reasoning_score=1.0,
            quality_score=5.0,
            valid_from=FIXED_TIME,
        )
    requested, created = store.request_manual_run(max_candidates=1)
    assert created
    assert requested["candidate_count"] == 1
    assert requested["max_candidates"] == 1


def test_manual_run_rejects_nonpositive_bound(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    with pytest.raises(ValueError, match="must be positive"):
        store.request_manual_run(max_candidates=0)


def test_daily_snapshot_yields_to_a_pending_manual_run() -> None:
    runtime = object.__new__(WorkerRuntime)
    runtime.config = SimpleNamespace(queue_poll_seconds=0)
    runtime._drain_stop = threading.Event()
    current = datetime.now(UTC)
    calls: list[str] = []
    manual = {
        "run_id": "manual-1",
        "cutoff_at": FIXED_TIME.isoformat(),
        "cutoff_ordinal": 1,
        "max_candidates": 1,
        "processed_count": 0,
    }
    claims = iter([manual, None])
    runtime.store = SimpleNamespace(
        claim_manual_run=lambda: next(claims),
        daily_run=lambda _day: {"candidate_count": 1, "processed_count": 0},
        finish_daily_run=lambda *_args, **_kwargs: calls.append("daily-finished"),
    )
    runtime._run_manual_snapshot = lambda _run, _log: calls.append("manual")

    def drain_one(**_kwargs: object) -> dict[str, str]:
        calls.append("daily")
        return {"status": "queue_empty"}

    runtime._drain_one = drain_one

    runtime._run_daily_snapshot(
        current.date(),
        {"cutoff_at": current.isoformat(), "cutoff_ordinal": 1},
        SimpleNamespace(),
    )

    assert calls == ["manual", "daily", "daily-finished"]


def test_repeated_model_snapshot_clears_transient_drift(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    first = ProviderModelSnapshot(
        provider="hetzner",
        discovered_at=FIXED_TIME,
        response_hash=sha256({"models": ["a"]}),
        models=[{"id": "a"}],
        configured_model_ids=["a"],
    )
    changed = first.model_copy(
        update={
            "discovered_at": FIXED_TIME + timedelta(seconds=1),
            "response_hash": sha256({"models": ["a", "b"]}),
            "models": [{"id": "a"}, {"id": "b"}],
        }
    )
    assert not store.record_model_snapshot(first).drifted
    assert store.record_model_snapshot(changed).drifted
    repeated = store.record_model_snapshot(
        changed.model_copy(update={"discovered_at": FIXED_TIME + timedelta(seconds=2)})
    )
    assert not repeated.drifted
    assert not store.model_snapshots()[0]["drifted"]


def test_artifact_human_audits_are_append_only_and_latest_is_inspectable(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    job_id, _ = store.start_job(
        paper_id="paper",
        paper_hash=sha256("paper"),
        doc_id="doc",
        policy_version="v1",
    )
    validation = ValidationReport(
        task_id="task",
        positive_pass=True,
        equivalent_pass=True,
        adversarial_pass=True,
        mutation_killed=1,
        mutation_total=1,
        metamorphic_pass=True,
        replay_pass=True,
        security_pass=True,
        false_positive_count=0,
        false_negative_count=0,
    )
    store.record_artifact(
        FoundryArtifactRecord(
            artifact_id="artifact",
            job_id=job_id,
            paper_id="paper",
            task_id="task",
            family="claim_evidence",
            kind="sft_trajectory",
            pool="sft",
            dataset_split="train",
            status="accepted",
            quality_label="verified_automatic",
            package_hash=sha256("package"),
            environment_hash=sha256("environment"),
            paper_hash=sha256("paper"),
            provider_trace_ids=[],
            constructor_family="qwen",
            critic_family="qwen",
            validation=validation,
            created_at=FIXED_TIME,
        )
    )

    first = store.audit_artifact(
        artifact_id="artifact",
        decision="approved",
        reviewer="First Reviewer",
    )
    second = store.audit_artifact(
        artifact_id="artifact",
        decision="rejected",
        reviewer="Second Reviewer",
        reason="Manual evidence mismatch",
    )
    # SQLite timestamps can legitimately tie on hosts whose wall-clock
    # resolution is coarser than two consecutive review writes. The latest
    # audit must still follow append order, not the random UUID ordering.
    store._conn.execute(
        "UPDATE artifact_audits SET created_at=? WHERE artifact_id=?",
        (FIXED_TIME.isoformat(), "artifact"),
    )

    history = store.artifact_audits(artifact_id="artifact")
    assert [value["audit_id"] for value in history] == [second.audit_id, first.audit_id]
    assert history[1]["decision"] == "approved"
    assert store.artifacts()[0]["human_audit"]["audit_id"] == second.audit_id
    artifact = store.artifact("artifact")
    assert artifact is not None
    assert artifact["human_audit"]["audit_id"] == second.audit_id
    assert store.dashboard()["human_audits"] == {"rejected": 1}


def test_pool_allocation_is_exact_per_five_and_retry_stable(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    sft = [
        store.assign_pool_split(allocation_key=f"sft:paper:{i}", pool="sft") for i in range(1, 11)
    ]
    rl = [store.assign_pool_split(allocation_key=f"rl:paper:{i}", pool="rl") for i in range(1, 6)]

    assert [split for split, _ in sft] == [
        "train",
        "train",
        "train",
        "train",
        "benchmark",
        "train",
        "train",
        "train",
        "train",
        "benchmark",
    ]
    assert [split for split, _ in rl] == [
        "train",
        "train",
        "train",
        "train",
        "benchmark",
    ]
    assert store.assign_pool_split(allocation_key="sft:paper:5", pool="sft") == ("benchmark", 5)


def test_all_tasks_from_one_paper_share_the_pool_split(tmp_path: Path) -> None:
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    first = store.assign_pool_split(allocation_key="rl:2608.00001", pool="rl")
    second = store.assign_pool_split(allocation_key="rl:2608.00001", pool="rl")
    assert first == second


def test_every_foundry_role_uses_the_single_hetzner_route() -> None:
    assert set(ROLE_PROVIDER.values()) == {"hetzner"}


def test_acceptance_suite_and_signed_package_are_reproducible(tmp_path: Path) -> None:
    bundle = _bundle()
    task = _task()
    graph = _graph()
    spec = _spec()
    trajectories = [
        Trajectory(
            trajectory_id=f"trajectory:{index}",
            task_id=task.task_id,
            provider_trace_id=f"trace:{index}",
            answer=_answer(),
            accepted=False,
            reward=0.0,
        )
        for index in range(2)
    ]
    report, validated, cases = run_acceptance_suite(
        task=task,
        spec=spec,
        bundle=bundle,
        graph=graph,
        trajectories=trajectories,
    )
    assert suite_passes(report)
    assert report.mutation_total > 0

    packager = EnvironmentPackager(signer=AttestationSigner())
    first = packager.build(
        bundle=bundle,
        graph=graph,
        task=task,
        trajectories=validated,
        validation=report,
        traces=[_trace()],
        verifier=spec,
        pool="rl",
        dataset_split="train",
        validation_cases=cases,
    )
    second = packager.build(
        bundle=bundle,
        graph=graph,
        task=task,
        trajectories=validated,
        validation=report,
        traces=[_trace()],
        verifier=spec,
        pool="rl",
        dataset_split="train",
        validation_cases=cases,
    )

    assert first.package_hash == second.package_hash
    assert verify_signature(first.content, first.signature_b64, first.signer_cert_pem)
    inspection = inspect_package(first.content)
    assert inspection["task"]["task_id"] == task.task_id
    assert len(inspection["trajectories"]) == len(validated)
    assert inspection["verifier"]["task_id"] == task.task_id
    assert inspection["validation"]["report"]["adversarial_pass"] is True
    assert any(value["path"] == "prompt.json" for value in inspection["files"])
    with tarfile.open(fileobj=io.BytesIO(first.content), mode="r:gz") as archive:
        names = set(archive.getnames())
        mutation = archive.extractfile("paper_environment/validation/mutations.jsonl")
        taskset = archive.extractfile("paper_environment/prime_verifiers/paper_foundry/taskset.py")
        assert mutation is not None and mutation.read().strip()
        assert taskset is not None and b"class PaperFoundryTools" in taskset.read()
        assert "paper_environment/hidden/verifier.py" in names

        # The normal project test environment keeps the sizeable training
        # framework optional. CI runs this same test once with the exact pin so
        # the generated package must import, load, and execute its reward.
        if importlib.util.find_spec("verifiers") is not None:
            archive.extractall(tmp_path, filter="data")
            environment_root = tmp_path / "paper_environment"
            export_root = environment_root / "prime_verifiers"
            sys.path[:0] = [str(environment_root), str(export_root)]
            try:
                from paper_foundry.taskset import PaperFoundryConfig, PaperFoundryTaskset

                config = PaperFoundryConfig()
                loaded = PaperFoundryTaskset(config).load()
                assert len(loaded) == 1
                assert loaded[0].data.network_allow == []
                reward = asyncio.run(
                    loaded[0].scientific_reward(
                        SimpleNamespace(
                            last_reply=validated[0].answer.model_dump_json(),
                            tool_messages=[],
                        )
                    )
                )
                assert reward == 1.0
            finally:
                del sys.path[:2]
                for module_name in tuple(sys.modules):
                    if module_name == "paper_foundry" or module_name.startswith(
                        ("paper_foundry.", "public_tools", "hidden")
                    ):
                        sys.modules.pop(module_name, None)


def test_relation_only_required_nodes_produce_effective_mutations() -> None:
    bundle = _bundle()
    edge = EvidenceEdge(source="claim:1", relation="depends_on", target="method:1")
    graph = PaperEvidenceGraph(
        graph_id="graph:relation-only",
        paper_id=bundle.paper_id,
        nodes=[
            *_graph().nodes,
            EvidenceNode(
                id="method:1",
                type="method_step",
                canonical_text="The method supports the claim.",
                supporting_spans=["section-1.span1"],
            ),
        ],
        edges=[edge],
    )
    task = _task().model_copy(
        update={
            "hidden_targets": HiddenTargets(
                required_nodes=["claim:1", "method:1"],
                required_relations=[edge],
                accepted_evidence_sets=[["section-1.span1"]],
            )
        }
    )
    answer = _answer().model_copy(
        update={
            "answer_manifest": AnswerManifest(
                evidence=["section-1.span1"],
                relations=[edge],
            )
        }
    )
    spec = normalize_spec(
        VerifierSpec(
            verifier_id="relation-only",
            task_id=task.task_id,
            version=1,
            determinism_seed=1,
            predicates=[
                VerifierPredicate(
                    id="nodes",
                    type="required_nodes",
                    targets=["claim:1", "method:1"],
                    weight=0.5,
                ),
                VerifierPredicate(
                    id="relations",
                    type="required_relations",
                    weight=0.5,
                ),
            ],
        ),
        task,
        bundle,
        graph,
    )
    report, _validated, cases = run_acceptance_suite(
        task=task,
        spec=spec,
        bundle=bundle,
        graph=graph,
        trajectories=[
            Trajectory(
                trajectory_id="trajectory:relation-only",
                task_id=task.task_id,
                provider_trace_id="trace:relation-only",
                answer=answer,
                accepted=False,
                reward=0.0,
            )
        ],
    )

    assert report.mutation_total == 4
    assert report.mutation_killed == report.mutation_total
    assert len(cases["mutations"]) == report.mutation_total
    assert suite_passes(report)


def test_required_node_mutations_remove_every_structured_commitment() -> None:
    bundle = _bundle()
    graph = PaperEvidenceGraph(
        graph_id="graph:structured-commitments",
        paper_id=bundle.paper_id,
        nodes=[
            *_graph().nodes,
            EvidenceNode(
                id="qualification:1",
                type="claim",
                canonical_text="The result requires a qualification.",
                supporting_spans=["section-1.span1"],
            ),
            EvidenceNode(
                id="numeric:1",
                type="metric",
                canonical_text="The measured result is one.",
                supporting_spans=["section-1.span1"],
            ),
        ],
        edges=[],
    )
    task = _task().model_copy(
        update={
            "hidden_targets": HiddenTargets(
                required_nodes=["qualification:1", "numeric:1"],
            )
        }
    )
    answer = _answer().model_copy(
        update={
            "answer_manifest": AnswerManifest(
                qualifications=["qualification:1"],
                numeric_results=[NumericResult(id="numeric:1", value=1.0)],
            )
        }
    )
    spec = normalize_spec(
        VerifierSpec(
            verifier_id="structured-commitments",
            task_id=task.task_id,
            version=1,
            determinism_seed=1,
            predicates=[
                VerifierPredicate(
                    id="nodes",
                    type="required_nodes",
                    targets=task.hidden_targets.required_nodes,
                    weight=1.0,
                )
            ],
        ),
        task,
        bundle,
        graph,
    )
    report, _validated, cases = run_acceptance_suite(
        task=task,
        spec=spec,
        bundle=bundle,
        graph=graph,
        trajectories=[
            Trajectory(
                trajectory_id="trajectory:structured-commitments",
                task_id=task.task_id,
                provider_trace_id="trace:structured-commitments",
                answer=answer,
                accepted=False,
                reward=0.0,
            )
        ],
    )

    assert report.mutation_total == 2
    assert report.mutation_killed == 2
    assert len(cases["mutations"]) == 2
    assert report.positive_pass


def test_artifact_inspector_returns_the_exact_packaged_dataset(tmp_path: Path) -> None:
    bundle = _bundle()
    task = _task()
    graph = _graph()
    spec = _spec()
    report, trajectories, cases = run_acceptance_suite(
        task=task,
        spec=spec,
        bundle=bundle,
        graph=graph,
        trajectories=[
            Trajectory(
                trajectory_id="trajectory:inspect",
                task_id=task.task_id,
                provider_trace_id="trace:inspect",
                answer=_answer(),
                accepted=False,
                reward=0.0,
            )
        ],
    )
    package = EnvironmentPackager(signer=AttestationSigner()).build(
        bundle=bundle,
        graph=graph,
        task=task,
        trajectories=trajectories,
        validation=report,
        traces=[_trace("trace:inspect")],
        verifier=spec,
        pool="rl",
        dataset_split="train",
        validation_cases=cases,
    )
    store = FoundryStore(str(tmp_path / "control.sqlite3"))
    job_id, _ = store.start_job(
        paper_id=bundle.paper_id,
        paper_hash=bundle.paper_hash,
        doc_id="doc:inspect",
        policy_version="v1",
    )
    store.save_bundle(job_id, bundle.model_dump_json().encode())
    store.save_graph(job_id, graph.model_dump_json().encode())
    store.record_artifact(
        FoundryArtifactRecord(
            artifact_id="rl-environment:inspect",
            job_id=job_id,
            paper_id=bundle.paper_id,
            task_id=task.task_id,
            family=task.family,
            kind="rl_environment",
            pool="rl",
            dataset_split="train",
            status="accepted",
            quality_label="verified_adversarial",
            package_uri="s3://posttrain/inspect.tar.gz",
            package_hash=package.package_hash,
            environment_hash=package.environment_hash,
            paper_hash=bundle.paper_hash,
            provider_trace_ids=["trace:inspect"],
            constructor_family="qwen",
            critic_family="qwen",
            validation=report,
            created_at=FIXED_TIME,
        )
    )

    class PackageClient:
        def get_object(self, **_: Any) -> dict[str, io.BytesIO]:
            return {"Body": io.BytesIO(package.content)}

    value = ArtifactInspector(store=store, s3_client=PackageClient()).inspect(
        "rl-environment:inspect"
    )
    assert value is not None
    assert value["source"] == "package"
    assert value["task"]["public_instruction"] == task.public_instruction
    assert value["trajectories"][0]["answer"]["report"] == _answer().report
    assert value["validation"]["mutations"]


def test_frozen_tools_reject_code_execution_without_an_arbitrary_call_cap() -> None:
    runtime = PaperRuntime(spans={"s1": "bounded paper evidence"})
    assert runtime.calculator("2 + 2") == 4.0
    with pytest.raises(ToolError):
        runtime.symbolic("simplify", "__import__('os').system('id')")
    assert runtime.search("evidence")
    assert runtime.search("evidence")


def test_invalid_calculator_syntax_is_a_tool_error() -> None:
    runtime = PaperRuntime(spans={"s1": "bounded paper evidence"})
    with pytest.raises(ToolError, match="invalid calculator expression"):
        runtime.calculator("\\frac{1}{2}")


def test_solver_rejects_repeated_invalid_tool_request() -> None:
    class RepeatingToolControl:
        def call(self, **_: Any) -> tuple[dict[str, Any], ProviderTrace]:
            return (
                {
                    "status": "tool_request",
                    "report": None,
                    "answer_manifest": None,
                    "tool_calls": [
                        {
                            "tool": "calculator",
                            "arguments": {"expression": "\\frac{1}{2}"},
                        }
                    ],
                },
                _trace("trace:repeating-tool"),
            )

    task = _task().model_copy(
        update={
            "public_context_policy": _task().public_context_policy.model_copy(
                update={"tool_access": ["calculator"]}
            )
        }
    )
    with pytest.raises(TaskOutputError, match="repeated the same invalid"):
        TaskFactory(RepeatingToolControl())._solve_one(  # type: ignore[arg-type]
            job_id="job",
            bundle=_bundle(),
            graph=_graph(),
            task=task,
            role="solver_a",
            plan="test",
        )


def test_extended_environment_predicates_are_deterministic() -> None:
    bundle = _bundle()
    edge = EvidenceEdge(
        source="limitation:1",
        relation="qualifies",
        target="claim:1",
    )
    graph = PaperEvidenceGraph(
        graph_id="graph:extended",
        paper_id=bundle.paper_id,
        nodes=[
            *_graph().nodes,
            EvidenceNode(
                id="limitation:1",
                type="limitation",
                canonical_text="The result has a bounded scope.",
                supporting_spans=["section-1.span1"],
                confidence=1.0,
            ),
        ],
        edges=[edge],
    )
    task = _task().model_copy(
        update={
            "hidden_targets": HiddenTargets(
                required_nodes=["claim:1"],
                required_relations=[edge],
                required_qualifications=["limitation:1"],
                accepted_evidence_sets=[["section-1.span1"]],
                configuration_constraints={
                    "required_values": {"optimizer": "adamw"},
                    "ranges": {"learning_rate": [0.0001, 0.001]},
                    "forbidden_keys": ["test_answer"],
                },
            )
        }
    )
    spec = normalize_spec(
        VerifierSpec(
            verifier_id="extended",
            task_id=task.task_id,
            version=1,
            predicates=[
                VerifierPredicate(
                    id="relations",
                    type="required_relations",
                    weight=0.34,
                ),
                VerifierPredicate(
                    id="qualifications",
                    type="required_qualifications",
                    weight=0.33,
                ),
                VerifierPredicate(
                    id="configuration",
                    type="configuration_constraints",
                    weight=0.33,
                ),
            ],
            determinism_seed=1,
        ),
        task,
        bundle,
        graph,
    )
    answer = _answer().model_copy(
        update={
            "answer_manifest": _answer().answer_manifest.model_copy(
                update={
                    "relations": [edge],
                    "qualifications": ["limitation:1"],
                    "configuration": {
                        "optimizer": "adamw",
                        "learning_rate": 0.0005,
                    },
                }
            )
        }
    )
    assert evaluate(spec, answer, task=task, graph=graph, bundle=bundle).passed
    broken = answer.model_copy(
        update={
            "answer_manifest": answer.answer_manifest.model_copy(
                update={"configuration": {"optimizer": "sgd"}}
            )
        }
    )
    assert not evaluate(spec, broken, task=task, graph=graph, bundle=bundle).passed


def test_kubernetes_oracle_job_is_digest_pinned_and_network_is_disabled() -> None:
    recipe = OracleRecipe(
        oracle_id="oracle:1",
        image="registry.example/oracle@sha256:" + "a" * 64,
        embedded_artifact_hash=sha256("artifact"),
        command=["/oracle", "--json"],
        cpu_millis=500,
        memory_mib=512,
        timeout_seconds=60,
    )
    manifest = kubernetes_job_manifest(
        name="oracle-test",
        namespace="stream2pretrain",
        recipe=recipe,
        runtime_class="gvisor",
    )
    pod = manifest["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert pod["automountServiceAccountToken"] is False
    assert pod["runtimeClassName"] == "gvisor"
    assert container["image"].endswith("a" * 64)
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_oracle_artifact_tree_hash_is_stable_and_symlinks_are_rejected(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    (artifact / "result.json").write_text('{"value": 1}', encoding="utf-8")
    first = tree_hash(artifact)
    second = tree_hash(artifact)
    assert first == second
    try:
        (artifact / "link").symlink_to(artifact / "result.json")
    except OSError as exc:
        pytest.skip(f"host does not permit symlink creation: {exc}")
    with pytest.raises(ValueError, match="symlink"):
        tree_hash(artifact)
