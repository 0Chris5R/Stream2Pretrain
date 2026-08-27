"""Rubric compilation and deterministic scientific predicate runtime."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from processor.foundry.control import ProviderControlPlane
from processor.foundry.symbolic import (
    symbolic_expression_is_checkable,
    symbolically_equivalent,
)
from processor.foundry.util import canonical_json, stable_id
from schemas.foundry import (
    FoundryAnswer,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderTrace,
    TaskSpec,
    ToolCall,
    VerifierPredicate,
    VerifierSpec,
)


class VerifierCritique(BaseModel):
    model_config = ConfigDict(extra="forbid")
    accepted: bool
    findings: list[str] = Field(default_factory=list)
    false_positive_risks: list[str] = Field(default_factory=list)
    false_negative_risks: list[str] = Field(default_factory=list)
    repair_instructions: list[str] = Field(default_factory=list)


@dataclass(frozen=True, slots=True)
class PredicateResult:
    predicate_id: str
    passed: bool
    score: float
    required: bool
    details: str


@dataclass(frozen=True, slots=True)
class RewardResult:
    reward: float
    passed: bool
    predicates: list[PredicateResult]


class VerifierCompiler:
    def __init__(self, control: ProviderControlPlane) -> None:
        self.control = control

    def compile(
        self,
        *,
        job_id: str,
        bundle: PaperBundle,
        graph: PaperEvidenceGraph,
        task: TaskSpec,
    ) -> tuple[VerifierSpec, list[ProviderTrace]]:
        data, compiler_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="verifier_compiler",
            system=_compiler_system(),
            user=_compiler_prompt(bundle, graph, task),
            max_output_tokens=8_000,
            call_key=f"verifier_compiler:{task.task_id}",
        )
        compiler_findings: list[dict[str, Any]] = []
        try:
            raw_spec = VerifierSpec.model_validate(data)
            spec = normalize_spec(raw_spec, task, bundle, graph)
        except (TypeError, ValueError) as exc:
            compiler_findings.append(
                {
                    "stage": "compiler_normalization",
                    "accepted": False,
                    "findings": [str(exc)],
                    "action": "deterministic_fallback",
                }
            )
            spec = deterministic_verifier(task, bundle, graph)
        critique_data, critic_trace = self.control.call(
            job_id=job_id,
            paper_id=bundle.paper_id,
            role="verifier_critic",
            system=_critic_system(),
            user=_critic_prompt(bundle, graph, task, spec),
            max_output_tokens=6_000,
            call_key=f"verifier_critic:{task.task_id}",
        )
        critique = VerifierCritique.model_validate(critique_data)
        critic_audit = [*compiler_findings, _critic_audit("initial", critique)]
        traces = [compiler_trace, critic_trace]
        if not critique.accepted:
            repair_data, repair_trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role="final_repair",
                system=_compiler_system(),
                user=_repair_prompt(bundle, graph, task, spec, critique),
                max_output_tokens=8_000,
                call_key=f"verifier_repair:{task.task_id}",
            )
            traces.append(repair_trace)
            try:
                spec = normalize_spec(
                    VerifierSpec.model_validate(repair_data),
                    task,
                    bundle,
                    graph,
                )
            except ValueError as exc:
                critic_audit.append(
                    {
                        "stage": "repair_normalization",
                        "accepted": False,
                        "findings": [str(exc)],
                        "action": "deterministic_fallback",
                    }
                )
                spec = deterministic_verifier(task, bundle, graph)
            recheck_data, recheck_trace = self.control.call(
                job_id=job_id,
                paper_id=bundle.paper_id,
                role="verifier_critic",
                system=_critic_system(),
                user=_critic_prompt(bundle, graph, task, spec),
                max_output_tokens=6_000,
                call_key=f"verifier_critic:{task.task_id}:post_repair",
            )
            traces.append(recheck_trace)
            recheck = VerifierCritique.model_validate(recheck_data)
            critic_audit.append(_critic_audit("post_repair", recheck))
            if not recheck.accepted:
                # A model critic may identify a real risk, but it is not itself
                # a deterministic verifier. Fall back to the task-derived
                # verifier and let the complete executable suite decide.
                spec = deterministic_verifier(task, bundle, graph)
                critic_audit.append(
                    {
                        "stage": "post_repair",
                        "accepted": False,
                        "findings": ["bounded repair rejected by independent critic"],
                        "action": "deterministic_fallback",
                    }
                )
        return spec.model_copy(update={"critic_audit": critic_audit}), traces


def normalize_spec(
    spec: VerifierSpec,
    task: TaskSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
) -> VerifierSpec:
    node_ids = {node.id for node in graph.nodes}
    span_ids = {span.span_id for span in bundle.stable_spans}
    expected_values = task.hidden_targets.expected_values
    expected_value_ids = set(expected_values)
    method_order = [
        [edge.source, edge.target]
        for edge in task.hidden_targets.required_relations
        if edge.relation == "precedes"
    ]
    node_types = {node.id: node.type for node in graph.nodes}
    derivation_order = _derivation_order(task, node_types)
    predicates: list[VerifierPredicate] = []
    for raw_predicate in spec.predicates:
        predicate = raw_predicate
        if predicate.type in {
            "nonempty_report",
            "manifest_required",
            "report_manifest_consistency",
        }:
            # These predicates address the answer contract itself. Providers
            # commonly label that contract as target="report", but no graph
            # node lookup is involved, so canonicalize the unused target fields
            # before validating graph references.
            predicate = predicate.model_copy(update={"target": None, "targets": []})
        elif predicate.type in {"required_nodes", "required_dependency_nodes"}:
            if not task.hidden_targets.required_nodes:
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": list(task.hidden_targets.required_nodes),
                }
            )
        elif predicate.type == "forbidden_nodes":
            if not task.hidden_targets.forbidden_nodes:
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": list(task.hidden_targets.forbidden_nodes),
                }
            )
        elif predicate.type == "evidence_membership":
            if task.family == "derivation_completion":
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": [],
                    "allowed_spans": sorted(task.public_context_policy.included_spans),
                }
            )
        elif predicate.type == "evidence_coverage":
            if task.family == "derivation_completion":
                continue
            config = dict(predicate.config)
            if task.hidden_targets.accepted_evidence_sets:
                config["accepted_sets"] = task.hidden_targets.accepted_evidence_sets
            elif not config.get("accepted_sets"):
                required_spans = config.get("required_spans")
                if isinstance(required_spans, list) and required_spans:
                    config["accepted_sets"] = [[str(value) for value in required_spans]]
            predicate = predicate.model_copy(
                update={"target": None, "targets": [], "config": config}
            )
        elif predicate.type == "required_relations":
            if not task.hidden_targets.required_relations:
                continue
            predicate = predicate.model_copy(update={"target": None, "targets": []})
        elif predicate.type == "method_partial_order":
            if not method_order:
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": [],
                    "config": {"precedes": method_order},
                }
            )
        elif predicate.type == "derivation_partial_order":
            if not derivation_order:
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": [],
                    "config": {"precedes": derivation_order},
                }
            )
        elif predicate.type == "required_qualifications":
            if not task.hidden_targets.required_qualifications:
                continue
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": list(task.hidden_targets.required_qualifications),
                }
            )
        elif predicate.type == "numeric_tolerance":
            numeric_ids = [
                key
                for key, value in expected_values.items()
                if isinstance(value, (int, float)) and not isinstance(value, bool)
            ]
            target = predicate.target
            if target not in expected_values and len(numeric_ids) == 1:
                target = numeric_ids[0]
            expected = expected_values.get(str(target)) if target else None
            if isinstance(expected, (int, float)) and not isinstance(expected, bool):
                numeric_expected = float(expected)
                predicate = predicate.model_copy(
                    update={
                        "target": target,
                        "expected": numeric_expected,
                        "tolerance": _numeric_tolerance(numeric_expected),
                    }
                )
            elif isinstance(expected, str) and task.family == "derivation_completion":
                predicate = VerifierPredicate(
                    id=predicate.id,
                    type="symbolic_equivalence",
                    target=str(target),
                    expected=expected,
                    weight=predicate.weight,
                    required=predicate.required,
                )
            elif isinstance(expected, str):
                predicate = VerifierPredicate(
                    id=predicate.id,
                    type="configuration_constraints",
                    weight=predicate.weight,
                    required=predicate.required,
                    config={"constraints": {"required_values": {str(target): expected}}},
                )
            else:
                continue
        elif predicate.type == "symbolic_equivalence":
            symbolic_ids = [key for key, value in expected_values.items() if isinstance(value, str)]
            target = predicate.target
            if target not in expected_values and len(symbolic_ids) == 1:
                target = symbolic_ids[0]
            if target not in expected_values:
                continue
            expected = expected_values[str(target)]
            if not isinstance(expected, str):
                continue
            if task.family == "derivation_completion":
                predicate = predicate.model_copy(update={"target": target, "expected": expected})
            else:
                predicate = VerifierPredicate(
                    id=predicate.id,
                    type="configuration_constraints",
                    weight=predicate.weight,
                    required=predicate.required,
                    config={"constraints": {"required_values": {str(target): expected}}},
                )
        elif predicate.type == "fault_identification":
            if not task.hidden_targets.required_faults:
                continue
            config = dict(predicate.config)
            forbidden = config.get("forbidden", config.get("forbidden_faults"))
            config["forbidden"] = (
                [str(value) for value in forbidden]
                if isinstance(forbidden, list)
                else list(task.hidden_targets.forbidden_faults)
            )
            predicate = predicate.model_copy(
                update={
                    "target": None,
                    "targets": list(task.hidden_targets.required_faults),
                    "config": config,
                }
            )
        elif predicate.type == "configuration_constraints":
            predicate = predicate.model_copy(update={"target": None, "targets": []})

        unknown_targets = set(predicate.targets) - node_ids - span_ids - expected_value_ids
        if (
            predicate.target
            and predicate.target not in node_ids
            and predicate.target not in span_ids
            and predicate.target not in expected_value_ids
        ):
            unknown_targets.add(predicate.target)
        if set(predicate.allowed_spans) - span_ids:
            raise ValueError(f"verifier {spec.verifier_id} contains unknown allowed spans")
        if unknown_targets and predicate.type not in {
            "configuration_constraints",
            "fault_identification",
        }:
            raise ValueError(
                f"verifier {spec.verifier_id} contains unknown targets: {unknown_targets}"
            )
        predicates.append(predicate)

    baseline = {
        "nonempty_report": VerifierPredicate(
            id="hard:nonempty_report",
            type="nonempty_report",
            weight=0.0,
            required=True,
        ),
        "manifest_required": VerifierPredicate(
            id="hard:manifest_required",
            type="manifest_required",
            weight=0.0,
            required=True,
        ),
        "report_manifest_consistency": VerifierPredicate(
            id="hard:report_manifest_consistency",
            type="report_manifest_consistency",
            weight=0.0,
            required=True,
        ),
    }
    if task.hidden_targets.required_nodes:
        baseline["required_nodes"] = VerifierPredicate(
            id="hard:required_nodes",
            type="required_nodes",
            targets=task.hidden_targets.required_nodes,
            weight=0.0,
            required=True,
        )
    if task.hidden_targets.forbidden_nodes:
        baseline["forbidden_nodes"] = VerifierPredicate(
            id="hard:forbidden_nodes",
            type="forbidden_nodes",
            targets=task.hidden_targets.forbidden_nodes,
            weight=0.0,
            required=True,
        )
    if task.hidden_targets.accepted_evidence_sets and task.family != "derivation_completion":
        baseline["evidence_membership"] = VerifierPredicate(
            id="hard:evidence_membership",
            type="evidence_membership",
            allowed_spans=sorted(task.public_context_policy.included_spans),
            weight=0.0,
            required=True,
        )
        baseline["evidence_coverage"] = VerifierPredicate(
            id="hard:evidence_coverage",
            type="evidence_coverage",
            weight=0.0,
            required=True,
            config={"accepted_sets": task.hidden_targets.accepted_evidence_sets},
        )
    if task.hidden_targets.required_relations:
        baseline["required_relations"] = VerifierPredicate(
            id="hard:required_relations",
            type="required_relations",
            weight=0.0,
            required=True,
        )
        if method_order:
            baseline["method_partial_order"] = VerifierPredicate(
                id="hard:method_partial_order",
                type="method_partial_order",
                weight=0.0,
                required=True,
                config={"precedes": method_order},
            )
        if task.family == "derivation_completion" and derivation_order:
            baseline["derivation_partial_order"] = VerifierPredicate(
                id="hard:derivation_partial_order",
                type="derivation_partial_order",
                weight=0.0,
                required=True,
                config={"precedes": derivation_order},
            )
    if task.hidden_targets.required_qualifications:
        baseline["required_qualifications"] = VerifierPredicate(
            id="hard:required_qualifications",
            type="required_qualifications",
            targets=task.hidden_targets.required_qualifications,
            weight=0.0,
            required=True,
        )
    if task.hidden_targets.required_faults:
        baseline["fault_identification"] = VerifierPredicate(
            id="hard:fault_identification",
            type="fault_identification",
            targets=task.hidden_targets.required_faults,
            weight=0.0,
            required=True,
            config={"forbidden": task.hidden_targets.forbidden_faults},
        )
    if task.hidden_targets.configuration_constraints:
        baseline["configuration_constraints"] = VerifierPredicate(
            id="hard:configuration_constraints",
            type="configuration_constraints",
            weight=0.0,
            required=True,
            config={"constraints": task.hidden_targets.configuration_constraints},
        )
    existing_types = {predicate.type for predicate in predicates}
    numeric_targets = {
        predicate.target
        for predicate in predicates
        if predicate.type == "numeric_tolerance" and predicate.target
    }
    required_numeric_targets = {
        key
        for key, expected in expected_values.items()
        if isinstance(expected, (int, float)) and not isinstance(expected, bool)
    }
    missing_numeric_targets = required_numeric_targets - numeric_targets
    for target in sorted(missing_numeric_targets):
        expected = float(expected_values[target])
        predicates.append(
            VerifierPredicate(
                id=f"hard:numeric:{target}",
                type="numeric_tolerance",
                target=target,
                expected=expected,
                tolerance=_numeric_tolerance(expected),
                weight=1.0,
                required=True,
            )
        )
    if task.family == "derivation_completion":
        symbolic_targets = {
            predicate.target
            for predicate in predicates
            if predicate.type == "symbolic_equivalence" and predicate.target
        }
        for target, expected in expected_values.items():
            if (
                not isinstance(expected, str)
                or target in symbolic_targets
                or not symbolic_expression_is_checkable(expected)
            ):
                continue
            predicates.append(
                VerifierPredicate(
                    id=f"hard:symbolic:{target}",
                    type="symbolic_equivalence",
                    target=target,
                    expected=expected,
                    weight=1.0,
                    required=True,
                )
            )
        graph_by_id = {node.id: node for node in graph.nodes}
        symbolic_targets.update(
            predicate.target
            for predicate in predicates
            if predicate.type == "symbolic_equivalence" and predicate.target
        )
        for target in task.hidden_targets.required_nodes:
            node = graph_by_id.get(target)
            expected = node.canonical_symbolic_form or node.latex if node is not None else None
            if (
                node is None
                or node.type != "equation"
                or target in symbolic_targets
                or not expected
                or not symbolic_expression_is_checkable(expected)
            ):
                continue
            predicates.append(
                VerifierPredicate(
                    id=f"hard:symbolic-node:{target}",
                    type="symbolic_equivalence",
                    target=target,
                    expected=expected,
                    weight=1.0,
                    required=True,
                )
            )
    for predicate in predicates:
        if predicate.type == "numeric_tolerance":
            try:
                float(predicate.expected)  # type: ignore[arg-type]
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"numeric predicate {predicate.id} has a non-numeric expectation"
                ) from exc
    predicates.extend(predicate for key, predicate in baseline.items() if key not in existing_types)
    if not any(predicate.weight > 0 for predicate in predicates):
        outcome_order = (
            "numeric_tolerance",
            "symbolic_equivalence",
            "required_relations",
            "method_partial_order",
            "derivation_partial_order",
            "required_nodes",
            "evidence_coverage",
            "fault_identification",
            "configuration_constraints",
            "report_manifest_consistency",
            "manifest_required",
        )
        selected_index = next(
            (
                index
                for predicate_type in outcome_order
                for index, predicate in enumerate(predicates)
                if predicate.type == predicate_type
            ),
            None,
        )
        if selected_index is None:
            raise ValueError("verifier has no executable outcome predicate")
        predicates[selected_index] = predicates[selected_index].model_copy(update={"weight": 1.0})
    return spec.model_copy(
        update={
            "verifier_id": stable_id("verifier", task.task_id, "v1"),
            "task_id": task.task_id,
            "version": max(1, spec.version),
            "predicates": predicates,
            "runtime_dependencies": sorted(set([*spec.runtime_dependencies, "sympy==1.13.3"])),
            "network_required": False,
            "determinism_seed": _task_seed(task.task_id),
        }
    )


def deterministic_verifier(
    task: TaskSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
) -> VerifierSpec:
    """Compile the hidden TaskSpec contract without another model call."""
    return normalize_spec(
        VerifierSpec(
            verifier_id="deterministic-task-contract",
            task_id=task.task_id,
            version=1,
            predicates=[],
            runtime_dependencies=[],
            network_required=False,
            determinism_seed=0,
        ),
        task,
        bundle,
        graph,
    )


def _numeric_tolerance(expected: float) -> float:
    return max(1e-9, abs(expected) * 1e-6)


def _critic_audit(stage: str, critique: VerifierCritique) -> dict[str, Any]:
    return {
        "stage": stage,
        "accepted": critique.accepted,
        "findings": list(critique.findings),
        "false_positive_risks": list(critique.false_positive_risks),
        "false_negative_risks": list(critique.false_negative_risks),
        "repair_instructions": list(critique.repair_instructions),
    }


def evaluate(
    spec: VerifierSpec,
    answer: FoundryAnswer,
    *,
    task: TaskSpec,
    graph: PaperEvidenceGraph,
    bundle: PaperBundle,
    tool_calls: list[ToolCall] | None = None,
) -> RewardResult:
    results = [
        _evaluate_predicate(
            predicate,
            answer,
            task=task,
            graph=graph,
            bundle=bundle,
            tool_calls=tool_calls or [],
        )
        for predicate in spec.predicates
    ]
    hard_pass = all(result.passed for result in results if result.required)
    positive_weight = sum(predicate.weight for predicate in spec.predicates if predicate.weight > 0)
    weighted = sum(
        result.score * predicate.weight
        for result, predicate in zip(results, spec.predicates, strict=True)
    )
    reward = weighted / positive_weight if positive_weight else 0.0
    if not hard_pass:
        reward = 0.0
    return RewardResult(
        reward=max(0.0, min(1.0, reward)), passed=hard_pass and reward >= 0.999, predicates=results
    )


def _evaluate_predicate(
    predicate: VerifierPredicate,
    answer: FoundryAnswer,
    *,
    task: TaskSpec,
    graph: PaperEvidenceGraph,
    bundle: PaperBundle,
    tool_calls: list[ToolCall],
) -> PredicateResult:
    manifest = answer.answer_manifest
    committed = set(
        [
            *manifest.claims,
            *manifest.method_nodes,
            *manifest.faults,
            *(equation.id for equation in manifest.equations),
            *manifest.qualifications,
            *(result.id for result in manifest.numeric_results),
            *(edge.source for edge in manifest.relations),
            *(edge.target for edge in manifest.relations),
        ]
    )
    passed = False
    score = 0.0
    details = ""
    if predicate.type == "nonempty_report":
        passed = bool(answer.report.strip())
        score = float(passed)
        details = "report is present" if passed else "report is empty"
    elif predicate.type == "manifest_required":
        count = len(committed) + len(manifest.evidence) + len(manifest.numeric_results)
        passed = count > 0
        score = float(passed)
        details = f"{count} structured commitments"
    elif predicate.type in {"required_nodes", "required_dependency_nodes"}:
        targets = set(predicate.targets or ([predicate.target] if predicate.target else []))
        resolved = targets & committed
        if task.family == "derivation_completion":
            node_types = {node.id: node.type for node in graph.nodes}
            submitted_equations = {equation.id for equation in manifest.equations}
            equation_targets = {
                target for target in targets if node_types.get(target) == "equation"
            }
            resolved = (resolved - equation_targets) | (equation_targets & submitted_equations)
        passed = targets <= resolved
        score = len(resolved) / len(targets) if targets else 1.0
        details = f"resolved {len(resolved)}/{len(targets)} required nodes"
    elif predicate.type == "forbidden_nodes":
        targets = set(predicate.targets)
        passed = not (targets & committed)
        score = float(passed)
        details = f"forbidden overlap: {sorted(targets & committed)}"
    elif predicate.type == "evidence_membership":
        allowed = set(predicate.allowed_spans)
        submitted = set(manifest.evidence)
        passed = bool(submitted) and submitted <= allowed
        score = len(submitted & allowed) / len(submitted) if submitted else 0.0
        details = f"{len(submitted & allowed)}/{len(submitted)} evidence IDs allowed"
    elif predicate.type == "evidence_coverage":
        accepted_sets = predicate.config.get(
            "accepted_sets", task.hidden_targets.accepted_evidence_sets
        )
        submitted = set(manifest.evidence)
        coverages = [
            len(submitted & set(values)) / len(values) if values else 1.0
            for values in accepted_sets
        ]
        score = max(coverages, default=0.0)
        passed = math.isclose(score, 1.0)
        details = f"best accepted evidence-set coverage {score:.3f}"
    elif predicate.type == "symbolic_equivalence":
        node = next((item for item in graph.nodes if item.id == predicate.target), None)
        expected = predicate.expected or (
            node.canonical_symbolic_form or node.latex if node is not None else None
        )
        comparisons = [
            equation.latex
            for equation in manifest.equations
            if not predicate.target or equation.id == predicate.target
        ]
        passed = bool(expected) and any(
            symbolically_equivalent(value, str(expected)) for value in comparisons
        )
        score = float(passed)
        details = f"checked {len(comparisons)} submitted equations"
    elif predicate.type == "numeric_tolerance":
        if predicate.expected is None:
            return PredicateResult(
                predicate_id=predicate.id,
                passed=False,
                score=0.0,
                required=predicate.required,
                details="numeric predicate has no expected value",
            )
        expected = float(predicate.expected)
        tolerance = predicate.tolerance or 0.0
        expected_unit = predicate.config.get("unit")
        candidates = [
            value.value
            for value in manifest.numeric_results
            if not predicate.target or value.id == predicate.target
            if expected_unit is None or value.unit == expected_unit
        ]
        errors = [abs(value - expected) for value in candidates]
        best = min(errors, default=float("inf"))
        passed = best <= tolerance
        score = 1.0 if passed else max(0.0, 1.0 - best / max(abs(expected), tolerance, 1e-12))
        details = f"best absolute error {best}; tolerance {tolerance}"
    elif predicate.type == "method_partial_order":
        order = {node: index for index, node in enumerate(manifest.method_nodes)}
        pairs = predicate.config.get("precedes", [])
        checks = [
            left in order and right in order and order[left] < order[right] for left, right in pairs
        ]
        passed = bool(checks) and all(checks)
        score = sum(checks) / len(checks) if checks else 0.0
        details = f"{sum(checks)}/{len(checks)} ordering edges valid"
    elif predicate.type == "derivation_partial_order":
        order = {equation.id: index for index, equation in enumerate(manifest.equations)}
        pairs = predicate.config.get("precedes", [])
        checks = [
            left in order and right in order and order[left] < order[right] for left, right in pairs
        ]
        passed = bool(checks) and all(checks)
        score = sum(checks) / len(checks) if checks else 0.0
        details = f"{sum(checks)}/{len(checks)} derivation edges ordered"
    elif predicate.type == "fault_identification":
        targets = set(predicate.targets)
        forbidden = set(predicate.config.get("forbidden", []))
        submitted = set(manifest.faults)
        passed = targets <= submitted and not (forbidden & submitted) and not (submitted - targets)
        score = len(targets & submitted) / len(targets | submitted) if targets | submitted else 0.0
        details = f"fault overlap {sorted(targets & submitted)}"
    elif predicate.type == "required_relations":
        required_relations = {
            (edge.source, edge.relation, edge.target)
            for edge in task.hidden_targets.required_relations
        }
        submitted_relations = {
            (edge.source, edge.relation, edge.target) for edge in manifest.relations
        }
        relation_overlap = required_relations & submitted_relations
        passed = bool(required_relations) and required_relations <= submitted_relations
        score = len(relation_overlap) / len(required_relations) if required_relations else 0.0
        details = f"resolved {len(relation_overlap)}/{len(required_relations)} required relations"
    elif predicate.type == "required_qualifications":
        targets = set(predicate.targets or task.hidden_targets.required_qualifications)
        submitted_qualifications = set(manifest.qualifications)
        qualification_overlap = targets & submitted_qualifications
        passed = bool(targets) and targets <= submitted_qualifications
        score = len(qualification_overlap) / len(targets) if targets else 0.0
        details = f"resolved {len(qualification_overlap)}/{len(targets)} qualifications"
    elif predicate.type == "configuration_constraints":
        passed, score, details = _configuration_constraints(
            manifest.configuration,
            predicate.config.get("constraints", task.hidden_targets.configuration_constraints),
        )
    elif predicate.type == "report_manifest_consistency":
        passed = bool(answer.report.strip()) and bool(committed | set(manifest.evidence))
        score = float(passed)
        details = "report and structured manifest are both present"
    else:
        details = f"unsupported predicate {predicate.type}"
    return PredicateResult(
        predicate_id=predicate.id,
        passed=passed,
        score=max(0.0, min(1.0, score)),
        required=predicate.required,
        details=details,
    )


def _configuration_constraints(
    submitted: dict[str, bool | int | float | str],
    constraints: Any,
) -> tuple[bool, float, str]:
    if not isinstance(constraints, dict) or not constraints:
        return False, 0.0, "configuration constraints are absent"
    checks: list[bool] = []
    required_values = constraints.get("required_values", {})
    if isinstance(required_values, dict):
        checks.extend(submitted.get(str(key)) == value for key, value in required_values.items())
    ranges = constraints.get("ranges", {})
    if isinstance(ranges, dict):
        for key, bounds in ranges.items():
            value = submitted.get(str(key))
            try:
                check = (
                    isinstance(bounds, list)
                    and len(bounds) == 2
                    and isinstance(value, (int, float))
                    and not isinstance(value, bool)
                    and float(bounds[0]) <= float(value) <= float(bounds[1])
                )
            except (TypeError, ValueError):
                check = False
            checks.append(bool(check))
    forbidden = constraints.get("forbidden_keys", [])
    if isinstance(forbidden, list):
        checks.extend(str(key) not in submitted for key in forbidden)
    if not checks:
        return False, 0.0, "configuration constraints contain no supported checks"
    score = sum(checks) / len(checks)
    return all(checks), score, f"{sum(checks)}/{len(checks)} configuration checks passed"


def _derivation_order(task: TaskSpec, node_types: dict[str, str]) -> list[list[str]]:
    pairs: list[list[str]] = []
    for edge in task.hidden_targets.required_relations:
        if node_types.get(edge.source) != "equation" or node_types.get(edge.target) != "equation":
            continue
        if edge.relation in {"precedes", "derives", "enables", "produces"}:
            pair = [edge.source, edge.target]
        elif edge.relation in {"depends_on", "uses"}:
            pair = [edge.target, edge.source]
        else:
            continue
        if pair not in pairs:
            pairs.append(pair)
    return pairs


def _task_seed(task_id: str) -> int:
    return int.from_bytes(task_id.encode("utf-8")[:8].ljust(8, b"0"), "big") % (2**31)


def _compiler_system() -> str:
    schema = canonical_json(VerifierSpec.model_json_schema()).decode()
    return f"""Compile an English scientific rubric into a strict VerifierSpec using only these
predicate types: nonempty_report, manifest_required, required_nodes, forbidden_nodes,
required_dependency_nodes, evidence_membership, evidence_coverage, symbolic_equivalence,
numeric_tolerance, method_partial_order, fault_identification, required_relations,
derivation_partial_order,
required_qualifications, configuration_constraints, report_manifest_consistency.
Return one JSON VerifierSpec. Use finite hidden targets, hard gates,
weighted outcome checks, no prose judgement, no network, and no executable model-generated code.
The response must validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _compiler_prompt(bundle: PaperBundle, graph: PaperEvidenceGraph, task: TaskSpec) -> str:
    return (
        f"TASK:\n{canonical_json(task).decode()}\n"
        f"GRAPH:\n{canonical_json(graph).decode()}\n"
        f"PAPER_SPAN_IDS:\n{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}"
    )


def _critic_system() -> str:
    schema = canonical_json(VerifierCritique.model_json_schema()).decode()
    return f"""Independently inspect a deterministic scientific VerifierSpec for false positives,
false negatives, equivalent correct answers, missing hard gates, reward hacks, circular target use,
and brittle ordering or tolerance checks. Return strict JSON with accepted, findings,
false_positive_risks, false_negative_risks, repair_instructions. Set accepted=false only when a
listed risk is release-blocking and requires a repair; accepted=true may retain explicitly
documented residual risks that do not invalidate the deterministic verifier. The response must
validate exactly against REQUIRED_JSON_SCHEMA.
REQUIRED_JSON_SCHEMA:
{schema}"""


def _critic_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
    spec: VerifierSpec,
) -> str:
    return (
        f"TASK:\n{canonical_json(task).decode()}\nGRAPH:\n{canonical_json(graph).decode()}\n"
        f"VERIFIER:\n{canonical_json(spec).decode()}\n"
        f"SPAN_IDS:\n{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}"
    )


def _repair_prompt(
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
    spec: VerifierSpec,
    critique: VerifierCritique,
) -> str:
    return (
        "Return a complete replacement VerifierSpec using only the allowlisted predicates.\n"
        f"TASK:\n{canonical_json(task).decode()}\nGRAPH:\n{canonical_json(graph).decode()}\n"
        f"CURRENT_VERIFIER:\n{canonical_json(spec).decode()}\n"
        f"CRITIQUE:\n{canonical_json(critique).decode()}\n"
        f"SPAN_IDS:\n{canonical_json([span.span_id for span in bundle.stable_spans]).decode()}"
    )


__all__ = [
    "PredicateResult",
    "RewardResult",
    "VerifierCompiler",
    "VerifierCritique",
    "deterministic_verifier",
    "evaluate",
    "normalize_spec",
]
