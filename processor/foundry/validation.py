"""Positive, negative, mutation, metamorphic, replay, and security gates."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import asdict

from processor.foundry.util import canonical_json
from processor.foundry.verifier import RewardResult, evaluate
from schemas.foundry import (
    AnswerManifest,
    FoundryAnswer,
    PaperBundle,
    PaperEvidenceGraph,
    TaskSpec,
    Trajectory,
    ValidationReport,
    VerifierSpec,
)


def run_acceptance_suite(
    *,
    task: TaskSpec,
    spec: VerifierSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    trajectories: list[Trajectory],
) -> tuple[
    ValidationReport,
    list[Trajectory],
    dict[str, list[FoundryAnswer]],
]:
    positive_results = [
        evaluate(
            spec,
            trajectory.answer,
            task=task,
            graph=graph,
            bundle=bundle,
            tool_calls=trajectory.tool_calls,
        )
        for trajectory in trajectories
    ]
    validated = [
        trajectory.model_copy(
            update={
                "accepted": result.passed,
                "reward": result.reward,
                "validation": {
                    "predicates": [asdict(value) for value in result.predicates],
                },
            }
        )
        for trajectory, result in zip(trajectories, positive_results, strict=True)
    ]
    false_negatives = sum(not result.passed for result in positive_results)

    equivalent_answers = [_equivalent_variant(trajectory.answer) for trajectory in trajectories]
    equivalent_results = [
        evaluate(spec, answer, task=task, graph=graph, bundle=bundle)
        for answer in equivalent_answers
    ]

    base = (
        trajectories[0].answer
        if trajectories
        else FoundryAnswer(report="", answer_manifest=AnswerManifest())
    )
    adversarial_answers = list(_adversarial_answers(base, spec))
    adversarial_results = [
        evaluate(spec, answer, task=task, graph=graph, bundle=bundle)
        for answer in adversarial_answers
    ]
    false_positives = sum(result.passed for result in adversarial_results)

    mutations = list(_mutations(base, task, spec))
    mutation_results = [
        evaluate(spec, answer, task=task, graph=graph, bundle=bundle) for answer in mutations
    ]
    mutations_killed = sum(not result.passed for result in mutation_results)

    metamorphic_answers = list(_metamorphic_variants(base, spec))
    base_result = evaluate(spec, base, task=task, graph=graph, bundle=bundle)
    metamorphic_results = [
        evaluate(spec, answer, task=task, graph=graph, bundle=bundle)
        for answer in metamorphic_answers
    ]
    metamorphic_pass = all(
        result.passed == base_result.passed and abs(result.reward - base_result.reward) < 1e-12
        for result in metamorphic_results
    )

    replay_a = evaluate(spec, base, task=task, graph=graph, bundle=bundle)
    replay_b = evaluate(spec, base, task=task, graph=graph, bundle=bundle)
    replay_pass = canonical_json(_wire_reward(replay_a)) == canonical_json(_wire_reward(replay_b))
    security_pass = _security_check(spec, bundle, graph, task)

    report = ValidationReport(
        task_id=task.task_id,
        positive_pass=bool(positive_results) and all(result.passed for result in positive_results),
        equivalent_pass=bool(equivalent_results)
        and all(result.passed for result in equivalent_results),
        adversarial_pass=bool(adversarial_results) and false_positives == 0,
        mutation_killed=mutations_killed,
        mutation_total=len(mutations),
        metamorphic_pass=metamorphic_pass,
        replay_pass=replay_pass,
        security_pass=security_pass,
        false_positive_count=false_positives,
        false_negative_count=false_negatives,
        details={
            "positive_rewards": [result.reward for result in positive_results],
            "equivalent_rewards": [result.reward for result in equivalent_results],
            "adversarial_rewards": [result.reward for result in adversarial_results],
            "mutation_rewards": [result.reward for result in mutation_results],
            "metamorphic_rewards": [result.reward for result in metamorphic_results],
        },
    )
    return (
        report,
        validated,
        {
            "equivalent": equivalent_answers,
            "adversarial": adversarial_answers,
            "mutations": mutations,
            "metamorphic": metamorphic_answers,
        },
    )


def suite_passes(report: ValidationReport) -> bool:
    return (
        report.positive_pass
        and report.equivalent_pass
        and report.adversarial_pass
        and report.mutation_total > 0
        and report.mutation_killed == report.mutation_total
        and report.metamorphic_pass
        and report.replay_pass
        and report.security_pass
        and report.false_positive_count == 0
        and report.false_negative_count == 0
    )


def _equivalent_variant(answer: FoundryAnswer) -> FoundryAnswer:
    manifest = answer.answer_manifest
    return answer.model_copy(
        update={
            "answer_manifest": manifest.model_copy(
                update={
                    "claims": list(dict.fromkeys(reversed(manifest.claims))),
                    "evidence": list(dict.fromkeys(reversed(manifest.evidence))),
                    "numeric_results": list(reversed(manifest.numeric_results)),
                    "relations": list(reversed(manifest.relations)),
                    "qualifications": list(dict.fromkeys(reversed(manifest.qualifications))),
                }
            )
        }
    )


def _adversarial_answers(
    valid: FoundryAnswer,
    spec: VerifierSpec,
) -> Iterable[FoundryAnswer]:
    yield FoundryAnswer(report="", answer_manifest=AnswerManifest())
    yield FoundryAnswer(report=valid.report, answer_manifest=AnswerManifest())
    predicate_types = {predicate.type for predicate in spec.predicates}
    if predicate_types & {"evidence_membership", "evidence_coverage"}:
        yield valid.model_copy(
            update={
                "answer_manifest": valid.answer_manifest.model_copy(
                    update={"evidence": ["external:invented-span"]}
                )
            }
        )
    yield valid.model_copy(
        update={
            "answer_manifest": valid.answer_manifest.model_copy(
                update={
                    "claims": [],
                    "method_nodes": [],
                    "faults": [],
                    "equations": [],
                    "relations": [],
                    "qualifications": [],
                    "configuration": {},
                }
            )
        }
    )
    numeric_targets = {
        predicate.target
        for predicate in spec.predicates
        if predicate.type == "numeric_tolerance" and predicate.target
    }
    targeted_numeric = [
        result for result in valid.answer_manifest.numeric_results if result.id in numeric_targets
    ]
    if targeted_numeric:
        values = [
            result.model_copy(update={"value": -result.value})
            if result.id in numeric_targets
            else result
            for result in valid.answer_manifest.numeric_results
        ]
        yield valid.model_copy(
            update={
                "answer_manifest": valid.answer_manifest.model_copy(
                    update={"numeric_results": values}
                )
            }
        )


def _mutations(
    valid: FoundryAnswer,
    task: TaskSpec,
    spec: VerifierSpec,
) -> Iterable[FoundryAnswer]:
    valid_wire = canonical_json(valid)
    for mutation in _mutation_candidates(valid, task, spec):
        # Compiler output and unusual manifests can otherwise create a no-op
        # "mutation". Such a case measures nothing and can make a sound
        # verifier look mutation-incomplete.
        if canonical_json(mutation) != valid_wire:
            yield mutation


def _mutation_candidates(
    valid: FoundryAnswer,
    task: TaskSpec,
    spec: VerifierSpec,
) -> Iterable[FoundryAnswer]:
    manifest = valid.answer_manifest
    required = sorted(set(task.hidden_targets.required_nodes))
    for node_id in required:
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={
                        "claims": [value for value in manifest.claims if value != node_id],
                        "method_nodes": [
                            value for value in manifest.method_nodes if value != node_id
                        ],
                        "faults": [value for value in manifest.faults if value != node_id],
                        "equations": [value for value in manifest.equations if value.id != node_id],
                        "numeric_results": [
                            value for value in manifest.numeric_results if value.id != node_id
                        ],
                        "relations": [
                            value
                            for value in manifest.relations
                            if value.source != node_id and value.target != node_id
                        ],
                        "qualifications": [
                            value for value in manifest.qualifications if value != node_id
                        ],
                    }
                )
            }
        )
    for fault_id in sorted(set(task.hidden_targets.required_faults)):
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={"faults": [value for value in manifest.faults if value != fault_id]}
                )
            }
        )
    predicate_types = {predicate.type for predicate in spec.predicates}
    if manifest.evidence and predicate_types & {"evidence_membership", "evidence_coverage"}:
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={"evidence": [*manifest.evidence[:-1], "mutated:span"]}
                )
            }
        )
    if len(manifest.method_nodes) >= 2 and "method_partial_order" in predicate_types:
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={"method_nodes": list(reversed(manifest.method_nodes))}
                )
            }
        )
    if len(manifest.equations) >= 2 and "derivation_partial_order" in predicate_types:
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={"equations": list(reversed(manifest.equations))}
                )
            }
        )
    numeric_targets = {
        predicate.target
        for predicate in spec.predicates
        if predicate.type == "numeric_tolerance" and predicate.target
    }
    for value in (item for item in manifest.numeric_results if item.id in numeric_targets):
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={
                        "numeric_results": [
                            item.model_copy(
                                update={"value": item.value + max(abs(item.value), 1.0)}
                            )
                            if item.id == value.id
                            else item
                            for item in manifest.numeric_results
                        ]
                    }
                )
            }
        )
    symbolic_targets = {
        predicate.target
        for predicate in spec.predicates
        if predicate.type == "symbolic_equivalence" and predicate.target
    }
    for equation in (item for item in manifest.equations if item.id in symbolic_targets):
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={
                        "equations": [
                            item.model_copy(update={"latex": f"-({item.latex})"})
                            if item.id == equation.id
                            else item
                            for item in manifest.equations
                        ]
                    }
                )
            }
        )
    if manifest.relations and "required_relations" in predicate_types:
        required = {
            (edge.source, edge.relation, edge.target)
            for edge in task.hidden_targets.required_relations
        }
        relation_to_remove = next(
            (
                edge
                for edge in manifest.relations
                if (edge.source, edge.relation, edge.target) in required
            ),
            None,
        )
        if relation_to_remove is not None:
            yield valid.model_copy(
                update={
                    "answer_manifest": manifest.model_copy(
                        update={
                            "relations": [
                                edge for edge in manifest.relations if edge != relation_to_remove
                            ]
                        }
                    )
                }
            )
    if manifest.qualifications and "required_qualifications" in predicate_types:
        required_qualifications = set(task.hidden_targets.required_qualifications)
        qualification_to_remove = next(
            (value for value in manifest.qualifications if value in required_qualifications), None
        )
        if qualification_to_remove is not None:
            yield valid.model_copy(
                update={
                    "answer_manifest": manifest.model_copy(
                        update={
                            "qualifications": [
                                value
                                for value in manifest.qualifications
                                if value != qualification_to_remove
                            ]
                        }
                    )
                }
            )
    if "configuration_constraints" in predicate_types:
        constraints = task.hidden_targets.configuration_constraints
        mutated_configuration = dict(manifest.configuration)
        constrained_keys = [
            *(
                str(key)
                for key in constraints.get("required_values", {})
                if isinstance(constraints.get("required_values"), dict)
            ),
            *(
                str(key)
                for key in constraints.get("ranges", {})
                if isinstance(constraints.get("ranges"), dict)
            ),
        ]
        if constrained_keys:
            mutated_configuration.pop(constrained_keys[0], None)
        else:
            forbidden = constraints.get("forbidden_keys", [])
            if isinstance(forbidden, list) and forbidden:
                mutated_configuration[str(forbidden[0])] = "mutated"
        if mutated_configuration != manifest.configuration:
            yield valid.model_copy(
                update={
                    "answer_manifest": manifest.model_copy(
                        update={"configuration": mutated_configuration}
                    )
                }
            )
    if not required and not manifest.evidence:
        yield FoundryAnswer(report="mutated", answer_manifest=AnswerManifest())


def _metamorphic_variants(
    valid: FoundryAnswer,
    spec: VerifierSpec,
) -> Iterable[FoundryAnswer]:
    manifest = valid.answer_manifest
    yield valid.model_copy(
        update={
            "answer_manifest": manifest.model_copy(
                update={
                    "claims": list(reversed(manifest.claims)),
                    "evidence": list(reversed(manifest.evidence)),
                    "relations": list(reversed(manifest.relations)),
                    "qualifications": list(reversed(manifest.qualifications)),
                }
            )
        }
    )
    allowed = {
        span_id
        for predicate in spec.predicates
        if predicate.type == "evidence_membership"
        for span_id in predicate.allowed_spans
    }
    allowed_extra = next(
        (span_id for span_id in sorted(allowed) if span_id not in manifest.evidence), None
    )
    if allowed_extra:
        yield valid.model_copy(
            update={
                "answer_manifest": manifest.model_copy(
                    update={"evidence": [*manifest.evidence, allowed_extra]}
                )
            }
        )


def _security_check(
    spec: VerifierSpec,
    bundle: PaperBundle,
    graph: PaperEvidenceGraph,
    task: TaskSpec,
) -> bool:
    payload = canonical_json({"spec": spec, "bundle": bundle, "graph": graph, "task": task}).decode(
        "utf-8"
    )
    forbidden = ("HETZNER_INFERENCE_API_KEY", "ZAI_API_KEY", "GLM_API_KEY")
    return not spec.network_required and not any(name in payload for name in forbidden)


def _wire_reward(result: RewardResult) -> dict[str, object]:
    return {
        "reward": result.reward,
        "passed": result.passed,
        "predicates": [asdict(value) for value in result.predicates],
    }


__all__ = ["run_acceptance_suite", "suite_passes"]
