"""Standalone JSON verifier copied into every immutable RL environment."""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any


def score_response(
    response: str,
    root: str | Path | None = None,
    *,
    tool_call_count: int = 0,
) -> float:
    base = Path(root) if root is not None else Path(__file__).resolve().parents[1]
    spec = _load(base / "hidden" / "verifier_spec.json")
    task = _load(base / "hidden" / "reference_state.json")["task"]
    graph = _load(base / "hidden" / "evidence_graph.json")
    try:
        answer = _parse_response(response)
    except Exception:
        return 0.0
    results: list[tuple[float, float, bool]] = []
    for predicate in spec["predicates"]:
        passed, partial = _predicate(
            predicate,
            answer,
            task,
            graph,
            tool_call_count,
        )
        if predicate.get("required", True) and not passed:
            return 0.0
        weight = float(predicate.get("weight", 0.0))
        results.append((weight, partial, passed))
    total = sum(weight for weight, _, _ in results if weight > 0)
    if not total:
        return 0.0
    return max(0.0, min(1.0, sum(weight * partial for weight, partial, _ in results) / total))


def _predicate(
    predicate: dict[str, Any],
    answer: dict[str, Any],
    task: dict[str, Any],
    graph: dict[str, Any],
    tool_call_count: int,
) -> tuple[bool, float]:
    kind = predicate["type"]
    report = str(answer.get("report", ""))
    manifest = answer.get("answer_manifest", {})
    claims = [str(value) for value in manifest.get("claims", [])]
    evidence = [str(value) for value in manifest.get("evidence", [])]
    equations = list(manifest.get("equations", []))
    method_nodes = [str(value) for value in manifest.get("method_nodes", [])]
    faults = [str(value) for value in manifest.get("faults", [])]
    numbers = list(manifest.get("numeric_results", []))
    relations = list(manifest.get("relations", []))
    qualifications = [str(value) for value in manifest.get("qualifications", [])]
    configuration = manifest.get("configuration", {})
    committed = set(
        [
            *claims,
            *method_nodes,
            *faults,
            *qualifications,
            *(str(item.get("id")) for item in equations),
            *(str(item.get("id")) for item in numbers),
            *(str(item.get("source")) for item in relations if isinstance(item, dict)),
            *(str(item.get("target")) for item in relations if isinstance(item, dict)),
        ]
    )
    targets = set(predicate.get("targets", []))
    if predicate.get("target"):
        targets.add(str(predicate["target"]))
    config = predicate.get("config", {})
    if kind == "nonempty_report":
        passed = bool(report.strip())
        return passed, float(passed)
    if kind == "manifest_required":
        passed = bool(committed or evidence or numbers or relations or configuration)
        return passed, float(passed)
    if kind in {"required_nodes", "required_dependency_nodes"}:
        overlap = targets & committed
        return targets <= committed, len(overlap) / len(targets) if targets else 1.0
    if kind == "forbidden_nodes":
        passed = not bool(targets & committed)
        return passed, float(passed)
    if kind == "evidence_membership":
        allowed = set(predicate.get("allowed_spans", []))
        submitted = set(evidence)
        passed = bool(submitted) and submitted <= allowed
        return passed, len(submitted & allowed) / len(submitted) if submitted else 0.0
    if kind == "evidence_coverage":
        accepted_sets = config.get(
            "accepted_sets", task.get("hidden_targets", {}).get("accepted_evidence_sets", [])
        )
        submitted = set(evidence)
        values = [
            len(submitted & set(group)) / len(group) if group else 1.0 for group in accepted_sets
        ]
        partial = max(values, default=0.0)
        return math.isclose(partial, 1.0), partial
    if kind == "symbolic_equivalence":
        expected = predicate.get("expected")
        if expected is None:
            node: dict[str, Any] = next(
                (item for item in graph["nodes"] if item["id"] == predicate.get("target")),
                {},
            )
            expected = node.get("canonical_symbolic_form") or node.get("latex")
        passed = bool(expected) and any(
            _symbolically_equivalent(str(item.get("latex", "")), str(expected))
            for item in equations
        )
        return passed, float(passed)
    if kind == "numeric_tolerance":
        expected = float(predicate["expected"])
        tolerance = float(predicate.get("tolerance") or 0.0)
        expected_unit = config.get("unit")
        candidates = [
            float(item["value"])
            for item in numbers
            if not predicate.get("target") or item.get("id") == predicate.get("target")
            if expected_unit is None or item.get("unit") == expected_unit
        ]
        best = min((abs(value - expected) for value in candidates), default=float("inf"))
        passed = best <= tolerance
        partial = 1.0 if passed else max(0.0, 1.0 - best / max(abs(expected), tolerance, 1e-12))
        return passed, partial
    if kind == "method_partial_order":
        positions = {node: index for index, node in enumerate(method_nodes)}
        checks = [
            left in positions and right in positions and positions[left] < positions[right]
            for left, right in config.get("precedes", [])
        ]
        return bool(checks) and all(checks), sum(checks) / len(checks) if checks else 0.0
    if kind == "fault_identification":
        submitted = set(faults)
        forbidden = set(config.get("forbidden", []))
        passed = targets <= submitted and not (forbidden & submitted) and not (submitted - targets)
        partial = (
            len(targets & submitted) / len(targets | submitted) if targets | submitted else 0.0
        )
        return passed, partial
    if kind == "required_relations":
        required_relations = {
            (str(edge["source"]), str(edge["relation"]), str(edge["target"]))
            for edge in task.get("hidden_targets", {}).get("required_relations", [])
        }
        submitted_relations = {
            (str(edge["source"]), str(edge["relation"]), str(edge["target"]))
            for edge in relations
            if isinstance(edge, dict)
            and all(key in edge for key in ("source", "relation", "target"))
        }
        relation_overlap = required_relations & submitted_relations
        return bool(required_relations) and required_relations <= submitted_relations, (
            len(relation_overlap) / len(required_relations) if required_relations else 0.0
        )
    if kind == "required_qualifications":
        required_qualifications = set(
            predicate.get("targets")
            or task.get("hidden_targets", {}).get("required_qualifications", [])
        )
        submitted_qualifications = set(qualifications)
        qualification_overlap = required_qualifications & submitted_qualifications
        return bool(
            required_qualifications
        ) and required_qualifications <= submitted_qualifications, (
            len(qualification_overlap) / len(required_qualifications)
            if required_qualifications
            else 0.0
        )
    if kind == "configuration_constraints":
        constraints = config.get(
            "constraints",
            task.get("hidden_targets", {}).get("configuration_constraints", {}),
        )
        return _configuration_constraints(configuration, constraints)
    if kind == "report_manifest_consistency":
        passed = bool(report.strip()) and bool(committed | set(evidence))
        return passed, float(passed)
    return False, 0.0


def _parse_response(response: str) -> dict[str, Any]:
    value = response.strip()
    if value.startswith("```"):
        value = value.removeprefix("```json").removeprefix("```")
        value = value.removesuffix("```").strip()
    payload = json.loads(value)
    if not isinstance(payload, dict):
        raise ValueError("answer root must be an object")
    return payload


def _configuration_constraints(
    submitted: Any,
    constraints: Any,
) -> tuple[bool, float]:
    if not isinstance(submitted, dict) or not isinstance(constraints, dict) or not constraints:
        return False, 0.0
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
        return False, 0.0
    score = sum(checks) / len(checks)
    return all(checks), score


def _symbolically_equivalent(left: str, right: str) -> bool:
    try:
        import sympy
        from sympy.parsing.latex import parse_latex

        if len(left) > 2_000 or len(right) > 2_000:
            return False
        a = parse_latex(left.strip("$"))
        b = parse_latex(right.strip("$"))
        return bool(sympy.simplify(a - b) == 0)
    except Exception:

        def normalize(value: str) -> str:
            return re.sub(r"\s+|\\left|\\right|\$", "", value).replace("^", "**")

        return normalize(left) == normalize(right)


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


if __name__ == "__main__":
    import sys

    print(
        score_response(
            Path(sys.argv[1]).read_text(encoding="utf-8"),
            sys.argv[2] if len(sys.argv) > 2 else None,
        )
    )
