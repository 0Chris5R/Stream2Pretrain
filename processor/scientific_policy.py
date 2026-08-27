"""Explainable scientific-corpus scoring and routing policy.

The CPU models produce independent signals. This module combines them only
for presentation and routing; it never relabels a composite as a model score.
All decisions are deterministic and versioned by ``S2P_POLICY_REVISION``.
"""

from __future__ import annotations

from collections.abc import Set
from dataclasses import dataclass

from processor.source_policy import resolve_source_policy
from schemas.gold import CorpusRoute, SegmentScore
from schemas.silver import SilverRecord, SilverSegment


@dataclass(frozen=True, slots=True)
class ScientificScores:
    extraction_completeness: float
    structural_quality_score: float
    reasoning_score: float
    benchmark_score: float
    content_tags: list[str]


@dataclass(frozen=True, slots=True)
class RouteDecision:
    route: CorpusRoute
    eligible_routes: list[CorpusRoute]
    reasons: list[str]


def representative_segments(
    segments: list[SilverSegment], *, limit: int = 8
) -> list[SilverSegment]:
    """Select a bounded, deterministic, role-stratified inference sample.

    One representative is reserved for each scientific role family before
    spare capacity is filled by the longest remaining sections. This prevents
    repeated methods/results subsections from excluding the rest of a paper.
    """
    if limit <= 0:
        return []
    if len(segments) <= limit:
        return list(segments)

    role_families = (
        ("abstract",),
        ("introduction", "background"),
        ("methods",),
        ("results", "discussion"),
        ("conclusion", "limitations"),
        ("other", "appendix"),
    )
    indexed = list(enumerate(segments))
    chosen_indexes: set[int] = set()
    for family in role_families:
        candidates = [
            pair for pair in indexed if pair[1].role in family and pair[0] not in chosen_indexes
        ]
        if not candidates:
            continue
        index, _ = min(candidates, key=lambda pair: (-pair[1].word_count, pair[0]))
        chosen_indexes.add(index)
        if len(chosen_indexes) == limit:
            break

    if len(chosen_indexes) < limit:
        remaining = sorted(
            (pair for pair in indexed if pair[0] not in chosen_indexes),
            key=lambda pair: (-pair[1].word_count, pair[0]),
        )
        chosen_indexes.update(index for index, _ in remaining[: limit - len(chosen_indexes)])

    return [segments[index] for index in sorted(chosen_indexes)]


def aggregate_segment_scores(scores: list[SegmentScore]) -> tuple[float, float, str]:
    """Return weighted source quality, weighted-median KenLM, and bucket."""
    quality_measured = [
        (score, score.edu_score)
        for score in scores
        if score.decision == "included" and score.edu_score is not None
    ]
    if not quality_measured:
        return 0.0, 0.0, "tail"
    weights = [max(1, min(score.word_count, 512)) for score, _ in quality_measured]
    total = sum(weights)
    edu = (
        sum(
            edu_score * weight
            for (_, edu_score), weight in zip(quality_measured, weights, strict=True)
        )
        / total
    )
    measured = [
        (score.perplexity, score.perplexity_bucket, max(1, min(score.word_count, 512)))
        for score, _ in quality_measured
        if score.perplexity is not None and score.perplexity_bucket is not None
    ]
    if not measured:
        return edu, 0.0, "middle"
    total = sum(weight for _, _, weight in measured)
    ordered = sorted(measured, key=lambda item: item[0])
    halfway = total / 2
    running = 0
    median_perplexity, median_bucket, _ = ordered[-1]
    for perplexity, bucket, weight in ordered:
        running += weight
        if running >= halfway:
            median_perplexity = perplexity
            median_bucket = bucket
            break
    return edu, median_perplexity, median_bucket


def scientific_scores(silver: SilverRecord, *, edu_score: float) -> ScientificScores:
    roles = {segment.role for segment in silver.segments}
    has_abstract = "abstract" in roles
    has_methods = "methods" in roles
    has_results = bool(roles & {"results", "discussion", "conclusion"})
    evidence_kinds = sum(
        count > 0 for count in (silver.equation_count, silver.table_count, silver.figure_count)
    )

    completeness = 0.0
    completeness += 0.12 if silver.title else 0.0
    completeness += 0.18 if has_abstract else 0.0
    completeness += 0.20 if silver.included_section_count >= 3 else 0.10 if silver.segments else 0.0
    completeness += (
        0.20
        if silver.training_word_count >= 500
        else 0.10
        if silver.training_word_count >= 100
        else 0.0
    )
    completeness += 0.12 if evidence_kinds else 0.0
    completeness += 0.08 if silver.citation_count else 0.0
    completeness += 0.10 if not silver.extraction_warnings else 0.03
    completeness = min(1.0, completeness)

    role_coverage = sum((has_abstract, has_methods, has_results)) / 3
    evidence_coverage = evidence_kinds / 3
    structural = 5.0 * (0.55 * completeness + 0.25 * role_coverage + 0.20 * evidence_coverage)
    structural = max(0.0, min(5.0, structural))

    reasoning = (
        0.22 * float(has_methods)
        + 0.24 * float(has_results)
        + 0.14 * float(silver.equation_count > 0)
        + 0.12 * float(silver.table_count > 0)
        + 0.08 * float(silver.figure_count > 0)
        + 0.10 * (structural / 5.0)
        + 0.10 * (edu_score / 5.0)
    )
    reasoning = max(0.0, min(1.0, reasoning))

    tags = _content_tags(silver, roles)
    return ScientificScores(
        extraction_completeness=completeness,
        structural_quality_score=structural,
        reasoning_score=reasoning,
        # Benchmark allocation is a post-training artifact decision. The
        # pretraining curator deliberately does not estimate it.
        benchmark_score=0.0,
        content_tags=tags,
    )


def source_scores(silver: SilverRecord, *, quality_score: float) -> ScientificScores:
    """Dispatch structural/evidence signals without applying paper assumptions universally."""
    source_policy = resolve_source_policy(
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
    )
    if source_policy.family == "scientific_paper":
        return scientific_scores(silver, edu_score=quality_score)
    word_count = len((silver.model_text or silver.text).split())
    completeness = min(
        1.0,
        0.15 * float(bool(silver.title))
        + 0.55 * min(1.0, word_count / 500)
        + 0.20 * float(not silver.extraction_warnings)
        + 0.10 * float(bool(silver.text.strip())),
    )
    structural = max(0.0, min(5.0, 5.0 * completeness))
    if not source_policy.training_text:
        return ScientificScores(
            completeness,
            structural,
            0.0,
            0.0,
            ["discovery_metadata"],
        )
    reasoning = min(0.45, 0.10 + 0.15 * (quality_score / 5.0) + 0.20 * completeness)
    content_tag = {
        "hf_model_card": "hf_model_documentation",
        "hf_dataset_card": "hf_dataset_documentation",
    }.get(source_policy.family, "educational_web")
    return ScientificScores(
        completeness,
        structural,
        reasoning,
        0.0,
        [content_tag],
    )


def composite_quality_score(
    *,
    edu_score: float,
    structural_quality_score: float,
    lang_score: float,
    gopher_pass: bool,
    c4_pass: bool,
    perplexity_bucket: str,
    language_applicable: bool = True,
    web_heuristics_applicable: bool = True,
    perplexity_applicable: bool = True,
) -> float:
    """Explainable 0..5 convenience score built only from applicable signals."""
    typicality = {"head": 1.0, "middle": 0.72, "tail": 0.25}.get(perplexity_bucket, 0.0)
    heuristic = (float(gopher_pass) + float(c4_pass)) / 2
    weighted = [
        (0.35, edu_score / 5.0),
        (0.25, structural_quality_score / 5.0),
    ]
    if language_applicable:
        weighted.append((0.15, lang_score))
    if web_heuristics_applicable:
        weighted.append((0.15, heuristic))
    if perplexity_applicable:
        weighted.append((0.10, typicality))
    total_weight = sum(weight for weight, _ in weighted)
    normalized = sum(weight * value for weight, value in weighted) / total_weight
    return max(0.0, min(5.0, 5.0 * normalized))


def route_document(
    *,
    silver: SilverRecord,
    reject_reasons: list[str],
    reasoning_score: float,
) -> RouteDecision:
    """Choose one primary route and list every eligible downstream use."""
    retryable = {"insufficient_scientific_body", "incomplete_scientific_extraction"}
    blocking = [reason for reason in reject_reasons if reason not in retryable]
    if blocking:
        return RouteDecision(
            route="quarantine",
            eligible_routes=["quarantine"],
            reasons=[f"blocked by {reason}" for reason in blocking],
        )
    if retryable.intersection(reject_reasons):
        return RouteDecision(
            route="retry",
            eligible_routes=["retry"],
            reasons=["scientific extraction is incomplete; retry the full artifact"],
        )
    eligible: list[CorpusRoute] = ["pretrain"]
    reasons = ["clean body projection passed privacy, quality, dedup, and decontamination gates"]
    posttrain_ready = posttrain_candidate_eligible(silver)
    if reasoning_score >= 0.55 and posttrain_ready:
        eligible.append("posttrain_candidate")
        reasons.append("methods/results and structured evidence support post-training use")

    if reasoning_score >= 0.55 and posttrain_ready:
        return RouteDecision("posttrain_candidate", eligible, reasons)
    return RouteDecision("pretrain", eligible, reasons)


def posttrain_candidate_eligible(silver: SilverRecord) -> bool:
    """Return whether the current paper Foundry can consume this record.

    The current Foundry contract is deliberately paper-specific. Its durable
    input must name a successfully persisted ``ScientificDocument`` and expose
    at least one stable retained section. Web prose, cards, reviews, code, and
    legacy scientific rows without that artifact remain valid pretraining
    material, but cannot be mislabeled as runnable paper environments.
    """
    policy = resolve_source_policy(
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
    )
    return (
        policy.family == "scientific_paper"
        and silver.scientific_artifact_s3_uri is not None
        and any(segment.text.strip() for segment in silver.segments)
    )


def _content_tags(silver: SilverRecord, roles: Set[str]) -> list[str]:
    haystack = " ".join(
        [silver.title or "", *(segment.title for segment in silver.segments)]
    ).lower()
    tags: list[str] = []
    if silver.equation_count >= 3:
        tags.append("mathematical_reasoning")
    if roles & {"results", "discussion"} or silver.table_count or silver.figure_count:
        tags.append("empirical_evidence")
    if "methods" in roles:
        tags.append("methods_and_procedures")
    if any(term in haystack for term in ("benchmark", "dataset", "corpus", "evaluation")):
        tags.append("benchmark_or_dataset")
    if any(term in haystack for term in ("survey", "review", "overview")):
        tags.append("survey_synthesis")
    if any(term in haystack for term in ("system", "pipeline", "architecture", "infrastructure")):
        tags.append("systems_implementation")
    if silver.figure_count:
        tags.append("visual_evidence")
    return tags or ["general_scientific"]
