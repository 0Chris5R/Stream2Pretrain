"""Bytewax dataflow: ``docs.normalized`` -> ``docs.curated``.

End-to-end source-aware curation. The dataflow:

1. Consumes :class:`SilverRecord` payloads from ``docs.normalized``.
2. Runs the Gopher heuristic gate.
3. Runs the C4 nopunc / curly-brace / lorem-ipsum gate.
4. Re-scores perplexity (KenLM) and re-buckets - the fetcher emitted
   stub values; curation owns the real signals.
5. Recomputes the MinHash signature (cheap, ~us/doc) and tests the
   :class:`LSHBloomIndex` near-dup index.
6. Runs FinePDFs Edu v2 for scientific text, FineWeb-Edu for web text, or the
   versioned code-quality policy for source files.
7. Runs the PII regex pack plus Presidio.
8. Runs the Decon-Gate (n-gram Bloom + E5-small-v2 embedding sketch).
9. Emits a trainable :class:`GoldRecord` on ``docs.curated``.

Every scored outcome is published to ``curation.decisions`` for durable
audit and attestation. Only trainable rows are also published to
``docs.curated`` and materialized in the clean Gold table.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from typing import Any, cast

from ingest.common.license_admission import is_training_permitted
from processor import common
from processor.decision_cache import DecisionCache
from processor.decon_gate import DeconGate, _EmbeddingSketch  # type: ignore[attr-defined]
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.operators.c4 import C4Filter
from processor.operators.code_quality import CodeQualityPolicy
from processor.operators.gopher import GopherFilter
from processor.operators.kenlm_score import KenLMScorer
from processor.operators.lshbloom import LSHBloomIndex
from processor.operators.minhash import MinHasher
from processor.operators.pii import PiiScanner
from processor.operators.quality import QualityClassifier
from processor.probes import start_probe_server
from processor.scientific_policy import (
    aggregate_segment_scores,
    composite_quality_score,
    representative_segments,
    route_document,
    source_scores,
)
from processor.tokenize import Tokenizer
from schemas.decon import BenchmarkName
from schemas.gold import GoldRecord, PiiFlag, RejectReason, RiskTier, SegmentScore
from schemas.silver import SilverRecord, SilverSegment

POLICY_REVISION_ENV = "S2P_POLICY_REVISION"
SCORING_VERSION_ENV = "S2P_SCORING_VERSION"


@dataclass(slots=True)
class CurateState:
    """Per-worker state for the curation dataflow."""

    gopher: GopherFilter
    c4: C4Filter
    kenlm: KenLMScorer
    minhasher: MinHasher
    lsh: LSHBloomIndex
    finepdfs_quality: QualityClassifier
    fineweb_quality: QualityClassifier
    code_quality: CodeQualityPolicy
    pii: PiiScanner
    decon: DeconGate
    tokenizer: Tokenizer
    policy_revision: str
    scoring_version: str
    decision_cache: DecisionCache

    def close(self) -> None:
        self.decision_cache.close()
        self.lsh.close()


def build_state(cfg: common.ProcessorConfig) -> CurateState:
    """Construct a :class:`CurateState` from the runtime config."""
    models = cfg.models_dir
    require_real_models = os.environ.get("S2P_REQUIRE_REAL_MODELS") == "1"
    kenlm_path = os.path.join(models, "kenlm", "en.arpa.bin")
    kenlm_sentencepiece_path = os.path.join(models, "kenlm", "en.sp.model")
    finepdfs_quality_dir = os.path.join(models, "finepdfs-edu-v2")
    fineweb_quality_dir = os.path.join(models, "fineweb-edu")
    e5_dir = os.path.join(models, "e5-small")
    embedding = _EmbeddingSketch(
        e5_dir if os.path.isdir(e5_dir) else None,
        revision=os.environ.get("E5_SMALL_REVISION"),
        allow_fallback=not require_real_models,
    )
    kenlm = KenLMScorer(
        kenlm_path if os.path.isfile(kenlm_path) else None,
        kenlm_sentencepiece_path if os.path.isfile(kenlm_sentencepiece_path) else None,
        allow_fallback=not require_real_models,
    )
    finepdfs_quality = QualityClassifier(
        finepdfs_quality_dir if os.path.isdir(finepdfs_quality_dir) else None,
        revision=os.environ.get("S2P_FINEPDFS_EDU_REVISION"),
        model_family="finepdfs-edu-v2",
        allow_fallback=not require_real_models,
    )
    fineweb_quality = QualityClassifier(
        fineweb_quality_dir if os.path.isdir(fineweb_quality_dir) else None,
        revision=os.environ.get("S2P_FINEWEB_EDU_REVISION"),
        model_family="fineweb-edu",
        allow_fallback=not require_real_models,
    )
    pii = PiiScanner(allow_fallback=not require_real_models)
    minhasher = MinHasher()
    if require_real_models and minhasher.backend == "fallback-pyhash":
        raise RuntimeError("datasketch or rensa MinHash is required")
    lsh = LSHBloomIndex(state_dir=os.path.join(cfg.state_dir, "lshbloom"))
    if require_real_models and lsh.backend == "memory":
        raise RuntimeError("a durable LSHBloom backend is required")
    decon = DeconGate(
        benchmark_set_version=cfg.benchmark_set_version,
        benchmark_corpus=_load_benchmark_corpus(cfg.benchmark_corpus_path),
        embedding=embedding,
    )
    return CurateState(
        gopher=GopherFilter(),
        c4=C4Filter(),
        kenlm=kenlm,
        minhasher=minhasher,
        lsh=lsh,
        finepdfs_quality=finepdfs_quality,
        fineweb_quality=fineweb_quality,
        code_quality=CodeQualityPolicy(),
        pii=pii,
        decon=decon,
        tokenizer=Tokenizer(allow_fallback=not require_real_models),
        policy_revision=os.environ.get(POLICY_REVISION_ENV, "git:dev"),
        scoring_version=os.environ.get(SCORING_VERSION_ENV, "v0.1.0"),
        decision_cache=DecisionCache(os.path.join(cfg.state_dir, "decision-cache.sqlite3")),
    )


def _decision_cache_key(state: CurateState, payload: bytes) -> str:
    """Fingerprint the exact Silver bytes and every material scoring revision."""
    recipe = "\n".join(
        (
            state.policy_revision,
            state.scoring_version,
            state.finepdfs_quality.revision,
            state.fineweb_quality.revision,
            state.kenlm.scorer,
            state.pii.revision,
            state.decon.benchmark_set_version,
        )
    ).encode("utf-8")
    return hashlib.sha256(recipe + b"\0" + payload).hexdigest()


def _load_benchmark_corpus(path: str | None) -> dict[BenchmarkName, list[str]] | None:
    """Read benchmark prompts from a JSON file, or return ``None``."""
    if not path or not os.path.isfile(path):
        return None
    import orjson

    with open(path, "rb") as fh:
        data = orjson.loads(fh.read())
    if not isinstance(data, dict):
        return None
    valid = {"MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"}
    return {cast(BenchmarkName, k): list(v) for k, v in data.items() if k in valid}


def curate_one(state: CurateState, silver: SilverRecord) -> GoldRecord:
    """Run the full curation pipeline on one silver record.

    Always returns a scored GoldRecord. Callers must use
    :func:`is_trainable_gold` before publishing the record to ``docs.curated``.
    """
    source_segments = list(silver.segments) or [
        SilverSegment(
            segment_id="document",
            title=silver.title or "Document",
            text=silver.model_text or silver.text,
            word_count=len((silver.model_text or silver.text).split()),
        )
    ]
    is_scientific = _uses_scientific_quality_profile(silver)
    is_code = silver.source_format == "code"
    primary_quality = (
        state.code_quality
        if is_code
        else state.finepdfs_quality
        if is_scientific
        else state.fineweb_quality
    )
    comparison_quality = state.fineweb_quality if is_scientific else None
    max_scored_segments = max(1, int(os.environ.get("S2P_MAX_SCORED_SEGMENTS", "10")))
    sampled_ids = {
        segment.segment_id
        for segment in representative_segments(source_segments, limit=max_scored_segments)
    }
    segment_scores: list[SegmentScore] = []
    kept_segments: list[SilverSegment] = []
    runtime_excluded: list[str] = []
    removed_body_pii: list[PiiFlag] = []
    removed_for_c4 = False
    high_confidence_pii: set[PiiFlag] = {"email", "ssn", "credit_card"}

    for segment in source_segments:
        segment_c4 = state.c4.stats(segment.text)
        segment_pii = state.pii.flags(segment.text)
        segment_blocking_pii = state.pii.blocking_flags(segment.text)
        exclusion_reasons: list[str] = []
        if not segment_c4.curly_brace_pass and not is_scientific and not is_code:
            exclusion_reasons.append("C4 curly-brace signal isolated to this section")
            removed_for_c4 = True
        if not segment_c4.lorem_ipsum_pass:
            exclusion_reasons.append("placeholder boilerplate isolated to this section")
            removed_for_c4 = True
        sensitive = sorted(set(segment_blocking_pii) & high_confidence_pii)
        if "phone" in segment_blocking_pii:
            sensitive.append("phone")
        if "passport" in segment_blocking_pii:
            sensitive.append("passport")
        sensitive = sorted(set(sensitive))
        if sensitive:
            exclusion_reasons.append("high-confidence PII isolated to this section")
            removed_body_pii.extend(sensitive)

        edu_score: float | None = None
        finepdfs_edu_score: float | None = None
        fineweb_edu_score: float | None = None
        quality_classifier_revision: str | None = None
        comparison_classifier_revision: str | None = None
        segment_perplexity: float | None = None
        segment_bucket: str | None = None
        if segment.segment_id in sampled_ids:
            quality_result = (
                state.code_quality.score(segment.text, path=segment.title)
                if is_code
                else primary_quality.score(segment.text)
            )
            comparison_result = (
                comparison_quality.score(segment.text) if comparison_quality is not None else None
            )
            perplexity_result = None if is_code else state.kenlm.score(segment.text)
            edu_score = quality_result.edu_score
            quality_classifier_revision = quality_result.revision
            if is_scientific:
                finepdfs_edu_score = quality_result.edu_score
                fineweb_edu_score = (
                    comparison_result.edu_score if comparison_result is not None else None
                )
                comparison_classifier_revision = (
                    comparison_result.revision if comparison_result is not None else None
                )
            elif not is_code:
                fineweb_edu_score = quality_result.edu_score
            segment_perplexity = (
                perplexity_result.perplexity if perplexity_result is not None else None
            )
            segment_bucket = perplexity_result.bucket if perplexity_result is not None else None

        decision = "excluded" if exclusion_reasons else "included"
        segment_scores.append(
            SegmentScore(
                segment_id=segment.segment_id,
                title=segment.title,
                role=segment.role,
                word_count=segment.word_count,
                edu_score=edu_score,
                finepdfs_edu_score=finepdfs_edu_score,
                fineweb_edu_score=fineweb_edu_score,
                quality_classifier_revision=quality_classifier_revision,
                comparison_classifier_revision=comparison_classifier_revision,
                perplexity=segment_perplexity,
                perplexity_bucket=segment_bucket,  # type: ignore[arg-type]
                c4_pass=(
                    segment_c4.nopunc_pass
                    and (segment_c4.curly_brace_pass or is_code)
                    and segment_c4.lorem_ipsum_pass
                ),
                pii_flags=segment_pii,
                decision=decision,  # type: ignore[arg-type]
                exclusion_reasons=exclusion_reasons,
            )
        )
        if exclusion_reasons:
            runtime_excluded.append(f"{segment.title}: {'; '.join(exclusion_reasons)}")
        else:
            kept_segments.append(segment)

    # If every representative segment happened to be excluded while later
    # sections remain, score one retained section so aggregate signals never
    # silently describe removed text.
    measured_kept = [
        score
        for score in segment_scores
        if score.decision == "included" and score.edu_score is not None
    ]
    if kept_segments and not measured_kept:
        fallback_segment = kept_segments[0]
        quality_result = (
            state.code_quality.score(fallback_segment.text, path=fallback_segment.title)
            if is_code
            else primary_quality.score(fallback_segment.text)
        )
        comparison_result = (
            comparison_quality.score(fallback_segment.text)
            if comparison_quality is not None
            else None
        )
        perplexity_result = None if is_code else state.kenlm.score(fallback_segment.text)
        segment_scores = [
            score.model_copy(
                update={
                    "edu_score": quality_result.edu_score,
                    "finepdfs_edu_score": quality_result.edu_score if is_scientific else None,
                    "fineweb_edu_score": (
                        comparison_result.edu_score
                        if comparison_result is not None
                        else None
                        if is_code
                        else quality_result.edu_score
                    ),
                    "quality_classifier_revision": quality_result.revision,
                    "comparison_classifier_revision": comparison_result.revision
                    if comparison_result is not None
                    else None,
                    "perplexity": (
                        perplexity_result.perplexity if perplexity_result is not None else None
                    ),
                    "perplexity_bucket": (
                        perplexity_result.bucket if perplexity_result is not None else None
                    ),
                }
            )
            if score.segment_id == fallback_segment.segment_id
            else score
            for score in segment_scores
        ]

    model_text = "\n\n".join(segment.text.strip() for segment in kept_segments).strip()
    structured_text, structured_pii_flags, structured_exclusions = _filter_structured_projection(
        state.pii, silver.structured_text
    )
    removed_body_pii.extend(structured_pii_flags)
    runtime_excluded.extend(structured_exclusions)
    text = _training_projection(silver, kept_segments, structured_text=structured_text)
    reject: list[RejectReason] = []
    if len(model_text.split()) < (20 if is_code else 50):
        reject.append("insufficient_scientific_body")
    if removed_body_pii and not kept_segments:
        reject.append("pii_detected")
    if removed_for_c4 and not kept_segments:
        reject.append("c4_nopunc_filter")

    gopher_stats = state.gopher.stats(model_text)
    gopher_pass = True if is_code else state.gopher.passes(model_text)
    if not gopher_pass and "insufficient_scientific_body" not in reject:
        reject.append("gopher_filter")
    cstats = state.c4.stats(model_text)
    c4_pass = (
        cstats.nopunc_pass
        and cstats.lorem_ipsum_pass
        and (cstats.curly_brace_pass or is_scientific or is_code)
    )
    if not c4_pass and "insufficient_scientific_body" not in reject:
        reject.append("c4_nopunc_filter")

    edu_score, perplexity, perplexity_bucket = aggregate_segment_scores(segment_scores)
    measured_buckets = [
        score.perplexity_bucket
        for score in segment_scores
        if score.decision == "included" and score.perplexity_bucket is not None
    ]
    tail_fraction = (
        sum(bucket == "tail" for bucket in measured_buckets) / len(measured_buckets)
        if measured_buckets
        else 1.0
    )
    if not is_code and tail_fraction >= 0.75 and perplexity > 2000:
        reject.append("high_perplexity")

    retained_view = silver.model_copy(
        update={
            "segments": kept_segments,
            "training_word_count": len(text.split()),
            "included_section_count": len(kept_segments),
        }
    )
    structure = source_scores(retained_view, quality_score=edu_score)
    quality_score = composite_quality_score(
        edu_score=edu_score,
        structural_quality_score=structure.structural_quality_score,
        lang_score=silver.lang_score,
        gopher_pass=gopher_pass,
        c4_pass=c4_pass,
        perplexity_bucket=perplexity_bucket,
    )
    if edu_score < 0.75 and structure.structural_quality_score < 2.0:
        reject.append("low_quality_score")
    if silver.lang != "en" or silver.lang_score < 0.5:
        reject.append("language_filter")
    if _license_reject_reason(silver) is not None:
        reject.append("license_excluded")

    pii_flags = state.pii.blocking_flags(text)
    if pii_flags:
        reject.append("pii_detected")
    metadata_pii_flags = state.pii.flags(silver.source_metadata_text)
    pii_review_flags = sorted(set(state.pii.flags(text)) - set(pii_flags))
    pii_review_notes = (
        [f"non-blocking PII-like patterns retained for audit: {', '.join(pii_review_flags)}"]
        if pii_review_flags
        else []
    )

    # Deduplicate the actual projection that could enter a training export,
    # not author blocks, bibliography, or discarded sections.
    sig = state.minhasher.signature(text)
    near = state.lsh.observe(silver.doc_id, sig)
    if near.is_near_duplicate:
        reject.append("near_duplicate")

    reject = list(dict.fromkeys(reject))
    route = route_document(
        silver=silver,
        reject_reasons=list(reject),
        reasoning_score=structure.reasoning_score,
    )
    risk = _risk_from_reject(reject, pii_flags)
    license_id = silver.spdx_license or "unknown"
    token_count = state.tokenizer.count(text)
    pii_action = (
        "body_quarantine"
        if "pii_detected" in reject
        else "segments_removed"
        if removed_body_pii
        else "metadata_removed"
        if metadata_pii_flags
        else "none"
    )
    pre_record = GoldRecord(
        doc_id=silver.doc_id,
        text=text,
        lang=silver.lang,
        tokens=token_count.tokens,
        quality_score=quality_score,
        edu_score=edu_score,
        structural_quality_score=structure.structural_quality_score,
        extraction_completeness=structure.extraction_completeness,
        reasoning_score=structure.reasoning_score,
        benchmark_score=structure.benchmark_score,
        route=route.route,
        eligible_routes=route.eligible_routes,
        route_reasons=[*route.reasons, *runtime_excluded, *pii_review_notes],
        content_tags=structure.content_tags,
        segment_scores=segment_scores,
        projection_version=silver.projection_version,
        source_word_count=silver.source_word_count,
        training_word_count=len(text.split()),
        included_section_count=len(kept_segments),
        excluded_section_count=silver.excluded_section_count + len(runtime_excluded),
        excluded_sections=[*silver.excluded_sections, *runtime_excluded],
        lang_score=silver.lang_score,
        lang_detector_revision=silver.lang_detector_revision,
        tokenizer_revision=f"{token_count.backend}:cl100k_base",
        gopher_pass=gopher_pass,
        gopher_word_count=gopher_stats.word_count,
        gopher_mean_word_len=gopher_stats.mean_word_len,
        gopher_stopword_ratio=gopher_stats.stopword_ratio,
        gopher_bullet_line_ratio=gopher_stats.bullet_line_ratio,
        gopher_ellipsis_line_ratio=gopher_stats.ellipsis_line_ratio,
        gopher_symbol_word_ratio=gopher_stats.symbol_word_ratio,
        gopher_alpha_word_ratio=gopher_stats.alpha_word_ratio,
        c4_nopunc_pass=cstats.nopunc_pass,
        c4_curly_brace_pass=cstats.curly_brace_pass,
        c4_lorem_ipsum_pass=cstats.lorem_ipsum_pass,
        c4_fraction_lines_with_punct=cstats.fraction_lines_with_punct,
        perplexity=perplexity,
        perplexity_bucket=perplexity_bucket,  # type: ignore[arg-type]
        perplexity_scorer=state.kenlm.scorer,
        near_duplicate=near.is_near_duplicate,
        near_dup_cluster_id=near.cluster_id,
        minhash_backend=sig.backend,
        minhash_num_perms=sig.num_perms,
        lsh_backend=state.lsh.backend,
        license=license_id,
        # Legacy Gold licence-source enum is narrower than SPDX provenance;
        # the exact source remains in spdx_license_source and the admission ledger.
        license_source="unknown",
        risk_tier=risk,
        pii_flags=pii_flags,
        metadata_pii_flags=metadata_pii_flags,
        removed_body_pii_flags=sorted(set(removed_body_pii)),
        pii_action=pii_action,  # type: ignore[arg-type]
        pii_scanner_revision=state.pii.revision,
        contaminated_with=[],
        valid_from=silver.valid_from,
        valid_to=silver.valid_to,
        reject_reasons=reject,
        scoring_version=state.scoring_version,
        classifier_revision=primary_quality.revision,
        classifier_backend=primary_quality.backend,
        policy_revision=state.policy_revision,
        snapshot_id=None,
        trace_id=silver.trace_id,
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
        spdx_license=silver.spdx_license,
        spdx_license_source=silver.spdx_license_source,
        scientific_artifact_s3_uri=silver.scientific_artifact_s3_uri,
        figure_count=silver.figure_count,
        table_count=silver.table_count,
        equation_count=silver.equation_count,
        citation_count=silver.citation_count,
        extraction_warnings=list(silver.extraction_warnings),
    )
    post_record, hits = state.decon.scan(pre_record)
    if hits:
        new_reasons = list(post_record.reject_reasons)
        if "decontamination_hit" not in new_reasons:
            new_reasons.append("decontamination_hit")
        final_route = route_document(
            silver=silver,
            reject_reasons=list(new_reasons),
            reasoning_score=structure.reasoning_score,
        )
        return post_record.model_copy(
            update={
                "reject_reasons": new_reasons,
                "risk_tier": 3,
                "route": final_route.route,
                "eligible_routes": final_route.eligible_routes,
                "route_reasons": [
                    *final_route.reasons,
                    *runtime_excluded,
                    *pii_review_notes,
                ],
            }
        )
    return post_record


def _training_projection(
    silver: SilverRecord,
    segments: list[SilverSegment],
    *,
    structured_text: str | None = None,
) -> str:
    blocks: list[str] = []
    if silver.title:
        blocks.append(f"# {silver.title}")
    for segment in segments:
        blocks.append(f"## {segment.title}\n{segment.text.strip()}")
    structured = silver.structured_text if structured_text is None else structured_text
    if structured.strip():
        blocks.append(structured.strip())
    return "\n\n".join(block for block in blocks if block.strip()).strip()


_STRUCTURED_BLOCK_START = re.compile(r"(?=\[(?:TABLE|EQUATION|FIGURE)\])")


def _filter_structured_projection(
    scanner: PiiScanner, structured_text: str
) -> tuple[str, list[PiiFlag], list[str]]:
    """Remove only structured evidence blocks containing blocking PII.

    The full table/figure/equation remains in the immutable scientific
    artifact for provenance, while its text surrogate is kept out of the
    training projection.
    """
    if not structured_text.strip():
        return "", [], []
    blocks = [
        value.strip() for value in _STRUCTURED_BLOCK_START.split(structured_text) if value.strip()
    ]
    kept: list[str] = []
    removed: list[PiiFlag] = []
    exclusions: list[str] = []
    for index, block in enumerate(blocks, start=1):
        flags = scanner.blocking_flags(block)
        if flags:
            removed.extend(flags)
            exclusions.append(
                f"structured evidence block {index}: high-confidence PII removed from training projection"
            )
        else:
            kept.append(block)
    return "\n\n".join(kept), sorted(set(removed)), exclusions


def _risk_from_reject(reject: Sequence[RejectReason], pii_flags: Sequence[PiiFlag]) -> RiskTier:
    """Map current reject signals onto the 1/2/3 risk-tier ladder."""
    if "decontamination_hit" in reject or "pii_detected" in reject or pii_flags:
        return 3
    if reject:
        return 2
    return 1


def _license_reject_reason(silver: SilverRecord) -> RejectReason | None:
    """Apply the provisional non-code policy to legacy and replay rows."""
    return (
        None
        if is_training_permitted(silver.spdx_license, source_format=silver.source_format)
        else "license_excluded"
    )


def _uses_scientific_quality_profile(silver: SilverRecord) -> bool:
    """Select the PDF-trained classifier only for scientific-source records."""
    if silver.scientific_artifact_s3_uri:
        return True
    feed = silver.source_feed.lower()
    return silver.source_format in {"pdf", "latex"} or any(
        marker in feed for marker in ("arxiv", "openreview", "pes2o")
    )


def is_trainable_gold(record: GoldRecord) -> bool:
    """True when a GoldRecord is allowed onto ``docs.curated`` and Gold."""
    return (
        record.risk_tier == 1
        and record.route
        in {"pretrain", "broad_pretraining", "posttrain_candidate", "reasoning_candidate"}
        and not record.reject_reasons
        and not record.pii_flags
        and not record.contaminated_with
    )


def process_silver_payload(
    state: CurateState,
    payload: bytes,
    *,
    metrics: ProcessorMetrics | None = None,
) -> bytes | None:
    """Deserialize a SilverRecord payload and return trainable Gold JSON."""
    silver = common.silver_loads(payload)
    gold = curate_one(state, silver)
    if metrics is not None:
        metrics.record_decon_scan(benchmarks=gold.contaminated_with)
        metrics.record_route(route=gold.route)
    if not is_trainable_gold(gold):
        if metrics is not None and gold.reject_reasons:
            metrics.record_dropped(
                reasons=gold.reject_reasons,
                quality_score=gold.quality_score,
                edu_score=gold.edu_score,
            )
        return None
    if metrics is not None:
        metrics.record_curated(
            source_feed=gold.source_feed,
            quality_score=gold.quality_score,
            edu_score=gold.edu_score,
        )
    return common.gold_dumps(gold)


def process_silver_decision_payload(
    state: CurateState,
    payload: bytes,
    *,
    metrics: ProcessorMetrics | None = None,
) -> tuple[bytes, bool]:
    """Return the durable scored decision and whether it is trainable."""
    cache_key = _decision_cache_key(state, payload)
    cached = state.decision_cache.get(cache_key)
    if cached is not None:
        return cached
    silver = common.silver_loads(payload)
    gold = curate_one(state, silver)
    if metrics is not None:
        metrics.record_decon_scan(benchmarks=gold.contaminated_with)
        metrics.record_route(route=gold.route)
        if is_trainable_gold(gold):
            metrics.record_curated(
                source_feed=gold.source_feed,
                quality_score=gold.quality_score,
                edu_score=gold.edu_score,
            )
        elif gold.reject_reasons:
            metrics.record_dropped(
                reasons=gold.reject_reasons,
                quality_score=gold.quality_score,
                edu_score=gold.edu_score,
            )
    decision = common.gold_dumps(gold)
    trainable = is_trainable_gold(gold)
    state.decision_cache.put(cache_key, decision, trainable=trainable)
    return decision, trainable


def build_dataflow(cfg: common.ProcessorConfig) -> object:
    """Build the Bytewax dataflow object."""
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-curate", cfg)
    state = build_state(cfg)
    flow = Dataflow("s2p-curate")
    # Default to ``beginning`` so a restart with no committed group offset
    # replays the topic instead of dropping in-flight bytes (at-least-once
    # semantics; matches the Kappa/streaming-first contract). Operators can
    # override via ``S2P_KAFKA_START_OFFSET=end`` for short-lived debug runs.
    start_offset = common.kafka_starting_offset()
    source = KafkaSource(
        brokers=cfg.redpanda_brokers.split(","),
        topics=[cfg.normalized_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
    )
    inp = op.input("docs_normalized", flow, source)

    def _step(
        msg: object,
    ) -> tuple[KafkaSinkMessage, KafkaSinkMessage | None] | None:
        with tracer.start_as_current_span("curate.process") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                return None
            try:
                decision, trainable = process_silver_decision_payload(
                    state, payload, metrics=PROCESSOR_METRICS
                )
            except Exception as exc:
                span.record_exception(exc)
                return None
            key = getattr(msg, "key", None) or b""
            decision_message = KafkaSinkMessage(key=key, value=decision)
            curated_message = KafkaSinkMessage(key=key, value=decision) if trainable else None
            return decision_message, curated_message

    mapped = op.map("curate_run", inp, _step)
    filtered = op.filter("curate_drop_none", mapped, lambda m: m is not None)
    decisions = op.map("curate_decision_message", filtered, lambda pair: pair[0])
    decision_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=cfg.decisions_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_decision_sink", decisions, decision_sink)
    accepted_pairs = op.filter("curate_trainable_only", filtered, lambda pair: pair[1] is not None)
    accepted = op.map("curate_accepted_message", accepted_pairs, lambda pair: pair[1])
    curated_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=cfg.curated_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_sink", accepted, curated_sink)
    return flow


def main() -> None:
    """Entrypoint for the ``s2p-curate`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.curate")
    log.info(
        "starting curate dataflow",
        brokers=cfg.redpanda_brokers,
        topic=cfg.normalized_topic,
    )
    start_probe_server(metrics_provider=PROCESSOR_METRICS.render_prometheus)
    flow = build_dataflow(cfg)
    common.run_bytewax_flow(flow, cfg, "curate")


def now_utc() -> Any:
    """Re-exported for tests; returns a tz-aware UTC datetime."""
    from datetime import datetime

    return datetime.now(UTC)
