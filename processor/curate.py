"""Stateful curation worker: ``docs.normalized`` -> ``docs.curated``.

End-to-end source-aware curation. The worker:

1. Consumes :class:`SilverRecord` payloads from ``docs.normalized``.
2. Runs the Gopher heuristic gate.
3. Runs the C4 nopunc / curly-brace / lorem-ipsum gate.
4. Re-scores perplexity (KenLM) and re-buckets - the fetcher emitted
   stub values; curation owns the real signals.
5. Recomputes the MinHash signature (cheap, ~us/doc) and tests the
   :class:`LSHBloomIndex` near-dup index.
6. Dispatches by source family: FinePDFs Edu v2 for papers, FineWeb-Edu for
   Hugging Face cards and web prose, and a discovery-only metadata policy.
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
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC
from typing import Any, Protocol, cast

from ingest.common.license_admission import (
    is_posttrain_transform_permitted,
    is_training_permitted,
)
from processor import common
from processor.decision_cache import DecisionCache
from processor.decon_gate import (  # type: ignore[attr-defined]
    DeconGate,
    EmbeddingSketch,
    _EmbeddingSketch,
)
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.model_client import (
    CuratorModelClient,
    RemoteEmbeddingSketch,
    RemoteKenLMScorer,
    RemoteQualityClassifier,
)
from processor.operators.c4 import C4Filter
from processor.operators.gopher import GopherFilter
from processor.operators.hf_card_quality import assess_hf_card
from processor.operators.kenlm_score import KenLMScorer, PerplexityResult
from processor.operators.lshbloom import LSHBloomIndex
from processor.operators.minhash import MinHasher
from processor.operators.pii import PiiScanner
from processor.operators.quality import QualityClassifier, QualityScore
from processor.operators.source_quality import (
    MetadataDiscoveryPolicy,
)
from processor.probes import start_probe_server
from processor.scientific_policy import (
    RouteDecision,
    aggregate_segment_scores,
    composite_quality_score,
    posttrain_candidate_eligible,
    route_document,
    source_scores,
)
from processor.source_policy import resolve_source_policy
from processor.tokenize import Tokenizer
from schemas.bronze import BronzeRecord
from schemas.decon import BenchmarkName
from schemas.gold import GoldRecord, PiiFlag, RejectReason, RiskTier, SegmentScore
from schemas.silver import SilverRecord, SilverSegment

POLICY_REVISION_ENV = "S2P_POLICY_REVISION"
SCORING_VERSION_ENV = "S2P_SCORING_VERSION"
CURATOR_FLOW_NAME = "s2p-curate-live-v5"
CURATOR_RECOVERY_NAME = "curate-live-v5"
_ARXIV_CONTENT_URL = re.compile(
    r"^https?://(?:arxiv\.org/(?:html|pdf)|ar5iv\.labs\.arxiv\.org/html)/",
    re.IGNORECASE,
)


class QualityScorer(Protocol):
    @property
    def revision(self) -> str: ...

    @property
    def backend(self) -> str: ...

    def score(self, text: str) -> QualityScore: ...


class PerplexityScorer(Protocol):
    @property
    def scorer(self) -> str: ...

    def score(self, text: str) -> PerplexityResult: ...


@dataclass(slots=True)
class CurateState:
    """Per-worker state for the curation dataflow."""

    gopher: GopherFilter
    c4: C4Filter
    kenlm: PerplexityScorer
    minhasher: MinHasher
    lsh: LSHBloomIndex
    finepdfs_quality: QualityScorer
    fineweb_quality: QualityScorer
    metadata_discovery: MetadataDiscoveryPolicy
    pii: PiiScanner
    decon: DeconGate
    tokenizer: Tokenizer
    policy_revision: str
    scoring_version: str
    decision_cache: DecisionCache
    model_clients: tuple[CuratorModelClient, ...] = ()

    def close(self) -> None:
        self.decision_cache.close()
        self.lsh.close()
        for model_client in self.model_clients:
            model_client.close()


@dataclass(frozen=True, slots=True)
class _SegmentModelSignals:
    quality: QualityScore
    comparison: QualityScore | None
    perplexity: PerplexityResult | None


def build_state(cfg: common.ProcessorConfig) -> CurateState:
    """Construct a :class:`CurateState` from the runtime config."""
    models = cfg.models_dir
    require_real_models = os.environ.get("S2P_REQUIRE_REAL_MODELS") == "1"
    model_service_url = os.environ.get("S2P_MODEL_SERVICE_URL", "").strip()
    quality_service_url = os.environ.get("S2P_QUALITY_MODEL_SERVICE_URL", "").strip()
    kenlm_service_url = os.environ.get("S2P_KENLM_MODEL_SERVICE_URL", "").strip()
    embedding_service_url = os.environ.get("S2P_EMBEDDING_MODEL_SERVICE_URL", "").strip()
    kenlm_path = os.path.join(models, "kenlm", "en.arpa.bin")
    kenlm_sentencepiece_path = os.path.join(models, "kenlm", "en.sp.model")
    finepdfs_quality_dir = os.path.join(models, "finepdfs-edu-v2")
    fineweb_quality_dir = os.path.join(models, "fineweb-edu")
    e5_dir = os.path.join(models, "e5-small")
    model_clients: tuple[CuratorModelClient, ...] = ()
    embedding: EmbeddingSketch
    kenlm: PerplexityScorer
    finepdfs_quality: QualityScorer
    fineweb_quality: QualityScorer
    if model_service_url:
        model_client = CuratorModelClient(model_service_url)
        embedding = RemoteEmbeddingSketch(model_client)
        kenlm = RemoteKenLMScorer(model_client)
        finepdfs_quality = RemoteQualityClassifier(model_client, "finepdfs-edu-v2")
        fineweb_quality = RemoteQualityClassifier(model_client, "fineweb-edu")
        model_clients = (model_client,)
    elif any((quality_service_url, kenlm_service_url, embedding_service_url)):
        if not all((quality_service_url, kenlm_service_url, embedding_service_url)):
            raise RuntimeError("all split curator model service URLs are required")
        quality_client = CuratorModelClient(quality_service_url)
        kenlm_client = CuratorModelClient(kenlm_service_url)
        embedding_client = CuratorModelClient(embedding_service_url)
        embedding = RemoteEmbeddingSketch(embedding_client)
        kenlm = RemoteKenLMScorer(kenlm_client)
        finepdfs_quality = RemoteQualityClassifier(quality_client, "finepdfs-edu-v2")
        fineweb_quality = RemoteQualityClassifier(quality_client, "fineweb-edu")
        model_clients = (quality_client, kenlm_client, embedding_client)
    else:
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
        metadata_discovery=MetadataDiscoveryPolicy(),
        pii=pii,
        decon=decon,
        tokenizer=Tokenizer(allow_fallback=not require_real_models),
        policy_revision=os.environ.get(POLICY_REVISION_ENV, "git:dev"),
        scoring_version=os.environ.get(SCORING_VERSION_ENV, "v0.1.0"),
        decision_cache=DecisionCache(os.path.join(cfg.state_dir, "decision-cache.sqlite3")),
        model_clients=model_clients,
    )


def _decision_cache_key(state: CurateState, payload: bytes) -> str:
    """Fingerprint the exact Silver bytes and every material scoring revision."""
    recipe = "\n".join(
        (
            state.policy_revision,
            state.scoring_version,
            state.finepdfs_quality.revision,
            state.fineweb_quality.revision,
            state.metadata_discovery.revision,
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


def _score_segment_models(
    segments: Sequence[SilverSegment],
    *,
    primary_quality: QualityScorer,
    comparison_quality: QualityScorer | None,
    kenlm: PerplexityScorer,
    use_kenlm: bool,
) -> dict[str, _SegmentModelSignals]:
    """Run independent stateless model calls across the available replicas."""
    concurrency = max(1, int(os.environ.get("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "1")))
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        pending: dict[
            str,
            tuple[
                Future[QualityScore],
                Future[QualityScore] | None,
                Future[PerplexityResult] | None,
            ],
        ] = {}
        for segment in segments:
            pending[segment.segment_id] = (
                executor.submit(primary_quality.score, segment.text),
                (
                    executor.submit(comparison_quality.score, segment.text)
                    if comparison_quality is not None
                    else None
                ),
                executor.submit(kenlm.score, segment.text) if use_kenlm else None,
            )
        return {
            segment_id: _SegmentModelSignals(
                quality=quality.result(),
                comparison=comparison.result() if comparison is not None else None,
                perplexity=perplexity.result() if perplexity is not None else None,
            )
            for segment_id, (quality, comparison, perplexity) in pending.items()
        }


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
    source_policy = resolve_source_policy(
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
    )
    is_scientific = source_policy.family == "scientific_paper"
    is_hf_card = source_policy.family in {"hf_model_card", "hf_dataset_card"}
    is_metadata = not source_policy.training_text
    uses_fineweb = source_policy.quality_profile == "fineweb_edu"
    primary_quality: QualityScorer
    if is_scientific:
        primary_quality = state.finepdfs_quality
    elif is_metadata:
        primary_quality = state.metadata_discovery
    else:
        primary_quality = state.fineweb_quality
    comparison_quality: QualityScorer | None = (
        state.fineweb_quality if is_scientific else state.finepdfs_quality if is_hf_card else None
    )
    # Every segment is classified. The calls are stateless and may run across
    # independent model-service replicas, while this Bytewax worker remains the
    # sole owner of ordered curation state and recovery.
    sanitized_segments = [
        (segment, state.pii.sanitize(segment.text)) for segment in source_segments
    ]
    safe_segments = [
        segment.model_copy(
            update={
                "text": sanitization.text,
                "word_count": len(sanitization.text.split()),
            }
        )
        for segment, sanitization in sanitized_segments
    ]
    model_signals = _score_segment_models(
        safe_segments,
        primary_quality=primary_quality,
        comparison_quality=comparison_quality,
        kenlm=state.kenlm,
        use_kenlm=source_policy.kenlm_mode != "off",
    )
    segment_scores: list[SegmentScore] = []
    kept_segments: list[SilverSegment] = []
    runtime_excluded: list[str] = []
    removed_body_pii: list[PiiFlag] = []
    blocking_artifact_pii: list[PiiFlag] = []
    removed_for_c4 = False

    for (segment, sanitization), safe_segment in zip(
        sanitized_segments, safe_segments, strict=True
    ):
        segment_c4 = state.c4.stats(safe_segment.text)
        segment_pii = list(sanitization.flags)
        removed_body_pii.extend(sanitization.redacted_flags)
        blocking_artifact_pii.extend(sanitization.blocking_flags)
        exclusion_reasons: list[str] = []
        if source_policy.web_heuristic_gate and not segment_c4.curly_brace_pass:
            exclusion_reasons.append("C4 curly-brace signal isolated to this section")
            removed_for_c4 = True
        if source_policy.web_heuristic_gate and not segment_c4.lorem_ipsum_pass:
            exclusion_reasons.append("placeholder boilerplate isolated to this section")
            removed_for_c4 = True
        edu_score: float | None = None
        finepdfs_edu_score: float | None = None
        fineweb_edu_score: float | None = None
        quality_classifier_revision: str | None = None
        comparison_classifier_revision: str | None = None
        segment_perplexity: float | None = None
        segment_bucket: str | None = None
        signals = model_signals[segment.segment_id]
        quality_result = signals.quality
        comparison_result = signals.comparison
        perplexity_result = signals.perplexity
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
        elif is_hf_card:
            fineweb_edu_score = quality_result.edu_score
            finepdfs_edu_score = (
                comparison_result.edu_score if comparison_result is not None else None
            )
            comparison_classifier_revision = (
                comparison_result.revision if comparison_result is not None else None
            )
        elif uses_fineweb:
            fineweb_edu_score = quality_result.edu_score
        segment_perplexity = perplexity_result.perplexity if perplexity_result is not None else None
        segment_bucket = perplexity_result.bucket if perplexity_result is not None else None

        decision = "excluded" if exclusion_reasons else "included"
        segment_scores.append(
            SegmentScore(
                segment_id=segment.segment_id,
                title=segment.title,
                role=segment.role,
                word_count=safe_segment.word_count,
                edu_score=edu_score,
                finepdfs_edu_score=finepdfs_edu_score,
                fineweb_edu_score=fineweb_edu_score,
                quality_classifier_revision=quality_classifier_revision,
                comparison_classifier_revision=comparison_classifier_revision,
                perplexity=segment_perplexity,
                perplexity_bucket=segment_bucket,  # type: ignore[arg-type]
                c4_pass=(
                    segment_c4.nopunc_pass
                    and (segment_c4.curly_brace_pass or not source_policy.web_heuristic_gate)
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
            kept_segments.append(safe_segment)

    model_text = "\n\n".join(segment.text.strip() for segment in kept_segments).strip()
    (
        structured_text,
        structured_pii_flags,
        structured_audit_pii_flags,
        structured_exclusions,
    ) = _filter_structured_projection(state.pii, silver.structured_text)
    removed_body_pii.extend(structured_pii_flags)
    blocking_artifact_pii.extend(structured_audit_pii_flags)
    runtime_excluded.extend(structured_exclusions)
    text = _training_projection(silver, kept_segments, structured_text=structured_text)
    reject: list[RejectReason] = []
    minimum_words = 50
    if is_metadata:
        reject.append("metadata_only")
    if len(model_text.split()) < minimum_words:
        reject.append("insufficient_scientific_body" if is_scientific else "insufficient_body")
    if blocking_artifact_pii:
        reject.append("pii_detected")
    if removed_for_c4 and not kept_segments:
        reject.append("c4_nopunc_filter")

    gopher_stats = state.gopher.stats(model_text)
    # Gopher/FineWeb heuristics are web-crawl filters. They remain visible as
    # diagnostics on scientific and review prose but never hard-reject those
    # source families.
    gopher_pass = state.gopher.passes(model_text) if source_policy.web_heuristic_gate else True
    if not gopher_pass and not {
        "insufficient_body",
        "insufficient_scientific_body",
    }.intersection(reject):
        reject.append("gopher_filter")
    cstats = state.c4.stats(model_text)
    c4_pass = (
        cstats.nopunc_pass and cstats.lorem_ipsum_pass and cstats.curly_brace_pass
        if source_policy.web_heuristic_gate
        else True
    )
    if (
        source_policy.web_heuristic_gate
        and not c4_pass
        and not {
            "insufficient_body",
            "insufficient_scientific_body",
        }.intersection(reject)
    ):
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
    if source_policy.kenlm_mode == "gate" and tail_fraction >= 0.75 and perplexity > 2000:
        reject.append("high_perplexity")

    retained_view = silver.model_copy(
        update={
            "segments": kept_segments,
            "training_word_count": len(text.split()),
            "included_section_count": len(kept_segments),
        }
    )
    structure = source_scores(retained_view, quality_score=edu_score)
    hf_assessment = (
        assess_hf_card(
            kind="model" if source_policy.family == "hf_model_card" else "dataset",
            title=silver.title,
            text=model_text,
            segments=kept_segments,
        )
        if is_hf_card
        else None
    )
    quality_score = composite_quality_score(
        edu_score=edu_score,
        structural_quality_score=structure.structural_quality_score,
        lang_score=silver.lang_score,
        gopher_pass=gopher_pass,
        c4_pass=c4_pass,
        perplexity_bucket=perplexity_bucket,
        language_applicable=source_policy.language_gate,
        web_heuristics_applicable=source_policy.web_heuristic_gate,
        perplexity_applicable=source_policy.kenlm_mode != "off",
    )
    # FineWeb-Edu's official model card recommends retaining integer scores
    # 3 and above. It is a grounded gate only for ordinary web prose. Hub
    # cards and repository documentation use the score as an audit signal
    # because their structured Markdown is outside that web-crawl threshold.
    if source_policy.web_heuristic_gate and edu_score < 3.0:
        reject.append("low_quality_score")
    if hf_assessment is not None and not hf_assessment.accepted:
        reject.append("hf_card_quality_filter")
    if source_policy.language_gate and (silver.lang != "en" or silver.lang_score < 0.5):
        reject.append("language_filter")
    if _license_reject_reason(silver) is not None:
        reject.append("license_excluded")
    if is_scientific and any(
        warning.startswith("figure_enrichment_failed:")
        or warning in {"figure_limit_reached", "page_limit_reached"}
        for warning in silver.extraction_warnings
    ):
        reject.append("incomplete_scientific_extraction")

    # The projection has already been sanitized. Only high-risk findings remain
    # blocking; ordinary contact details are represented by typed placeholders.
    pii_flags = sorted(set(blocking_artifact_pii))
    metadata_pii_flags = state.pii.flags(silver.source_metadata_text)
    pii_review_flags = sorted(
        (
            {
                flag
                for score in segment_scores
                if score.decision == "included"
                for flag in score.pii_flags
            }
            | set(structured_audit_pii_flags)
        )
        - set(pii_flags)
    )
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
    retry_attempt = _extraction_retry_attempt(silver.extraction_pipeline)
    max_extraction_retries = int(os.environ.get("S2P_CURATOR_MAX_EXTRACTION_RETRIES", "2"))
    if route.route == "retry" and (
        silver.raw_html_s3_uri is None
        or retry_attempt >= max_extraction_retries
        or not _ARXIV_CONTENT_URL.match(str(silver.url))
    ):
        route = RouteDecision(
            route="quarantine",
            eligible_routes=["quarantine"],
            reasons=[
                "scientific extraction remained incomplete after bounded alternate retry"
                if retry_attempt >= max_extraction_retries
                else "scientific extraction retry currently requires an arXiv content URL"
                if not _ARXIV_CONTENT_URL.match(str(silver.url))
                else "scientific extraction cannot retry because the admitted Bronze pointer is absent"
            ],
        )
    if silver.training_usage == "posttrain_transform_only" and route.route not in {
        "quarantine",
        "retry",
    }:
        reason = (
            "source is restricted to derived post-training generation; "
            "verbatim pretraining export is forbidden"
        )
        if not posttrain_candidate_eligible(silver):
            reason += "; no generator for this source family is enabled yet"
        route = RouteDecision(
            route="posttrain_candidate",
            eligible_routes=["posttrain_candidate"],
            reasons=[reason],
        )
    risk: RiskTier = _risk_from_reject(reject, pii_flags)
    license_id = silver.spdx_license or "unknown"
    token_count = state.tokenizer.count(text)
    pii_action = (
        "body_quarantine"
        if "pii_detected" in reject
        else "body_redacted"
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
        content_tags=[
            *structure.content_tags,
            *(hf_assessment.categories if hf_assessment is not None else ()),
        ],
        segment_scores=segment_scores,
        projection_version=silver.projection_version,
        source_word_count=silver.source_word_count,
        training_word_count=len(text.split()),
        included_section_count=len(kept_segments),
        excluded_section_count=silver.excluded_section_count + len(runtime_excluded),
        excluded_sections=[*silver.excluded_sections, *runtime_excluded],
        lang_score=silver.lang_score,
        lang_detector_revision=(
            silver.lang_detector_revision if source_policy.language_gate else "not-applicable"
        ),
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
        perplexity_scorer=(
            state.kenlm.scorer if source_policy.kenlm_mode != "off" else "not-applicable"
        ),
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
        training_usage=silver.training_usage,
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
) -> tuple[str, list[PiiFlag], list[PiiFlag], list[str]]:
    """Sanitize structured evidence and report any artifact-blocking finding."""
    if not structured_text.strip():
        return "", [], [], []
    blocks = [
        value.strip() for value in _STRUCTURED_BLOCK_START.split(structured_text) if value.strip()
    ]
    kept: list[str] = []
    removed: list[PiiFlag] = []
    audit_flags: list[PiiFlag] = []
    exclusions: list[str] = []
    for index, block in enumerate(blocks, start=1):
        sanitization = scanner.sanitize(block)
        removed.extend(sanitization.redacted_flags)
        if sanitization.blocking_flags:
            audit_flags.extend(sanitization.blocking_flags)
            exclusions.append(
                f"structured evidence block {index}: high-risk identifier quarantines artifact"
            )
        kept.append(sanitization.text)
    return (
        "\n\n".join(kept),
        sorted(set(removed)),
        sorted(set(audit_flags)),
        exclusions,
    )


def _risk_from_reject(reject: Sequence[RejectReason], pii_flags: Sequence[PiiFlag]) -> RiskTier:
    """Map current reject signals onto the 1/2/3 risk-tier ladder."""
    if "decontamination_hit" in reject or "pii_detected" in reject or pii_flags:
        return 3
    if reject:
        return 2
    return 1


def _license_reject_reason(silver: SilverRecord) -> RejectReason | None:
    """Apply the purpose-aware licence policy to legacy and replay rows."""
    if silver.training_usage == "posttrain_transform_only" and is_posttrain_transform_permitted(
        silver.spdx_license
    ):
        return None
    return (
        None
        if is_training_permitted(silver.spdx_license, source_format=silver.source_format)
        else "license_excluded"
    )


def _uses_scientific_quality_profile(silver: SilverRecord) -> bool:
    """Select the PDF-trained classifier only for scientific-source records."""
    return (
        resolve_source_policy(
            source_feed=silver.source_feed,
            source_format=silver.source_format,
            extraction_pipeline=silver.extraction_pipeline,
        ).family
        == "scientific_paper"
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


def extraction_retry_payload(silver: SilverRecord, gold: GoldRecord) -> bytes | None:
    """Requeue an admitted Bronze body for a bounded full extraction rerun."""
    if (
        gold.route != "retry"
        or silver.raw_html_s3_uri is None
        or not _ARXIV_CONTENT_URL.match(str(silver.url))
    ):
        return None
    attempt = _extraction_retry_attempt(silver.extraction_pipeline) + 1
    record = BronzeRecord(
        doc_id=silver.doc_id,
        url=silver.url,
        fetched_at=silver.source_fetched_at or silver.valid_from,
        http_status=silver.source_http_status,
        http_last_modified=silver.source_http_last_modified,
        content_type=silver.source_content_type,
        raw_html_s3_uri=silver.raw_html_s3_uri,
        source_feed="arxiv-extraction-retry",
        trace_id=silver.trace_id,
        # This is a control envelope, not another copy of the original body.
        # The arXiv fulltext worker consumes it and deliberately refetches the
        # PDF; the core fetcher skips metadata before touching the pointer.
        source_format="metadata",
        extraction_pipeline=(f"arxiv-pdf-retry-v1|curation-retry={attempt}"),
        spdx_license=silver.spdx_license,
        spdx_license_source=silver.spdx_license_source,
        training_usage=silver.training_usage,
    )
    return record.model_dump_json().encode("utf-8")


def _extraction_retry_attempt(extraction_pipeline: str) -> int:
    match = re.search(r"\|curation-retry=(\d+)$", extraction_pipeline)
    return int(match.group(1)) if match else 0


def build_dataflow(
    cfg: common.ProcessorConfig,
    *,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
) -> object:
    """Build the Bytewax dataflow object."""
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage
    from bytewax.dataflow import Dataflow

    tracer = common.init_tracer("s2p-curate", cfg)
    flow_name = os.environ.get("S2P_BYTEWAX_FLOW_NAME", CURATOR_FLOW_NAME).strip()
    if not flow_name:
        raise RuntimeError("S2P_BYTEWAX_FLOW_NAME must not be empty")
    input_topic = os.environ.get("S2P_CURATOR_INPUT_TOPIC", cfg.normalized_topic).strip()
    if not input_topic:
        raise RuntimeError("S2P_CURATOR_INPUT_TOPIC must not be empty")
    decision_topic = os.environ.get("S2P_CURATOR_DECISIONS_TOPIC", cfg.decisions_topic).strip()
    curated_topic = os.environ.get("S2P_CURATOR_CURATED_TOPIC", cfg.curated_topic).strip()
    retry_topic = os.environ.get("S2P_CURATOR_RETRY_TOPIC", cfg.raw_topic).strip()
    if not decision_topic or not curated_topic or not retry_topic:
        raise RuntimeError("curator decision, curated, and retry output topics must not be empty")
    smoke_input = os.environ.get("S2P_SMOKE_NORMALIZED_TOPIC", "docs.normalized.smoke").strip()
    if input_topic == smoke_input and (
        decision_topic == cfg.decisions_topic
        or curated_topic == cfg.curated_topic
        or retry_topic == cfg.raw_topic
    ):
        raise RuntimeError("smoke curator outputs must not target production topics")
    state = build_state(cfg)
    failure_writer = common.DurableProcessingFailureWriter.from_config(cfg)
    flow = Dataflow(flow_name)
    payload_max_bytes = common.kafka_payload_max_bytes()
    # Default to ``beginning`` so a restart with no committed group offset
    # replays the topic instead of dropping in-flight bytes (at-least-once
    # semantics; matches the Kappa/streaming-first contract). Operators can
    # override via ``S2P_KAFKA_START_OFFSET=end`` for short-lived debug runs.
    start_offset = common.kafka_starting_offset()
    source = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="docs_normalized",
        brokers=cfg.redpanda_brokers.split(","),
        topics=[input_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
    )
    inp = op.input("docs_normalized", flow, source)

    def _step(
        msg: object,
    ) -> tuple[KafkaSinkMessage, KafkaSinkMessage | None, KafkaSinkMessage | None] | None:
        with tracer.start_as_current_span("curate.process") as span:
            payload = getattr(msg, "value", None)
            if payload is None:
                failure_writer.record(stage="curate", message=msg, reason="kafka_tombstone")
                PROCESSOR_METRICS.record_failure(stage="curate", reason="kafka_tombstone")
                return None
            try:
                silver = common.silver_loads(payload)
                decision, trainable = process_silver_decision_payload(
                    state, payload, metrics=PROCESSOR_METRICS
                )
                if len(decision) > payload_max_bytes:
                    raise common.DeterministicProcessingError(
                        f"curation decision is {len(decision)} bytes; limit is {payload_max_bytes}"
                    )
            except ValueError as exc:
                span.record_exception(exc)
                reason = type(exc).__name__
                failure_writer.record(stage="curate", message=msg, reason=reason)
                PROCESSOR_METRICS.record_failure(stage="curate", reason=reason)
                return None
            except Exception as exc:
                span.record_exception(exc)
                PROCESSOR_METRICS.record_failure(stage="curate", reason=type(exc).__name__)
                # Do not let Bytewax snapshot past transient, model, state, or
                # unexpected deterministic failures without a durable result.
                raise
            key = getattr(msg, "key", None) or b""
            decision_message = KafkaSinkMessage(key=key, value=decision)
            curated_message = KafkaSinkMessage(key=key, value=decision) if trainable else None
            retry_payload = extraction_retry_payload(silver, common.gold_loads(decision))
            retry_message = (
                KafkaSinkMessage(key=key, value=retry_payload)
                if retry_payload is not None
                else None
            )
            return decision_message, curated_message, retry_message

    mapped = op.map("curate_run", inp, _step)
    filtered = op.filter("curate_drop_none", mapped, lambda m: m is not None)
    decisions = op.map("curate_decision_message", filtered, lambda pair: pair[0])
    decision_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=decision_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_decision_sink", decisions, decision_sink)
    accepted_pairs = op.filter("curate_trainable_only", filtered, lambda pair: pair[1] is not None)
    accepted = op.map("curate_accepted_message", accepted_pairs, lambda pair: pair[1])
    curated_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=curated_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_sink", accepted, curated_sink)
    retry_pairs = op.filter("curate_retry_only", filtered, lambda pair: pair[2] is not None)
    retries = op.map("curate_retry_message", retry_pairs, lambda pair: pair[2])
    retry_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=retry_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_retry_sink", retries, retry_sink)
    return flow


def main() -> None:
    """Entrypoint for the ``s2p-curate`` console script."""
    cfg = common.load_config()
    common.configure_logging(cfg.log_level, json_output=not cfg.is_dev)
    log = common.get_logger("s2p.curate")
    input_topic = os.environ.get("S2P_CURATOR_INPUT_TOPIC", cfg.normalized_topic)
    log.info("starting Bytewax curator", brokers=cfg.redpanda_brokers, topic=input_topic)
    runtime_status = common.BytewaxRuntimeStatus()
    flow = build_dataflow(cfg, runtime_status=runtime_status)
    start_probe_server(
        metrics_provider=PROCESSOR_METRICS.render_prometheus,
        readiness_provider=runtime_status.is_ready,
    )
    recovery_name = os.environ.get("S2P_BYTEWAX_RECOVERY_NAME", CURATOR_RECOVERY_NAME).strip()
    if not recovery_name:
        raise RuntimeError("S2P_BYTEWAX_RECOVERY_NAME must not be empty")
    common.run_bytewax_flow(
        flow,
        cfg,
        recovery_name,
        runtime_status=runtime_status,
    )


def now_utc() -> Any:
    """Re-exported for tests; returns a tz-aware UTC datetime."""
    from datetime import datetime

    return datetime.now(UTC)
