"""Stateful curation worker: ``docs.normalized`` -> ``docs.curated``.

End-to-end source-aware curation. The worker:

1. Consumes :class:`SilverRecord` payloads from ``docs.normalized``.
2. Applies source-specific extraction, language and privacy checks.
3. Tests exact hashes and the durable :class:`LSHBloomIndex` near-dup index.
4. Scores all retained sections with the appropriate ModernBERT quality head.
5. Applies whole-document quality thresholds.
6. Scores both arXiv auxiliary heads only after source quality passes.
7. Applies web heuristics and KenLM only where the source policy enables them.
8. Emits a trainable :class:`GoldRecord` on ``docs.curated``.

Every scored outcome is published to ``curation.decisions`` for durable
audit. Only trainable rows are also published to
``docs.curated`` and materialized in the clean Gold table.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections.abc import Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC
from pathlib import Path
from typing import Any, Protocol

import boto3
from prometheus_client import generate_latest

from ingest.common.license_admission import (
    is_posttrain_transform_permitted,
    is_training_permitted,
)
from processor import common
from processor.content_policy import CONTENT_POLICY_GENERATION
from processor.decision_cache import DecisionCache
from processor.metrics import PROCESSOR_METRICS, ProcessorMetrics
from processor.model_client import (
    CuratorModelClient,
    ModelServiceError,
    RemoteKenLMScorer,
    RemoteQualityClassifier,
    headless_endpoint_resolver,
)
from processor.operators.c4 import C4Filter
from processor.operators.classifier_input import model_input, parse_sections
from processor.operators.gopher import GopherFilter
from processor.operators.hf_card_quality import assess_hf_card, is_hf_placeholder_section
from processor.operators.kenlm_score import KenLMScorer, PerplexityResult
from processor.operators.lshbloom import LSHBloomIndex
from processor.operators.minhash import MinHasher
from processor.operators.pii import PiiSanitization, PiiScanner
from processor.operators.quality import DevelopmentQualityScorer, QualityScore
from processor.operators.scientific_document_quality import is_publication_template
from processor.operators.source_classifiers import (
    SourcePosttrainClassifier,
    SourceQualityClassifier,
    bundle_revision,
)
from processor.operators.source_quality import (
    MetadataDiscoveryPolicy,
)
from processor.probes import start_probe_server
from processor.quality_cache import CachedQualityScorer
from processor.scientific import scientific_body_start
from processor.scientific_handoff import ScientificEvidenceUnavailableError, ScientificHandoff
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
from processor.work_cutoff import WorkCutoff
from schemas.bronze import BronzeRecord
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


class PiiSanitizer(Protocol):
    @property
    def revision(self) -> str: ...

    def sanitize(self, text: str) -> PiiSanitization: ...

    def flags(self, text: str) -> list[PiiFlag]: ...


@dataclass(slots=True)
class CurateState:
    """Per-worker state for the curation dataflow."""

    gopher: GopherFilter
    c4: C4Filter
    kenlm: PerplexityScorer
    minhasher: MinHasher
    lsh: LSHBloomIndex
    source_quality: QualityScorer
    metadata_discovery: MetadataDiscoveryPolicy
    pii: PiiSanitizer
    tokenizer: Tokenizer
    policy_revision: str
    scoring_version: str
    decision_cache: DecisionCache
    model_clients: tuple[CuratorModelClient, ...] = ()
    prefetched_quality_skips: frozenset[str] = frozenset()
    quality_cache: CachedQualityScorer | None = None
    scientific_handoff: ScientificHandoff | None = None
    posttrain_quality: QualityScorer | None = None
    posttrain_quality_cache: CachedQualityScorer | None = None

    def close(self) -> None:
        self.decision_cache.close()
        self.lsh.close()
        if self.quality_cache is not None:
            self.quality_cache.close()
        if self.posttrain_quality_cache is not None:
            self.posttrain_quality_cache.close()
        for model_client in self.model_clients:
            model_client.close()


@dataclass(frozen=True, slots=True)
class _SegmentModelSignals:
    quality: QualityScore | None
    perplexity: PerplexityResult | None


@dataclass(frozen=True, slots=True)
class _PayloadDecision:
    """One batch item result without conflating record-local and transient errors."""

    value: tuple[bytes, bool] | None = None
    error: ValueError | None = None
    expired: bool = False


class _MemoizedPiiSanitizer:
    """Reuse the exact body sanitization performed while preparing a micro-batch."""

    def __init__(self, scanner: PiiSanitizer) -> None:
        self._scanner = scanner
        self._sanitized: dict[str, PiiSanitization] = {}

    @property
    def revision(self) -> str:
        return self._scanner.revision

    def sanitize(self, text: str) -> PiiSanitization:
        cached = self._sanitized.get(text)
        if cached is None:
            cached = self._scanner.sanitize(text)
            self._sanitized[text] = cached
        return cached

    def flags(self, text: str) -> list[PiiFlag]:
        cached = self._sanitized.get(text)
        if cached is not None:
            return list(cached.flags)
        return self._scanner.flags(text)


class _PrefetchedQualityScorer:
    """Serve exact pinned-model results prepared across several documents."""

    def __init__(self, scorer: QualityScorer, scores: dict[str, QualityScore]) -> None:
        self._scorer = scorer
        self._scores = scores

    @property
    def revision(self) -> str:
        return self._scorer.revision

    @property
    def backend(self) -> str:
        return self._scorer.backend

    def score(self, text: str) -> QualityScore:
        cached = self._scores.get(text)
        return cached if cached is not None else self._scorer.score(text)

    def score_many(self, texts: Sequence[str]) -> list[QualityScore]:
        missing = [text for text in texts if text not in self._scores]
        if missing:
            missing_scores = _score_quality_batch(self._scorer, missing)
            self._scores.update(zip(missing, missing_scores, strict=True))
        return [self._scores[text] for text in texts]


class _PrefetchedPerplexityScorer:
    """Serve exact KenLM results prepared across several documents."""

    def __init__(
        self,
        scorer: PerplexityScorer,
        scores: dict[str, PerplexityResult],
    ) -> None:
        self._scorer = scorer
        self._scores = scores

    @property
    def scorer(self) -> str:
        return self._scorer.scorer

    def score(self, text: str) -> PerplexityResult:
        cached = self._scores.get(text)
        if cached is None:
            cached = self._scorer.score(text)
            self._scores[text] = cached
        return cached


def build_state(cfg: common.ProcessorConfig) -> CurateState:
    """Construct a :class:`CurateState` from the runtime config."""
    models = cfg.models_dir
    require_real_models = os.environ.get("S2P_REQUIRE_REAL_MODELS") == "1"
    model_service_url = os.environ.get("S2P_MODEL_SERVICE_URL", "").strip()
    quality_service_url = os.environ.get("S2P_QUALITY_MODEL_SERVICE_URL", "").strip()
    kenlm_service_url = os.environ.get("S2P_KENLM_MODEL_SERVICE_URL", "").strip()
    quality_discovery_host = os.environ.get("S2P_QUALITY_MODEL_SERVICE_DISCOVERY_HOST", "").strip()
    kenlm_discovery_host = os.environ.get("S2P_KENLM_MODEL_SERVICE_DISCOVERY_HOST", "").strip()
    kenlm_path = os.path.join(models, "kenlm", "en.arpa.bin")
    kenlm_sentencepiece_path = os.path.join(models, "kenlm", "en.sp.model")
    model_clients: tuple[CuratorModelClient, ...] = ()
    kenlm: PerplexityScorer
    source_quality: QualityScorer
    posttrain_quality: QualityScorer | None = None
    expected_quality_revision = (
        bundle_revision(json.loads(Path(__file__).with_name("source-classifiers.json").read_text()))
        if require_real_models
        else None
    )
    if model_service_url:
        model_client = CuratorModelClient(
            model_service_url,
            startup_wait_seconds=600 if require_real_models else 0,
            expected_quality_revision=expected_quality_revision,
            expected_classifier_protocol="quality-then-posttrain-v1"
            if require_real_models
            else None,
        )
        kenlm = RemoteKenLMScorer(model_client)
        source_quality = RemoteQualityClassifier(model_client, "source-pretrain-quality")
        posttrain_quality = RemoteQualityClassifier(model_client, "source-arxiv-posttrain")
        model_clients = (model_client,)
    elif any(
        (
            quality_service_url,
            kenlm_service_url,
        )
    ):
        if not all((quality_service_url, kenlm_service_url)):
            raise RuntimeError("all split curator model service URLs are required")

        def split_model_client(url: str, profile: str, discovery_host: str) -> CuratorModelClient:
            if not discovery_host:
                return CuratorModelClient(
                    url,
                    startup_wait_seconds=600 if require_real_models else 0,
                    expected_quality_revision=expected_quality_revision
                    if profile == "quality"
                    else None,
                    expected_classifier_protocol="quality-then-posttrain-v1"
                    if require_real_models and profile == "quality"
                    else None,
                )
            return CuratorModelClient(
                url,
                profile=profile,
                endpoint_resolver=headless_endpoint_resolver(url, discovery_host),
                startup_wait_seconds=600 if require_real_models else 0,
                expected_quality_revision=expected_quality_revision
                if profile == "quality"
                else None,
                expected_classifier_protocol="quality-then-posttrain-v1"
                if require_real_models and profile == "quality"
                else None,
            )

        quality_client = split_model_client(
            quality_service_url,
            "quality",
            quality_discovery_host,
        )
        kenlm_client = split_model_client(
            kenlm_service_url,
            "kenlm",
            kenlm_discovery_host,
        )
        kenlm = RemoteKenLMScorer(kenlm_client)
        source_quality = RemoteQualityClassifier(quality_client, "source-pretrain-quality")
        posttrain_quality = RemoteQualityClassifier(quality_client, "source-arxiv-posttrain")
        model_clients = tuple(dict.fromkeys((quality_client, kenlm_client)))
    else:
        kenlm = KenLMScorer(
            kenlm_path if os.path.isfile(kenlm_path) else None,
            kenlm_sentencepiece_path if os.path.isfile(kenlm_sentencepiece_path) else None,
            allow_fallback=not require_real_models,
        )
        source_quality = (
            SourceQualityClassifier(models)
            if require_real_models
            or os.path.isfile(os.path.join(models, "source-classifiers.json"))
            else DevelopmentQualityScorer()
        )
        if isinstance(source_quality, SourceQualityClassifier):
            posttrain_quality = SourcePosttrainClassifier(source_quality)
    pii = PiiScanner(allow_fallback=not require_real_models)
    scoring_version = os.environ.get(SCORING_VERSION_ENV, CONTENT_POLICY_GENERATION)
    minhasher = MinHasher()
    if require_real_models and minhasher.backend == "fallback-pyhash":
        raise RuntimeError("datasketch or rensa MinHash is required")
    # Each generation owns its dedup anchors so superseded projections cannot
    # reject the first clean record produced by a new policy.
    lsh = LSHBloomIndex(
        state_dir=os.path.join(cfg.state_dir, "lshbloom", scoring_version),
    )
    if require_real_models and lsh.backend == "memory":
        raise RuntimeError("a durable LSHBloom backend is required")
    quality_cache = CachedQualityScorer(
        source_quality, os.path.join(cfg.state_dir, "quality-scores.sqlite3")
    )
    posttrain_cache = (
        CachedQualityScorer(
            posttrain_quality, os.path.join(cfg.state_dir, "posttrain-scores.sqlite3")
        )
        if posttrain_quality is not None
        else None
    )
    return CurateState(
        gopher=GopherFilter(),
        c4=C4Filter(),
        kenlm=kenlm,
        minhasher=minhasher,
        lsh=lsh,
        source_quality=quality_cache,
        metadata_discovery=MetadataDiscoveryPolicy(),
        pii=pii,
        tokenizer=Tokenizer(allow_fallback=not require_real_models),
        policy_revision=os.environ.get(POLICY_REVISION_ENV, "git:dev") + ":source-gates-v1",
        scoring_version=scoring_version,
        decision_cache=DecisionCache(os.path.join(cfg.state_dir, "decision-cache.sqlite3")),
        model_clients=model_clients,
        quality_cache=quality_cache,
        posttrain_quality=posttrain_cache,
        posttrain_quality_cache=posttrain_cache,
        scientific_handoff=ScientificHandoff(
            boto3.client(
                "s3",
                endpoint_url=cfg.minio_endpoint,
                aws_access_key_id=cfg.minio_access_key,
                aws_secret_access_key=cfg.minio_secret_key,
                region_name="us-east-1",
            ),
            cfg.gold_bucket,
        )
        if require_real_models
        else None,
    )


def _decision_cache_key(state: CurateState, payload: bytes) -> str:
    """Fingerprint the exact Silver bytes and every material scoring revision."""
    recipe = "\n".join(
        (
            state.policy_revision,
            state.scoring_version,
            state.source_quality.revision,
            state.metadata_discovery.revision,
            state.kenlm.scorer,
            state.pii.revision,
        )
    ).encode("utf-8")
    return hashlib.sha256(recipe + b"\0" + payload).hexdigest()


def _score_segment_models(
    segments: Sequence[SilverSegment],
    *,
    quality: QualityScorer | None,
    kenlm: PerplexityScorer,
    use_kenlm: bool,
) -> dict[str, _SegmentModelSignals]:
    """Run source-quality and optional KenLM batches without changing segment order."""
    concurrency = max(1, int(os.environ.get("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "1")))
    quality_family_concurrency = concurrency
    batch_size = int(os.environ.get("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2"))
    if batch_size < 1:
        raise RuntimeError("S2P_CURATOR_CLASSIFIER_BATCH_SIZE must be positive")
    quality_results: dict[str, QualityScore] = {}
    perplexity_results: dict[str, PerplexityResult] = {}
    with (
        ThreadPoolExecutor(max_workers=quality_family_concurrency) as quality_executor,
        ThreadPoolExecutor(max_workers=1) as kenlm_executor,
    ):
        quality_batches: list[
            tuple[
                list[SilverSegment],
                Future[list[QualityScore]],
                dict[str, QualityScore],
            ]
        ] = []
        if quality is not None:
            for offset in range(0, len(segments), batch_size):
                batch = list(segments[offset : offset + batch_size])
                quality_batches.append(
                    (
                        batch,
                        quality_executor.submit(
                            _score_quality_batch,
                            quality,
                            [segment.text for segment in batch],
                        ),
                        quality_results,
                    )
                )
        perplexity_pending = {
            segment.segment_id: kenlm_executor.submit(kenlm.score, segment.text)
            for segment in segments
            if use_kenlm
        }
        for batch, future, destination in quality_batches:
            results = future.result()
            if len(results) != len(batch):
                raise RuntimeError(
                    "quality classifier returned a different number of batch results"
                )
            destination.update(
                (segment.segment_id, result) for segment, result in zip(batch, results, strict=True)
            )
        perplexity_results.update(
            (segment_id, future.result()) for segment_id, future in perplexity_pending.items()
        )
    return {
        segment.segment_id: _SegmentModelSignals(
            quality=quality_results.get(segment.segment_id),
            perplexity=perplexity_results.get(segment.segment_id),
        )
        for segment in segments
    }


def _score_quality_batch(
    scorer: QualityScorer,
    texts: Sequence[str],
) -> list[QualityScore]:
    """Use a remote batch facade when present and preserve local compatibility."""
    score_many = getattr(scorer, "score_many", None)
    if callable(score_many):
        values = list(score_many(texts))
    else:
        values = [scorer.score(text) for text in texts]
    return values


def _source_segments(silver: SilverRecord) -> list[SilverSegment]:
    """Return the exact segment projection consumed by classifier inference."""
    segments = list(silver.segments)
    if segments and _uses_scientific_quality_profile(silver):
        segments = segments[scientific_body_start([segment.role for segment in segments]) :]
    return segments or [
        SilverSegment(
            segment_id="document",
            title=silver.title or "Document",
            text=silver.model_text or silver.text,
            word_count=len((silver.model_text or silver.text).split()),
        )
    ]


def _quality_hard_rejected(
    state: CurateState,
    silver: SilverRecord,
    *,
    sanitized_segments: Sequence[tuple[SilverSegment, PiiSanitization]],
    safe_segments: Sequence[SilverSegment],
) -> tuple[bool, str]:
    """Return whether an existing deterministic gate makes inference unnecessary.

    This is deliberately only a preflight. The authoritative decision and its
    full audit fields are still built by :func:`curate_one`. Every predicate
    below is repeated there with the same sanitized projection, so skipping an
    expensive model call cannot create a new acceptance or rejection rule.
    """
    policy = resolve_source_policy(
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
    )
    is_scientific = policy.family == "scientific_paper"
    is_hf_card = policy.family in {"hf_model_card", "hf_dataset_card"}
    kept_segments = [
        safe_segment
        for safe_segment in safe_segments
        if not (is_hf_card and is_hf_placeholder_section(safe_segment.text))
        and not (
            policy.web_heuristic_gate
            and (
                not state.c4.stats(safe_segment.text).curly_brace_pass
                or not state.c4.stats(safe_segment.text).lorem_ipsum_pass
            )
        )
    ]
    model_text = "\n\n".join(segment.text.strip() for segment in kept_segments).strip()
    structured_text, _removed, structured_blocking, _exclusions = _filter_structured_projection(
        state.pii, silver.structured_text
    )
    text = _training_projection(silver, kept_segments, structured_text=structured_text)
    measured_body = text if is_hf_card else model_text
    hard_rejected = (
        not policy.training_text
        or len(measured_body.split()) < 50
        or any(sanitization.blocking_flags for _segment, sanitization in sanitized_segments)
        or bool(structured_blocking)
        or (
            policy.web_heuristic_gate
            and (
                not state.gopher.passes(model_text)
                or not (
                    state.c4.stats(model_text).nopunc_pass
                    and state.c4.stats(model_text).lorem_ipsum_pass
                    and state.c4.stats(model_text).curly_brace_pass
                )
            )
        )
        or (
            is_hf_card
            and not assess_hf_card(
                kind="model" if policy.family == "hf_model_card" else "dataset",
                title=silver.title,
                text=model_text,
                segments=list(kept_segments),
            ).accepted
        )
        or (policy.language_gate and (silver.lang != "en" or silver.lang_score < 0.5))
        or _license_reject_reason(silver) is not None
        or (
            is_scientific
            and any(
                warning.startswith("figure_enrichment_failed:")
                or warning in {"figure_limit_reached", "page_limit_reached"}
                for warning in silver.extraction_warnings
            )
        )
        or (
            is_scientific
            and is_publication_template(
                title=silver.title,
                text=text,
                segments=list(kept_segments),
            )
        )
    )
    return hard_rejected, text


def _unique_texts(texts: Sequence[str]) -> list[str]:
    """Deduplicate immutable model inputs while preserving first-seen order."""
    return list(dict.fromkeys(texts))


def _score_quality_texts(
    scorer: QualityScorer,
    texts: Sequence[str],
) -> dict[str, QualityScore]:
    """Score one model family's unique texts across all documents in a micro-batch."""
    unique = _unique_texts(texts)
    if not unique:
        return {}
    concurrency = max(1, int(os.environ.get("S2P_CURATOR_CLASSIFIER_CONCURRENCY", "1")))
    family_concurrency = concurrency
    batch_size = int(os.environ.get("S2P_CURATOR_CLASSIFIER_BATCH_SIZE", "2"))
    if batch_size < 1:
        raise RuntimeError("S2P_CURATOR_CLASSIFIER_BATCH_SIZE must be positive")
    pending: list[tuple[list[str], Future[list[QualityScore]]]] = []
    with ThreadPoolExecutor(max_workers=family_concurrency) as executor:
        for offset in range(0, len(unique), batch_size):
            batch = unique[offset : offset + batch_size]
            pending.append((batch, executor.submit(_score_quality_batch, scorer, batch)))
        scores: dict[str, QualityScore] = {}
        for batch, future in pending:
            results = future.result()
            if len(results) != len(batch):
                raise RuntimeError(
                    "quality classifier returned a different number of batch results"
                )
            scores.update(zip(batch, results, strict=True))
    return scores


def _score_perplexity_texts(
    scorer: PerplexityScorer,
    texts: Sequence[str],
) -> dict[str, PerplexityResult]:
    """Score unique KenLM inputs in their original deterministic order."""
    return {text: scorer.score(text) for text in _unique_texts(texts)}


def _quality_cutoff(source: str) -> float:
    return 3.5 if source in {"hf-models", "hf-datasets"} else 3.0


def _weighted_quality(results: Sequence[QualityScore]) -> float:
    total = sum(max(1, result.tokens) for result in results)
    return (
        sum(result.edu_score * max(1, result.tokens) for result in results) / total
        if total
        else 0.0
    )


def _source_quality_report(
    scorer: QualityScorer,
    silver: SilverRecord,
    text: str,
    posttrain_scorer: QualityScorer | None = None,
) -> dict[str, object]:
    """Preserve the training/evaluation aggregation, including overflow weighting."""
    _, sections = parse_sections(text, source=silver.source_feed)
    inputs = [model_input(section, source=silver.source_feed) for section in sections]
    results = _score_quality_texts(scorer, inputs)
    eligible = _weighted_quality([results[value] for value in inputs]) >= _quality_cutoff(
        silver.source_feed
    )
    if eligible and silver.source_feed == "arxiv-html-fetcher" and posttrain_scorer is not None:
        extra = _score_quality_texts(posttrain_scorer, inputs)
        results = {
            value: replace(results[value], diagnostic_scores=extra[value].diagnostic_scores)
            for value in inputs
        }
    rows: list[dict[str, Any]] = []
    for section, value in zip(sections, inputs, strict=True):
        result = results[value]
        rows.append(
            {
                "section_id": section.section_id,
                "title": section.title,
                "section_type": section.section_type,
                "text_sha256": hashlib.sha256(section.text.encode()).hexdigest(),
                "score": result.edu_score,
                "confidence": result.confidence,
                "class": result.score_class,
                "probabilities": list(result.probabilities),
                "tokens": result.tokens or max(1, len(section.text.split())),
                "chunks": result.chunks,
                "model_revision": result.model_revision or result.revision,
                "classifiers": (result.diagnostic_scores or {}) if eligible else {},
            }
        )
    total = sum(row["tokens"] for row in rows)
    score = sum(row["score"] * row["tokens"] for row in rows) / total if total else 0.0
    confidence = (
        sum(row["confidence"] * row["tokens"] for row in rows) / total
        if rows and all(row["confidence"] is not None for row in rows)
        else None
    )
    diagnostic_heads: dict[str, dict[str, object]] = {}
    tasks = {task for row in rows for task in row["classifiers"]}
    for task in sorted(tasks):
        # Do not silently aggregate a partially scored paper.
        if any(task not in row["classifiers"] for row in rows):
            raise RuntimeError(f"Incomplete diagnostic sections for {task}")
        head_rows = [(row, row["classifiers"][task]) for row in rows]
        best_row, best = max(head_rows, key=lambda pair: pair[1]["edu_score"])
        head_tokens = sum(value["tokens"] for _, value in head_rows)
        diagnostic_heads[task] = {
            "mode": "active",
            "score": best["edu_score"],
            "class": best["score_class"],
            "confidence": best["confidence"],
            "aggregation": "maximum",
            "weighted_mean": sum(value["edu_score"] * value["tokens"] for _, value in head_rows)
            / head_tokens,
            "mean": sum(value["edu_score"] for _, value in head_rows) / len(head_rows),
            "best_section_id": best_row["section_id"],
            "model_revision": best["model_revision"],
            "sections": len(head_rows),
            "class_5_sections": sum(value["score_class"] == 5 for _, value in head_rows),
        }
    return {
        "mode": "active",
        "cutoff": _quality_cutoff(silver.source_feed),
        "passed": score >= _quality_cutoff(silver.source_feed),
        "score": score,
        "confidence": confidence,
        "class": max(0, min(5, round(score))),
        "aggregation": "token_weighted_mean",
        "input_contract": "stream2pretrain-section-labels-v1",
        "bundle_revision": scorer.revision,
        "model_revision": rows[0]["model_revision"] if rows else scorer.revision,
        "sections": rows,
        "classifiers": diagnostic_heads,
    }


def _prefetched_curate_state(
    state: CurateState,
    silvers: Sequence[SilverRecord],
) -> CurateState:
    """Prepare exact model outputs across documents without touching ordered state.

    Existing deterministic rejection predicates run before model inference.
    Rejected documents still receive a complete durable decision, but their
    SegmentScore model fields stay null. Eligible documents fill all ready
    stateless quality Pods with bounded requests and replay the exact results
    as :func:`curate_one` finalizes ordered LSH mutations.
    """
    pii = _MemoizedPiiSanitizer(state.pii)
    quality_texts: list[str] = []
    kenlm_texts: list[str] = []
    quality_skips: set[str] = set()
    paper_inputs: list[tuple[SilverRecord, list[str]]] = []
    for silver in silvers:
        policy = resolve_source_policy(
            source_feed=silver.source_feed,
            source_format=silver.source_format,
            extraction_pipeline=silver.extraction_pipeline,
        )
        source_segments = _source_segments(silver)
        sanitized_segments = [(segment, pii.sanitize(segment.text)) for segment in source_segments]
        safe_segments = [
            segment.model_copy(
                update={
                    "text": sanitization.text,
                    "word_count": len(sanitization.text.split()),
                }
            )
            for segment, sanitization in sanitized_segments
        ]
        hard_rejected, training_text = _quality_hard_rejected(
            replace(state, pii=pii),
            silver,
            sanitized_segments=sanitized_segments,
            safe_segments=safe_segments,
        )
        signature = state.minhasher.signature(training_text)
        near_duplicate = state.lsh.probe(silver.doc_id, signature).is_near_duplicate
        if hard_rejected or near_duplicate:
            quality_skips.add(silver.doc_id)
        else:
            _, sections = parse_sections(training_text, source=silver.source_feed)
            inputs = [model_input(section, source=silver.source_feed) for section in sections]
            quality_texts.extend(inputs)
            paper_inputs.append((silver, inputs))
        for safe_segment in safe_segments:
            if policy.kenlm_mode != "off":
                kenlm_texts.append(safe_segment.text)

    with ThreadPoolExecutor(max_workers=2) as executor:
        quality_future = executor.submit(
            _score_quality_texts,
            state.source_quality,
            quality_texts,
        )
        kenlm_future = executor.submit(
            _score_perplexity_texts,
            state.kenlm,
            kenlm_texts,
        )
        quality_scores = quality_future.result()
        kenlm_scores = kenlm_future.result()

    # The paper-level quality gate runs BEFORE either independent reasoning
    # encoder. Passing papers still score every section with both heads.
    if state.posttrain_quality is not None:
        posttrain_inputs = _unique_texts(
            [
                text
                for silver, inputs in paper_inputs
                if silver.source_feed == "arxiv-html-fetcher"
                and _weighted_quality([quality_scores[value] for value in inputs]) >= 3.0
                for text in inputs
                if not quality_scores[text].diagnostic_scores
            ]
        )
        for text, result in _score_quality_texts(state.posttrain_quality, posttrain_inputs).items():
            quality_scores[text] = replace(
                quality_scores[text], diagnostic_scores=result.diagnostic_scores
            )

    return replace(
        state,
        source_quality=_PrefetchedQualityScorer(state.source_quality, quality_scores),
        kenlm=_PrefetchedPerplexityScorer(state.kenlm, kenlm_scores),
        pii=pii,
        prefetched_quality_skips=frozenset(quality_skips),
        posttrain_quality=None,
    )


def curate_one(state: CurateState, silver: SilverRecord) -> GoldRecord:
    """Run the full curation pipeline on one silver record.

    Always returns a scored GoldRecord. Callers must use
    :func:`is_trainable_gold` before publishing the record to ``docs.curated``.
    """
    source_segments = _source_segments(silver)
    source_policy = resolve_source_policy(
        source_feed=silver.source_feed,
        source_format=silver.source_format,
        extraction_pipeline=silver.extraction_pipeline,
    )
    is_scientific = source_policy.family == "scientific_paper"
    is_hf_card = source_policy.family in {"hf_model_card", "hf_dataset_card"}
    is_metadata = not source_policy.training_text
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
    hard_rejected_before_quality, _training_text = _quality_hard_rejected(
        state,
        silver,
        sanitized_segments=sanitized_segments,
        safe_segments=safe_segments,
    )
    quality_scorer = (
        state.source_quality
        if source_policy.training_text
        and not hard_rejected_before_quality
        and silver.doc_id not in state.prefetched_quality_skips
        else None
    )
    model_signals = _score_segment_models(
        safe_segments,
        quality=None,
        kenlm=state.kenlm,
        use_kenlm=source_policy.kenlm_mode != "off",
    )
    segment_scores: list[SegmentScore] = []
    kept_segments: list[SilverSegment] = []
    retained_ids = {segment.segment_id for segment in source_segments}
    runtime_excluded: list[str] = [
        f"{segment.title}: front matter and author metadata retained for provenance"
        for segment in silver.segments
        if segment.segment_id not in retained_ids
    ]
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
        if is_hf_card and is_hf_placeholder_section(safe_segment.text):
            exclusion_reasons.append("unfilled Hugging Face template section isolated from corpus")
        if source_policy.web_heuristic_gate and not segment_c4.curly_brace_pass:
            exclusion_reasons.append("C4 curly-brace signal isolated to this section")
            removed_for_c4 = True
        if source_policy.web_heuristic_gate and not segment_c4.lorem_ipsum_pass:
            exclusion_reasons.append("placeholder boilerplate isolated to this section")
            removed_for_c4 = True
        edu_score: float | None = None
        quality_classifier_revision: str | None = None
        segment_perplexity: float | None = None
        segment_bucket: str | None = None
        signals = model_signals[segment.segment_id]
        quality_result = signals.quality
        perplexity_result = signals.perplexity
        if quality_result is not None:
            edu_score = quality_result.edu_score
            quality_classifier_revision = quality_result.revision
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
                quality_classifier_revision=quality_classifier_revision,
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
    # Score exactly the retained pretraining projection, including its
    # structured surrogates, with the same section parser as the label set.
    quality_diagnostics = (
        _source_quality_report(quality_scorer, silver, text, state.posttrain_quality)
        if quality_scorer is not None
        else None
    )
    reject: list[RejectReason] = []
    if quality_diagnostics is not None and not quality_diagnostics["passed"]:
        reject.append("low_quality_score")
    minimum_words = 50
    if is_metadata:
        reject.append("metadata_only")
    measured_body = text if is_hf_card else model_text
    if len(measured_body.split()) < minimum_words:
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
    edu_score = float(quality_diagnostics["score"]) if quality_diagnostics else 0.0
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
    structure = source_scores(retained_view, quality_score=edu_score, quality_applicable=False)
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
        quality_applicable=False,
    )
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
    if is_scientific and is_publication_template(
        title=silver.title,
        text=text,
        segments=kept_segments,
    ):
        reject.append("document_template")

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
        quality_diagnostics=quality_diagnostics,
        structural_quality_score=structure.structural_quality_score,
        extraction_completeness=structure.extraction_completeness,
        reasoning_score=structure.reasoning_score,
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
        valid_from=silver.valid_from,
        valid_to=silver.valid_to,
        reject_reasons=reject,
        scoring_version=state.scoring_version,
        classifier_revision=(
            str(quality_diagnostics["model_revision"])
            if quality_diagnostics is not None
            else "not-run:deterministic-reject"
        ),
        classifier_backend=(
            state.source_quality.backend
            if quality_scorer is not None
            else "not-run:deterministic-reject"
        ),
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
    return pre_record


def _training_projection(
    silver: SilverRecord,
    segments: list[SilverSegment],
    *,
    structured_text: str | None = None,
) -> str:
    blocks: list[str] = []
    if silver.title:
        blocks.append(f"# {silver.title}")
    for index, segment in enumerate(segments):
        duplicate_document_title = (
            index == 0
            and silver.title is not None
            and segment.title.strip().casefold() == silver.title.strip().casefold()
        )
        if duplicate_document_title:
            blocks.append(segment.text.strip())
        else:
            blocks.append(f"## {segment.title}\n{segment.text.strip()}")
    structured = silver.structured_text if structured_text is None else structured_text
    if structured.strip():
        blocks.append(structured.strip())
    return "\n\n".join(block for block in blocks if block.strip()).strip()


_STRUCTURED_BLOCK_START = re.compile(r"(?=\[(?:TABLE|EQUATION|FIGURE)\])")


def _filter_structured_projection(
    scanner: PiiSanitizer, structured_text: str
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
    if "pii_detected" in reject or pii_flags:
        return 3
    if reject:
        return 2
    return 1


def _license_reject_reason(silver: SilverRecord) -> RejectReason | None:
    """Apply the purpose-aware licence policy at the curation boundary."""
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
    outcome = process_silver_decision_payloads(state, [payload], metrics=metrics)[0]
    if outcome.error is not None:
        raise outcome.error
    assert outcome.value is not None
    return outcome.value


def _record_decision_metrics(metrics: ProcessorMetrics | None, gold: GoldRecord) -> None:
    if metrics is None:
        return
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


def _materialize_uncached_decision(
    state: CurateState,
    scoring_state: CurateState,
    *,
    silver: SilverRecord,
    cache_key: str,
    metrics: ProcessorMetrics | None,
) -> tuple[bytes, bool]:
    """Finalize one decision and mutate cache/dedup state in input order."""
    gold = curate_one(scoring_state, silver)
    if is_trainable_gold(gold) and state.scientific_handoff is not None:
        try:
            uri = state.scientific_handoff.preserve(
                silver.doc_id,
                silver.scientific_evidence_gzip,
                silver.scientific_artifact_s3_uri,
            )
            if silver.source_feed == "arxiv-html-fetcher" and not uri:
                raise ScientificEvidenceUnavailableError("structured evidence URI is absent")
            if uri != gold.scientific_artifact_s3_uri:
                gold = gold.model_copy(update={"scientific_artifact_s3_uri": uri})
        except ScientificEvidenceUnavailableError as exc:
            if silver.scientific_evidence_gzip is None and "pretrain" in gold.eligible_routes:
                # Legacy normalized records already contain the complete clean
                # pretraining projection. Expired source evidence disqualifies
                # Foundry, not that independently usable, licensed text.
                gold = gold.model_copy(
                    update={
                        "route": "pretrain",
                        "eligible_routes": ["pretrain"],
                        "scientific_artifact_s3_uri": None,
                        "route_reasons": [
                            *gold.route_reasons,
                            "post-training requires retained structured evidence",
                        ],
                    }
                )
            else:
                gold = gold.model_copy(
                    update={
                        "route": "quarantine",
                        "eligible_routes": ["quarantine"],
                        "risk_tier": 3,
                        "reject_reasons": [
                            *gold.reject_reasons,
                            "incomplete_scientific_extraction",
                        ],
                        "route_reasons": [str(exc)],
                    }
                )
    _record_decision_metrics(metrics, gold)
    decision = common.gold_dumps(gold)
    trainable = is_trainable_gold(gold)
    state.decision_cache.put(cache_key, decision, trainable=trainable)
    return decision, trainable


def process_silver_decision_payloads(
    state: CurateState,
    payloads: Sequence[bytes],
    *,
    metrics: ProcessorMetrics | None = None,
    work_cutoff: WorkCutoff | None = None,
) -> list[_PayloadDecision]:
    """Score a bounded document batch while preserving serial state semantics.

    Cache lookup, finalization, near-duplicate observation, metric emission,
    and cache writes retain input order. Only immutable PII projections and
    stateless pinned-model calls are prepared across documents. A record-local
    ``ValueError`` remains attached to that record so the Bytewax step can
    preserve the previous drop-and-continue behavior.
    """
    results: list[_PayloadDecision | None] = [None] * len(payloads)
    pending: list[tuple[int, str, SilverRecord]] = []

    def expired(silver: SilverRecord) -> bool:
        return work_cutoff is not None and work_cutoff.expired(
            silver.source_fetched_at,
            stage="curate",
            source_feed=silver.source_feed,
            metrics=metrics,
        )

    for index, payload in enumerate(payloads):
        try:
            silver = common.silver_loads(payload)
        except ValueError as exc:
            results[index] = _PayloadDecision(error=exc)
            continue
        if expired(silver):
            results[index] = _PayloadDecision(expired=True)
            continue
        cache_key = _decision_cache_key(state, payload)
        cached = state.decision_cache.get(cache_key)
        if cached is not None:
            results[index] = _PayloadDecision(value=cached)
            continue
        pending.append((index, cache_key, silver))

    if pending:
        try:
            retry = 0
            scoring_state = state
            while True:
                eligible: list[tuple[int, str, SilverRecord]] = []
                for index, cache_key, silver in pending:
                    if expired(silver):
                        results[index] = _PayloadDecision(expired=True)
                    else:
                        eligible.append((index, cache_key, silver))
                pending = eligible
                if not pending:
                    break
                try:
                    scoring_state = _prefetched_curate_state(
                        state,
                        [silver for _, _, silver in pending],
                    )
                    break
                except ModelServiceError as exc:
                    retry += 1
                    common.get_logger("s2p.curate").warning(
                        "model temporarily unavailable; retaining input and completed scores",
                        attempt=retry,
                        error=str(exc),
                    )
                    if metrics is not None:
                        metrics.record_failure(stage="curate", reason="model_service_retry")
                    time.sleep(min(30, 2 * retry))
        except ValueError:
            # A record-local rejection from a shared batch request cannot be
            # attributed safely. Re-run each item through the unchanged
            # singleton path, which restores the former error boundary.
            scoring_state = state

        for index, cache_key, silver in pending:
            if expired(silver):
                results[index] = _PayloadDecision(expired=True)
                continue
            # Another item earlier in this same runtime batch may carry the
            # identical payload. Match one-by-one behavior by observing the
            # decision cache again after each preceding ordered write.
            cached = state.decision_cache.get(cache_key)
            if cached is not None:
                results[index] = _PayloadDecision(value=cached)
                continue
            try:
                item_scoring_state = (
                    _prefetched_curate_state(state, [silver])
                    if scoring_state is state
                    else scoring_state
                )
                value = _materialize_uncached_decision(
                    state,
                    item_scoring_state,
                    silver=silver,
                    cache_key=cache_key,
                    metrics=metrics,
                )
            except ValueError as exc:
                results[index] = _PayloadDecision(error=exc)
            else:
                results[index] = _PayloadDecision(value=value)

    assert all(result is not None for result in results)
    return [result for result in results if result is not None]


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


def _curator_document_batch_size() -> int:
    """Return the bounded stateless inference micro-batch size."""
    value = int(os.environ.get("S2P_CURATOR_DOCUMENT_BATCH_SIZE", "12"))
    if value < 1:
        raise RuntimeError("S2P_CURATOR_DOCUMENT_BATCH_SIZE must be positive")
    return value


def build_dataflow(
    cfg: common.ProcessorConfig,
    *,
    runtime_status: common.BytewaxRuntimeStatus | None = None,
) -> object:
    """Build the Bytewax dataflow object."""
    from bytewax import operators as op
    from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage
    from bytewax.dataflow import Dataflow, operator

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
    work_cutoff = WorkCutoff.from_env()
    failure_writer = common.DurableProcessingFailureWriter.from_config(cfg)
    flow = Dataflow(flow_name)
    payload_max_bytes = common.kafka_payload_max_bytes()
    # Default to ``beginning`` so a restart with no committed group offset
    # replays the topic instead of dropping in-flight bytes (at-least-once
    # semantics; matches the Kappa/streaming-first contract). Operators can
    # override via ``S2P_KAFKA_START_OFFSET=end`` for short-lived debug runs.
    start_offset = common.kafka_starting_offset()
    source: Any = common.tracked_kafka_source(
        runtime_status=runtime_status,
        source_name="docs_normalized",
        brokers=cfg.redpanda_brokers.split(","),
        topics=[input_topic],
        starting_offset=start_offset,
        add_config=common.kafka_consumer_config(cfg.consumer_group),
        batch_size=common.kafka_source_batch_size(),
    )
    inp: Any = op.input("docs_normalized", flow, source)

    document_batch_size = _curator_document_batch_size()

    def _batch_step(
        messages: list[object],
    ) -> list[
        tuple[
            KafkaSinkMessage[bytes, bytes],
            KafkaSinkMessage[bytes, bytes] | None,
            KafkaSinkMessage[bytes, bytes] | None,
        ]
    ]:
        emitted: list[
            tuple[
                KafkaSinkMessage[bytes, bytes],
                KafkaSinkMessage[bytes, bytes] | None,
                KafkaSinkMessage[bytes, bytes] | None,
            ]
        ] = []
        # ``flat_map_batch`` receives runtime batches from the Kafka source.
        # Chunk again here so a future partition-count change cannot inflate
        # the model/input memory bound beyond twelve one-segment documents:
        # independent RPCs across the available quality replicas.
        for offset in range(0, len(messages), document_batch_size):
            batch = messages[offset : offset + document_batch_size]
            valid: list[tuple[object, bytes, SilverRecord]] = []
            for msg in batch:
                payload = getattr(msg, "value", None)
                if payload is None:
                    failure_writer.record(stage="curate", message=msg, reason="kafka_tombstone")
                    PROCESSOR_METRICS.record_failure(stage="curate", reason="kafka_tombstone")
                    continue
                try:
                    silver = common.silver_loads(payload)
                except ValueError as exc:
                    with tracer.start_as_current_span("curate.process") as span:
                        span.record_exception(exc)
                    reason = type(exc).__name__
                    failure_writer.record(stage="curate", message=msg, reason=reason)
                    PROCESSOR_METRICS.record_failure(stage="curate", reason=reason)
                    continue
                valid.append((msg, payload, silver))

            if not valid:
                continue
            try:
                outcomes = process_silver_decision_payloads(
                    state,
                    [payload for _, payload, _ in valid],
                    metrics=PROCESSOR_METRICS,
                    work_cutoff=work_cutoff,
                )
            except Exception as exc:
                PROCESSOR_METRICS.record_failure(stage="curate", reason=type(exc).__name__)
                # Stateless calls may finish out of order, but no Bytewax
                # output or source frontier advances after a transient model,
                # state, or unexpected failure.
                raise

            for (msg, _payload, silver), outcome in zip(valid, outcomes, strict=True):
                if outcome.expired:
                    continue
                with tracer.start_as_current_span("curate.process") as span:
                    if outcome.error is not None:
                        span.record_exception(outcome.error)
                        reason = type(outcome.error).__name__
                        failure_writer.record(stage="curate", message=msg, reason=reason)
                        PROCESSOR_METRICS.record_failure(stage="curate", reason=reason)
                        continue
                    assert outcome.value is not None
                    decision, trainable = outcome.value
                    if len(decision) > payload_max_bytes:
                        deterministic_error = common.DeterministicProcessingError(
                            f"curation decision is {len(decision)} bytes; "
                            f"limit is {payload_max_bytes}"
                        )
                        span.record_exception(deterministic_error)
                        reason = type(deterministic_error).__name__
                        PROCESSOR_METRICS.record_failure(stage="curate", reason=reason)
                        # Preserve the former one-by-one failure contract: an
                        # oversized durable decision must stop the frontier,
                        # not be converted into a record-local drop.
                        raise deterministic_error
                    key = getattr(msg, "key", None) or b""
                    decision_message = KafkaSinkMessage(key=key, value=decision)
                    curated_message = (
                        KafkaSinkMessage(key=key, value=decision) if trainable else None
                    )
                    retry_payload = extraction_retry_payload(
                        silver,
                        common.gold_loads(decision),
                    )
                    retry_message = (
                        KafkaSinkMessage(key=key, value=retry_payload)
                        if retry_payload is not None
                        else None
                    )
                    emitted.append((decision_message, curated_message, retry_message))
        return emitted

    @operator  # type: ignore[untyped-decorator]
    def _curate_run(step_id: str, up: Any) -> Any:
        # Recovery snapshots bind this exact operator hierarchy and step ID.
        return op.flat_map_batch("flat_map_batch", up, _batch_step)

    mapped: Any = _curate_run("curate_run", inp)
    # This identity boundary is part of the durable recovery topology.
    filtered: Any = op.filter("curate_drop_none", mapped, lambda _pair: True)
    decisions: Any = op.map("curate_decision_message", filtered, lambda pair: pair[0])
    decision_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=decision_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_decision_sink", decisions, decision_sink)
    accepted_pairs: Any = op.filter(
        "curate_trainable_only", filtered, lambda pair: pair[1] is not None
    )
    accepted: Any = op.map("curate_accepted_message", accepted_pairs, lambda pair: pair[1])
    curated_sink = KafkaSink(
        brokers=cfg.redpanda_brokers.split(","),
        topic=curated_topic,
        add_config=common.kafka_producer_config(),
    )
    op.output("curate_sink", accepted, curated_sink)
    retry_pairs: Any = op.filter("curate_retry_only", filtered, lambda pair: pair[2] is not None)
    retries: Any = op.map("curate_retry_message", retry_pairs, lambda pair: pair[2])
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
        # Model-client demand uses the default registry. Expose it alongside
        # stage counters so KEDA actually sees requests waiting for a Pod.
        metrics_provider=lambda: PROCESSOR_METRICS.render_prometheus() + generate_latest(),
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
