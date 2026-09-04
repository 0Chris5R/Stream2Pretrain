"""Adapt existing Gold plus ScientificDocument artifacts into PaperBundle."""

from __future__ import annotations

import re
from collections.abc import Iterable
from urllib.parse import urlparse

from processor.foundry.util import canonical_json, sha256
from schemas.foundry import (
    BundleEquation,
    BundleFigure,
    BundleTable,
    BundleTableCell,
    OfficialArtifact,
    PaperBundle,
    StableSpan,
)
from schemas.gold import GoldRecord
from schemas.scientific import ScientificDocument, ScientificSection


class ScientificArtifactUnavailableError(ValueError):
    """A referenced scientific artifact cannot ever satisfy foundry preflight."""

    def __init__(
        self,
        *,
        uri: str,
        bucket: str,
        key: str,
        reason: str,
    ) -> None:
        self.uri = uri
        self.bucket = bucket
        self.key = key
        self.reason = reason
        super().__init__(f"scientific artifact {reason}: s3://{bucket}/{key}")


def paper_bundle_from_gold(
    gold: GoldRecord,
    scientific: ScientificDocument,
    *,
    official_artifacts: list[OfficialArtifact] | None = None,
) -> PaperBundle:
    """Create an immutable adapter without duplicating upstream extraction."""
    if not {
        "posttrain_candidate",
        "reasoning_candidate",
    }.intersection({gold.route, *gold.eligible_routes}):
        raise ValueError(f"document route {gold.route!r} is not post-training eligible")
    if gold.doc_id != scientific.doc_id:
        raise ValueError("GoldRecord and ScientificDocument doc_id differ")
    spans = list(_stable_spans(scientific.sections))
    if not spans:
        raise ValueError("scientific artifact has no stable training spans")
    paper_id = scientific.source_identifier or gold.doc_id
    scientific_hash = sha256(scientific)
    gold_hash = sha256(gold)
    paper_hash = sha256(
        {
            "paper_id": paper_id,
            "scientific_hash": scientific_hash,
            "gold_hash": gold_hash,
        }
    )
    return PaperBundle(
        paper_id=paper_id,
        paper_family_id=_family_id(paper_id),
        paper_hash=paper_hash,
        source_uri=scientific.source_url,
        metadata={
            "title": scientific.title,
            "authors": scientific.authors,
            "abstract": scientific.abstract,
            "publication_date": scientific.publication_date,
            "source_feed": gold.source_feed,
            "source_format": gold.source_format,
            "quality_score": gold.quality_score,
            "edu_score": gold.edu_score,
            "reasoning_score": gold.reasoning_score,
            "content_tags": gold.content_tags,
            "extraction_pipeline": scientific.extraction_pipeline,
            "projection_version": scientific.projection_version,
            "valid_from": gold.valid_from.isoformat(),
            "classifier_section_hints": classifier_section_hints(gold),
        },
        sections=[
            {
                "section_id": section.section_id,
                "title": section.title,
                "role": section.role,
                "level": section.level,
                "include_in_training": section.include_in_training,
            }
            for section in scientific.sections
            if section.include_in_training
        ],
        stable_spans=spans,
        equations=[
            BundleEquation(
                equation_id=equation.equation_id,
                latex=equation.latex,
                mathml=equation.mathml,
                source_span_ids=_nearest_span_ids(
                    equation.equation_id,
                    spans,
                    hint=equation.latex or equation.mathml,
                ),
            )
            for equation in scientific.equations
        ],
        tables=[
            BundleTable(
                table_id=table.table_id,
                caption=table.caption,
                rows=table.rows,
                cells=[
                    BundleTableCell(
                        cell_id=f"{table.table_id}.cell:r{row_index + 1}c{column_index + 1}",
                        row=row_index,
                        column=column_index,
                        value=value,
                    )
                    for row_index, row in enumerate(table.rows)
                    for column_index, value in enumerate(row)
                ],
                source_span_ids=_nearest_span_ids(
                    table.table_id,
                    spans,
                    hint=table.caption,
                ),
            )
            for table in scientific.tables
        ],
        figures=[
            BundleFigure(
                figure_id=figure.figure_id,
                caption=figure.caption,
                alt_text=figure.alt_text,
                ocr_text=figure.ocr_text if figure.ocr_training_eligible else None,
                asset_uri=figure.asset_s3_uri,
                image_hash=(f"sha256:{figure.image_sha256}" if figure.image_sha256 else None),
                source_span_ids=_nearest_span_ids(
                    figure.source_element_id or figure.figure_id,
                    spans,
                    hint=" ".join(
                        value
                        for value in (
                            figure.caption,
                            figure.alt_text,
                            figure.nearby_text,
                        )
                        if value
                    ),
                ),
            )
            for figure in scientific.figures
        ],
        captions=[
            {"object_id": figure.figure_id, "text": figure.caption}
            for figure in scientific.figures
            if figure.caption
        ]
        + [
            {"object_id": table.table_id, "text": table.caption}
            for table in scientific.tables
            if table.caption
        ],
        quality_labels={
            "route": "posttrain_candidate",
            "quality_score": gold.quality_score,
            "structural_quality_score": gold.structural_quality_score,
            "extraction_completeness": gold.extraction_completeness,
            "risk_tier": gold.risk_tier,
            "pii_action": gold.pii_action,
        },
        official_artifacts=official_artifacts or [],
        source_gold_hash=gold_hash,
        scientific_artifact_hash=scientific_hash,
    )


def classifier_section_hints(gold: GoldRecord) -> str:
    """Optional pointers only: never select, remove or rewrite paper content."""
    report = gold.quality_diagnostics or {}
    if report.get("mode") != "active":
        return ""
    sentences = []
    for task, template in (
        ("arxiv-posttrain-suitability", "Sections {titles} seem especially relevant."),
        (
            "arxiv-math-reasoning",
            "Sections {titles} seem mathematically suited to potentially creating a derivation or reasoning task.",
        ),
    ):
        candidates = [
            section
            for section in report.get("sections", [])
            if float(section.get("classifiers", {}).get(task, {}).get("edu_score", 0)) >= 4.0
            and section.get("title")
        ]
        candidates.sort(key=lambda section: -float(section["classifiers"][task]["edu_score"]))
        titles = list(dict.fromkeys(str(section["title"]) for section in candidates))[:3]
        if titles:
            # Quoted document-derived titles are data, not new instructions.
            sentences.append(template.format(titles=canonical_json(titles).decode()))
    return " ".join(sentences)


def load_scientific_artifact(gold: GoldRecord, *, s3_client: object) -> ScientificDocument:
    document, _ = load_scientific_artifact_payload(gold, s3_client=s3_client)
    return document


def load_scientific_artifact_payload(
    gold: GoldRecord,
    *,
    s3_client: object,
) -> tuple[ScientificDocument, bytes]:
    """Load and validate the exact structured artifact referenced by Gold.

    Missing objects and malformed immutable artifacts are permanent candidate
    failures. Other storage exceptions remain transient so the stream runtime
    can retry them without silently discarding a valid paper.
    """
    uri = gold.scientific_artifact_s3_uri
    if not uri:
        raise ScientificArtifactUnavailableError(
            uri="",
            bucket="unknown",
            key="unknown",
            reason="URI is absent",
        )
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ScientificArtifactUnavailableError(
            uri=uri,
            bucket=parsed.netloc or "unknown",
            key=parsed.path.lstrip("/") or "unknown",
            reason="URI is invalid",
        )
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)  # type: ignore[attr-defined]
    except Exception as exc:
        response_data = getattr(exc, "response", None)
        error = response_data.get("Error", {}) if isinstance(response_data, dict) else {}
        code = str(error.get("Code", ""))
        if code in {"NoSuchKey", "404", "NotFound"} or exc.__class__.__name__ == "NoSuchKey":
            raise ScientificArtifactUnavailableError(
                uri=uri,
                bucket=bucket,
                key=key,
                reason="object is missing",
            ) from exc
        raise
    payload = bytes(response["Body"].read())
    return validate_scientific_artifact_payload(gold, payload)


def validate_scientific_artifact_payload(
    gold: GoldRecord,
    payload: bytes,
) -> tuple[ScientificDocument, bytes]:
    """Validate a queue-cached artifact under the same immutable URI contract."""
    uri = gold.scientific_artifact_s3_uri or ""
    parsed = urlparse(uri)
    bucket = parsed.netloc or "unknown"
    key = parsed.path.lstrip("/") or "unknown"
    try:
        document = ScientificDocument.model_validate_json(payload)
    except Exception as exc:
        raise ScientificArtifactUnavailableError(
            uri=uri,
            bucket=bucket,
            key=key,
            reason="payload is invalid",
        ) from exc
    if document.doc_id != gold.doc_id:
        raise ScientificArtifactUnavailableError(
            uri=uri,
            bucket=bucket,
            key=key,
            reason="document identity does not match Gold",
        )
    has_training_body = any(
        section.include_in_training
        and (
            any(
                paragraph.include_in_training and paragraph.text.strip()
                for paragraph in section.paragraphs
            )
            or section.text.strip()
        )
        for section in document.sections
    )
    if not has_training_body:
        raise ScientificArtifactUnavailableError(
            uri=uri,
            bucket=bucket,
            key=key,
            reason="payload has no retained scientific body",
        )
    return document, payload


def _stable_spans(sections: Iterable[ScientificSection]) -> Iterable[StableSpan]:
    for section in sections:
        if not section.include_in_training:
            continue
        paragraphs = [
            paragraph.text for paragraph in section.paragraphs if paragraph.include_in_training
        ]
        if not paragraphs:
            paragraphs = _paragraphs(section.text)
        for ordinal, text in enumerate(paragraphs):
            cleaned = re.sub(r"\s+", " ", text).strip()
            if not cleaned:
                continue
            span_id = f"{section.section_id}.span{ordinal + 1}"
            yield StableSpan(
                span_id=span_id,
                section_id=section.section_id,
                section_role=section.role,
                ordinal=ordinal,
                text=cleaned,
                text_hash=sha256(cleaned),
            )


def _paragraphs(text: str) -> list[str]:
    blocks = [item.strip() for item in re.split(r"\n\s*\n", text) if item.strip()]
    if len(blocks) > 1:
        return blocks
    return [
        sentence.strip()
        for sentence in re.split(r"(?<=[.!?])\s+(?=[A-Z])", text)
        if sentence.strip()
    ]


def _nearest_span_ids(
    object_id: str,
    spans: list[StableSpan],
    *,
    hint: str | None = None,
) -> list[str]:
    hint_terms = set(re.findall(r"[a-z0-9]+", (hint or "").casefold()))
    if hint_terms:
        ranked = sorted(
            (
                (
                    len(hint_terms & set(re.findall(r"[a-z0-9]+", span.text.casefold())))
                    / len(hint_terms),
                    span.span_id,
                )
                for span in spans
            ),
            key=lambda value: (-value[0], value[1]),
        )
        matched = [span_id for score, span_id in ranked[:3] if score > 0]
        if matched:
            return matched
    marker = re.search(r"(\d+)", object_id)
    if marker:
        section_marker = marker.group(1)
        matches = [span.span_id for span in spans if section_marker in span.section_id]
        if matches:
            return matches[:3]
    return [span.span_id for span in spans[:1]]


def _family_id(paper_id: str) -> str:
    return re.sub(r"v\d+$", "", paper_id)


def bundle_json(bundle: PaperBundle) -> bytes:
    return canonical_json(bundle)


def bundle_prompt_json(
    bundle: PaperBundle,
    *,
    span_ids: set[str] | None = None,
    section_roles: set[str] | None = None,
    max_bytes: int = 450_000,
) -> bytes:
    """Build a bounded, non-duplicated scientific prompt projection.

    The durable PaperBundle remains unchanged. The prompt projection keeps every
    selected stable span and its scientific objects while removing the duplicate
    full section bodies. Oversized papers are covered by role-sharded graph
    passes and task-specific projections rather than rejected or ranked lower.
    """
    payload = bundle.model_dump(mode="json")
    # Only task_designer gets the optional sentence; other role inputs are
    # unchanged and no classifier signal selects their source spans.
    payload.get("metadata", {}).pop("classifier_section_hints", None)
    payload["prompt_projection"] = "paper-bundle-model-view-v2"
    payload.pop("captions", None)
    payload["sections"] = [
        {
            key: value
            for key, value in section.items()
            if key in {"section_id", "title", "role", "ordinal"}
        }
        for section in payload.get("sections", [])
    ]
    candidates = [
        span
        for span in bundle.stable_spans
        if (span_ids is None or span.span_id in span_ids)
        and (section_roles is None or span.section_role in section_roles)
    ]
    if not candidates and span_ids is not None:
        candidates = [span for span in bundle.stable_spans if span.span_id in span_ids]
    selected_spans: list[dict[str, object]] = []
    payload["stable_spans"] = selected_spans
    payload["equations"] = []
    payload["tables"] = []
    payload["figures"] = []
    for span in sorted(candidates, key=lambda value: (value.ordinal, value.span_id)):
        candidate = span.model_dump(mode="json")
        selected_spans.append(candidate)
        if len(canonical_json(payload)) > max_bytes:
            selected_spans.pop()
    selected_ids = {str(span["span_id"]) for span in selected_spans}
    payload["equations"] = [
        {
            "equation_id": equation.equation_id,
            "representation_format": "latex" if equation.latex else "mathml",
            "representation": equation.latex or equation.mathml,
            "source_span_ids": equation.source_span_ids,
        }
        for equation in bundle.equations
        if set(equation.source_span_ids) & selected_ids
    ]
    payload["tables"] = [
        {
            "table_id": table.table_id,
            "caption": table.caption,
            "rows": table.rows,
            "source_span_ids": table.source_span_ids,
        }
        for table in bundle.tables
        if set(table.source_span_ids) & selected_ids
    ]
    payload["figures"] = [
        {
            key: value
            for key, value in figure.model_dump(mode="json").items()
            if key not in {"asset_uri", "image_hash"}
        }
        for figure in bundle.figures
        if set(figure.source_span_ids) & selected_ids
    ]
    # Objects can themselves be large. Add them in stable order only while the
    # complete JSON remains inside the window-derived projection budget.
    for key in ("equations", "tables", "figures"):
        values = list(payload[key])
        payload[key] = []
        for value in values:
            payload[key].append(value)
            if len(canonical_json(payload)) > max_bytes:
                payload[key].pop()
    payload["projection_coverage"] = {
        "selected_spans": len(selected_spans),
        "available_spans": len(candidates),
        "selection": "stable-complete-spans-with-role-sharding",
    }
    return canonical_json(payload)


__all__ = [
    "bundle_json",
    "bundle_prompt_json",
    "load_scientific_artifact",
    "paper_bundle_from_gold",
]
