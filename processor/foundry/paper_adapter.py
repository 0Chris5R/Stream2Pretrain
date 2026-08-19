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
            "benchmark_set_version": gold.benchmark_set_version,
        },
        official_artifacts=official_artifacts or [],
        source_gold_hash=gold_hash,
        scientific_artifact_hash=scientific_hash,
    )


def load_scientific_artifact(gold: GoldRecord, *, s3_client: object) -> ScientificDocument:
    uri = gold.scientific_artifact_s3_uri
    if not uri:
        raise ValueError("post-training candidates require a scientific artifact")
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ValueError(f"invalid scientific artifact URI: {uri}")
    response = s3_client.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))  # type: ignore[attr-defined]
    payload = response["Body"].read()
    return ScientificDocument.model_validate_json(payload)


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


def bundle_prompt_json(bundle: PaperBundle) -> bytes:
    """Losslessly compact duplicate scientific representations for model prompts.

    The durable PaperBundle remains unchanged. The prompt projection keeps every
    training span and scientific object while choosing one equation encoding,
    one table encoding, and the object-local caption instead of sending large
    duplicate representations through the model context window.
    """
    payload = bundle.model_dump(mode="json")
    payload["prompt_projection"] = "paper-bundle-model-view-v1"
    payload.pop("captions", None)
    payload["equations"] = [
        {
            "equation_id": equation["equation_id"],
            "representation_format": "latex" if equation.get("latex") else "mathml",
            "representation": equation.get("latex") or equation.get("mathml"),
            "source_span_ids": equation.get("source_span_ids", []),
        }
        for equation in payload.get("equations", [])
    ]
    payload["tables"] = [
        {
            "table_id": table["table_id"],
            "caption": table.get("caption"),
            "rows": table.get("rows", []),
            "source_span_ids": table.get("source_span_ids", []),
        }
        for table in payload.get("tables", [])
    ]
    payload["figures"] = [
        {key: value for key, value in figure.items() if key not in {"asset_uri", "image_hash"}}
        for figure in payload.get("figures", [])
    ]
    return canonical_json(payload)


__all__ = [
    "bundle_json",
    "bundle_prompt_json",
    "load_scientific_artifact",
    "paper_bundle_from_gold",
]
