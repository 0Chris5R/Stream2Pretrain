"""Structured scientific-document artifact retained beside training text."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from schemas.bronze import DocId

SectionRole = Literal[
    "abstract",
    "introduction",
    "background",
    "methods",
    "results",
    "discussion",
    "conclusion",
    "limitations",
    "appendix",
    "acknowledgements",
    "references",
    "metadata",
    "other",
]


class ScientificParagraph(BaseModel):
    """Stable paragraph-sized unit used to build the training projection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    paragraph_id: str
    text: str
    include_in_training: bool = True
    exclusion_reason: str | None = None


class ScientificSection(BaseModel):
    """One heading-delimited section from the source document."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    section_id: str
    level: int = Field(..., ge=1, le=6)
    title: str
    text: str
    role: SectionRole = "other"
    include_in_training: bool = True
    exclusion_reason: str | None = None
    word_count: int = Field(default=0, ge=0)
    paragraphs: list[ScientificParagraph] = Field(default_factory=list)


class ScientificEquation(BaseModel):
    """MathML or TeX representation retained from the source."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    equation_id: str
    latex: str | None = None
    mathml: str | None = None
    display: bool = False
    page_number: int | None = Field(default=None, ge=1)
    bbox: tuple[float, float, float, float] | None = None


class ScientificTable(BaseModel):
    """A table represented as ordered rows and cells."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    table_id: str
    caption: str | None = None
    rows: list[list[str]] = Field(default_factory=list)
    footnotes: list[str] = Field(default_factory=list)
    page_number: int | None = Field(default=None, ge=1)
    bbox: tuple[float, float, float, float] | None = None


class ScientificCitation(BaseModel):
    """A source citation or internal bibliography reference."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    citation_id: str
    text: str
    target: str | None = None


FigureType = Literal[
    "logo",
    "photograph",
    "icon",
    "engineering_drawing",
    "line_chart",
    "bar_chart",
    "other",
    "table",
    "flow_chart",
    "screenshot_from_computer",
    "signature",
    "screenshot_from_manual",
    "geographical_map",
    "pie_chart",
    "page_thumbnail",
    "stamp",
    "music",
    "calendar",
    "qr_code",
    "bar_code",
    "full_page_image",
    "scatter_plot",
    "chemistry_structure",
    "topographical_map",
    "crossword_puzzle",
    "box_plot",
    "unknown",
]


class ScientificFigure(BaseModel):
    """Figure asset plus deterministic CPU enrichment and provenance."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    figure_id: str
    source_element_id: str | None = None
    source_url: str
    asset_s3_uri: str | None = None
    image_sha256: str | None = None
    mime_type: str | None = None
    width: int | None = Field(default=None, ge=1)
    height: int | None = Field(default=None, ge=1)
    caption: str | None = None
    alt_text: str | None = None
    nearby_text: str | None = None
    page_number: int | None = Field(default=None, ge=1)
    bbox: tuple[float, float, float, float] | None = None
    figure_type: FigureType = "unknown"
    figure_type_confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    classifier_revision: str | None = None
    ocr_text: str | None = None
    ocr_revision: str | None = None
    ocr_training_eligible: bool = Field(
        default=False,
        description=(
            "True only after a versioned OCR quality policy has accepted this output for "
            "the text training projection. Raw OCR is audit-only by default."
        ),
    )
    ocr_quality_score: float | None = Field(default=None, ge=0.0, le=1.0)
    ocr_policy_revision: str | None = None
    warnings: list[str] = Field(default_factory=list)


class ScientificDocument(BaseModel):
    """Versioned structured view whose plain-text projection enters curation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = "scientific-document-v2"
    doc_id: DocId
    source_url: str
    title: str | None = None
    authors: list[str] = Field(default_factory=list)
    author_metadata: list[str] = Field(
        default_factory=list,
        description="Raw bounded author/affiliation blocks kept out of the training projection.",
    )
    abstract: str | None = None
    source_identifier: str | None = None
    publication_date: str | None = None
    license: str | None = None
    text_sha256: str
    extraction_pipeline: str
    projection_version: str = "scientific-body-v3"
    source_word_count: int = Field(default=0, ge=0)
    training_word_count: int = Field(default=0, ge=0)
    included_section_count: int = Field(default=0, ge=0)
    excluded_section_count: int = Field(default=0, ge=0)
    excluded_sections: list[str] = Field(default_factory=list)
    raw_extractor_s3_uri: str | None = Field(
        default=None,
        pattern=r"^s3://[^/]+/.+",
        description="Lossless extractor-native artifact, such as Docling JSON.",
    )
    sections: list[ScientificSection] = Field(default_factory=list)
    equations: list[ScientificEquation] = Field(default_factory=list)
    tables: list[ScientificTable] = Field(default_factory=list)
    figures: list[ScientificFigure] = Field(default_factory=list)
    citations: list[ScientificCitation] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)

    def visual_text_surrogate(self, *, include_accepted_ocr: bool = True) -> str:
        """Return bounded visual evidence without leaking unverified OCR.

        Captions and source alt text are source-authored evidence. Tesseract output
        is retained in the artifact for search and review, but only enters a text
        export after an explicit versioned OCR policy marks it eligible.
        """
        blocks: list[str] = []
        for figure in self.figures:
            values = [
                "[FIGURE]",
                f"ID: {figure.figure_id}",
                f"Type: {figure.figure_type}",
            ]
            if figure.caption:
                values.append(f"Caption: {figure.caption}")
            if figure.alt_text:
                values.append(f"Alt text: {figure.alt_text}")
            if include_accepted_ocr and figure.ocr_text and figure.ocr_training_eligible:
                values.append(f"Visible text: {figure.ocr_text}")
            values.append("[/FIGURE].")
            blocks.append(" ".join(values))
        return "\n\n".join(blocks)

    def training_text_projection(self) -> str:
        """Return the deterministic body-only projection used downstream.

        Author metadata, acknowledgements, declarations, and bibliography
        sections remain in this structured artifact but never enter the
        projection. Equations, bounded tables, and figure surrogates are kept
        because they are part of the scientific evidence.
        """
        blocks: list[str] = []
        if self.title:
            blocks.append(f"# {self.title}")
        for section in self.sections:
            if not section.include_in_training or not section.text.strip():
                continue
            blocks.append(f"{'#' * min(6, max(2, section.level))} {section.title}\n{section.text}")
        structured = self.structured_text_surrogate()
        if structured:
            blocks.append(structured)
        return "\n\n".join(block.strip() for block in blocks if block.strip()).strip()

    def structured_text_surrogate(self) -> str:
        """Return bounded non-prose evidence blocks for training exports."""
        blocks: list[str] = []
        for table in self.tables[:64]:
            values = ["[TABLE]", f"ID: {table.table_id}"]
            if table.caption:
                values.append(f"Caption: {table.caption}")
            for row in table.rows[:40]:
                values.append(" | ".join(cell[:400] for cell in row[:20]))
            values.append("[/TABLE]")
            blocks.append("\n".join(values))
        # Inline equations already remain in their surrounding prose. Only
        # display equations need a separate bounded surrogate.
        for equation in [item for item in self.equations if item.display][:128]:
            value = equation.latex or equation.mathml
            if value:
                blocks.append(f"[EQUATION] {value[:2000]} [/EQUATION]")
        surrogate = self.visual_text_surrogate(include_accepted_ocr=True)
        if surrogate:
            blocks.append(surrogate)
        return "\n\n".join(block.strip() for block in blocks if block.strip()).strip()
