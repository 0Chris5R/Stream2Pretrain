"""CPU-only structured scientific HTML, figure classification, and OCR."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import io
import os
import re
import shutil
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import orjson

from schemas.scientific import (
    FigureType,
    ScientificCitation,
    ScientificDocument,
    ScientificEquation,
    ScientificFigure,
    ScientificParagraph,
    ScientificSection,
    ScientificTable,
    SectionRole,
)

FIGURE_LABELS: tuple[FigureType, ...] = (
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
)
_SPACE = re.compile(r"\s+")


@dataclass(frozen=True, slots=True)
class ScientificProcessingResult:
    """Structured artifact pointer and the text projection used downstream."""

    text: str
    model_text: str
    source_metadata_text: str
    structured_text: str
    artifact_s3_uri: str
    document: ScientificDocument


class PdfExceedsDoclingLimitError(ValueError):
    """The exact expanded PDF body exceeds the configured Docling limit."""

    def __init__(self, *, actual_bytes: int, limit_bytes: int) -> None:
        self.actual_bytes = actual_bytes
        self.limit_bytes = limit_bytes
        super().__init__(
            f"PDF body is {actual_bytes} bytes; configured Docling limit is {limit_bytes} bytes"
        )


class DoclingDocumentConversionError(ValueError):
    """Docling conclusively rejected one document rather than its runtime."""


def _is_docling_conversion_error(exc: Exception) -> bool:
    """Recognize Docling's document-level exception without importing its optional runtime."""
    exception_type = type(exc)
    return exception_type.__name__ == "ConversionError" and exception_type.__module__.startswith(
        "docling"
    )


class FigureClassifier:
    """Docling DocumentFigureClassifier-v2.5 ONNX CPU wrapper."""

    def __init__(
        self,
        model_dir: str | Path | None,
        *,
        revision: str,
        allow_fallback: bool,
    ) -> None:
        self._path = Path(model_dir) if model_dir else None
        self.revision = revision
        self._session: Any | None = self._load()
        if not allow_fallback and self._session is None:
            raise RuntimeError("the pinned document figure classifier is required")

    def _load(self) -> Any | None:
        if self._path is None or not (self._path / "model.onnx").is_file():
            return None
        try:
            import onnxruntime as ort  # type: ignore[import-untyped]

            return ort.InferenceSession(
                str(self._path / "model.onnx"), providers=["CPUExecutionProvider"]
            )
        except Exception:
            return None

    @property
    def is_model_loaded(self) -> bool:
        return self._session is not None

    def classify(self, image: Any) -> tuple[FigureType, float]:
        if self._session is None:
            return "unknown", 0.0
        import numpy as np

        resized = image.convert("RGB").resize((224, 224))  # type: ignore[union-attr]
        array = np.asarray(resized, dtype=np.float32) / 255.0
        mean = np.asarray([0.485, 0.456, 0.406], dtype=np.float32)
        std = np.asarray([0.47853944, 0.4732864, 0.47434163], dtype=np.float32)
        tensor = ((array - mean) / std).transpose(2, 0, 1)[None, :, :, :]
        input_name = self._session.get_inputs()[0].name  # type: ignore[union-attr]
        logits = self._session.run(None, {input_name: tensor})[0][0]  # type: ignore[union-attr]
        shifted = logits - logits.max()
        probs = np.exp(shifted) / np.exp(shifted).sum()
        index = int(probs.argmax())
        if index >= len(FIGURE_LABELS):
            return "unknown", 0.0
        return FIGURE_LABELS[index], float(probs[index])


class TesseractOCR:
    """Tesseract English OCR with an explicit timeout and revision tag."""

    def __init__(self, *, allow_fallback: bool, timeout_seconds: float = 20.0) -> None:
        self._available = shutil.which("tesseract") is not None
        self._allow_fallback = allow_fallback
        self._timeout = timeout_seconds
        self.revision = "tesseract-eng"
        if not allow_fallback and not self._available:
            raise RuntimeError("Tesseract with English language data is required")

    @property
    def is_available(self) -> bool:
        return self._available

    def read(self, image: Any) -> str:
        if not self._available:
            return ""
        try:
            import pytesseract  # type: ignore[import-not-found]

            text = pytesseract.image_to_string(
                image, lang="eng", config="--psm 6", timeout=self._timeout
            )
            return _clean(text)[:8000]
        except Exception:
            if not self._allow_fallback:
                raise
            return ""


class ScientificProcessor:
    """Extract, enrich, persist, and project one scientific HTML document."""

    def __init__(
        self,
        *,
        s3_client: Any,
        bucket: str,
        models_dir: str | Path,
        user_agent: str,
        require_real_models: bool,
        disable_docling_document_timeout: bool = False,
    ) -> None:
        figure_revision = os.environ.get(
            "S2P_FIGURE_CLASSIFIER_REVISION",
            "docling-figure-v2.5@f859dfbff5c9916cd996942d4b0db7fa25808220",
        )
        self._s3 = s3_client
        self._bucket = bucket
        self._user_agent = user_agent
        self._models_dir = Path(models_dir)
        self._require_real_models = require_real_models
        self._disable_docling_document_timeout = disable_docling_document_timeout
        self._docling_converter: Any | None = None
        self._classifier = FigureClassifier(
            Path(models_dir) / "figure-classifier",
            revision=figure_revision,
            allow_fallback=not require_real_models,
        )
        self._ocr = TesseractOCR(allow_fallback=not require_real_models)
        self._max_image_bytes = int(os.environ.get("S2P_MAX_FIGURE_BYTES", "10485760"))
        # Zero means all figures. A non-zero value remains an emergency
        # operator guard, but hitting it marks the document incomplete and the
        # curator routes it to retry rather than accepting partial data.
        self._max_figures = int(os.environ.get("S2P_MAX_FIGURES_PER_DOCUMENT", "0"))
        self._http_timeout = float(os.environ.get("S2P_FIGURE_HTTP_TIMEOUT", "20"))
        self._docling_enabled = os.environ.get("S2P_DOCLING_ENABLED", "1") == "1"
        self._docling_models = self._models_dir / "docling"
        self._max_pdf_pages = int(os.environ.get("S2P_DOCLING_MAX_PAGES", "0"))
        self._max_pdf_bytes = int(os.environ.get("S2P_DOCLING_MAX_BYTES", "67108864"))
        if self._max_pdf_bytes <= 0:
            raise RuntimeError("S2P_DOCLING_MAX_BYTES must be positive")
        if require_real_models and self._docling_enabled:
            if importlib.util.find_spec("docling") is None:
                raise RuntimeError("the pinned Docling CPU PDF fallback is required")
            if not self._docling_models.is_dir() or not any(
                path.is_file() for path in self._docling_models.rglob("*")
            ):
                raise RuntimeError("prefetched Docling layout/table/formula models are required")

    def process(
        self,
        *,
        doc_id: str,
        source_url: str,
        html: bytes,
        plain_text: str,
        title: str | None,
        extraction_pipeline: str,
    ) -> ScientificProcessingResult:
        document = extract_scientific_html(
            doc_id=doc_id,
            source_url=source_url,
            html=html,
            plain_text=plain_text,
            title=title,
            extraction_pipeline=extraction_pipeline,
        )
        figures: list[ScientificFigure] = []
        warnings = list(document.warnings)
        selected_figures = (
            document.figures[: self._max_figures] if self._max_figures else document.figures
        )
        for figure in selected_figures:
            try:
                figures.append(self._enrich_figure(doc_id, source_url, figure))
            except Exception as exc:
                warning = f"figure_enrichment_failed:{figure.figure_id}:{type(exc).__name__}"
                warnings.append(warning)
                figures.append(figure.model_copy(update={"warnings": [*figure.warnings, warning]}))
        if self._max_figures and len(document.figures) > self._max_figures:
            warnings.append("figure_limit_reached")
        document = document.model_copy(update={"figures": figures, "warnings": warnings})
        return self._store_document(document=document, plain_text=plain_text)

    def process_pdf(
        self,
        *,
        doc_id: str,
        source_url: str,
        pdf: bytes,
        extraction_pipeline: str,
    ) -> ScientificProcessingResult:
        """Convert a PDF with Docling, falling back to bounded text extraction."""
        # Bronze ``bytes_size`` is the stored gzip size, so the exact guard
        # belongs here after decompression and before converter/model startup.
        # Oversized input is not sent through pypdf because that would silently
        # discard tables, figures, and OCR at a configurable capacity boundary.
        if len(pdf) > self._max_pdf_bytes:
            raise PdfExceedsDoclingLimitError(
                actual_bytes=len(pdf),
                limit_bytes=self._max_pdf_bytes,
            )
        if self._docling_enabled:
            try:
                return self._process_pdf_docling(
                    doc_id=doc_id,
                    source_url=source_url,
                    pdf=pdf,
                    extraction_pipeline=extraction_pipeline,
                )
            except Exception as exc:
                if _is_docling_conversion_error(exc):
                    raise DoclingDocumentConversionError(str(exc)) from exc
                if self._require_real_models:
                    raise
                fallback_warning = f"docling_fallback:{type(exc).__name__}"
        else:
            fallback_warning = "docling_disabled:pypdf"
        return self._process_pdf_fallback(
            doc_id=doc_id,
            source_url=source_url,
            pdf=pdf,
            extraction_pipeline=extraction_pipeline,
            warning=fallback_warning,
        )

    def process_text(
        self,
        *,
        doc_id: str,
        source_url: str,
        text: str,
        title: str | None,
        source_format: str,
        extraction_pipeline: str,
    ) -> ScientificProcessingResult:
        """Structure scientific Markdown or LaTeX without treating it as HTML."""
        document = extract_scientific_text(
            doc_id=doc_id,
            source_url=source_url,
            text=text,
            title=title,
            source_format=source_format,
            extraction_pipeline=extraction_pipeline,
        )
        return self._store_document(document=document, plain_text=text)

    def _process_pdf_docling(
        self,
        *,
        doc_id: str,
        source_url: str,
        pdf: bytes,
        extraction_pipeline: str,
    ) -> ScientificProcessingResult:
        """Convert one bounded PDF with Docling's standard CPU pipeline."""
        converter = self._get_docling_converter()
        from docling.datamodel.base_models import DocumentStream  # type: ignore[import-not-found]
        from docling_core.types.doc import (  # type: ignore[import-not-found]
            FormulaItem,
            PictureItem,
            SectionHeaderItem,
            TableItem,
            TextItem,
            TitleItem,
        )

        stream = DocumentStream(
            name=f"{doc_id.removeprefix('sha256:')}.pdf", stream=io.BytesIO(pdf)
        )
        convert_options: dict[str, int] = {"max_file_size": self._max_pdf_bytes}
        if self._max_pdf_pages:
            convert_options["max_num_pages"] = self._max_pdf_pages
        result = converter.convert(stream, **convert_options)
        source_document = result.document
        raw_key = f"scientific/{doc_id.removeprefix('sha256:')}/document.docling.json"
        self._s3.put_object(  # type: ignore[union-attr]
            Bucket=self._bucket,
            Key=raw_key,
            Body=orjson.dumps(source_document.export_to_dict()),
            ContentType="application/json",
        )

        title: str | None = None
        sections: list[ScientificSection] = []
        equations: list[ScientificEquation] = []
        citations: list[ScientificCitation] = []
        current_title = "Document body"
        current_level = 1
        current_text: list[str] = []

        def flush_section() -> None:
            nonlocal current_text
            text = "\n".join(value for value in current_text if value).strip()
            if text:
                role = _section_role(current_title)
                exclusion_reason = _section_exclusion_reason(role, current_title)
                sections.append(
                    ScientificSection(
                        section_id=f"section-{len(sections) + 1}",
                        level=current_level,
                        title=current_title,
                        text=text,
                        role=role,
                        include_in_training=exclusion_reason is None,
                        exclusion_reason=exclusion_reason,
                        word_count=_word_count(text),
                        paragraphs=[
                            ScientificParagraph(
                                paragraph_id=(f"section-{len(sections) + 1}-paragraph-{index + 1}"),
                                text=value,
                                include_in_training=exclusion_reason is None,
                                exclusion_reason=exclusion_reason,
                            )
                            for index, value in enumerate(current_text)
                            if value
                        ],
                    )
                )
            current_text = []

        for item, _level in source_document.iterate_items():
            if isinstance(item, TitleItem) and title is None:
                title = _clean(item.text) or None
                continue
            if isinstance(item, SectionHeaderItem):
                flush_section()
                current_title = _clean(item.text) or f"Section {len(sections) + 1}"
                current_level = max(1, min(int(item.level), 6))
                continue
            if isinstance(item, FormulaItem):
                page_number, bbox = _docling_provenance(item)
                equations.append(
                    ScientificEquation(
                        equation_id=f"equation-{len(equations) + 1}",
                        latex=_clean(item.text) or None,
                        display=True,
                        page_number=page_number,
                        bbox=bbox,
                    )
                )
                continue
            if isinstance(item, TextItem):
                label = str(getattr(item.label, "value", item.label))
                value = _clean(item.text)
                if not value or label in {"caption", "page_header", "page_footer"}:
                    continue
                abstract = re.match(
                    r"^(?i:abstract)(?:[.:\-\u2013\u2014]\s*|\s+(?=[A-Z])|$)(.*)$", value
                )
                body_roles = {"abstract", "introduction", "background", "methods", "results"}
                if (
                    abstract
                    and _section_role(current_title) not in body_roles
                    and not any(section.role in body_roles for section in sections)
                ):
                    flush_section()
                    current_title = "Abstract"
                    current_level = 2
                    if abstract.group(1):
                        current_text.append(abstract.group(1))
                    continue
                if label == "reference":
                    citations.append(
                        ScientificCitation(citation_id=f"citation-{len(citations) + 1}", text=value)
                    )
                else:
                    current_text.append(value)
        flush_section()

        tables: list[ScientificTable] = []
        for index, table in enumerate(source_document.tables):
            if not isinstance(table, TableItem):
                continue
            dataframe = table.export_to_dataframe(doc=source_document)
            rows = [
                [_clean(str(cell)) for cell in row]
                for row in dataframe.fillna("").astype(str).values.tolist()
            ]
            if list(dataframe.columns):
                rows.insert(0, [_clean(str(column)) for column in dataframe.columns])
            page_number, bbox = _docling_provenance(table)
            tables.append(
                ScientificTable(
                    table_id=f"table-{index + 1}",
                    caption=_clean(table.caption_text(source_document)) or None,
                    rows=rows,
                    page_number=page_number,
                    bbox=bbox,
                )
            )

        figures: list[ScientificFigure] = []
        warnings: list[str] = []
        selected_pictures = (
            source_document.pictures[: self._max_figures]
            if self._max_figures
            else source_document.pictures
        )
        for index, picture in enumerate(selected_pictures):
            if not isinstance(picture, PictureItem):
                continue
            figure_id = f"figure-{index + 1}"
            page_number, bbox = _docling_provenance(picture)
            figure = ScientificFigure(
                figure_id=figure_id,
                source_url=f"{source_url}#page={page_number or 1}&figure={index + 1}",
                caption=_clean(picture.caption_text(source_document)) or None,
                page_number=page_number,
                bbox=bbox,
            )
            try:
                image = picture.get_image(source_document)
                if image is None:
                    raise ValueError("Docling did not retain the figure crop")
                buffer = io.BytesIO()
                image.convert("RGB").save(buffer, format="PNG")
                figures.append(
                    self._enrich_image_payload(doc_id, figure, buffer.getvalue(), "image/png")
                )
            except Exception as exc:
                warning = f"figure_enrichment_failed:{figure_id}:{type(exc).__name__}"
                warnings.append(warning)
                figures.append(figure.model_copy(update={"warnings": [warning]}))
        if self._max_figures and len(source_document.pictures) > self._max_figures:
            warnings.append("figure_limit_reached")

        plain_text = source_document.export_to_markdown(strict_text=True).strip()
        if title is None:
            title = _infer_pdf_title(plain_text)
            if title is None:
                warnings.append("title_not_detected")
        document = ScientificDocument(
            doc_id=doc_id,
            source_url=source_url,
            title=title,
            text_sha256=hashlib.sha256(plain_text.encode("utf-8")).hexdigest(),
            extraction_pipeline=extraction_pipeline,
            raw_extractor_s3_uri=f"s3://{self._bucket}/{raw_key}",
            sections=sections,
            equations=equations,
            tables=tables,
            figures=figures,
            citations=citations,
            warnings=warnings,
        )
        return self._store_document(document=document, plain_text=plain_text)

    def _process_pdf_fallback(
        self,
        *,
        doc_id: str,
        source_url: str,
        pdf: bytes,
        extraction_pipeline: str,
        warning: str,
    ) -> ScientificProcessingResult:
        """Extract bounded per-page text when the heavier Docling path is unavailable."""
        from pypdf import PdfReader  # type: ignore[import-untyped]

        reader = PdfReader(io.BytesIO(pdf), strict=False)
        page_limit = (
            min(len(reader.pages), self._max_pdf_pages)
            if self._max_pdf_pages
            else len(reader.pages)
        )
        page_texts: list[str] = []
        for page in reader.pages[:page_limit]:
            value = (page.extract_text() or "").strip()
            if not value:
                continue
            page_texts.append(value)
        plain_text = "\n\n".join(page_texts).strip()
        blocks, _ = _parse_heading_text(_pdf_text_headings(plain_text))
        sections = _scientific_text_sections(blocks)
        metadata = reader.metadata
        metadata_title = _clean(str(getattr(metadata, "title", "") or "")) or None
        title = metadata_title or _infer_pdf_title(plain_text)
        fallback_warnings = [warning]
        if len(reader.pages) > page_limit:
            fallback_warnings.append("page_limit_reached")
        if title is None:
            fallback_warnings.append("title_not_detected")
        document = ScientificDocument(
            doc_id=doc_id,
            source_url=source_url,
            title=title,
            text_sha256=hashlib.sha256(plain_text.encode("utf-8")).hexdigest(),
            extraction_pipeline=f"{extraction_pipeline}+pypdf",
            sections=sections,
            warnings=fallback_warnings,
        )
        return self._store_document(document=document, plain_text=plain_text)

    def _get_docling_converter(self) -> Any:
        if self._docling_converter is not None:
            return self._docling_converter
        try:
            from docling.datamodel.accelerator_options import (  # type: ignore[import-not-found]
                AcceleratorDevice,
                AcceleratorOptions,
            )
            from docling.datamodel.base_models import InputFormat  # type: ignore[import-not-found]
            from docling.datamodel.pipeline_options import (  # type: ignore[import-not-found]
                PdfPipelineOptions,
                TableFormerMode,
                TableStructureOptions,
                TesseractCliOcrOptions,
            )
            from docling.document_converter import (  # type: ignore[import-not-found]
                DocumentConverter,
                PdfFormatOption,
            )
        except Exception as exc:
            raise RuntimeError("Docling 2.114.0 is unavailable") from exc

        options = PdfPipelineOptions()
        options.artifacts_path = self._docling_models
        options.accelerator_options = AcceleratorOptions(
            num_threads=int(os.environ.get("S2P_DOCLING_CPU_THREADS", "2")),
            device=AcceleratorDevice.CPU,
        )
        # A cooperative Docling timeout can abandon a native OCR thread and
        # poison the next PDF in the same process. Production PDF conversion
        # disables that internal timer and relies on the parent-enforced hard
        # process deadline, which can terminate every native resource safely.
        options.document_timeout = (
            None
            if self._disable_docling_document_timeout
            else float(os.environ.get("S2P_DOCLING_DOCUMENT_TIMEOUT", "180"))
        )
        options.do_ocr = True
        options.ocr_options = TesseractCliOcrOptions(lang=["eng"])
        options.do_table_structure = True
        options.table_structure_options = TableStructureOptions(
            # FAST keeps the TableFormer cell-structure path while reducing
            # CPU-worker pressure. ACCURATE was repeatedly OOM-killed at the
            # 2 GiB cgroup boundary; FAST peak RSS is needs-measurement.
            do_cell_matching=True,
            mode=TableFormerMode.FAST,
        )
        # Docling's formula enrichment loads the multi-billion-parameter
        # CodeFormulaV2 vision model. It exceeds the bounded CPU worker's
        # memory before the first page is processed. Native arXiv HTML keeps
        # source LaTeX; PDF fallback retains Docling layout, text, tables,
        # figures, and Tesseract OCR without this optional VLM enrichment.
        options.do_formula_enrichment = False
        options.generate_picture_images = True
        options.generate_page_images = False
        options.images_scale = 1.5
        self._docling_converter = DocumentConverter(
            allowed_formats=[InputFormat.PDF],
            format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)},
        )
        return self._docling_converter

    def _store_document(
        self, *, document: ScientificDocument, plain_text: str
    ) -> ScientificProcessingResult:
        document = document.model_copy(
            update={"sections": _exclude_front_matter(document.sections)}
        )
        included = [section for section in document.sections if section.include_in_training]
        excluded = [section for section in document.sections if not section.include_in_training]
        document = document.model_copy(
            update={
                "source_word_count": document.source_word_count or _word_count(plain_text),
                "included_section_count": len(included),
                "excluded_section_count": len(excluded),
                "excluded_sections": [
                    f"{section.title}: {section.exclusion_reason or 'policy'}"
                    for section in excluded
                ],
            }
        )
        projected_text = document.training_text_projection()
        document = document.model_copy(update={"training_word_count": _word_count(projected_text)})
        artifact_key = f"scientific/{document.doc_id.removeprefix('sha256:')}/document.json"
        self._s3.put_object(  # type: ignore[union-attr]
            Bucket=self._bucket,
            Key=artifact_key,
            Body=orjson.dumps(document.model_dump(mode="json")),
            ContentType="application/json",
        )
        model_text = "\n\n".join(
            section.text.strip()
            for section in document.sections
            if section.include_in_training and section.text.strip()
        ).strip()
        source_metadata_text = "\n".join(
            value for value in [document.title or "", *document.author_metadata] if value.strip()
        )[:32768]
        return ScientificProcessingResult(
            text=projected_text,
            model_text=model_text or projected_text,
            source_metadata_text=source_metadata_text,
            structured_text=document.structured_text_surrogate(),
            artifact_s3_uri=f"s3://{self._bucket}/{artifact_key}",
            document=document,
        )

    def _enrich_figure(
        self,
        doc_id: str,
        base_url: str,
        figure: ScientificFigure,
    ) -> ScientificFigure:
        payload, mime = self._read_image(base_url, figure.source_url)
        return self._enrich_image_payload(doc_id, figure, payload, mime)

    def _enrich_image_payload(
        self,
        doc_id: str,
        figure: ScientificFigure,
        payload: bytes,
        mime: str,
    ) -> ScientificFigure:
        from PIL import Image  # type: ignore[import-not-found]

        # Pillow's built-in decompression-bomb threshold is based on decoded
        # pixels rather than compressed bytes. Promote its warning to an
        # exception before ``convert`` allocates the full raster; the caller
        # records the skipped figure as an extraction warning and continues.
        with warnings.catch_warnings():
            warnings.simplefilter("error", Image.DecompressionBombWarning)
            image = Image.open(io.BytesIO(payload)).convert("RGB")
        figure_type, confidence = self._classifier.classify(image)
        ocr_text = self._ocr.read(image)
        digest = hashlib.sha256(payload).hexdigest()
        suffix = _extension_for_mime(mime)
        key = f"scientific/{doc_id.removeprefix('sha256:')}/figures/{figure.figure_id}.{suffix}"
        self._s3.put_object(  # type: ignore[union-attr]
            Bucket=self._bucket,
            Key=key,
            Body=payload,
            ContentType=mime,
            Metadata={"sha256": digest, "figure-id": figure.figure_id},
        )
        return figure.model_copy(
            update={
                "asset_s3_uri": f"s3://{self._bucket}/{key}",
                "image_sha256": digest,
                "mime_type": mime,
                "width": image.width,
                "height": image.height,
                "figure_type": figure_type,
                "figure_type_confidence": confidence,
                "classifier_revision": self._classifier.revision,
                "ocr_text": ocr_text or None,
                "ocr_revision": self._ocr.revision,
            }
        )

    def _read_image(self, base_url: str, source: str) -> tuple[bytes, str]:
        if source.startswith("data:"):
            header, _, encoded = source.partition(",")
            if ";base64" not in header:
                raise ValueError("unsupported non-base64 figure data URI")
            mime = header[5:].split(";", 1)[0] or "application/octet-stream"
            payload = base64.b64decode(encoded, validate=True)
        else:
            import httpx

            target = urljoin(base_url, source)
            with httpx.Client(
                timeout=self._http_timeout,
                follow_redirects=True,
                headers={"User-Agent": self._user_agent},
            ) as client:
                response = client.get(target)
                response.raise_for_status()
                payload = response.content
                mime = response.headers.get("content-type", "application/octet-stream")
                mime = mime.split(";", 1)[0].strip().lower()
        if not mime.startswith("image/"):
            raise ValueError(f"figure response is not an image: {mime}")
        if len(payload) > self._max_image_bytes:
            raise ValueError("figure exceeds configured byte limit")
        return payload, mime


def extract_scientific_html(
    *,
    doc_id: str,
    source_url: str,
    html: bytes,
    plain_text: str,
    title: str | None,
    extraction_pipeline: str,
) -> ScientificDocument:
    """Build a section-aware view from native arXiv/ar5iv-compatible HTML.

    The raw page remains immutable in Bronze. This parser deliberately keeps
    author metadata and bibliography entries in the structured artifact while
    marking them excluded from the downstream training projection.
    """
    from bs4 import BeautifulSoup  # type: ignore[import-not-found]

    soup = BeautifulSoup(html, "lxml")
    root = soup.find("article") or soup.find(class_="ltx_page_main") or soup.body or soup
    resolved_title = _clean(title or "") or _document_title(root) or None
    author_metadata = _extract_author_metadata(root)
    authors = _extract_authors(author_metadata)
    equations: list[ScientificEquation] = []
    math_nodes = list(root.find_all("math"))
    for index, math_node in enumerate(math_nodes):
        annotation = math_node.find("annotation", attrs={"encoding": "application/x-tex"})
        latex = _clean(annotation.get_text(" ", strip=True)) if annotation else None
        equations.append(
            ScientificEquation(
                equation_id=str(math_node.get("id") or f"equation-{index + 1}"),
                latex=latex or math_node.get("alttext"),
                mathml=str(math_node),
                display=str(math_node.get("display", "")).lower() == "block",
            )
        )
        # arXiv HTML commonly nests visual MathML, accessibility text, and a
        # TeX annotation under the same node. ``get_text`` would emit every
        # representation. Replace it with one canonical inline form before
        # extracting prose while retaining the complete MathML above.
        inline_value = latex or math_node.get("alttext")
        math_node.replace_with(f" {inline_value} " if inline_value else " ")

    sections = _extract_sections(root, document_title=resolved_title)
    abstract = next(
        (section.text for section in sections if section.role == "abstract" and section.text),
        None,
    )

    tables: list[ScientificTable] = []
    table_nodes = [table for table in root.find_all("table") if _is_data_table(table)]
    # Do not emit both a composite semantic table and a nested layout table.
    table_nodes = [
        table
        for table in table_nodes
        if not any(_is_data_table(parent) for parent in table.find_parents("table"))
    ]
    for index, table in enumerate(table_nodes):
        rows = [
            [_clean(cell.get_text(" ", strip=True)) for cell in row.find_all(["th", "td"])]
            for row in table.find_all("tr")
        ]
        rows = [row for row in rows if row]
        caption_node = table.find("caption")
        if caption_node is None and table.parent is not None:
            caption_node = table.parent.find("figcaption")
        tables.append(
            ScientificTable(
                table_id=str(table.get("id") or f"table-{index + 1}"),
                caption=_clean(caption_node.get_text(" ", strip=True)) if caption_node else None,
                rows=rows,
            )
        )

    figures: list[ScientificFigure] = []
    seen_sources: set[str] = set()
    # Scientific figures in native arXiv/ar5iv HTML are represented by a
    # semantic ``figure`` ancestor. Bare images before the abstract are logos,
    # badges, or resource icons and must not inflate evidence counts or tags.
    figure_images = [image for image in root.find_all("img") if image.find_parent("figure")]
    for index, image in enumerate(figure_images):
        source = str(image.get("src") or image.get("data-src") or "").strip()
        if not source or source in seen_sources:
            continue
        seen_sources.add(source)
        parent_figure = image.find_parent("figure")
        caption_node = parent_figure.find("figcaption") if parent_figure else None
        nearby = image.find_previous("p")
        figures.append(
            ScientificFigure(
                figure_id=f"figure-{index + 1}",
                source_element_id=str(image.get("id")) if image.get("id") else None,
                source_url=source,
                caption=_clean(caption_node.get_text(" ", strip=True)) if caption_node else None,
                alt_text=_clean(str(image.get("alt") or "")) or None,
                nearby_text=_clean(nearby.get_text(" ", strip=True))[:1000] if nearby else None,
            )
        )

    citations: list[ScientificCitation] = []
    seen_citations: set[tuple[str, str]] = set()
    for item in root.select("li.ltx_bibitem, li[role='doc-biblioentry']"):
        value = _clean(item.get_text(" ", strip=True))
        target = f"#{item.get('id')}" if item.get("id") else None
        key = (value, target or "")
        if not value or key in seen_citations:
            continue
        seen_citations.add(key)
        citations.append(
            ScientificCitation(
                citation_id=f"bibliography-{len(citations) + 1}",
                text=value,
                target=target,
            )
        )
    for link in root.find_all("a", href=True):
        target = str(link.get("href"))
        classes = {str(value) for value in (link.get("class") or [])}
        if not (target.startswith("#bib") or "ltx_ref" in classes or "ltx_cite" in classes):
            continue
        value = _clean(link.get_text(" ", strip=True))
        key = (value, target)
        if not value or key in seen_citations:
            continue
        seen_citations.add(key)
        citations.append(
            ScientificCitation(
                citation_id=f"citation-{len(citations) + 1}", text=value, target=target
            )
        )

    excluded = [section for section in sections if not section.include_in_training]
    return ScientificDocument(
        doc_id=doc_id,
        source_url=source_url,
        title=resolved_title,
        authors=authors,
        author_metadata=author_metadata,
        abstract=abstract,
        source_identifier=_source_identifier(root, source_url),
        publication_date=_publication_date(root),
        license=_license_text(root),
        text_sha256=hashlib.sha256(plain_text.encode("utf-8")).hexdigest(),
        extraction_pipeline=extraction_pipeline,
        source_word_count=_word_count(plain_text),
        included_section_count=sum(1 for section in sections if section.include_in_training),
        excluded_section_count=len(excluded),
        excluded_sections=[
            f"{section.title}: {section.exclusion_reason or 'policy'}" for section in excluded
        ],
        sections=sections,
        equations=equations,
        tables=tables,
        figures=figures,
        citations=citations,
    )


def extract_scientific_text(
    *,
    doc_id: str,
    source_url: str,
    text: str,
    title: str | None,
    source_format: str,
    extraction_pipeline: str,
) -> ScientificDocument:
    """Build section-aware artifacts from native scientific Markdown or LaTeX."""
    if source_format not in {"markdown", "latex"}:
        raise ValueError(f"unsupported scientific text format: {source_format}")

    author_metadata: list[str] = []
    authors: list[str] = []
    normalized = text
    detected_title: str | None = None
    if source_format == "latex":
        title_match = re.search(r"\\title\s*\{([^{}]+)\}", normalized, re.DOTALL)
        if title_match:
            detected_title = _latex_plain(title_match.group(1)) or None
        author_match = re.search(r"\\author\s*\{([^{}]+)\}", normalized, re.DOTALL)
        if author_match:
            raw_author = _clean(author_match.group(1))
            if raw_author:
                author_metadata = [raw_author[:32768]]
                authors = [
                    value
                    for value in (
                        _latex_plain(part) for part in re.split(r"\\and|\\\\|,", raw_author)
                    )
                    if value
                ][:256]
        normalized = _latex_to_heading_text(normalized)

    blocks, markdown_title = _parse_heading_text(normalized)
    resolved_title = markdown_title or detected_title or _clean(title or "") or None
    sections = _scientific_text_sections(blocks)
    abstract = next(
        (section.text for section in sections if section.role == "abstract" and section.text),
        None,
    )
    citations: list[ScientificCitation] = []
    for section in sections:
        if section.role != "references":
            continue
        for paragraph in section.paragraphs:
            if paragraph.text:
                citations.append(
                    ScientificCitation(
                        citation_id=f"bibliography-{len(citations) + 1}",
                        text=paragraph.text,
                    )
                )
    equations = _text_equations(text)
    excluded = [section for section in sections if not section.include_in_training]
    return ScientificDocument(
        doc_id=doc_id,
        source_url=source_url,
        title=resolved_title,
        authors=authors,
        author_metadata=author_metadata,
        abstract=abstract,
        text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        extraction_pipeline=extraction_pipeline,
        source_word_count=_word_count(text),
        included_section_count=sum(1 for section in sections if section.include_in_training),
        excluded_section_count=len(excluded),
        excluded_sections=[
            f"{section.title}: {section.exclusion_reason or 'policy'}" for section in excluded
        ],
        sections=sections,
        equations=equations,
        citations=citations,
    )


def _latex_to_heading_text(text: str) -> str:
    """Expose LaTeX document structure as heading-delimited text."""
    value = re.sub(r"(?m)(?<!\\)%.*$", "", text)
    value = re.sub(r"\\title\s*\{[^{}]+\}", "", value, flags=re.DOTALL)
    value = re.sub(r"\\author\s*\{[^{}]+\}", "", value, flags=re.DOTALL)
    value = re.sub(r"\\begin\s*\{abstract\}", "\n## Abstract\n", value)
    value = re.sub(r"\\end\s*\{abstract\}", "\n", value)
    levels = {"section": 2, "subsection": 3, "subsubsection": 4, "paragraph": 5}

    def heading(match: re.Match[str]) -> str:
        command = match.group(1).lower()
        return f"\n{'#' * levels[command]} {_latex_plain(match.group(2))}\n"

    value = re.sub(
        r"\\(section|subsection|subsubsection|paragraph)\*?\s*\{([^{}]+)\}",
        heading,
        value,
    )
    value = re.sub(r"\\(?:bibliography|printbibliography)\b[^\n]*", "\n## References\n", value)
    value = re.sub(r"\\(?:documentclass|usepackage)(?:\[[^\]]*\])?\{[^{}]*\}", "", value)
    value = re.sub(r"\\(?:begin|end)\s*\{document\}|\\maketitle\b", "", value)
    return value


def _latex_plain(value: str) -> str:
    """Return bounded readable text for simple LaTeX metadata and headings."""
    previous = value
    for _ in range(4):
        current = re.sub(r"\\[A-Za-z]+\*?(?:\[[^\]]*\])?\{([^{}]*)\}", r"\1", previous)
        if current == previous:
            break
        previous = current
    return _clean(re.sub(r"[{}]", "", previous.replace("~", " ")))


def _parse_heading_text(text: str) -> tuple[list[tuple[int, str, list[str]]], str | None]:
    """Parse ATX/Setext headings and blank-line-delimited paragraphs."""
    blocks: list[tuple[int, str, list[str]]] = []
    level = 2
    heading_title = "Document body"
    paragraphs: list[str] = []
    paragraph_lines: list[str] = []
    detected_title: str | None = None

    def flush_paragraph() -> None:
        nonlocal paragraph_lines
        value = "\n".join(paragraph_lines).strip()
        if value:
            paragraphs.append(value)
        paragraph_lines = []

    def flush_block() -> None:
        nonlocal paragraphs
        flush_paragraph()
        if paragraphs:
            blocks.append((level, heading_title, paragraphs))
        paragraphs = []

    lines = text.splitlines()
    index = 0
    while index < len(lines):
        raw = lines[index].rstrip()
        stripped = raw.strip()
        next_line = lines[index + 1].strip() if index + 1 < len(lines) else ""
        atx = re.match(r"^(#{1,6})\s+(.+?)\s*#*$", stripped)
        setext = bool(stripped and re.fullmatch(r"=+|-+", next_line))
        if atx or setext:
            flush_block()
            new_level = len(atx.group(1)) if atx else (1 if next_line.startswith("=") else 2)
            new_title = _clean(atx.group(2) if atx else stripped)
            if new_level == 1 and detected_title is None and not blocks:
                detected_title = new_title or None
                heading_title = "Document body"
                level = 2
            else:
                heading_title = new_title or f"Section {len(blocks) + 1}"
                level = new_level
            index += 2 if setext else 1
            continue
        if not stripped:
            flush_paragraph()
        else:
            paragraph_lines.append(raw)
        index += 1
    flush_block()
    return blocks, detected_title


def _scientific_text_sections(
    blocks: list[tuple[int, str, list[str]]],
) -> list[ScientificSection]:
    sections: list[ScientificSection] = []
    for index, (level, title, paragraph_values) in enumerate(blocks):
        role = _section_role(title)
        if title == "Document body" and len(blocks) > 1:
            role = "metadata"
        reason = _section_exclusion_reason(role, title)
        include = reason is None
        paragraphs = [
            ScientificParagraph(
                paragraph_id=f"section-{index + 1}-paragraph-{paragraph_index + 1}",
                text=value,
                include_in_training=include,
                exclusion_reason=reason,
            )
            for paragraph_index, value in enumerate(paragraph_values)
            if value
        ]
        body = "\n\n".join(paragraph.text for paragraph in paragraphs)
        sections.append(
            ScientificSection(
                section_id=f"section-{index + 1}",
                level=max(1, min(level, 6)),
                title=title,
                text=body,
                role=role,
                include_in_training=include,
                exclusion_reason=reason,
                word_count=_word_count(body),
                paragraphs=paragraphs,
            )
        )
    return _exclude_front_matter(sections)


def _exclude_front_matter(sections: list[ScientificSection]) -> list[ScientificSection]:
    """Keep the pre-abstract/title-page region out of every training projection.

    Layout extraction can promote author names and affiliations to headings.
    Document order, rather than the heading's guessed semantic role, defines
    front matter when an explicit Abstract or Introduction boundary is present.
    Unstructured documents without either boundary are left unchanged.
    """
    abstract = next((i for i, section in enumerate(sections) if section.role == "abstract"), None)
    boundary = (
        abstract
        if abstract is not None
        else next((i for i, section in enumerate(sections) if section.role == "introduction"), None)
    )
    if boundary is None:
        return sections
    reason = _section_exclusion_reason("metadata", "Front matter")
    return [
        section.model_copy(
            update={
                "role": "metadata",
                "include_in_training": False,
                "exclusion_reason": reason,
                "paragraphs": [
                    paragraph.model_copy(
                        update={"include_in_training": False, "exclusion_reason": reason}
                    )
                    for paragraph in section.paragraphs
                ],
            }
        )
        if index < boundary
        else section
        for index, section in enumerate(sections)
    ]


def _pdf_text_headings(text: str) -> str:
    """Expose explicit PDF text headings while preserving ordinary prose lines."""
    lines: list[str] = []
    for raw in text.splitlines():
        value = raw.strip()
        abstract = re.fullmatch(r"(?i:abstract)(?:[.:\-\u2013\u2014]\s*|$)(.*)", value)
        if abstract:
            lines.extend(["", "## Abstract", "", abstract.group(1)])
        elif re.fullmatch(
            r"(?i)(?:(?:\d+(?:\.\d+)*|[IVX]+)[.\s]+)?"
            r"(?:introduction|background|related work|methods?|results?|discussion|"
            r"conclusions?|limitations|references|bibliography|acknowledg(?:e)?ments)[.:]?",
            value,
        ):
            lines.extend(["", f"## {value}", ""])
        else:
            lines.append(raw)
    return "\n".join(lines)


def _text_equations(text: str) -> list[ScientificEquation]:
    values: list[str] = []
    patterns = (
        r"\$\$(.+?)\$\$",
        r"\\\[(.+?)\\\]",
        r"\\begin\{equation\*?\}(.+?)\\end\{equation\*?\}",
    )
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.DOTALL):
            value = match.group(1).strip()
            if value and value not in values:
                values.append(value)
    return [
        ScientificEquation(equation_id=f"equation-{index + 1}", latex=value, display=True)
        for index, value in enumerate(values[:128])
    ]


_HEADING_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
_TEXT_TAGS = {"p", "li", "blockquote", "pre"}
_SECTION_PREFIX = re.compile(r"^(?:appendix\s+)?[A-Z]?\d+(?:\.\d+)*\.?\s+", re.IGNORECASE)


def _extract_sections(root: Any, *, document_title: str | None) -> list[ScientificSection]:
    """Assign document-order paragraphs to the nearest preceding heading."""
    sections: list[ScientificSection] = []
    current_title = "Document body"
    current_level = 2
    # Native arXiv HTML places logos, affiliations, contribution notes, and
    # project links between the document title and the Abstract heading. Keep
    # that front matter in the structured artifact, but never treat it as body
    # text merely because it precedes the first section heading.
    current_role: SectionRole = "metadata"
    current_id = "section-body"
    paragraph_values: list[tuple[str, str]] = []

    def flush() -> None:
        nonlocal paragraph_values
        values = [(pid, value) for pid, value in paragraph_values if value]
        paragraph_values = []
        if not values:
            return
        reason = _section_exclusion_reason(current_role, current_title)
        include = reason is None
        paragraphs = [
            ScientificParagraph(
                paragraph_id=pid,
                text=value,
                include_in_training=include,
                exclusion_reason=reason,
            )
            for pid, value in values
        ]
        text = "\n".join(value for _, value in values)
        sections.append(
            ScientificSection(
                section_id=current_id,
                level=current_level,
                title=current_title,
                text=text,
                role=current_role,
                include_in_training=include,
                exclusion_reason=reason,
                word_count=_word_count(text),
                paragraphs=paragraphs,
            )
        )

    for node in root.find_all([*_HEADING_TAGS, *_TEXT_TAGS]):
        name = str(getattr(node, "name", ""))
        if name in _HEADING_TAGS:
            heading_text = _clean(node.get_text(" ", strip=True))
            classes = {str(value) for value in (node.get("class") or [])}
            is_document_title = "ltx_title_document" in classes or (
                name == "h1" and document_title and heading_text == document_title
            )
            if is_document_title:
                continue
            flush()
            current_title = heading_text or f"Section {len(sections) + 1}"
            current_level = int(name[1])
            current_role = _section_role(current_title, classes)
            current_id = str(node.get("id") or f"section-{len(sections) + 1}")
            continue
        if node.find_parent(["figure", "table", "nav", "footer"]):
            continue
        # Keep the outermost prose container once. arXiv list items often
        # wrap a paragraph, and visiting both nodes duplicates every bullet.
        if node.find_parent(list(_TEXT_TAGS)) is not None:
            continue
        classes = {str(value) for value in (node.get("class") or [])}
        if classes & {"ltx_authors", "ltx_author_notes", "ltx_note", "ltx_pagination"}:
            continue
        value = _clean(node.get_text(" ", strip=True))
        if not value:
            continue
        paragraph_id = str(node.get("id") or f"{current_id}-paragraph-{len(paragraph_values) + 1}")
        paragraph_values.append((paragraph_id, value))
    flush()
    # Some non-arXiv scientific HTML has no headings at all. In that case the
    # sole text block is the only available body rather than identifiable
    # front matter, so retain it instead of producing an empty document.
    if len(sections) == 1 and sections[0].section_id == "section-body":
        sections[0] = sections[0].model_copy(
            update={
                "role": "other",
                "include_in_training": True,
                "exclusion_reason": None,
                "paragraphs": [
                    paragraph.model_copy(
                        update={"include_in_training": True, "exclusion_reason": None}
                    )
                    for paragraph in sections[0].paragraphs
                ],
            }
        )
    return sections


def _section_role(title: str, classes: set[str] | None = None) -> SectionRole:
    normalized = _SECTION_PREFIX.sub("", title).lower().strip(" .:-")
    classes = classes or set()
    if "ltx_title_abstract" in classes or normalized == "abstract":
        return "abstract"
    if "bibliography" in " ".join(classes) or re.search(
        r"\b(references|bibliography)\b", normalized
    ):
        return "references"
    if "appendix" in " ".join(classes) or normalized.startswith("appendix"):
        return "appendix"
    if re.search(
        r"acknowledg|author contribution|funding|conflict of interest|declaration", normalized
    ):
        return "acknowledgements"
    if "introduction" in normalized:
        return "introduction"
    if re.search(r"background|related work|preliminar", normalized):
        return "background"
    if re.search(r"method|approach|experimental setup|materials|implementation", normalized):
        return "methods"
    if re.search(r"result|evaluation|experiment|analysis|finding", normalized):
        return "results"
    if "discussion" in normalized:
        return "discussion"
    if re.search(r"conclusion|summary|future work", normalized):
        return "conclusion"
    if re.search(r"limitation|threats to validity", normalized):
        return "limitations"
    return "other"


def _section_exclusion_reason(role: SectionRole, title: str) -> str | None:
    if role == "metadata":
        return "front matter and author metadata retained for provenance"
    if role == "references":
        return "bibliography retained for provenance, excluded from training"
    if role == "acknowledgements":
        return "non-scientific personal and funding metadata"
    normalized = title.lower()
    if re.search(
        r"ethics statement|author contribution|conflict of interest|declaration", normalized
    ):
        return "administrative metadata"
    return None


def _is_data_table(table: Any) -> bool:
    """Separate semantic tabular data from LaTeXML equation layout tables."""
    classes = {str(value) for value in (table.get("class") or [])}
    if classes & {"ltx_equation", "ltx_equationgroup", "ltx_eqn_table"}:
        return False
    if classes & {"ltx_tabular", "ltx_table"}:
        return True
    # Preserve ordinary standards-based HTML tables from non-LaTeXML sources.
    return not any(value.startswith("ltx_") for value in classes)


def _document_title(root: Any) -> str:
    node = root.select_one("h1.ltx_title_document") or root.find("h1")
    return _clean(node.get_text(" ", strip=True)) if node else ""


def _extract_author_metadata(root: Any) -> list[str]:
    metadata: list[str] = []
    for node in root.select(".ltx_creator.ltx_role_author"):
        value = _clean(node.get_text(" ", strip=True))
        if value and value not in metadata:
            metadata.append(value)
    return metadata[:256]


def _extract_authors(author_metadata: list[str]) -> list[str]:
    authors: list[str] = []
    for metadata in author_metadata:
        # Notes frequently contain affiliations and email addresses nested
        # inside the author span. Keep only the creator name in this field.
        value = re.split(r"\b(?:Affiliation|Email)\s*:", metadata, maxsplit=1)[0].strip()
        if value and value not in authors:
            authors.append(value)
    return authors[:256]


def _source_identifier(root: Any, source_url: str) -> str | None:
    # The canonical source URL identifies the paper. Searching the body first
    # can accidentally select an arXiv id from the bibliography and corrupt
    # paper-family grouping, split allocation, and audit provenance.
    url_match = re.search(r"/(?:html|pdf)/([^?#]+)", source_url)
    if url_match:
        return url_match.group(1)
    match = re.search(
        r"arXiv:\s*([a-z-]+/)?\d{4}\.\d{4,6}(?:v\d+)?",
        root.get_text(" ", strip=True),
        re.IGNORECASE,
    )
    if match:
        return match.group(0).split(":", 1)[1].strip()
    return None


def _publication_date(root: Any) -> str | None:
    node = root.select_one(".ltx_dates")
    return _clean(node.get_text(" ", strip=True)) if node else None


def _license_text(root: Any) -> str | None:
    for node in root.find_all("a", href=True):
        value = _clean(node.get_text(" ", strip=True))
        if value.lower().startswith("license:"):
            return value.split(":", 1)[1].strip() or str(node.get("href"))
    return None


def _word_count(value: str) -> int:
    return sum(1 for _ in re.finditer(r"\b\w+\b", value, flags=re.UNICODE))


def _clean(value: str) -> str:
    return _SPACE.sub(" ", value).strip()


def _infer_pdf_title(markdown: str) -> str | None:
    """Recover a bounded title when Docling emits it as ordinary text."""
    for raw_line in markdown.splitlines():
        candidate = re.sub(r"^#{1,6}\s+", "", raw_line.strip())
        candidate = re.sub(r"^[*_]{1,3}|[*_]{1,3}$", "", candidate).strip()
        candidate = _clean(candidate)
        if not 8 <= len(candidate) <= 300:
            continue
        lowered = candidate.casefold()
        if lowered.startswith(("http://", "https://", "arxiv:")):
            continue
        if re.fullmatch(r"(?:page\s+)?\d+", lowered):
            continue
        return candidate
    return None


def _extension_for_mime(mime: str) -> str:
    return {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp",
        "image/gif": "gif",
        "image/tiff": "tiff",
    }.get(mime, "img")


def _docling_provenance(
    item: object,
) -> tuple[int | None, tuple[float, float, float, float] | None]:
    """Read a Docling item's first page and bounding box without coupling schemas."""
    provenance = getattr(item, "prov", None) or []
    if not provenance:
        return None, None
    first = provenance[0]
    page_number = getattr(first, "page_no", None)
    box = getattr(first, "bbox", None)
    if box is None:
        return int(page_number) if page_number else None, None
    coordinates = (
        float(box.l),
        float(box.t),
        float(box.r),
        float(box.b),
    )
    return int(page_number) if page_number else None, coordinates
