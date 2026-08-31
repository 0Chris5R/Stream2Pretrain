"""PDF fallback coverage independent of optional HTML-extraction packages."""

from __future__ import annotations

import io

import pytest

from processor.scientific import (
    DoclingDocumentConversionError,
    PdfExceedsDoclingLimitError,
    ScientificProcessor,
)


class _FakeS3:
    def put_object(self, **_kwargs: object) -> None:
        return None


def test_pdf_processing_uses_bounded_fallback_when_docling_is_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pypdf = pytest.importorskip("pypdf")
    payload = io.BytesIO()
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.add_metadata({"/Title": "Fallback Paper"})
    writer.write(payload)
    monkeypatch.setenv("S2P_DOCLING_ENABLED", "0")
    processor = ScientificProcessor(
        s3_client=_FakeS3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )

    result = processor.process_pdf(
        doc_id="sha256:" + "a" * 64,
        source_url="https://example.invalid/paper.pdf",
        pdf=payload.getvalue(),
        extraction_pipeline="test-pdf",
    )

    assert result.document.title == "Fallback Paper"
    assert "docling_disabled:pypdf" in result.document.warnings
    assert result.document.extraction_pipeline == "test-pdf+pypdf"


def test_oversized_pdf_is_rejected_before_docling_or_lossy_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_DOCLING_ENABLED", "1")
    monkeypatch.setenv("S2P_DOCLING_MAX_BYTES", "8")
    processor = ScientificProcessor(
        s3_client=_FakeS3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )

    with pytest.raises(PdfExceedsDoclingLimitError) as error:
        processor.process_pdf(
            doc_id="sha256:" + "b" * 64,
            source_url="https://example.invalid/oversized.pdf",
            pdf=b"%PDF-1.7\noversized",
            extraction_pipeline="test-pdf",
        )

    assert error.value.actual_bytes == len(b"%PDF-1.7\noversized")
    assert error.value.limit_bytes == 8


def test_pdf_at_exact_docling_limit_remains_eligible_for_normal_processing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pypdf = pytest.importorskip("pypdf")
    payload = io.BytesIO()
    writer = pypdf.PdfWriter()
    writer.add_blank_page(width=72, height=72)
    writer.write(payload)
    pdf = payload.getvalue()
    monkeypatch.setenv("S2P_DOCLING_ENABLED", "0")
    monkeypatch.setenv("S2P_DOCLING_MAX_BYTES", str(len(pdf)))
    processor = ScientificProcessor(
        s3_client=_FakeS3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )

    result = processor.process_pdf(
        doc_id="sha256:" + "c" * 64,
        source_url="https://example.invalid/exact-limit.pdf",
        pdf=pdf,
        extraction_pipeline="test-pdf",
    )

    assert result.document.extraction_pipeline == "test-pdf+pypdf"


def test_docling_document_conversion_error_is_record_local(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conversion_error_type = type(
        "ConversionError", (Exception,), {"__module__": "docling.exceptions"}
    )
    monkeypatch.setenv("S2P_DOCLING_ENABLED", "1")
    processor = ScientificProcessor(
        s3_client=_FakeS3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )

    def reject_document(**_kwargs: object) -> object:
        raise conversion_error_type("document conversion failed conclusively")

    monkeypatch.setattr(processor, "_process_pdf_docling", reject_document)

    with pytest.raises(DoclingDocumentConversionError) as error:
        processor.process_pdf(
            doc_id="sha256:" + "d" * 64,
            source_url="https://example.invalid/invalid.pdf",
            pdf=b"%PDF-1.7\ninvalid",
            extraction_pipeline="test-pdf",
        )

    assert isinstance(error.value, ValueError)
    assert error.value.__cause__ is not None


def test_non_positive_docling_byte_limit_fails_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("S2P_DOCLING_MAX_BYTES", "0")

    with pytest.raises(RuntimeError, match="S2P_DOCLING_MAX_BYTES must be positive"):
        ScientificProcessor(
            s3_client=_FakeS3(),
            bucket="silver",
            models_dir="/tmp/models",
            user_agent="test",
            require_real_models=False,
        )
