"""PDF fallback coverage independent of optional HTML-extraction packages."""

from __future__ import annotations

import io

import pytest

from processor.scientific import ScientificProcessor


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
