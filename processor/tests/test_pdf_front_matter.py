"""PDF projection contracts using layout records without loading inference models."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace

import pytest

from processor.scientific import ScientificProcessor, extract_scientific_text


class _S3:
    def put_object(self, **_kwargs: object) -> None:
        pass


def _processor() -> ScientificProcessor:
    return ScientificProcessor(
        s3_client=_S3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )


@pytest.mark.parametrize("abstract_style", ["heading", "inline", "text-label", "no-abstract"])
def test_docling_author_heading_is_audit_only(
    monkeypatch: pytest.MonkeyPatch, abstract_style: str
) -> None:
    class TextItem:
        def __init__(self, text: str, label: str = "text") -> None:
            self.text = text
            self.label = label

    class TitleItem(TextItem):
        pass

    class SectionHeaderItem(TextItem):
        level = 2

    class FormulaItem(TextItem):
        pass

    module = ModuleType("docling_core.types.doc")
    for item_type in (TextItem, TitleItem, SectionHeaderItem, FormulaItem):
        setattr(module, item_type.__name__, item_type)
    module.PictureItem = type("PictureItem", (), {})
    module.TableItem = type("TableItem", (), {})
    base = ModuleType("docling.datamodel.base_models")
    base.DocumentStream = SimpleNamespace
    monkeypatch.setitem(sys.modules, "docling_core.types.doc", module)
    monkeypatch.setitem(sys.modules, "docling.datamodel.base_models", base)

    items = [
        TitleItem("A Scientific Study"),
        SectionHeaderItem("Ada Researcher, Grace Scientist"),
        TextItem("Example University, Department of Computing"),
        TextItem("Contact: ada@example.invalid"),
    ]
    if abstract_style == "heading":
        items.extend([SectionHeaderItem("Abstract"), TextItem("We derive a scaling relation.")])
    elif abstract_style == "inline":
        items.append(TextItem("Abstract. We derive a scaling relation."))
    elif abstract_style == "text-label":
        items.extend([TextItem("Abstract"), TextItem("We derive a scaling relation.")])
    items.extend(
        [
            SectionHeaderItem("1 Introduction"),
            TextItem("The problem concerns scientific inference."),
            SectionHeaderItem("2 Methods"),
            TextItem("Abstract interpretation remains a substantive technical method."),
            FormulaItem("x + y"),
            SectionHeaderItem("References"),
            TextItem("An excluded bibliography entry."),
        ]
    )
    document = SimpleNamespace(
        iterate_items=lambda: [(item, 0) for item in items],
        export_to_dict=lambda: {},
        export_to_markdown=lambda **_: "# A Scientific Study",
        tables=[],
        pictures=[],
    )
    processor = _processor()
    processor._docling_converter = SimpleNamespace(
        convert=lambda *_args, **_kwargs: SimpleNamespace(document=document)
    )
    result = processor._process_pdf_docling(
        doc_id="sha256:" + "a" * 64,
        source_url="https://example.invalid/paper.pdf",
        pdf=b"layout fixture",
        extraction_pipeline="pdf-docling",
    )

    for projected in (result.text, result.model_text):
        assert "Ada Researcher" not in projected
        assert "Example University" not in projected
        assert "ada@example.invalid" not in projected
        assert "excluded bibliography" not in projected
        assert "scientific inference" in projected
        assert "Abstract interpretation" in projected
    front = result.document.sections[0]
    assert front.title == "Ada Researcher, Grace Scientist"
    assert front.role == "metadata"
    assert not front.include_in_training
    assert all(not paragraph.include_in_training for paragraph in front.paragraphs)
    assert result.document.equations[0].latex == "x + y"
    assert "x + y" in result.text
    if abstract_style != "no-abstract":
        assert "We derive a scaling relation." in result.text


def test_pdf_text_fallback_excludes_author_prefix_and_references(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = ModuleType("pypdf")
    module.PdfReader = lambda *_args, **_kwargs: SimpleNamespace(
        metadata=SimpleNamespace(title="A Scientific Study"),
        pages=[
            SimpleNamespace(
                extract_text=lambda: (
                    "A Scientific Study\nAda Researcher\nExample University\n"
                    "Abstract. We derive a scaling relation.\n\n"
                    "1 Introduction\nThe scientific problem.\n"
                )
            ),
            SimpleNamespace(
                extract_text=lambda: "2 Methods\nA valid derivation.\nReferences\nPrior work."
            ),
        ],
    )
    monkeypatch.setitem(sys.modules, "pypdf", module)
    result = _processor()._process_pdf_fallback(
        doc_id="sha256:" + "b" * 64,
        source_url="https://example.invalid/paper.pdf",
        pdf=b"text fixture",
        extraction_pipeline="pdf",
        warning="docling_disabled:pypdf",
    )
    assert "Ada Researcher" not in result.text
    assert "Example University" not in result.model_text
    assert "Prior work" not in result.text
    assert "We derive a scaling relation." in result.text
    assert "A valid derivation." in result.text


def test_markdown_author_heading_is_excluded_without_removing_body() -> None:
    document = extract_scientific_text(
        doc_id="sha256:" + "c" * 64,
        source_url="https://example.invalid/paper",
        title="A Scientific Study",
        text=(
            "# A Scientific Study\n\n## Ada Researcher\n\nExample University\n\n"
            "## Abstract\n\nWe derive a scaling relation.\n\n"
            "## Methods\n\nKeep this derivation."
        ),
        source_format="markdown",
        extraction_pipeline="markdown",
    )
    assert "Ada Researcher" not in document.training_text_projection()
    assert "Example University" not in document.training_text_projection()
    assert "Keep this derivation" in document.training_text_projection()
