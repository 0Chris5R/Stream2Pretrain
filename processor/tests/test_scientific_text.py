"""Tests for native Markdown and LaTeX scientific structure extraction."""

from __future__ import annotations

from processor.scientific import ScientificProcessor, extract_scientific_text


class _FakeS3:
    def __init__(self) -> None:
        self.objects: list[dict[str, object]] = []

    def put_object(self, **kwargs: object) -> None:
        self.objects.append(kwargs)


def test_markdown_sections_exclude_front_matter_and_references() -> None:
    text = """# A Structured OCR Paper

Ada Researcher, Example University

## Abstract

We study a reproducible data pipeline.

## 2 Methods

We compare controlled variants with the objective $$L = x + y$$.

## References

Researcher, A. Prior work.
"""
    document = extract_scientific_text(
        doc_id="sha256:" + "a" * 64,
        source_url="https://example.invalid/paper",
        text=text,
        title="paper",
        source_format="markdown",
        extraction_pipeline="source-ocr-markdown-v1",
    )

    assert document.title == "A Structured OCR Paper"
    assert [section.role for section in document.sections] == [
        "metadata",
        "abstract",
        "methods",
        "references",
    ]
    assert document.abstract == "We study a reproducible data pipeline."
    assert document.equations[0].latex == "L = x + y"
    assert document.citations[0].text == "Researcher, A. Prior work."
    projection = document.training_text_projection()
    assert "controlled variants" in projection
    assert "Example University" not in projection
    assert "Prior work" not in projection


def test_latex_sections_and_metadata_are_structured() -> None:
    text = r"""
\documentclass{article}
\title{A LaTeX Scientific Paper}
\author{Ada Researcher \and Grace Scientist}
\begin{document}
\maketitle
\begin{abstract}
We evaluate a distributed training pipeline.
\end{abstract}
\section{Methods}
We use a deterministic protocol.
\begin{equation}
z = x + y
\end{equation}
\section{Acknowledgements}
We thank the funding body.
\section{References}
Prior work appears here.
\end{document}
"""
    document = extract_scientific_text(
        doc_id="sha256:" + "b" * 64,
        source_url="https://example.invalid/paper.tex",
        text=text,
        title=None,
        source_format="latex",
        extraction_pipeline="latex-source-v1",
    )

    assert document.title == "A LaTeX Scientific Paper"
    assert document.authors == ["Ada Researcher", "Grace Scientist"]
    assert {section.role for section in document.sections} >= {
        "abstract",
        "methods",
        "acknowledgements",
        "references",
    }
    assert document.equations[0].latex == "z = x + y"
    projection = document.training_text_projection()
    assert "deterministic protocol" in projection
    assert "funding body" not in projection
    assert "Prior work" not in projection


def test_headingless_markdown_remains_usable_body_text() -> None:
    document = extract_scientific_text(
        doc_id="sha256:" + "c" * 64,
        source_url="https://example.invalid/paper",
        text="A substantive scientific body without explicit headings.",
        title="Headingless paper",
        source_format="markdown",
        extraction_pipeline="markdown-v1",
    )

    assert len(document.sections) == 1
    assert document.sections[0].include_in_training is True
    assert "substantive scientific body" in document.training_text_projection()


def test_processor_persists_native_text_artifact(tmp_path: object) -> None:
    s3 = _FakeS3()
    processor = ScientificProcessor(
        s3_client=s3,
        bucket="silver",
        models_dir=str(tmp_path),
        user_agent="test",
        require_real_models=False,
    )

    result = processor.process_text(
        doc_id="sha256:" + "d" * 64,
        source_url="https://example.invalid/paper",
        text="# Paper title\n\n## Methods\n\nA reproducible protocol.",
        title=None,
        source_format="markdown",
        extraction_pipeline="markdown-v1",
    )

    assert result.artifact_s3_uri.endswith("/document.json")
    assert result.document.training_word_count > 0
    assert s3.objects[0]["Key"] == f"scientific/{'d' * 64}/document.json"
