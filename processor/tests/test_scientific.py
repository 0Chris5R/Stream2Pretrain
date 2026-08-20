"""Tests for the body-only scientific-document projection."""

from __future__ import annotations

import io

import pytest

from processor.scientific import ScientificProcessor, _infer_pdf_title, extract_scientific_html
from schemas.scientific import ScientificDocument, ScientificFigure

pytest.importorskip("bs4")
pytest.importorskip("lxml")


class _FakeS3:
    def put_object(self, **_kwargs: object) -> None:
        return None


def test_html_projection_keeps_structure_but_excludes_metadata_and_references() -> None:
    html = b"""
    <html><body><article>
      <h1 class="ltx_title_document">A Principled Pipeline</h1>
      <div class="ltx_creator ltx_role_author">Ada Researcher Email: ada@example.invalid</div>
      <h6 class="ltx_title_abstract">Abstract</h6>
      <p id="abstract-p1">We study a reproducible scientific data pipeline.</p>
      <h2 id="method">2 Methods</h2>
      <p id="method-p1">We record every transformation and compare controlled variants.</p>
      <h2 id="results">3 Results</h2>
      <p id="results-p1">The measured pipeline retains the strongest evidence.</p>
      <figure><img src="figure.png" alt="Increasing score"/>
        <figcaption>Figure 1: Quality increases after curation.</figcaption></figure>
      <h2 id="ack">Acknowledgements</h2>
      <p>We thank a named collaborator and funding body.</p>
      <h2 id="bib">References</h2>
      <ol><li class="ltx_bibitem" id="bib-1">Researcher, A. Private reference.</li></ol>
    </article></body></html>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "a" * 64,
        source_url="https://arxiv.org/html/2608.00001",
        html=html,
        plain_text=html.decode(),
        title=None,
        extraction_pipeline="test-native-html",
    )

    roles = {section.role for section in document.sections}
    assert {"abstract", "methods", "results", "acknowledgements", "references"} <= roles
    assert document.authors == ["Ada Researcher"]
    assert document.author_metadata == ["Ada Researcher Email: ada@example.invalid"]
    assert document.included_section_count == 3
    assert document.excluded_section_count == 2
    assert document.citations[0].text == "Researcher, A. Private reference."

    projection = document.training_text_projection()
    assert "controlled variants" in projection
    assert "strongest evidence" in projection
    assert "[FIGURE]" in projection
    assert "Private reference" not in projection
    assert "named collaborator" not in projection
    assert "ada@example.invalid" not in projection


def test_source_url_id_wins_over_cited_arxiv_id_in_body() -> None:
    html = b"""
    <article>
      <h1 class="ltx_title_document">Identity-safe extraction</h1>
      <h6 class="ltx_title_abstract">Abstract</h6>
      <p>We extend arXiv:2106.05735 with a new controlled evaluation.</p>
      <h2>Methods</h2>
      <p>The experiment uses a reproducible protocol.</p>
    </article>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "f" * 64,
        source_url="https://arxiv.org/html/2112.10074",
        html=html,
        plain_text=html.decode(),
        title=None,
        extraction_pipeline="test-native-html",
    )

    assert document.source_identifier == "2112.10074"


def test_front_matter_before_abstract_is_excluded_from_training() -> None:
    html = b"""
    <article>
      <h1 class="ltx_title_document">A Clean Scientific Projection</h1>
      <div class="ltx_authors"><p>Researcher One, Researcher Two</p></div>
      <p>1]Example University 2]Example Lab</p>
      <p>Equal contribution. Project Website https://example.invalid</p>
      <h6 class="ltx_title_abstract">Abstract</h6>
      <p>The actual scientific abstract begins here.</p>
      <h2>1 Methods</h2>
      <p>The method uses a controlled comparison.</p>
    </article>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "d" * 64,
        source_url="https://arxiv.org/html/2608.00004",
        html=html,
        plain_text=html.decode(),
        title=None,
        extraction_pipeline="test-native-html",
    )

    front_matter = document.sections[0]
    assert front_matter.role == "metadata"
    assert front_matter.include_in_training is False
    assert "front matter" in (front_matter.exclusion_reason or "")
    projection = document.training_text_projection()
    assert "actual scientific abstract" in projection
    assert "controlled comparison" in projection
    assert "Example University" not in projection
    assert "Equal contribution" not in projection


def test_headingless_scientific_html_retains_its_only_body() -> None:
    html = b"""<article><p>A headingless but substantive scientific document body.</p></article>"""

    document = extract_scientific_html(
        doc_id="sha256:" + "e" * 64,
        source_url="https://example.invalid/paper",
        html=html,
        plain_text=html.decode(),
        title="A Headingless Paper",
        extraction_pipeline="test-html",
    )

    assert len(document.sections) == 1
    assert document.sections[0].role == "other"
    assert document.sections[0].include_in_training is True
    assert "substantive scientific document body" in document.training_text_projection()


def test_latexml_equation_tables_are_not_scientific_tables() -> None:
    html = b"""
    <article><h1>Table Semantics</h1><h2>Results</h2>
      <p>The result is supported by one equation and one data table.</p>
      <table class="ltx_equation ltx_eqn_table"><tr><td>x = 1</td></tr></table>
      <figure class="ltx_table"><figcaption>Table 1: Accuracy</figcaption>
        <table class="ltx_tabular"><tr><th>Method</th><th>Score</th></tr>
          <tr><td>Ours</td><td>0.91</td></tr></table>
      </figure>
    </article>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "6" * 64,
        source_url="https://arxiv.org/html/2608.00006",
        html=html,
        plain_text=html.decode(),
        title="Table Semantics",
        extraction_pipeline="test-native-html",
    )

    assert len(document.tables) == 1
    assert document.tables[0].caption == "Table 1: Accuracy"
    assert document.tables[0].rows == [["Method", "Score"], ["Ours", "0.91"]]
    projection = document.training_text_projection()
    assert projection.count("[TABLE]") == 1
    assert "x = 1" not in document.tables[0].rows[0]


def test_unwrapped_header_images_do_not_count_as_scientific_figures() -> None:
    html = b"""
    <article><h1>Figure Semantics</h1>
      <p><img src="project-logo.png" alt="Project logo"/></p>
      <h2>Results</h2><p>The comparison improves accuracy.</p>
      <figure><img src="result-plot.png" alt="Accuracy by method"/>
        <figcaption>Figure 1: Test accuracy.</figcaption></figure>
    </article>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "7" * 64,
        source_url="https://arxiv.org/html/2608.00007",
        html=html,
        plain_text=html.decode(),
        title="Figure Semantics",
        extraction_pipeline="test-native-html",
    )

    assert len(document.figures) == 1
    assert document.figures[0].source_url == "result-plot.png"
    assert document.figures[0].caption == "Figure 1: Test accuracy."


def test_paragraph_ids_are_stable_when_source_ids_exist() -> None:
    html = b"""
    <article><h1>Title</h1><h2 id="s1">Methods</h2>
    <p id="p-source">A method paragraph with a source identifier.</p></article>
    """
    document = extract_scientific_html(
        doc_id="sha256:" + "b" * 64,
        source_url="https://arxiv.org/html/2608.00002",
        html=html,
        plain_text=html.decode(),
        title="Title",
        extraction_pipeline="test-native-html",
    )

    assert document.sections[0].section_id == "s1"
    assert document.sections[0].paragraphs[0].paragraph_id == "p-source"


def test_nested_list_paragraph_and_math_are_emitted_once() -> None:
    html = b"""
    <article><h1>Title</h1><h2 id="methods">Methods</h2>
      <ul><li id="item"><p>The set is
        <math alttext="S"><semantics><mrow>S</mrow>
          <annotation encoding="application/x-tex">S = \\{n-k,n\\}</annotation>
        </semantics></math> for each shard.</p></li></ul>
      <p>A separate paragraph appears once.</p>
    </article>
    """

    document = extract_scientific_html(
        doc_id="sha256:" + "c" * 64,
        source_url="https://arxiv.org/html/2608.00003",
        html=html,
        plain_text=html.decode(),
        title="Title",
        extraction_pipeline="test-native-html",
    )

    text = document.sections[0].text
    assert text.count("The set is") == 1
    assert text.count("S = \\{n-k,n\\}") == 1
    assert text.count("A separate paragraph") == 1
    assert "[EQUATION]" not in document.structured_text_surrogate()


def test_pdf_title_fallback_uses_first_meaningful_markdown_line() -> None:
    markdown = "\n\n# A Principled Scientific Pipeline\n\nAda Researcher\n"

    assert _infer_pdf_title(markdown) == "A Principled Scientific Pipeline"


def test_pdf_title_fallback_rejects_only_metadata_lines() -> None:
    assert _infer_pdf_title("\nhttps://arxiv.org/pdf/1234.5678\n\n2\n") is None


def test_raw_ocr_is_audit_only_until_policy_accepts_it() -> None:
    figure = ScientificFigure(
        figure_id="figure-1",
        source_url="https://example.com/figure.png",
        caption="Accuracy by method",
        ocr_text="Method A 91.3 Method B 89.7",
        ocr_revision="tesseract-5",
    )
    document = ScientificDocument(
        doc_id="sha256:" + "f" * 64,
        source_url="https://example.com/paper",
        text_sha256="f" * 64,
        extraction_pipeline="test",
        figures=[figure],
    )

    assert "Accuracy by method" in document.structured_text_surrogate()
    assert "Method A 91.3" not in document.structured_text_surrogate()
    assert (
        "Method A 91.3"
        in document.model_copy(
            update={
                "figures": [
                    figure.model_copy(
                        update={
                            "ocr_training_eligible": True,
                            "ocr_quality_score": 0.99,
                            "ocr_policy_revision": "ocr-policy-test",
                        }
                    )
                ]
            }
        ).structured_text_surrogate()
    )


def test_oversized_decoded_figure_is_rejected_before_rgb_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image_module = pytest.importorskip("PIL.Image")
    source = image_module.new("RGB", (2, 2), color="white")
    payload = io.BytesIO()
    source.save(payload, format="PNG")
    monkeypatch.setattr(image_module, "MAX_IMAGE_PIXELS", 3)

    processor = ScientificProcessor(
        s3_client=_FakeS3(),
        bucket="silver",
        models_dir="/tmp/models",
        user_agent="test",
        require_real_models=False,
    )
    figure = ScientificFigure(figure_id="figure-1", source_url="data:image/png;base64,")

    with pytest.raises(image_module.DecompressionBombWarning):
        processor._enrich_image_payload(
            "sha256:" + "a" * 64,
            figure,
            payload.getvalue(),
            "image/png",
        )
