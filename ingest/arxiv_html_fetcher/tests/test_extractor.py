"""Deterministic golden-output tests for the extractor.

The samples here are hand-crafted to exercise the math-preservation rules
and the metadata recovery path. They are intentionally small so a frozen
output assertion stays readable - we only assert on the substrings that
matter, not on Resiliparse's exact whitespace, because Resiliparse's
output formatting is not part of our public contract.
"""

from __future__ import annotations

from ingest.arxiv_html_fetcher.extractor import (
    AR5IV_PIPELINE,
    ARXIV_PIPELINE,
    extract_arxiv_html,
    map_license_url,
    parse_submission_date,
    preserve_math,
)

ARXIV_NATIVE = """<!DOCTYPE html>
<html><head>
<title>Streaming Scientific Curation</title>
<meta name="citation_publication_date" content="2026/05/12">
<meta name="citation_license" content="https://creativecommons.org/licenses/by/4.0/">
</head><body>
<article>
<h1>Streaming Scientific Curation</h1>
<h2>1. Introduction</h2>
<p>We propose a streaming curation pipeline for fresh pretraining data.</p>
<math display="block"><annotation encoding="application/x-tex">\\mathcal{L} = -\\sum_i p_i \\log q_i</annotation></math>
<h2>2. Method</h2>
<p>The inline rate <math display="inline"><annotation encoding="application/x-tex">\\lambda</annotation></math> tracks per-snapshot lag.</p>
<h3>2.1 Validity intervals</h3>
<p>Each document carries a <code>valid_from</code> and <code>valid_to</code> column.</p>
</article>
</body></html>
"""

AR5IV_LEGACY = """<!DOCTYPE html>
<html><head>
<title>Older Paper</title>
<meta name="citation_publication_date" content="2022-04-01">
</head><body>
<div class="ltx_page_main">
<h1>Older Paper</h1>
<h2>Background</h2>
<p>Body of the paper.</p>
</div>
</body></html>
"""

NO_META = """<html><body><article><p>No metadata at all.</p></article></body></html>"""


def test_preserve_math_replaces_block_math_with_dollar_dollar() -> None:
    src = '<math display="block"><annotation encoding="application/x-tex">x+y</annotation></math>'
    out = preserve_math(src)
    assert "$$" in out
    assert "x+y" in out


def test_preserve_math_replaces_inline_math_with_single_dollar() -> None:
    src = '<math display="inline"><annotation encoding="application/x-tex">z</annotation></math>'
    out = preserve_math(src)
    assert "$z$" in out


def test_map_license_url_resolves_creative_commons_to_spdx() -> None:
    assert map_license_url("https://creativecommons.org/licenses/by/4.0/") == "CC-BY-4.0"
    assert (
        map_license_url("http://arxiv.org/licenses/nonexclusive-distrib/1.0/")
        == "arxiv-non-exclusive-distribution"
    )
    assert map_license_url("") is None
    # Unknown URLs pass through verbatim so curate can decide.
    assert map_license_url("https://example.com/lic") == "https://example.com/lic"


def test_parse_submission_date_handles_known_arxiv_formats() -> None:
    a = parse_submission_date("2026/05/12")
    assert a is not None
    assert (a.year, a.month, a.day) == (2026, 5, 12)
    b = parse_submission_date("2022-04-01")
    assert b is not None
    assert (b.year, b.month, b.day) == (2022, 4, 1)
    assert parse_submission_date(None) is None
    assert parse_submission_date("") is None
    assert parse_submission_date("garbage") is None


def test_extract_arxiv_html_parses_metadata_and_math() -> None:
    doc = extract_arxiv_html(ARXIV_NATIVE)
    assert doc.extraction_pipeline == ARXIV_PIPELINE
    assert doc.title == "Streaming Scientific Curation"
    assert doc.spdx_license == "CC-BY-4.0"
    assert doc.submission_date is not None
    assert doc.submission_date.year == 2026
    # The display-math block must appear with $$ delimiters in the body.
    assert "$$" in doc.text
    assert "\\mathcal{L}" in doc.text
    # Inline math must keep the lambda token wrapped in single dollars.
    assert "$\\lambda$" in doc.text
    # Headings must be captured with their original level prefix.
    assert any(h.startswith("# Streaming Scientific Curation") for h in doc.headings)
    assert any(h.startswith("## 1. Introduction") for h in doc.headings)
    assert any(h.startswith("### 2.1 Validity intervals") for h in doc.headings)
    assert doc.word_count > 0


def test_extract_arxiv_html_supports_ar5iv_layout_when_pipeline_overridden() -> None:
    doc = extract_arxiv_html(AR5IV_LEGACY, pipeline=AR5IV_PIPELINE)
    assert doc.extraction_pipeline == AR5IV_PIPELINE
    assert doc.title == "Older Paper"
    assert doc.spdx_license is None  # No license meta on the older mirror.
    assert "Body of the paper." in doc.text
    assert doc.submission_date is not None
    assert doc.submission_date.year == 2022


def test_extract_arxiv_html_handles_documents_without_metadata() -> None:
    doc = extract_arxiv_html(NO_META)
    assert doc.title is None
    assert doc.spdx_license is None
    assert doc.submission_date is None
    assert "No metadata at all." in doc.text


def test_extract_arxiv_html_accepts_bytes_input() -> None:
    doc = extract_arxiv_html(ARXIV_NATIVE.encode("utf-8"))
    assert doc.title == "Streaming Scientific Curation"
    assert "$$" in doc.text
