"""Unit tests for the read-only processing-failure classifier."""

from scripts.audit_processing_failures import classify_failure, inspect_markdown


def test_markdown_inspection_ignores_frontmatter_and_fenced_code() -> None:
    inspected = inspect_markdown(
        b"---\nlicense: apache-2.0\n---\n```python\nprint('only code')\n```\n"
    )

    assert inspected == {"has_prose": False, "first_heading_length": 0}


def test_markdown_inspection_reports_oversized_first_heading() -> None:
    heading = "x" * 2049
    inspected = inspect_markdown(f"# {heading}\n\nUseful prose.".encode())

    assert inspected == {"has_prose": True, "first_heading_length": 2049}


def test_classifies_expected_empty_hf_card_separately_from_parser_defect() -> None:
    failure = {"reason": "RawObjectEmpty"}
    record = {"source_feed": "hf-datasets"}

    assert classify_failure(failure, record, b"") == "intentional_empty_hf_card"
    assert (
        classify_failure(
            {"reason": "ValueError"},
            {"source_feed": "hf-models"},
            b"---\nlicense: mit\n---\n",
        )
        == "intentional_no_prose_hf_card"
    )


def test_classifies_oversized_hf_heading_as_projection_defect() -> None:
    heading = "x" * 2049

    assert (
        classify_failure(
            {"reason": "ValidationError"},
            {"source_feed": "hf-models"},
            f"# {heading}\n\nUseful prose.".encode(),
        )
        == "oversized_hf_card_title"
    )
