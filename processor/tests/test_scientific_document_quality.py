"""Tests for narrow scientific authoring-template rejection."""

from processor.operators.scientific_document_quality import is_publication_template
from schemas.silver import SilverSegment


def _segment(title: str, text: str) -> SilverSegment:
    return SilverSegment(
        segment_id=title.casefold().replace(" ", "-"),
        title=title,
        text=text,
        word_count=len(text.split()),
    )


def test_ieeetran_starter_is_rejected() -> None:
    segments = [
        _segment(
            "Introduction",
            "This starter file demonstrates IEEEtran.cls for a conference paper.",
        ),
        _segment("Subsection Heading Here", "Subsection text here."),
        _segment("Conclusion", "The conclusion goes here."),
    ]

    assert is_publication_template(
        title="Bare Demo of IEEEtran.cls for Conferences",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )


def test_short_real_demo_paper_is_preserved() -> None:
    segments = [
        _segment(
            "Abstract",
            "We demonstrate a quantum payment protocol and report measured fidelity of 0.94.",
        ),
        _segment(
            "Results",
            "The experiment used 18 optical trials and reproduced the predicted interference.",
        ),
    ]

    assert not is_publication_template(
        title="Demonstration of quantum-digital payments",
        text="\n".join(segment.text for segment in segments),
        segments=segments,
    )
