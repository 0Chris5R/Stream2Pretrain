"""Deterministic rejection of publication templates masquerading as papers.

This is intentionally narrow. Short papers, position papers, and demo papers
remain valid; a record is blocked only when multiple literal starter/template
signals occur in the retained scientific body.
"""

from __future__ import annotations

import re

from schemas.silver import SilverSegment

_TITLE_TEMPLATE = re.compile(
    r"\b(?:bare demo of|conference paper template|journal paper template|sample paper)\b",
    re.IGNORECASE,
)
_BODY_TEMPLATE_SIGNALS = (
    "starter file",
    "subsection text here",
    "subsubsection text here",
    "the conclusion goes here",
    "replace this text",
    "ieeetran.cls",
    "template instructions",
)


def is_publication_template(*, title: str | None, text: str, segments: list[SilverSegment]) -> bool:
    """Return true only for strongly evidenced authoring templates."""
    normalized = " ".join(text.casefold().split())
    title_signal = bool(_TITLE_TEMPLATE.search(title or ""))
    literal_hits = sum(signal in normalized for signal in _BODY_TEMPLATE_SIGNALS)
    placeholder_sections = sum(
        any(
            signal in " ".join(f"{segment.title} {segment.text}".casefold().split())
            for signal in (
                "subsection text here",
                "subsubsection text here",
                "the conclusion goes here",
            )
        )
        for segment in segments
    )
    return (title_signal and literal_hits >= 1) or literal_hits >= 2 or placeholder_sections >= 2


__all__ = ["is_publication_template"]
