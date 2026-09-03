"""The section/input contract shared by Luna labeling and deployed classifiers."""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

_HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")


@dataclass(frozen=True)
class Section:
    section_id: str
    title: str
    section_type: str
    text: str


def section_type(title: str, *, source: str) -> str:
    value = title.casefold()
    candidates: Sequence[tuple[str, Sequence[str]]]
    if source == "arxiv-html-fetcher":
        candidates = (
            ("abstract", ("abstract",)),
            ("introduction", ("introduction", "motivation")),
            ("background", ("background", "related work", "preliminar")),
            ("methods", ("method", "approach", "architecture", "algorithm", "model")),
            ("results", ("result", "experiment", "evaluation", "ablation", "analysis")),
            ("discussion", ("discussion",)),
            ("limitations", ("limitation", "ethic", "broader impact")),
            ("conclusion", ("conclusion",)),
            ("appendix", ("appendix", "supplement")),
        )
    else:
        candidates = (
            ("summary", ("summary", "description", "overview", "model card", "dataset card")),
            ("architecture", ("architecture", "model details")),
            ("training", ("training", "fine-tun")),
            ("evaluation", ("evaluation", "results", "benchmark", "performance")),
            ("usage", ("usage", "use", "inference", "how to")),
            ("data", ("dataset structure", "data fields", "data instances", "collection")),
            ("limitations", ("limitation", "bias", "risk", "out-of-scope")),
        )
    for role, markers in candidates:
        if any(marker in value for marker in markers):
            return role
    return "other"


def parse_sections(text: str, *, source: str) -> tuple[str | None, list[Section]]:
    """Exactly the Markdown segmentation used for the 7,999 labeled documents."""
    title: str | None = None
    sections: list[Section] = []
    current_title = "Document body"
    current_lines: list[str] = []

    def flush() -> None:
        body = "\n".join(current_lines).strip()
        if body:
            sections.append(
                Section(
                    section_id=f"section-{len(sections) + 1}",
                    title=current_title,
                    section_type=section_type(current_title, source=source),
                    text=body,
                )
            )

    for line in text.splitlines():
        match = _HEADING.match(line)
        if not match:
            current_lines.append(line)
            continue
        heading = match.group(2).strip()
        if len(match.group(1)) == 1 and title is None and not current_lines and not sections:
            title = heading
            current_title = heading
            continue
        flush()
        current_lines = []
        current_title = heading
    flush()
    if not sections and text.strip():
        sections.append(Section("section-1", title or "Document body", "other", text.strip()))
    return title, sections


def model_input(section: Section, *, source: str) -> str:
    # Matches the labeling builder; other live training-text sources are HF.
    family = "arxiv" if source == "arxiv-html-fetcher" else "hf"
    return (
        f"[SOURCE={family}] [SECTION_TYPE={section.section_type}] "
        f"[SECTION_TITLE={section.title}]\n{section.text}"
    )
