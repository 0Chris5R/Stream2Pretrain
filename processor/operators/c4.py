"""C4-style line-level filters.

Three rules from the original C4 paper (Raffel et al. 2020) plus the
``lorem ipsum`` boilerplate filter that DataTrove inherited from RefinedWeb:

- nopunc: drop documents whose fraction of lines ending in valid sentence
  punctuation is too low (text dumps with no real prose end up here).
- curly-brace: drop documents containing ``{`` or ``}`` (machine-generated
  config / source code that slips through the HTML extractor).
- lorem-ipsum: drop pages whose visible text is dominated by placeholder
  copy.

Each rule is exposed both as a free function (cheap, easy to unit-test) and
through the :class:`C4Filter` facade used by the curation dataflow.
"""

from __future__ import annotations

from dataclasses import dataclass

_VALID_SENTENCE_TERMINATORS: tuple[str, ...] = (".", "!", "?", '"', "\u201d", "\u2019", ")")
_LOREM_TOKENS: tuple[str, ...] = (
    "lorem ipsum",
    "dolor sit amet",
    "consectetur adipiscing elit",
)


def fraction_lines_with_punct(text: str) -> float:
    """Fraction of non-empty lines that end in sentence-style punctuation."""
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if not lines:
        return 0.0
    good = sum(1 for ln in lines if ln.endswith(_VALID_SENTENCE_TERMINATORS))
    return good / len(lines)


def has_curly_braces(text: str) -> bool:
    """True iff the document contains ``{`` or ``}``."""
    return "{" in text or "}" in text


def looks_like_lorem_ipsum(text: str) -> bool:
    """Detect actual placeholder copy without rejecting prose that discusses it.

    A short block containing a canonical phrase is almost certainly filler. In
    longer prose, require several occurrences so a scientific paper describing
    C4's own filters does not lose an otherwise useful section merely for
    mentioning ``lorem ipsum`` once.
    """
    haystack = text.lower()
    hits = sum(haystack.count(token) for token in _LOREM_TOKENS)
    if hits == 0:
        return False
    return len(text.split()) <= 40 or hits >= 3


@dataclass(frozen=True, slots=True)
class C4Stats:
    """Diagnostics surfaced via the silver record's ``tags`` block."""

    nopunc_pass: bool
    curly_brace_pass: bool
    lorem_ipsum_pass: bool
    fraction_lines_with_punct: float


class C4Filter:
    """Aggregator of the three C4-style rules."""

    def __init__(self, *, min_punct_fraction: float = 0.5) -> None:
        if not 0.0 <= min_punct_fraction <= 1.0:
            raise ValueError("min_punct_fraction must lie in [0, 1]")
        self._min_punct_fraction = min_punct_fraction

    def stats(self, text: str) -> C4Stats:
        """Compute every rule's outcome for ``text``."""
        frac = fraction_lines_with_punct(text)
        return C4Stats(
            nopunc_pass=frac >= self._min_punct_fraction,
            curly_brace_pass=not has_curly_braces(text),
            lorem_ipsum_pass=not looks_like_lorem_ipsum(text),
            fraction_lines_with_punct=frac,
        )

    def passes(self, text: str) -> bool:
        """Composite gate: every C4 rule must hold."""
        s = self.stats(text)
        return s.nopunc_pass and s.curly_brace_pass and s.lorem_ipsum_pass
