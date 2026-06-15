"""Gopher heuristic quality filters.

Implements the suite of stateless rules from "Scaling Language Models"
(DeepMind, Rae et al. 2021), which DataTrove and Dolma both use as their
default heuristic stage. Each rule is a small Python function returning a
boolean; the ``GopherFilter`` aggregator returns ``True`` only when every
configured rule passes.

The exact thresholds match the FineWeb / DCLM defaults so Stream2Pretrain
silver records are directly comparable to those corpora. Bytes-on-disk and
tokens-per-doc figures are recorded as ``needs-measurement`` because they
depend on the corpus and are reported in Week 5 benchmarks.
"""

from __future__ import annotations

from dataclasses import dataclass

# Common English stopwords - list deliberately short so the rule is cheap.
# Source: DeepMind Gopher paper appendix table.
_STOPWORDS: frozenset[str] = frozenset(
    "the be to of and a in that have I it for not on with he as you do at this "
    "but his by from they we say her she or an will my one all would there their "
    "what so up out if about who get which go me".split()
)
_BULLET_PREFIXES: tuple[str, ...] = ("•", "*", "-", "●", "▪", "‣", "⁃")


@dataclass(frozen=True, slots=True)
class GopherStats:
    """Diagnostic counters - useful for the UI's per-source telemetry."""

    word_count: int
    mean_word_len: float
    stopword_ratio: float
    bullet_line_ratio: float
    ellipsis_line_ratio: float
    symbol_word_ratio: float
    alpha_word_ratio: float


class GopherFilter:
    """Composable Gopher heuristic gate.

    Defaults match Rae et al. 2021 / FineWeb. Override individual thresholds
    via constructor kwargs - tests do this to exercise edge cases.
    """

    def __init__(
        self,
        *,
        min_words: int = 50,
        max_words: int = 100_000,
        min_mean_word_len: float = 3.0,
        max_mean_word_len: float = 10.0,
        max_symbol_word_ratio: float = 0.10,
        min_alpha_word_ratio: float = 0.80,
        min_stopword_count: int = 2,
        max_bullet_line_ratio: float = 0.90,
        max_ellipsis_line_ratio: float = 0.30,
    ) -> None:
        self.min_words = min_words
        self.max_words = max_words
        self.min_mean_word_len = min_mean_word_len
        self.max_mean_word_len = max_mean_word_len
        self.max_symbol_word_ratio = max_symbol_word_ratio
        self.min_alpha_word_ratio = min_alpha_word_ratio
        self.min_stopword_count = min_stopword_count
        self.max_bullet_line_ratio = max_bullet_line_ratio
        self.max_ellipsis_line_ratio = max_ellipsis_line_ratio

    def stats(self, text: str) -> GopherStats:
        """Compute the diagnostics used by both the gate and the UI."""
        words = text.split()
        word_count = len(words)
        if word_count == 0:
            return GopherStats(0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        mean_word_len = sum(len(w) for w in words) / word_count
        lower_words = [w.lower() for w in words]
        stop_hits = sum(1 for w in lower_words if w in _STOPWORDS)
        stopword_ratio = stop_hits / word_count
        symbol_words = sum(1 for w in words if any(c in "#@&*<>{}[]\\" for c in w))
        symbol_word_ratio = symbol_words / word_count
        alpha_words = sum(1 for w in words if any(c.isalpha() for c in w))
        alpha_word_ratio = alpha_words / word_count
        lines = [ln for ln in text.splitlines() if ln.strip()]
        if lines:
            bullet_lines = sum(1 for ln in lines if ln.lstrip().startswith(_BULLET_PREFIXES))
            ellipsis_lines = sum(
                1 for ln in lines if ln.rstrip().endswith(("...", "…"))
            )
            bullet_line_ratio = bullet_lines / len(lines)
            ellipsis_line_ratio = ellipsis_lines / len(lines)
        else:
            bullet_line_ratio = 0.0
            ellipsis_line_ratio = 0.0
        return GopherStats(
            word_count=word_count,
            mean_word_len=mean_word_len,
            stopword_ratio=stopword_ratio,
            bullet_line_ratio=bullet_line_ratio,
            ellipsis_line_ratio=ellipsis_line_ratio,
            symbol_word_ratio=symbol_word_ratio,
            alpha_word_ratio=alpha_word_ratio,
        )

    def passes(self, text: str) -> bool:
        """True iff every configured Gopher rule fires positively."""
        s = self.stats(text)
        if not (self.min_words <= s.word_count <= self.max_words):
            return False
        if not (self.min_mean_word_len <= s.mean_word_len <= self.max_mean_word_len):
            return False
        if s.symbol_word_ratio > self.max_symbol_word_ratio:
            return False
        if s.alpha_word_ratio < self.min_alpha_word_ratio:
            return False
        if s.stopword_ratio * s.word_count < self.min_stopword_count:
            return False
        if s.bullet_line_ratio > self.max_bullet_line_ratio:
            return False
        if s.ellipsis_line_ratio > self.max_ellipsis_line_ratio:
            return False
        return True
