"""Transparent source-code quality policy for code-format records."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass

from processor.operators.quality import QualityScore

_COMMENT = re.compile(r"^\s*(?:#|//|/\*|\*|<!--)|\"\"\"|'''", re.MULTILINE)
_GENERATED_PATH = re.compile(
    r"(?:^|/)(?:dist|build|vendor|node_modules|coverage|minified)(?:/|$)|(?:\.min\.(?:js|css)$)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class CodeQualityPolicy:
    """Bounded code-specific checks used instead of a prose classifier."""

    revision: str = "code-quality-rules-v1"
    backend: str = "rules"

    def score(self, text: str, *, path: str = "") -> QualityScore:
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines or _GENERATED_PATH.search(path):
            return QualityScore(edu_score=0.0, revision=self.revision)
        score = 0.0
        score += 1.0 if len(lines) >= 8 else 0.0
        score += 1.0 if len(text) >= 300 else 0.0
        score += 1.0 if max(map(len, lines)) <= 500 else 0.0
        comment_fraction = len(_COMMENT.findall(text)) / max(1, len(lines))
        score += 1.0 if comment_fraction >= 0.02 else 0.0
        score += 1.0 if self._syntax_signal(text, path) else 0.0
        return QualityScore(edu_score=score, revision=self.revision)

    @staticmethod
    def _syntax_signal(text: str, path: str) -> bool:
        if path.lower().endswith(".py"):
            try:
                ast.parse(text)
            except SyntaxError:
                return False
            return True
        pairs = (("{", "}"), ("(", ")"), ("[", "]"))
        return all(text.count(left) == text.count(right) for left, right in pairs)
