"""Stack v2/Dolma-grounded source-code quality policy."""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass

from processor.operators.quality import QualityScore

_COMMENT = re.compile(r"^\s*(?:#|//|/\*|\*|<!--)|\"\"\"|'''", re.MULTILINE)
_GENERATED_PATH = re.compile(
    r"(?:^|/)(?:\.git|__pycache__|build|coverage|dist|generated|minified|node_modules|target|third_party|vendor|vendors)(?:/|$)|(?:\.min\.(?:js|css)$)",
    re.IGNORECASE,
)
_SECRET = re.compile(
    r"(?:AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{35}|gh[pousr]_[A-Za-z0-9]{20,}|"
    r"xox[baprs]-[A-Za-z0-9-]{10,}|"
    r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|"
    r"(?:api[_-]?key|secret|token|password)\s*[:=]\s*['\"][^'\"\s]{12,}['\"])",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class CodeQualityPolicy:
    """Bounded code-specific checks used instead of a prose classifier."""

    revision: str = "stack-v2-dolma-code-rules-v2"
    backend: str = "rules"

    def score(self, text: str, *, path: str = "") -> QualityScore:
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines or self.rejection_reasons(text, path=path):
            return QualityScore(edu_score=0.0, revision=self.revision)
        score = 0.0
        score += 1.0 if len(lines) >= 8 else 0.0
        score += 1.0 if len(text) >= 300 else 0.0
        score += 1.0 if max(map(len, lines)) <= 1000 else 0.0
        comment_fraction = len(_COMMENT.findall(text)) / max(1, len(lines))
        score += 1.0 if comment_fraction >= 0.02 else 0.0
        score += 1.0 if self._syntax_signal(text, path) else 0.0
        return QualityScore(edu_score=score, revision=self.revision)

    def rejection_reasons(self, text: str, *, path: str = "") -> list[str]:
        """Return deterministic hard failures from published code recipes."""
        lines = [line for line in text.splitlines() if line.strip()]
        if not lines:
            return ["empty_code"]
        reasons: list[str] = []
        if _GENERATED_PATH.search(path):
            reasons.append("generated_or_vendored_path")
        if "\x00" in text:
            reasons.append("binary_content")
        if _SECRET.search(text):
            reasons.append("credential_like_secret")
        if sum(map(len, lines)) / len(lines) > 100:
            reasons.append("average_line_too_long")
        if max(map(len, lines)) > 1000:
            reasons.append("maximum_line_too_long")
        alphanumeric = sum(char.isalnum() for char in text) / max(1, len(text))
        if alphanumeric < 0.25:
            reasons.append("low_alphanumeric_fraction")
        tokens = re.findall(r"\w+|[^\w\s]", text)
        alphabetic = sum(char.isalpha() for char in text)
        if tokens and alphabetic / len(tokens) < 1.5:
            reasons.append("low_alphabetic_characters_per_token")
        if not self._syntax_signal(text, path):
            reasons.append("syntax_check_failed")
        return reasons

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
