"""PII detection operator.

Two layers:

1. A regex pack covering email, phone (E.164), IBAN, credit card (with
   Luhn verification to drop false positives), SSN-shape, IPv4, IPv6,
   and passport-shape strings. Cheap, deterministic, language-agnostic.
2. Optional Microsoft Presidio analyzer plug. Loaded lazily; only used if
   the wheel is installed and the env-var ``S2P_USE_PRESIDIO`` is truthy.

Ordinary contact information is replaced by typed placeholders. Credentials,
financial identifiers, and government identifiers are classified as high risk
and force the containing artifact to quarantine. Only flag names and counts are
persisted: matched values never enter Gold audit metadata.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterator
from dataclasses import dataclass

from schemas.gold import PiiFlag

# Regex pack. None of these are perfect; they are designed to cap false
# negatives at the cost of moderate false positives, which is the right
# trade-off for a pretraining filter.
_EMAIL = re.compile(r"\b[\w.+-]+@[\w-]+\.[\w.-]+\b")
# Require a recognisable telephone layout rather than accepting arbitrary
# groups of digits separated by spaces. The previous expression classified
# scientific values such as ``2023-06 32 32 32`` as phone numbers.
_PHONE = re.compile(
    r"(?<![\w.])(?:"
    r"\+\d{1,3}[\s.-]?(?:\(?\d{2,4}\)?[\s.-]?){1,3}\d{3,4}"
    r"|\(\d{2,4}\)[\s.-]?\d{3,4}[\s.-]\d{3,4}"
    r"|\d{3}[.-]\d{3}[.-]\d{4}"
    r")(?![\w.])"
)
_IBAN = re.compile(r"\b[A-Z]{2}\d{2}[A-Z0-9]{11,30}\b")
_CC = re.compile(r"\b(?:\d[ -]?){13,19}\b")
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_IPV6 = re.compile(r"\b(?:[A-Fa-f0-9]{1,4}:){2,7}[A-Fa-f0-9]{1,4}\b")
_PASSPORT = re.compile(r"\b[A-Z]{1,2}\d{6,9}\b")
_SECRET = re.compile(
    r"(?ix)(?:"
    r"-----BEGIN\s+(?:RSA |EC |OPENSSH )?PRIVATE KEY-----"
    r"|\b(?:api[_ -]?key|access[_ -]?token|client[_ -]?secret|password)\b\s*[:=]\s*['\"]?[A-Za-z0-9_+/.=-]{16,}"
    r"|\b(?:gh[pousr]_[A-Za-z0-9]{20,}|AKIA[0-9A-Z]{16})\b"
    r"|\bBearer\s+[A-Za-z0-9._~+/=-]{20,}"
    r")"
)

_REDACTION_TOKENS: dict[PiiFlag, str] = {
    "email": "[EMAIL]",
    "phone": "[PHONE]",
    "ipv4": "[IP_ADDRESS]",
    "ipv6": "[IP_ADDRESS]",
}
_HIGH_RISK_FLAGS: frozenset[PiiFlag] = frozenset(
    {"credit_card", "iban", "ssn", "passport", "secret"}
)

_MIN_DIGITS_PHONE = 9
# Operational NLP chunk bound. Peak RSS on the target cluster remains
# needs-measurement; every character is still inspected, but spaCy never has
# to materialize an entire scientific paper in one analysis call.
_DEFAULT_PRESIDIO_CHUNK_CHARS = 32_768


@dataclass(frozen=True, slots=True)
class PiiHit:
    """A private in-memory match. The snippet must never be persisted."""

    flag: PiiFlag
    snippet: str
    start: int = -1
    end: int = -1


@dataclass(frozen=True, slots=True)
class PiiSanitization:
    """Safe projection plus the minimum audit information needed downstream."""

    text: str
    flags: tuple[PiiFlag, ...]
    redacted_flags: tuple[PiiFlag, ...]
    blocking_flags: tuple[PiiFlag, ...]
    redaction_count: int


def luhn_ok(digits: str) -> bool:
    """Standard Luhn checksum validator on a digits-only string."""
    if not digits or not digits.isdigit():
        return False
    total = 0
    for i, ch in enumerate(reversed(digits)):
        d = int(ch)
        if i % 2 == 1:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def is_valid_ipv4(text: str) -> bool:
    """All four octets must be 0-255."""
    parts = text.split(".")
    if len(parts) != 4:
        return False
    return all(p.isdigit() and 0 <= int(p) <= 255 for p in parts)


class PiiScanner:
    """Regex + Luhn PII detector with optional Presidio enhancement."""

    def __init__(
        self,
        *,
        use_presidio: bool | None = None,
        max_text_chars: int = 1_000_000,
        presidio_chunk_chars: int | None = None,
        allow_fallback: bool = True,
    ) -> None:
        self._max_text_chars = max_text_chars
        self._presidio_chunk_chars = (
            int(os.environ.get("S2P_PRESIDIO_CHUNK_CHARS", str(_DEFAULT_PRESIDIO_CHUNK_CHARS)))
            if presidio_chunk_chars is None
            else int(presidio_chunk_chars)
        )
        if self._presidio_chunk_chars < 1:
            raise ValueError("presidio_chunk_chars must be positive")
        self._allow_fallback = allow_fallback
        self._use_presidio = (
            os.environ.get("S2P_USE_PRESIDIO") == "1" if use_presidio is None else use_presidio
        )
        self._presidio = self._load_presidio() if self._use_presidio else None
        if self._use_presidio and not allow_fallback and self._presidio is None:
            raise RuntimeError("Presidio with the bundled spaCy model is required")

    @property
    def is_presidio_loaded(self) -> bool:
        """Whether the optional Presidio analyzer loaded successfully."""
        return self._presidio is not None

    @property
    def revision(self) -> str:
        """Human-readable scanner bundle persisted with each decision."""
        return "regex-luhn-v1+presidio-en_core_web_sm" if self._presidio else "regex-luhn-v1"

    @staticmethod
    def _load_presidio() -> object | None:
        try:
            from presidio_analyzer import (  # type: ignore[import-untyped]
                AnalyzerEngine,
                RecognizerRegistry,
            )
            from presidio_analyzer.nlp_engine import (  # type: ignore[import-untyped]
                NlpEngineProvider,
            )
            from presidio_analyzer.predefined_recognizers import (  # type: ignore[import-untyped]
                CreditCardRecognizer,
                EmailRecognizer,
                IpRecognizer,
                PhoneRecognizer,
                UsPassportRecognizer,
                UsSsnRecognizer,
            )

            configuration = {
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
            }
            nlp_engine = NlpEngineProvider(nlp_configuration=configuration).create_engine()
            # The default Presidio registry eagerly installs every bundled
            # country-specific recognizer, although this scanner maps only the
            # six entities below. A non-empty explicit registry prevents that
            # global recognizer set from being loaded and keeps the CPU curator
            # inside its memory envelope without weakening mapped PII coverage.
            registry = RecognizerRegistry(
                recognizers=[
                    CreditCardRecognizer(),
                    EmailRecognizer(),
                    IpRecognizer(),
                    PhoneRecognizer(),
                    UsPassportRecognizer(),
                    UsSsnRecognizer(),
                ],
                supported_languages=["en"],
            )
            return AnalyzerEngine(
                registry=registry,
                nlp_engine=nlp_engine,
                supported_languages=["en"],
            )
        except Exception:
            return None

    def scan(self, text: str) -> list[PiiHit]:
        """Return the deduplicated list of hits in ``text``."""
        if not text:
            return []
        # Cap pathological inputs - regexes are O(n) but the snippets list grows.
        snippet = text[: self._max_text_chars]
        hits: list[PiiHit] = []
        for m in _EMAIL.finditer(snippet):
            hits.append(PiiHit("email", m.group(0), m.start(), m.end()))
        for m in _PHONE.finditer(snippet):
            digits = "".join(c for c in m.group(0) if c.isdigit())
            if len(digits) >= _MIN_DIGITS_PHONE:
                hits.append(PiiHit("phone", m.group(0), m.start(), m.end()))
        for m in _CC.finditer(snippet):
            digits = "".join(c for c in m.group(0) if c.isdigit())
            if 13 <= len(digits) <= 19 and luhn_ok(digits):
                hits.append(PiiHit("credit_card", m.group(0), m.start(), m.end()))
        for m in _IBAN.finditer(snippet):
            hits.append(PiiHit("iban", m.group(0), m.start(), m.end()))
        for m in _SSN.finditer(snippet):
            hits.append(PiiHit("ssn", m.group(0), m.start(), m.end()))
        for m in _IPV4.finditer(snippet):
            if is_valid_ipv4(m.group(0)):
                hits.append(PiiHit("ipv4", m.group(0), m.start(), m.end()))
        for m in _IPV6.finditer(snippet):
            hits.append(PiiHit("ipv6", m.group(0), m.start(), m.end()))
        for m in _PASSPORT.finditer(snippet):
            context = snippet[max(0, m.start() - 48) : m.end() + 48].lower()
            if "passport" in context:
                hits.append(PiiHit("passport", m.group(0), m.start(), m.end()))
        for m in _SECRET.finditer(snippet):
            hits.append(PiiHit("secret", m.group(0), m.start(), m.end()))
        if self._presidio is not None:
            try:
                chunk_search_start = 0
                for chunk in _bounded_text_chunks(snippet, self._presidio_chunk_chars):
                    chunk_start = snippet.find(chunk, chunk_search_start)
                    if chunk_start < 0:
                        chunk_start = chunk_search_start
                    chunk_search_start = chunk_start + len(chunk)
                    results = self._presidio.analyze(  # type: ignore[union-attr]
                        text=chunk,
                        language="en",
                    )
                    for r in results:
                        value = chunk[r.start : r.end]
                        flag = (
                            "ipv6"
                            if r.entity_type == "IP_ADDRESS" and ":" in value
                            else self._presidio_flag(r.entity_type)
                        )
                        if flag:
                            start = chunk_start + r.start
                            hits.append(PiiHit(flag, value, start, chunk_start + r.end))
            except Exception:
                if not self._allow_fallback:
                    raise
        return _deduplicate(hits)

    def flags(self, text: str) -> list[PiiFlag]:
        """Return the sorted unique :class:`PiiFlag` list for ``text``."""
        return sorted({h.flag for h in self.scan(text)})

    def blocking_flags(self, text: str) -> list[PiiFlag]:
        """Return findings that cannot safely be handled by contact redaction."""
        return sorted(set(self.flags(text)) & _HIGH_RISK_FLAGS)

    def sanitize(self, text: str) -> PiiSanitization:
        """Redact ordinary contact data and report high-risk identifiers.

        High-risk values are also replaced in this safe projection so a
        quarantined Gold decision cannot expose the matched secret through its
        text field. Callers must quarantine whenever ``blocking_flags`` is not
        empty.
        """
        hits = self.scan(text)
        spans: list[tuple[int, int, str, PiiFlag]] = []
        for hit in hits:
            if hit.start < 0 or hit.end <= hit.start:
                continue
            replacement = _REDACTION_TOKENS.get(hit.flag, f"[{hit.flag.upper()}]")
            spans.append((hit.start, hit.end, replacement, hit.flag))
        # Resolve overlaps deterministically, then replace from right to left.
        chosen: list[tuple[int, int, str, PiiFlag]] = []
        for span in sorted(spans, key=lambda value: (value[0], -(value[1] - value[0]))):
            if chosen and span[0] < chosen[-1][1]:
                continue
            chosen.append(span)
        sanitized = text
        for start, end, replacement, _ in reversed(chosen):
            sanitized = sanitized[:start] + replacement + sanitized[end:]
        flags = tuple(sorted({hit.flag for hit in hits}))
        blocking = tuple(sorted(set(flags) & _HIGH_RISK_FLAGS))
        redacted = tuple(sorted({span[3] for span in chosen}))
        return PiiSanitization(
            text=sanitized,
            flags=flags,
            redacted_flags=redacted,
            blocking_flags=blocking,
            redaction_count=len(chosen),
        )

    @staticmethod
    def _presidio_flag(entity_type: str) -> PiiFlag | None:
        mapping: dict[str, PiiFlag] = {
            "EMAIL_ADDRESS": "email",
            "PHONE_NUMBER": "phone",
            "US_SSN": "ssn",
            "CREDIT_CARD": "credit_card",
            "IP_ADDRESS": "ipv4",
            "US_PASSPORT": "passport",
        }
        return mapping.get(entity_type)


def _deduplicate(hits: list[PiiHit]) -> list[PiiHit]:
    """Drop duplicates while preserving first-seen order."""
    seen: set[tuple[PiiFlag, str]] = set()
    out: list[PiiHit] = []
    for h in hits:
        key = (h.flag, h.snippet)
        if key in seen:
            continue
        seen.add(key)
        out.append(h)
    return out


def _bounded_text_chunks(text: str, max_chars: int) -> Iterator[str]:
    """Cover ``text`` with whitespace-aligned chunks bounded for spaCy RSS."""
    start = 0
    text_length = len(text)
    while start < text_length:
        end = min(start + max_chars, text_length)
        if end < text_length:
            newline = text.rfind("\n", start, end)
            space = text.rfind(" ", start, end)
            boundary = max(newline, space)
            if boundary > start:
                end = boundary
        chunk = text[start:end]
        if chunk:
            yield chunk
        start = end
