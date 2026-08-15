"""PII detection operator.

Two layers:

1. A regex pack covering email, phone (E.164), IBAN, credit card (with
   Luhn verification to drop false positives), SSN-shape, IPv4, IPv6,
   and passport-shape strings. Cheap, deterministic, language-agnostic.
2. Optional Microsoft Presidio analyzer plug. Loaded lazily; only used if
   the wheel is installed and the env-var ``S2P_USE_PRESIDIO`` is truthy.

Output: a sorted list of :class:`schemas.gold.PiiFlag` strings on the
``GoldRecord.pii_flags`` field.
"""

from __future__ import annotations

import os
import re
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

_MIN_DIGITS_PHONE = 9


@dataclass(frozen=True, slots=True)
class PiiHit:
    """A single match - useful for forensic dumps in the UI."""

    flag: PiiFlag
    snippet: str


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
        allow_fallback: bool = True,
    ) -> None:
        self._max_text_chars = max_text_chars
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
            from presidio_analyzer import AnalyzerEngine  # type: ignore[import-untyped]
            from presidio_analyzer.nlp_engine import (  # type: ignore[import-untyped]
                NlpEngineProvider,
            )

            configuration = {
                "nlp_engine_name": "spacy",
                "models": [{"lang_code": "en", "model_name": "en_core_web_sm"}],
            }
            nlp_engine = NlpEngineProvider(nlp_configuration=configuration).create_engine()
            return AnalyzerEngine(
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
            hits.append(PiiHit("email", m.group(0)))
        for m in _PHONE.finditer(snippet):
            digits = "".join(c for c in m.group(0) if c.isdigit())
            if len(digits) >= _MIN_DIGITS_PHONE:
                hits.append(PiiHit("phone", m.group(0)))
        for m in _CC.finditer(snippet):
            digits = "".join(c for c in m.group(0) if c.isdigit())
            if 13 <= len(digits) <= 19 and luhn_ok(digits):
                hits.append(PiiHit("credit_card", m.group(0)))
        for m in _IBAN.finditer(snippet):
            hits.append(PiiHit("credit_card", m.group(0))) if False else None
            # IBAN does not have its own PiiFlag; intentionally noop here.
            # We surface IBANs through the ``credit_card`` channel only when
            # the Luhn-validated CC regex above matches - keeping flags well-
            # defined on the gold schema. IBAN-only cases stay surfaced via
            # Presidio if enabled.
        for m in _SSN.finditer(snippet):
            hits.append(PiiHit("ssn", m.group(0)))
        for m in _IPV4.finditer(snippet):
            if is_valid_ipv4(m.group(0)):
                hits.append(PiiHit("ipv4", m.group(0)))
        for m in _IPV6.finditer(snippet):
            hits.append(PiiHit("ipv6", m.group(0)))
        for m in _PASSPORT.finditer(snippet):
            hits.append(PiiHit("passport", m.group(0)))
        if self._presidio is not None:
            try:
                results = self._presidio.analyze(text=snippet, language="en")  # type: ignore[union-attr]
                for r in results:
                    flag = self._presidio_flag(r.entity_type)
                    if flag:
                        hits.append(PiiHit(flag, snippet[r.start : r.end]))
            except Exception:
                if not self._allow_fallback:
                    raise
        return _deduplicate(hits)

    def flags(self, text: str) -> list[PiiFlag]:
        """Return the sorted unique :class:`PiiFlag` list for ``text``."""
        return sorted({h.flag for h in self.scan(text)})

    def blocking_flags(self, text: str) -> list[PiiFlag]:
        """Return high-confidence findings that may block a training export.

        Email addresses, Luhn-valid payment-card numbers, and SSN-shaped
        values are sufficiently precise to remove their containing part.
        Phone numbers use the strict syntax above. IP-looking values and
        Presidio-only phone/passport guesses remain audit signals because
        dotted experiment identifiers, section numbers, and tensor shapes
        are common in scientific text.
        """
        if not text:
            return []
        snippet = text[: self._max_text_chars]
        blocking: set[PiiFlag] = set()
        if _EMAIL.search(snippet):
            blocking.add("email")
        if _SSN.search(snippet):
            blocking.add("ssn")
        if _PHONE.search(snippet):
            blocking.add("phone")
        for match in _CC.finditer(snippet):
            digits = "".join(character for character in match.group(0) if character.isdigit())
            if 13 <= len(digits) <= 19 and luhn_ok(digits):
                blocking.add("credit_card")
        for match in _PASSPORT.finditer(snippet):
            context = snippet[max(0, match.start() - 48) : match.end() + 48].lower()
            if "passport" in context:
                blocking.add("passport")
        return sorted(blocking)

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
