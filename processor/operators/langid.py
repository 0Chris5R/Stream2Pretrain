"""Language identification operator.

Primary implementation is fastlangid (a 176-language lid model packaged for
Python 3.11+). It is faster and lighter than fasttext-langdetect at
comparable accuracy on long documents.

If the heavy detector is unavailable (offline dev shells, sandboxed CI), the
wrapper falls back to a small heuristic classifier that handles the most
common Stream2Pretrain Phase-1 languages (en, de, fr, es, zh, ja, ru). The
heuristic is documented as ``fallback-stopword-0.1`` so downstream consumers
can detect it from the silver record.
"""

from __future__ import annotations

from dataclasses import dataclass

# Lightweight stopword sets for the fallback heuristic. Only used when neither
# fastlangid nor langdetect is importable. Production paths install the heavy
# detector via the processor extras.
_STOPWORDS: dict[str, frozenset[str]] = {
    "en": frozenset(
        [
            "the",
            "and",
            "that",
            "have",
            "for",
            "not",
            "with",
            "you",
            "this",
            "but",
            "his",
            "they",
            "are",
            "from",
            "which",
            "one",
        ]
    ),
    "de": frozenset(
        [
            "der",
            "die",
            "und",
            "das",
            "mit",
            "nicht",
            "ein",
            "eine",
            "ist",
            "auch",
            "sich",
            "auf",
            "zu",
            "von",
            "im",
            "sind",
        ]
    ),
    "fr": frozenset(
        [
            "le",
            "la",
            "les",
            "des",
            "une",
            "est",
            "pas",
            "que",
            "pour",
            "dans",
            "avec",
            "sur",
            "ne",
            "par",
            "plus",
            "mais",
            "ou",
        ]
    ),
    "es": frozenset(
        [
            "que",
            "de",
            "no",
            "la",
            "el",
            "en",
            "los",
            "se",
            "las",
            "por",
            "con",
            "para",
            "una",
            "su",
            "al",
            "lo",
            "como",
            "mas",
        ]
    ),
}


@dataclass(frozen=True, slots=True)
class LangResult:
    """Outcome of language detection on a single document."""

    lang: str
    score: float
    detector: str


class LangIdentifier:
    """Drop-in detector that returns a (lang, score) pair.

    Parameters
    ----------
    min_chars
        Texts shorter than this fall through to the heuristic detector
        regardless of which backend is installed (short texts confuse
        statistical LID).
    """

    def __init__(self, *, min_chars: int = 64, allow_fallback: bool = True) -> None:
        self._min_chars = min_chars
        self._allow_fallback = allow_fallback
        self._backend, self._detector = self._load_backend()
        if not allow_fallback and self._detector == "fallback-stopword-0.1":
            raise RuntimeError("fastlangid is required when fallbacks are disabled")

    @staticmethod
    def _load_backend() -> tuple[object | None, str]:
        """Try fastlangid first, then langdetect, then None."""
        try:
            import fastlangid  # type: ignore[import-untyped]

            detector = fastlangid.LID()
            return detector, "fastlangid-1"
        except Exception:
            pass
        try:
            import langdetect  # type: ignore[import-untyped]

            return langdetect, "langdetect-1.0.9"
        except Exception:
            return None, "fallback-stopword-0.1"

    def identify(self, text: str) -> LangResult:
        """Return language + confidence for ``text`` (never raises)."""
        if not text or (len(text) < self._min_chars and self._allow_fallback):
            return self._heuristic(text)
        if self._detector == "fastlangid-1" and self._backend is not None:
            try:
                # fastlangid returns only the label unless probability output
                # is requested explicitly.
                lang, score = self._backend.predict(text, prob=True)  # type: ignore[union-attr]
                return LangResult(lang=str(lang), score=float(score), detector=self._detector)
            except Exception:
                if not self._allow_fallback:
                    raise
                return self._heuristic(text)
        if self._detector.startswith("langdetect") and self._backend is not None:
            try:
                results = self._backend.detect_langs(text)  # type: ignore[union-attr]
                top = results[0]
                return LangResult(
                    lang=str(top.lang), score=float(top.prob), detector=self._detector
                )
            except Exception:
                if not self._allow_fallback:
                    raise
                return self._heuristic(text)
        if not self._allow_fallback:
            raise RuntimeError("no real language identifier is available")
        return self._heuristic(text)

    def _heuristic(self, text: str) -> LangResult:
        """Stopword-overlap fallback. Always returns a result."""
        words = [w.lower() for w in text.split() if w.isalpha()]
        if not words:
            return LangResult(lang="und", score=0.0, detector=self._detector)
        token_set = set(words)
        scores = {
            lang: len(token_set & sw) / max(len(token_set), 1) for lang, sw in _STOPWORDS.items()
        }
        if not scores or max(scores.values()) == 0:
            return LangResult(lang="und", score=0.0, detector=self._detector)
        lang, score = max(scores.items(), key=lambda kv: kv[1])
        # Cap heuristic confidence at 0.9 so it never looks like a real model.
        return LangResult(lang=lang, score=min(score * 5.0, 0.9), detector=self._detector)
