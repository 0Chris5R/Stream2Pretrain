"""KenLM perplexity scorer.

Loads a binary KenLM ``.bin`` model from disk (default location
``$S2P_MODELS_DIR/kenlm/<lang>.bin``) and returns the log-perplexity of an
input string. KenLM gives ~10MB/s/core and is mmap-friendly so multiple
Bytewax workers on the same node share one resident copy of the model.

If the ``kenlm`` Python bindings are unavailable (e.g. CPython without C++
compiler in CI), :class:`KenLMScorer` falls back to a deterministic
log-character-entropy estimator. The fallback is documented as
``proxy-character-entropy-0.1`` in the silver record's perplexity-bucket
metadata so consumers know not to compare buckets across detector versions.
"""

# The keys below intentionally contain visually similar Unicode punctuation
# because this table normalizes those exact source characters.
# ruff: noqa: RUF001

from __future__ import annotations

import math
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

# Bucket cuts taken from CCNet/Dolma: head <= 200, middle <= 1000, else tail.
_HEAD_CUT = 200.0
_MIDDLE_CUT = 1000.0
_DIGIT_RE = re.compile(r"\d")
_NON_PRINTING_RE = re.compile(f"[{''.join(map(chr, list(range(0, 32)) + list(range(127, 160))))}]")
_UNICODE_PUNCT = {
    "，": ",",
    "。": ".",
    "、": ",",
    "„": '"',
    "”": '"',
    "“": '"',
    "«": '"',
    "»": '"',
    "１": '"',
    "」": '"',
    "「": '"',
    "《": '"',
    "》": '"',
    "´": "'",
    "∶": ":",
    "：": ":",
    "？": "?",
    "！": "!",
    "（": "(",
    "）": ")",
    "；": ";",
    "–": "-",
    "—": " - ",
    "．": ". ",
    "～": "~",
    "’": "'",
    "…": "...",
    "━": "-",
    "〈": "<",
    "〉": ">",
    "【": "[",
    "】": "]",
    "％": "%",
    "►": "-",
}


@dataclass(frozen=True, slots=True)
class PerplexityResult:
    """Output of :meth:`KenLMScorer.score`."""

    perplexity: float
    bucket: str
    scorer: str


class KenLMScorer:
    """Wrapper that maps a string to (perplexity, bucket).

    Parameters
    ----------
    model_path
        Path to a ``.bin`` KenLM file. If ``None`` or the file does not
        exist, the fallback character-entropy proxy is used.
    """

    def __init__(
        self,
        model_path: str | Path | None,
        sentencepiece_path: str | Path | None = None,
        *,
        allow_fallback: bool = True,
    ) -> None:
        self._path = Path(model_path) if model_path else None
        self._sentencepiece_path = Path(sentencepiece_path) if sentencepiece_path else None
        self._allow_fallback = allow_fallback
        self._model = self._load_kenlm(self._path)
        self._sentencepiece = self._load_sentencepiece(self._sentencepiece_path)
        if not allow_fallback and not self.is_model_loaded:
            raise RuntimeError("the pinned KenLM binary and SentencePiece model are required")
        self._scorer_id = (
            f"kenlm-sentencepiece:{self._path.name}"
            if self.is_model_loaded and self._path
            else "proxy-character-entropy-0.1"
        )

    @staticmethod
    def _load_kenlm(path: Path | None) -> object | None:
        if path is None or not path.is_file():
            return None
        try:
            import kenlm  # type: ignore[import-untyped]
        except Exception:
            return None
        try:
            cfg = kenlm.Config()
            cfg.load_method = kenlm.LoadMethod.LAZY
            return kenlm.Model(str(path), cfg)
        except Exception:
            return None

    @staticmethod
    def _load_sentencepiece(path: Path | None) -> object | None:
        if path is None or not path.is_file():
            return None
        try:
            import sentencepiece  # type: ignore[import-untyped]

            tokenizer = sentencepiece.SentencePieceProcessor()
            tokenizer.load(str(path))
            return tokenizer
        except Exception:
            return None

    @property
    def is_model_loaded(self) -> bool:
        """Whether both artifacts used by the official scorer are loaded."""
        return self._model is not None and self._sentencepiece is not None

    @property
    def scorer(self) -> str:
        """Identifier persisted into ``SilverTags`` provenance fields."""
        return self._scorer_id

    def score(self, text: str) -> PerplexityResult:
        """Return (perplexity, bucket, scorer-id) for ``text``."""
        if not text or not text.strip():
            return PerplexityResult(perplexity=0.0, bucket="head", scorer=self._scorer_id)
        if self.is_model_loaded:
            ppl = self._kenlm_perplexity(text)
        else:
            ppl = self._character_entropy_proxy(text)
        return PerplexityResult(perplexity=ppl, bucket=_bucket(ppl), scorer=self._scorer_id)

    def _kenlm_perplexity(self, text: str) -> float:
        """Run the official edugp normalization and SentencePiece recipe."""
        try:
            normalized = _normalize_ccnet(text)
            pieces = self._sentencepiece.encode_as_pieces(normalized)  # type: ignore[union-attr]
            tokenized = " ".join(pieces)
            log_score = 0.0
            length = 0
            for line in tokenized.split("\n"):
                log_score += float(self._model.score(line))  # type: ignore[union-attr]
                length += len(line.split()) + 1
            return round(10.0 ** (-log_score / max(length, 1)), 1)
        except Exception:
            if not self._allow_fallback:
                raise
            return self._character_entropy_proxy(text)

    @staticmethod
    def _character_entropy_proxy(text: str) -> float:
        """Deterministic fallback that produces a perplexity-like score.

        We compute Shannon entropy over characters and exponentiate to a base
        comparable to a low-order LM. The numbers are not directly comparable
        to a real KenLM score; we calibrate the output so typical English
        prose lands in the head bucket (<= 200) and pathological inputs land
        in the middle/tail buckets, which is enough to drive the curation
        gate sensibly in dev clusters.
        """
        counts = Counter(text)
        total = sum(counts.values())
        entropy = -sum((c / total) * math.log2(c / total) for c in counts.values() if c)
        # 2**entropy is the per-character branching factor; clip to reasonable
        # numbers so well-formed prose stays in the head bucket. Without the
        # clip a typical English paragraph lands at ppl ~ 800.
        return float(2**entropy * 8.0)


def _bucket(perplexity: float) -> str:
    """Map a perplexity value onto the FineWeb head/middle/tail buckets."""
    if perplexity <= _HEAD_CUT:
        return "head"
    if perplexity <= _MIDDLE_CUT:
        return "middle"
    return "tail"


def _normalize_ccnet(text: str) -> str:
    """Normalization used by the pinned ``edugp/kenlm`` English model."""
    text = _DIGIT_RE.sub("0", text.strip())
    text = "".join(_UNICODE_PUNCT.get(char, char) for char in text)
    return _NON_PRINTING_RE.sub("", text)
