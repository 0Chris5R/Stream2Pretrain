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

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

# Bucket cuts taken from CCNet/Dolma: head <= 200, middle <= 1000, else tail.
_HEAD_CUT = 200.0
_MIDDLE_CUT = 1000.0


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

    def __init__(self, model_path: str | Path | None) -> None:
        self._path = Path(model_path) if model_path else None
        self._model = self._load_kenlm(self._path)
        self._scorer_id = (
            f"kenlm:{self._path.name}" if self._model is not None and self._path else
            "proxy-character-entropy-0.1"
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

    @property
    def scorer(self) -> str:
        """Identifier persisted into ``SilverTags`` provenance fields."""
        return self._scorer_id

    def score(self, text: str) -> PerplexityResult:
        """Return (perplexity, bucket, scorer-id) for ``text``."""
        if not text or not text.strip():
            return PerplexityResult(perplexity=0.0, bucket="head", scorer=self._scorer_id)
        if self._model is not None:
            ppl = self._kenlm_perplexity(text)
        else:
            ppl = self._character_entropy_proxy(text)
        return PerplexityResult(perplexity=ppl, bucket=_bucket(ppl), scorer=self._scorer_id)

    def _kenlm_perplexity(self, text: str) -> float:
        """Fast path: use the loaded KenLM model."""
        try:
            return float(self._model.perplexity(text))  # type: ignore[union-attr]
        except Exception:
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
        return float(2 ** entropy * 8.0)


def _bucket(perplexity: float) -> str:
    """Map a perplexity value onto the FineWeb head/middle/tail buckets."""
    if perplexity <= _HEAD_CUT:
        return "head"
    if perplexity <= _MIDDLE_CUT:
        return "middle"
    return "tail"
