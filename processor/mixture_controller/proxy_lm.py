"""Proxy language model for the shadow-mode A/B mixture comparison.

This is a *stub* in the sense the task brief allows: we intentionally
implement a small character-bigram language model with a clean
:class:`ProxyLM` interface so a real KenLM / transformers proxy can be
swapped in without changing the controller. The class is deterministic
and CPU-cheap so two branches can be trained in parallel inside a single
pod alongside the kopf reconciler.

Design contract
---------------
``train(text)``           - update model state on a curated document.
``perplexity(text)``      - return a per-token perplexity for ``text``.
``snapshot()``            - return a serialisable view (used by the UI).
``reset()``               - clear all state.

The bigram model is *not* what production should ship; mark this swap-in
in the gold record metadata via ``GoldRecord.classifier_revision`` so a
forensic replay can reconstruct which proxy LM was responsible for a
promotion decision.
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from collections.abc import Iterable
from dataclasses import dataclass

PROXY_LM_REVISION: str = "proxy-bigram-0.1"


@dataclass(slots=True)
class ProxyLMSnapshot:
    """Serialisable snapshot of the proxy LM state."""

    revision: str
    total_tokens: int
    vocab_size: int


class ProxyLM:
    """Character-bigram language model with add-one smoothing.

    The model is intentionally tiny: production should swap in a real LM
    via the same interface.
    """

    revision: str = PROXY_LM_REVISION

    def __init__(self, *, smoothing: float = 1.0) -> None:
        self._smoothing = smoothing
        self._unigram: Counter[str] = Counter()
        self._bigram: dict[str, Counter[str]] = defaultdict(Counter)
        self._total = 0

    def train(self, text: str) -> None:
        """Update bigram counts on one document."""
        if not text:
            return
        previous = ""
        for ch in text:
            self._unigram[ch] += 1
            self._bigram[previous][ch] += 1
            self._total += 1
            previous = ch

    def train_many(self, texts: Iterable[str]) -> None:
        """Bulk training helper."""
        for t in texts:
            self.train(t)

    def perplexity(self, text: str) -> float:
        """Return the per-character perplexity of ``text``.

        Untrained models return a large perplexity (the smoothed uniform
        distribution over a baseline alphabet of 256 chars) so that
        ``perplexity(text)`` is a monotonically decreasing function of
        training tokens for any non-trivial text.
        """
        if not text:
            return 0.0
        log_prob = 0.0
        previous = ""
        # Use a baseline alphabet size so untrained models do not collapse
        # to perplexity 1.0; observed unigrams are added on top.
        v = max(len(self._unigram), 256)
        for ch in text:
            ctx = self._bigram.get(previous)
            num = (ctx[ch] if ctx else 0) + self._smoothing
            denom = (sum(ctx.values()) if ctx else 0) + self._smoothing * v
            log_prob += math.log(num / denom)
            previous = ch
        avg = log_prob / len(text)
        return float(math.exp(-avg))

    def snapshot(self) -> ProxyLMSnapshot:
        """Lightweight view used by the metrics exporter."""
        return ProxyLMSnapshot(
            revision=self.revision,
            total_tokens=self._total,
            vocab_size=len(self._unigram),
        )

    def reset(self) -> None:
        """Clear the model. Used by rolling-window training."""
        self._unigram.clear()
        self._bigram.clear()
        self._total = 0
