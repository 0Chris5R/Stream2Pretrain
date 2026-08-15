"""Token counting wrapper used to populate ``GoldRecord.tokens``.

Primary backend is ``tiktoken`` with the GPT-2 / cl100k base BPE - the same
tokeniser DataTrove and FineWeb use to fix the corpus token count column.

Fallback is ``sentencepiece`` if ``tiktoken`` is missing. Last resort is a
whitespace-tokenizer count, marked ``proxy-whitespace-0.1`` in the
silver/gold provenance string so the UI never confuses it with a real BPE
count.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TokenizerName = Literal["tiktoken", "sentencepiece", "proxy-whitespace"]


@dataclass(frozen=True, slots=True)
class TokenCount:
    """Result of :meth:`Tokenizer.count`."""

    tokens: int
    backend: TokenizerName


class Tokenizer:
    """Drop-in token counter with deterministic fallbacks.

    Parameters
    ----------
    encoding
        Tiktoken encoding name. ``cl100k_base`` matches GPT-4 / Llama 3
        and is what FineWeb publishes.
    """

    def __init__(
        self,
        *,
        encoding: str = "cl100k_base",
        allow_fallback: bool = True,
    ) -> None:
        self._encoding_name = encoding
        self._allow_fallback = allow_fallback
        self._impl, self._backend = self._load(encoding)
        if not allow_fallback and self._backend != "tiktoken":
            raise RuntimeError("tiktoken is required when fallbacks are disabled")

    @property
    def backend(self) -> TokenizerName:
        return self._backend

    @staticmethod
    def _load(encoding: str) -> tuple[object | None, TokenizerName]:
        try:
            import tiktoken  # type: ignore[import-untyped]

            return tiktoken.get_encoding(encoding), "tiktoken"
        except Exception:
            pass
        try:
            import sentencepiece as spm  # type: ignore[import-untyped]

            sp = spm.SentencePieceProcessor()
            return sp, "sentencepiece"
        except Exception:
            return None, "proxy-whitespace"

    def count(self, text: str) -> TokenCount:
        """Return the token count for ``text`` using the active backend."""
        if not text:
            return TokenCount(tokens=0, backend=self._backend)
        if self._backend == "tiktoken" and self._impl is not None:
            try:
                return TokenCount(tokens=len(self._impl.encode(text)), backend="tiktoken")  # type: ignore[union-attr]
            except Exception:
                if not self._allow_fallback:
                    raise
                pass
        if self._backend == "sentencepiece" and self._impl is not None:
            try:
                return TokenCount(
                    tokens=len(self._impl.encode(text, out_type=int)),  # type: ignore[union-attr]
                    backend="sentencepiece",
                )
            except Exception:
                if not self._allow_fallback:
                    raise
                pass
        if not self._allow_fallback:
            raise RuntimeError("the required tiktoken backend failed")
        return TokenCount(tokens=len(text.split()), backend="proxy-whitespace")
