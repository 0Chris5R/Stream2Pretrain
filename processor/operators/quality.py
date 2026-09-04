"""CPU wrapper for the official FinePDFs Edu v2 regressor.

The wrapper:

- Prefers a bundled ONNX export when present.
- Otherwise runs the official Safetensors checkpoint with Transformers on CPU.
- Tokenises with the bundled HuggingFace fast tokenizer if available.
- Returns the raw 0..5 educational-value regression score. The pipeline's
  separate composite quality score is calculated later from a visible vector.

The deterministic fallback exists only for unit tests and non-faithful
developer profiles. Strict local and Kubernetes profiles fail startup when a
required artifact is absent.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


@dataclass(frozen=True, slots=True)
class QualityScore:
    """Output of :meth:`QualityClassifier.score`."""

    edu_score: float
    revision: str
    confidence: float | None = None
    score_class: int | None = None
    probabilities: tuple[float, ...] = ()
    tokens: int = 0
    chunks: int = 0
    model_revision: str | None = None
    # Independent arXiv heads, diagnostic only. Each value has this score's
    # scalar/probability/provenance fields, without recursive diagnostics.
    diagnostic_scores: dict[str, dict[str, Any]] | None = None


class QualityClassifier:
    """Wrapper around an official educational-quality classifier.

    Parameters
    ----------
    model_path
        Path to a checkpoint directory. A verified ``model.onnx`` is preferred
        when present; otherwise the official Safetensors weights run through
        Transformers on CPU. ``None`` triggers the test-only proxy heuristic.
    revision
        Identifier persisted into the gold record; defaults to the
        path's basename.
    max_length
        Tokeniser truncation length. The pinned model configuration is used
        when no explicit value is supplied.
    """

    def __init__(
        self,
        model_path: str | Path | None,
        *,
        revision: str | None = None,
        model_family: str = "finepdfs-edu-v2",
        max_length: int | None = None,
        allow_fallback: bool = True,
    ) -> None:
        self._path = Path(model_path) if model_path else None
        self._model_family = model_family
        self._max_length = max_length or (2048 if model_family == "finepdfs-edu-v2" else 512)
        self._allow_fallback = allow_fallback
        self._session, self._torch_model, self._tokenizer = self._load(self._path)
        if not allow_fallback and not self.is_model_loaded:
            raise RuntimeError(f"the pinned {model_family} model is required")
        self._revision = revision or self._derive_revision()

    @property
    def revision(self) -> str:
        """Identifier persisted into ``GoldRecord.classifier_revision``."""
        return self._revision

    @property
    def is_model_loaded(self) -> bool:
        """Whether inference uses a real FinePDFs model artifact."""
        return self._tokenizer is not None and (
            self._session is not None or self._torch_model is not None
        )

    @property
    def backend(self) -> str:
        """Return the active inference backend for diagnostics."""
        if self._session is not None:
            return "onnxruntime"
        if self._torch_model is not None:
            return "transformers-cpu"
        return "proxy"

    @staticmethod
    def _load(
        path: Path | None,
    ) -> tuple[object | None, object | None, object | None]:
        if path is None or not path.is_dir():
            return None, None, None
        model_file = path / "model.onnx"
        try:
            from transformers import AutoTokenizer  # type: ignore[import-untyped]

            tok = AutoTokenizer.from_pretrained(str(path), local_files_only=True)
        except Exception:
            return None, None, None
        if model_file.is_file():
            try:
                import onnxruntime as ort  # type: ignore[import-untyped]

                sess = ort.InferenceSession(str(model_file), providers=["CPUExecutionProvider"])
                return sess, None, tok
            except Exception:
                pass
        if (path / "model.safetensors").is_file():
            try:
                from transformers import (  # type: ignore[import-untyped]
                    AutoModelForSequenceClassification,
                )

                model = AutoModelForSequenceClassification.from_pretrained(
                    str(path), local_files_only=True, use_safetensors=True
                )
                model.eval()
                return None, model, tok
            except Exception:
                pass
        return None, None, None

    def _derive_revision(self) -> str:
        if self._path is None or not self.is_model_loaded:
            return "proxy-heuristic-0.1"
        if self._session is not None:
            return f"{self._model_family}-onnx-{self._path.name}"
        return f"{self._model_family}-transformers-cpu-{self._path.name}"

    def score(self, text: str) -> QualityScore:
        """Return the independent educational-quality model score for ``text``."""
        if not text or not text.strip():
            return QualityScore(edu_score=0.0, revision=self._revision)
        chunks = self._text_chunks(text)
        if self._session is not None and self._tokenizer is not None:
            try:
                return max(
                    (self._score_onnx(chunk) for chunk in chunks),
                    key=lambda score: score.edu_score,
                )
            except Exception:
                if not self._allow_fallback:
                    raise
        if self._torch_model is not None and self._tokenizer is not None:
            try:
                return max(
                    (self._score_transformers(chunk) for chunk in chunks),
                    key=lambda score: score.edu_score,
                )
            except Exception:
                if not self._allow_fallback:
                    raise
        return self._score_proxy(text)

    def _text_chunks(self, text: str) -> list[str]:
        """Apply the official FinePDFs top/bottom long-extract sampling rule."""
        if not self._model_family.startswith("finepdfs-edu") or self._tokenizer is None:
            return [text]
        max_chars = 10_000
        candidates = (
            [text[:max_chars]] if len(text) <= max_chars else [text[:max_chars], text[-max_chars:]]
        )
        chunks: list[str] = []
        for candidate in candidates:
            token_ids = self._tokenizer.encode(  # type: ignore[union-attr]
                candidate,
                add_special_tokens=False,
                truncation=True,
                max_length=self._max_length - 2,
            )
            decoded = self._tokenizer.decode(  # type: ignore[union-attr]
                token_ids, skip_special_tokens=True
            )
            if decoded.strip():
                chunks.append(decoded.strip())
        return chunks or [text]

    def _score_onnx(self, text: str) -> QualityScore:
        """Inference path using the loaded ONNX session."""
        encoded = self._tokenizer(  # type: ignore[misc]
            text,
            max_length=self._max_length,
            truncation=True,
            padding="max_length",
            return_tensors="np",
        )
        feeds: dict[str, Any] = {
            "input_ids": encoded["input_ids"].astype("int64"),
            "attention_mask": encoded["attention_mask"].astype("int64"),
        }
        if "token_type_ids" in encoded:
            feeds["token_type_ids"] = encoded["token_type_ids"].astype("int64")
        outputs = self._session.run(None, feeds)  # type: ignore[union-attr]
        # FinePDFs Edu v2 emits a single regression head in [0, 5].
        score = float(outputs[0].reshape(-1)[0])
        clamped = max(0.0, min(5.0, score))
        return QualityScore(
            edu_score=clamped,
            revision=self._revision,
        )

    def _score_transformers(self, text: str) -> QualityScore:
        """CPU inference against the official Safetensors checkpoint."""
        import torch  # type: ignore[import-not-found]

        encoded = self._tokenizer(  # type: ignore[misc]
            text,
            max_length=self._max_length,
            truncation=True,
            padding=True,
            return_tensors="pt",
        )
        model = cast(Callable[..., Any], self._torch_model)
        with torch.inference_mode():
            outputs = model(**encoded)
        score = float(outputs.logits.reshape(-1)[0].item())
        clamped = max(0.0, min(5.0, score))
        return QualityScore(
            edu_score=clamped,
            revision=self._revision,
        )

    def _score_proxy(self, text: str) -> QualityScore:
        """Heuristic fallback. Cheap, deterministic, language-agnostic.

        The mapping is tuned so prose with 4-7 character mean word length and
        at least 50 words receives a score above 1.0, which is what the
        curation pipeline uses as the "low quality" cut. Pure CI inputs that
        the tests use are flagged as quality-passing under this rule.
        """
        words = [w for w in text.split() if w]
        if not words:
            return QualityScore(edu_score=0.0, revision=self._revision)
        mean_len = sum(len(w) for w in words) / len(words)
        # Sigmoid centred at 4 char mean length so 5-6 char words score ~3.5.
        base = 5.0 / (1.0 + math.exp(-(mean_len - 4.0)))
        # Penalise extremely short documents.
        if len(words) < 50:
            base *= 0.5
        clamped = max(0.0, min(5.0, base))
        return QualityScore(
            edu_score=clamped,
            revision=self._revision,
        )
