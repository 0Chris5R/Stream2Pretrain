"""Strict CPU inference for the team's two independent ModernBERT classifiers."""

from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path
from typing import Any

from processor.operators.quality import QualityScore

FAMILY = "source-pretrain-quality"
MAX_LENGTH = 8192
STRIDE = 512


def ordinal_output(logits: list[float]) -> tuple[float, float, int, tuple[float, ...]]:
    """Same six-bin expectation, entropy confidence and rounding as training."""
    if len(logits) != 6 or not all(math.isfinite(value) for value in logits):
        raise ValueError("The classifier must return six finite ordinal logits")
    exponentials = [math.exp(value - max(logits)) for value in logits]
    total = sum(exponentials)
    probabilities = tuple(value / total for value in exponentials)
    score = sum(index * value for index, value in enumerate(probabilities))
    entropy = -sum(value * math.log(max(value, 1e-12)) for value in probabilities)
    confidence = max(0.0, min(1.0, 1.0 - entropy / math.log(6)))
    return score, confidence, max(0, min(5, round(score))), probabilities


class SourceQualityClassifier:
    """Route section inputs to arXiv or HF weights, with no heuristic fallback."""

    backend = "transformers-cpu"
    is_model_loaded = True

    def __init__(self, model_path: str | Path) -> None:
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        root = Path(model_path)
        manifest = json.loads((root / "source-classifiers.json").read_text())
        digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
        self.revision = f"{manifest['version']}@sha256:{digest}"
        self.models: dict[str, Any] = {}
        self.tokenizers: dict[str, Any] = {}
        self.model_revisions: dict[str, str] = {}
        for source in ("arxiv", "hf"):
            task = f"{source}-pretrain-quality"
            path = root / task
            config = json.loads((path / "config.json").read_text())
            if config.get("stream2pretrain_task") != task or len(config["id2label"]) != 6:
                raise RuntimeError(f"Unexpected task or head in {path}")
            self.model_revisions[source] = str(manifest["models"][task]["revision"])
            self.tokenizers[source] = AutoTokenizer.from_pretrained(path, local_files_only=True)
            model = AutoModelForSequenceClassification.from_pretrained(
                path,
                local_files_only=True,
                use_safetensors=True,
                attn_implementation="sdpa",
                reference_compile=False,
            )
            model.eval()
            self.models[source] = model

    def score(self, text: str) -> QualityScore:
        import torch

        source = next((key for key in self.models if text.startswith(f"[SOURCE={key}] ")), None)
        if source is None:
            raise ValueError("Quality input must use the trained source/section header")
        encoded = self.tokenizers[source](
            text,
            truncation=True,
            max_length=MAX_LENGTH,
            stride=STRIDE,
            return_overflowing_tokens=True,
            return_length=True,
        )
        logits = []
        lengths = []
        # One chunk at a time bounds CPU memory even for very long sections.
        # No top/bottom sampling or discarded middle content.
        with torch.inference_mode():
            for ids, mask in zip(encoded["input_ids"], encoded["attention_mask"], strict=True):
                result = self.models[source](
                    input_ids=torch.tensor([ids], dtype=torch.long),
                    attention_mask=torch.tensor([mask], dtype=torch.long),
                )
                logits.append(result.logits[0].float().tolist())
                lengths.append(len(ids))
        mean_logits = [sum(values) / len(logits) for values in zip(*logits, strict=True)]
        score, confidence, score_class, probabilities = ordinal_output(mean_logits)
        return QualityScore(
            edu_score=score,
            revision=self.revision,
            confidence=confidence,
            score_class=score_class,
            probabilities=probabilities,
            tokens=max(1, sum(lengths) - STRIDE * max(0, len(lengths) - 1)),
            chunks=len(lengths),
            model_revision=self.model_revisions[source],
        )
