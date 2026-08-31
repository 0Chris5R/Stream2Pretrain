"""Pinned public classifiers used by the non-gating shadow evaluation lane."""

from __future__ import annotations

import math
import os
from pathlib import Path
from typing import Any


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    value = int(raw) if raw else default
    if value < 1:
        raise RuntimeError(f"{name} must be positive")
    return value


def _evenly_spaced(items: list[list[int]], limit: int) -> list[list[int]]:
    if len(items) <= limit:
        return items
    if limit == 1:
        return [items[len(items) // 2]]
    indices = {round(position * (len(items) - 1) / (limit - 1)) for position in range(limit)}
    return [items[index] for index in sorted(indices)]


class TransformerShadowScorer:
    """Score substantial text spans and retain transparent aggregation details."""

    def __init__(
        self,
        model_dir: str | Path,
        *,
        family: str,
        revision: str,
        max_length: int,
        max_chunks: int,
        stride: int,
    ) -> None:
        import torch
        from transformers import AutoModelForSequenceClassification, AutoTokenizer

        self.family = family
        self.revision = revision
        self.max_length = max_length
        self.max_chunks = max_chunks
        self.stride = stride
        self.tokenizer = AutoTokenizer.from_pretrained(
            str(model_dir), local_files_only=True, trust_remote_code=False
        )
        self.model = AutoModelForSequenceClassification.from_pretrained(
            str(model_dir), local_files_only=True, trust_remote_code=False
        )
        self.model.eval()
        torch.set_num_threads(_positive_int_env("S2P_SHADOW_TORCH_THREADS", 2))

    def score(self, text: str) -> dict[str, Any]:
        import torch

        token_ids = self.tokenizer(text, add_special_tokens=False)["input_ids"]
        if not isinstance(token_ids, list):
            raise RuntimeError("shadow tokenizer returned an invalid token sequence")
        content_size = max(1, self.max_length - self.tokenizer.num_special_tokens_to_add())
        step = max(1, content_size - self.stride)
        all_chunks = [
            token_ids[start : start + content_size] for start in range(0, len(token_ids), step)
        ]
        if not all_chunks:
            all_chunks = [[]]
        chunks = _evenly_spaced(all_chunks, self.max_chunks)
        scores: list[float] = []
        batch_size = _positive_int_env("S2P_SHADOW_INFERENCE_BATCH_SIZE", 2)
        with torch.inference_mode():
            for start in range(0, len(chunks), batch_size):
                features = [
                    self.tokenizer.prepare_for_model(
                        chunk,
                        truncation=True,
                        max_length=self.max_length,
                        return_attention_mask=True,
                    )
                    for chunk in chunks[start : start + batch_size]
                ]
                inputs = self.tokenizer.pad(features, padding=True, return_tensors="pt")
                logits = self.model(**inputs).logits.detach().float().cpu()
                if self.family == "meta-rater-reasoning":
                    probabilities = torch.softmax(logits, dim=-1)
                    classes = torch.arange(probabilities.shape[-1], dtype=probabilities.dtype)
                    scores.extend(
                        float(value) for value in (probabilities * classes).sum(dim=-1).tolist()
                    )
                elif self.family == "finemath":
                    scores.extend(float(value) for value in logits.reshape(-1).tolist())
                else:  # pragma: no cover - constructor contract
                    raise RuntimeError(f"unsupported transformer shadow family: {self.family}")
        bounded = [max(0.0, min(5.0, value)) for value in scores]
        ordered = sorted(bounded)
        p90_index = min(len(ordered) - 1, math.ceil(0.9 * len(ordered)) - 1)
        top_count = max(1, math.ceil(len(ordered) / 4))
        return {
            "model_family": self.family,
            "model_revision": self.revision,
            "score": sum(bounded) / len(bounded),
            "max_score": max(bounded),
            "p90_score": ordered[p90_index],
            "top_quartile_mean": sum(ordered[-top_count:]) / top_count,
            "chunk_scores": bounded,
            "input_tokens": len(token_ids),
            "total_chunks": len(all_chunks),
            "scored_chunks": len(chunks),
            "coverage_ratio": min(1.0, len(chunks) / len(all_chunks)),
            "max_length": self.max_length,
            "stride": self.stride,
        }


class CsoShadowClassifier:
    """Run the public CSO 4.0.1 topic classifier over the retained document text."""

    revision = "cso-classifier-4.0.1+ontology-3.5+cached-model-v2"

    def __init__(self) -> None:
        from cso_classifier import CSOClassifier

        self.classifier = CSOClassifier(
            modules="both",
            enhancement="first",
            explanation=False,
            delete_outliers=False,
            fast_classification=True,
            get_weights=True,
            silent=True,
        )

    def score(self, text: str) -> dict[str, Any]:
        result = self.classifier.run(text)
        topics = sorted(
            {str(topic) for field in ("union", "enhanced") for topic in result.get(field, [])}
        )
        syntactic_weights = {
            str(key): float(value) for key, value in result.get("syntactic_weights", {}).items()
        }
        semantic_weights = {
            str(key): float(value) for key, value in result.get("semantic_weights", {}).items()
        }
        return {
            "model_family": "cso-topics",
            "model_revision": self.revision,
            "topics": topics[:100],
            "topic_count": len(topics),
            "syntactic_weights": dict(sorted(syntactic_weights.items())[:100]),
            "semantic_weights": dict(sorted(semantic_weights.items())[:100]),
            "input_characters": len(text),
        }


__all__ = ["CsoShadowClassifier", "TransformerShadowScorer"]
