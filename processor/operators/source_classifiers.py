"""Strict CPU inference for the team's four independent ModernBERT classifiers."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

from prometheus_client import Counter, Histogram

from processor.operators.quality import QualityScore

FAMILY = "source-pretrain-quality"
MAX_LENGTH = 8192
STRIDE = 512
ARXIV_DIAGNOSTIC_TASKS = ("arxiv-math-reasoning", "arxiv-posttrain-suitability")
TASKS = ("arxiv-pretrain-quality", "hf-pretrain-quality", *ARXIV_DIAGNOSTIC_TASKS)
HEAD_SECONDS = Histogram(
    "s2p_classifier_head_seconds",
    "Full section inference including every window.",
    ["task"],
    buckets=(0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 180, 600),
)
HEAD_TOKENS = Counter("s2p_classifier_head_tokens_total", "Scored unique tokens.", ["task"])
HEAD_WINDOWS = Counter("s2p_classifier_head_windows_total", "Scored model windows.", ["task"])
HEAD_SCORES = Histogram(
    "s2p_classifier_head_score",
    "Live section scores before document aggregation.",
    ["task"],
    buckets=(0, 0.5, 1, 1.5, 2, 2.5, 3, 3.5, 4, 4.5, 5),
)


def bundle_revision(manifest: dict[str, Any]) -> str:
    digest = hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest()
    return f"{manifest['version']}@sha256:{digest}"


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
        self.revision = bundle_revision(manifest)
        self.models: dict[str, Any] = {}
        self.tokenizers: dict[str, Any] = {}
        self.model_revisions: dict[str, str] = {}
        for task in TASKS:
            path = root / task
            config = json.loads((path / "config.json").read_text())
            if config.get("stream2pretrain_task") != task or len(config["id2label"]) != 6:
                raise RuntimeError(f"Unexpected task or head in {path}")
            self.model_revisions[task] = str(manifest["models"][task]["revision"])
            self.tokenizers[task] = AutoTokenizer.from_pretrained(path, local_files_only=True)
            model = AutoModelForSequenceClassification.from_pretrained(
                path,
                local_files_only=True,
                use_safetensors=True,
                attn_implementation="sdpa",
                reference_compile=False,
            )
            model.eval()
            self.models[task] = model

    def score(self, text: str) -> QualityScore:
        source = next((key for key in ("arxiv", "hf") if text.startswith(f"[SOURCE={key}] ")), None)
        if source is None:
            raise ValueError("Quality input must use the trained source/section header")
        return self._score_task(f"{source}-pretrain-quality", text)

    def score_posttrain(self, text: str) -> QualityScore:
        """Second stage, invoked only after the full paper passes quality."""
        if not text.startswith("[SOURCE=arxiv] "):
            raise ValueError("Post-training classifiers only accept arXiv sections")
        results = {task: self._score_task(task, text) for task in ARXIV_DIAGNOSTIC_TASKS}
        return replace(
            results["arxiv-posttrain-suitability"],
            diagnostic_scores={task: asdict(value) for task, value in results.items()},
        )

    def _score_task(self, task: str, text: str) -> QualityScore:
        import torch

        started = time.monotonic()
        encoded = self.tokenizers[task](
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
                result = self.models[task](
                    input_ids=torch.tensor([ids], dtype=torch.long),
                    attention_mask=torch.tensor([mask], dtype=torch.long),
                )
                logits.append(result.logits[0].float().tolist())
                lengths.append(len(ids))
        mean_logits = [sum(values) / len(logits) for values in zip(*logits, strict=True)]
        score, confidence, score_class, probabilities = ordinal_output(mean_logits)
        result = QualityScore(
            edu_score=score,
            revision=self.revision,
            confidence=confidence,
            score_class=score_class,
            probabilities=probabilities,
            tokens=max(1, sum(lengths) - STRIDE * max(0, len(lengths) - 1)),
            chunks=len(lengths),
            model_revision=self.model_revisions[task],
        )
        HEAD_SECONDS.labels(task).observe(time.monotonic() - started)
        HEAD_TOKENS.labels(task).inc(result.tokens)
        HEAD_WINDOWS.labels(task).inc(result.chunks)
        HEAD_SCORES.labels(task).observe(result.edu_score)
        return result


class SourcePosttrainClassifier:
    """Reuse the loaded encoders with a separately cacheable second-stage API."""

    def __init__(self, classifier: SourceQualityClassifier) -> None:
        self.classifier = classifier

    @property
    def revision(self) -> str:
        return self.classifier.revision

    @property
    def backend(self) -> str:
        return self.classifier.backend

    def score(self, text: str) -> QualityScore:
        return self.classifier.score_posttrain(text)
