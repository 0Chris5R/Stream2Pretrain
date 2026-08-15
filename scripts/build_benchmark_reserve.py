"""Build the restricted Decon-Gate corpus from pinned public benchmark revisions.

The output contains benchmark content and must be mounted from a Kubernetes
Secret or access-controlled PVC. Only the hash, version, and counts are shown
in the cockpit. GPQA requires an accepted Hugging Face access request and an
``HF_TOKEN``; the command fails rather than silently creating partial coverage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from datasets import load_dataset

REVISIONS = {
    "MMLU": ("cais/mmlu", "c30699e8356da336a370243923dbaf21066bb9fe"),
    "GSM8K": ("openai/gsm8k", "740312add88f781978c0658806c59bc2815b9866"),
    "HumanEval": (
        "openai/openai_humaneval",
        "7dce6050a7d6d172f3cc5c32aa97f52fa1a2e544",
    ),
    "MATH": (
        "nlile/hendrycks-MATH-benchmark",
        "465bcdb36f5962aa3512891498966df785fc3c18",
    ),
    "GPQA": ("Idavidrein/gpqa", "633f5ee89ab8ad4522a9f850766b73f62147ffdd"),
}


def _text(*values: object) -> str:
    return "\n".join(str(value).strip() for value in values if str(value).strip())


def _mmlu(row: Mapping[str, Any]) -> str:
    return _text(row.get("question", ""), *(row.get("choices") or []))


def _gsm8k(row: Mapping[str, Any]) -> str:
    return _text(row.get("question", ""), row.get("answer", ""))


def _humaneval(row: Mapping[str, Any]) -> str:
    return _text(
        row.get("prompt", ""),
        row.get("canonical_solution", ""),
        row.get("test", ""),
    )


def _math(row: Mapping[str, Any]) -> str:
    return _text(row.get("problem", ""), row.get("solution", ""), row.get("answer", ""))


def _gpqa(row: Mapping[str, Any]) -> str:
    return _text(
        row.get("Question", row.get("question", "")),
        row.get("Correct Answer", row.get("correct_answer", "")),
        row.get("Incorrect Answer 1", row.get("incorrect_answer_1", "")),
        row.get("Incorrect Answer 2", row.get("incorrect_answer_2", "")),
        row.get("Incorrect Answer 3", row.get("incorrect_answer_3", "")),
    )


def _load(
    name: str,
    config: str,
    split: str,
    transform: Callable[[Mapping[str, Any]], str],
    *,
    token: str | None = None,
) -> list[str]:
    repo, revision = REVISIONS[name]
    dataset = load_dataset(repo, config, split=split, revision=revision, token=token)
    values = [transform(row).strip() for row in dataset]
    if not values or any(not value for value in values):
        raise RuntimeError(f"{name} produced missing or empty benchmark items")
    return values


def build_reserve(token: str | None) -> dict[str, list[str]]:
    """Download and normalize every required benchmark split."""
    if not token:
        raise RuntimeError("HF_TOKEN is required for the gated GPQA reserve")
    return {
        "MMLU": _load("MMLU", "all", "test", _mmlu),
        "GSM8K": _load("GSM8K", "main", "test", _gsm8k),
        "HumanEval": _load("HumanEval", "openai_humaneval", "test", _humaneval),
        "MATH": _load("MATH", "default", "test", _math),
        "GPQA": _load("GPQA", "gpqa_main", "train", _gpqa, token=token),
    }


def _canonical_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def write_reserve(output: Path, manifest_output: Path, reserve: dict[str, list[str]]) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    payload = _canonical_bytes(reserve)
    output.write_bytes(payload)
    manifest = {
        "kind": "restricted_reserve",
        "created_at": datetime.now(UTC).isoformat(),
        "corpus_sha256": hashlib.sha256(payload).hexdigest(),
        "item_count": sum(len(items) for items in reserve.values()),
        "per_benchmark_items": {name: len(items) for name, items in reserve.items()},
        "dataset_revisions": {
            name: {"repository": repository, "revision": revision}
            for name, (repository, revision) in REVISIONS.items()
        },
    }
    manifest_output.parent.mkdir(parents=True, exist_ok=True)
    manifest_output.write_bytes(_canonical_bytes(manifest))


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    reserve = build_reserve(os.environ.get("HF_TOKEN"))
    write_reserve(args.output, args.manifest, reserve)


if __name__ == "__main__":
    main()
