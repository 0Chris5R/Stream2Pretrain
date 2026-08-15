"""Compare pinned FinePDFs Edu v1 and v2 on the same document sample.

Models are loaded sequentially so the CPU pilot never holds both ModernBERT
checkpoints in RAM. Input is JSONL with ``doc_id`` and ``text``; an optional
``expected_score`` enables MAE reporting against reviewed labels.
"""

from __future__ import annotations

import argparse
import gc
import json
import statistics
from pathlib import Path
from typing import Any

from huggingface_hub import snapshot_download

from processor.operators.quality import QualityClassifier

MODELS = {
    "v1": (
        "HuggingFaceFW/finepdfs_edu_classifier_eng_Latn",
        "d1d20d432b6588831bfec203e11aeb9195ef32fd",
    ),
    "v2": (
        "HuggingFaceFW/finepdfs_edu_classifier_v2_eng_Latn",
        "90ddef285f67230389057c14b2f6bbfeb70d40ea",
    ),
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path, help="JSONL with doc_id and text")
    parser.add_argument("--cache-dir", type=Path, default=Path(".cache/finepdfs-comparison"))
    parser.add_argument(
        "--v1-path",
        type=Path,
        help="Use an existing pinned v1 directory instead of downloading it",
    )
    parser.add_argument(
        "--v2-path",
        type=Path,
        help="Use an existing pinned v2 directory instead of downloading it",
    )
    parser.add_argument("--output", type=Path, default=Path("finepdfs-comparison.json"))
    parser.add_argument("--limit", type=int, default=64)
    parser.add_argument(
        "--sample-unit",
        choices=("document", "section"),
        default="document",
        help="Describe the input rows in the generated report",
    )
    return parser.parse_args()


def _records(path: Path, limit: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            row = json.loads(line)
            if row.get("doc_id") and row.get("text"):
                rows.append(row)
            if len(rows) >= limit:
                break
    if not rows:
        raise ValueError("input contains no rows with doc_id and text")
    return rows


def _model_path(cache_dir: Path, version: str, existing: Path | None) -> Path:
    if existing is not None:
        if not (existing / "config.json").exists():
            raise ValueError(f"{version} model directory is incomplete: {existing}")
        return existing
    repo_id, revision = MODELS[version]
    target = cache_dir / version
    snapshot_download(
        repo_id=repo_id,
        revision=revision,
        local_dir=target,
        allow_patterns=["*.json", "*.txt", "*.safetensors"],
    )
    return target


def _score(
    rows: list[dict[str, Any]],
    cache_dir: Path,
    version: str,
    existing: Path | None = None,
) -> list[float]:
    classifier = QualityClassifier(
        _model_path(cache_dir, version, existing),
        revision=f"finepdfs-edu-{version}@{MODELS[version][1]}",
        model_family=f"finepdfs-edu-{version}",
        allow_fallback=False,
    )
    values = [classifier.score(str(row["text"])).edu_score for row in rows]
    del classifier
    gc.collect()
    return values


def _mean_absolute_error(expected: list[float], observed: list[float]) -> float:
    return statistics.fmean(
        abs(left - right) for left, right in zip(expected, observed, strict=True)
    )


def main() -> None:
    args = _arguments()
    rows = _records(args.input, max(1, args.limit))
    paths = {"v1": args.v1_path, "v2": args.v2_path}
    scores = {
        version: _score(rows, args.cache_dir, version, paths[version]) for version in ("v1", "v2")
    }
    labelled = all(row.get("expected_score") is not None for row in rows)
    report: dict[str, Any] = {
        "models": {
            version: {"repo_id": MODELS[version][0], "revision": MODELS[version][1]}
            for version in MODELS
        },
        "sample_size": len(rows),
        "sample_unit": args.sample_unit,
        "summary": {
            version: {
                "mean": statistics.fmean(values),
                "median": statistics.median(values),
            }
            for version, values in scores.items()
        },
        "comparison": {
            "mean_v2_minus_v1": statistics.fmean(
                right - left for left, right in zip(scores["v1"], scores["v2"], strict=True)
            ),
            "v2_higher_count": sum(
                right > left for left, right in zip(scores["v1"], scores["v2"], strict=True)
            ),
        },
        "samples": [
            {
                "doc_id": row["doc_id"],
                "expected_score": row.get("expected_score"),
                "metadata": {
                    key: value
                    for key, value in row.items()
                    if key not in {"doc_id", "text", "expected_score"}
                },
                "v1": scores["v1"][index],
                "v2": scores["v2"][index],
                "v2_minus_v1": scores["v2"][index] - scores["v1"][index],
            }
            for index, row in enumerate(rows)
        ],
    }
    if labelled:
        expected = [float(row["expected_score"]) for row in rows]
        report["summary"]["v1"]["mae"] = _mean_absolute_error(expected, scores["v1"])
        report["summary"]["v2"]["mae"] = _mean_absolute_error(expected, scores["v2"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
