"""Join prepared source text with Luna labels for source-specific classifiers."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import random
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "stream2pretrain-section-labels-v1"
SPLIT_SEED = 20260901


def _iter_jsonl(paths: Iterable[Path]) -> Iterator[dict[str, Any]]:
    for path in paths:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                try:
                    value = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{path}:{line_number}: invalid JSON") from exc
                if not isinstance(value, dict):
                    raise ValueError(f"{path}:{line_number}: expected an object")
                yield value


def _request_payload(request: dict[str, Any]) -> dict[str, Any]:
    try:
        text = request["body"]["input"][1]["content"][0]["text"]
        payload = json.loads(text)
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{request.get('custom_id')}: malformed prepared request") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{request.get('custom_id')}: request payload is not an object")
    return payload


def _label(value: Any, *, name: str, custom_id: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 5:
        raise ValueError(f"{custom_id}: invalid {name} label {value!r}")
    return value


def _confidence(value: Any, *, custom_id: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{custom_id}: invalid confidence {value!r}")
    result = float(value)
    if not 0.0 <= result <= 1.0:
        raise ValueError(f"{custom_id}: confidence outside [0, 1]")
    return result


def _split_stratum(source_family: str, source_feed: str, labels: dict[str, Any]) -> str:
    pretrain = int(labels["pretrain_quality"])
    if source_family == "arxiv":
        pretrain_band = "low" if pretrain <= 2 else str(pretrain)
        return f"arxiv:{pretrain_band}"
    return f"{source_feed}:{pretrain}"


def _assign_document_splits(
    documents: dict[str, tuple[str, str, dict[str, Any]]],
) -> dict[str, str]:
    """Assign 90/10 train/test splits within source/quality document strata."""

    strata: dict[str, list[str]] = defaultdict(list)
    for document_id, (source_family, source_feed, labels) in documents.items():
        strata[_split_stratum(source_family, source_feed, labels)].append(document_id)

    assignments: dict[str, str] = {}
    for stratum, document_ids in sorted(strata.items()):
        rng = random.Random(f"{SPLIT_SEED}:{stratum}")
        rng.shuffle(document_ids)
        count = len(document_ids)
        test_count = max(1, round(count * 0.1)) if count >= 2 else 0
        for index, document_id in enumerate(document_ids):
            split = "test" if index < test_count else "train"
            assignments[document_id] = split
    return assignments


@dataclass
class Audit:
    requests: int = 0
    responses: int = 0
    emitted_sections: int = 0
    missing_response: int = 0
    mismatched_document_id: int = 0
    documents_with_extra_labels: int = 0
    extra_section_labels: int = 0
    documents_with_missing_labels: int = 0
    missing_section_labels: int = 0
    duplicate_section_labels: int = 0


def build(
    request_paths: list[Path],
    response_path: Path,
    output_path: Path,
    manifest_path: Path,
) -> dict[str, Any]:
    requests: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for request in _iter_jsonl(request_paths):
        custom_id = str(request.get("custom_id", ""))
        if not custom_id or custom_id in requests:
            raise ValueError(f"missing or duplicate request custom_id {custom_id!r}")
        requests[custom_id] = (request, _request_payload(request))

    responses: dict[str, dict[str, Any]] = {}
    response_meta: dict[str, dict[str, Any]] = {}
    for response in _iter_jsonl([response_path]):
        custom_id = str(response.get("custom_id", ""))
        output = response.get("output")
        if not custom_id or custom_id in responses or not isinstance(output, dict):
            raise ValueError(f"missing, duplicate, or malformed response {custom_id!r}")
        responses[custom_id] = output
        response_meta[custom_id] = response

    unknown = sorted(set(responses) - set(requests))
    if unknown:
        raise ValueError(f"responses contain {len(unknown)} unknown custom IDs")

    audit = Audit(requests=len(requests), responses=len(responses))
    documents: dict[str, tuple[str, str, dict[str, Any]]] = {}
    prepared: list[dict[str, Any]] = []
    source_documents: Counter[str] = Counter()
    source_sections: Counter[str] = Counter()
    label_distributions: dict[str, Counter[int]] = defaultdict(Counter)

    for custom_id, (_request, payload) in requests.items():
        output = responses.get(custom_id)
        if output is None:
            audit.missing_response += 1
            continue

        document_id = str(payload["document_id"])
        source_feed = str(payload["source_feed"])
        source_family = "arxiv" if source_feed == "arxiv-html-fetcher" else "hf"
        document_labels = output.get("document_labels")
        sections = output.get("sections")
        if not isinstance(document_labels, dict) or not isinstance(sections, list):
            raise ValueError(f"{custom_id}: malformed label output")
        if str(output.get("document_id")) != document_id:
            audit.mismatched_document_id += 1

        document_labels = {
            "pretrain_quality": _label(
                document_labels.get("pretrain_quality"),
                name="document pretrain_quality",
                custom_id=custom_id,
            ),
            "math_reasoning": (
                _label(
                    document_labels.get("math_reasoning"),
                    name="document math_reasoning",
                    custom_id=custom_id,
                )
                if source_family == "arxiv"
                else None
            ),
            "posttrain_suitability": (
                _label(
                    document_labels.get("posttrain_suitability"),
                    name="document posttrain_suitability",
                    custom_id=custom_id,
                )
                if source_family == "arxiv"
                else None
            ),
        }
        if document_id in documents:
            raise ValueError(f"duplicate document_id {document_id}")
        documents[document_id] = (source_family, source_feed, document_labels)
        source_documents[source_feed] += 1

        output_by_id: dict[str, dict[str, Any]] = {}
        duplicate_ids: set[str] = set()
        for section_label in sections:
            if not isinstance(section_label, dict):
                raise ValueError(f"{custom_id}: section label is not an object")
            section_id = str(section_label.get("section_id", ""))
            if section_id in output_by_id:
                duplicate_ids.add(section_id)
            else:
                output_by_id[section_id] = section_label
        audit.duplicate_section_labels += len(duplicate_ids)

        input_sections = payload.get("sections")
        if not isinstance(input_sections, list):
            raise ValueError(f"{custom_id}: input sections are not a list")
        input_ids = {str(section["section_id"]) for section in input_sections}
        extra_ids = set(output_by_id) - input_ids
        if extra_ids:
            audit.documents_with_extra_labels += 1
            audit.extra_section_labels += len(extra_ids)
        missing_ids = input_ids - set(output_by_id)
        if missing_ids:
            audit.documents_with_missing_labels += 1
            audit.missing_section_labels += len(missing_ids)

        meta = response_meta[custom_id]
        for section in input_sections:
            section_id = str(section["section_id"])
            section_label = output_by_id.get(section_id)
            if section_label is None or section_id in duplicate_ids:
                continue
            title = str(section.get("title") or "Document body")
            section_type = str(section.get("section_type") or "other")
            text = str(section.get("text") or "").strip()
            if not text:
                continue
            pretrain_quality = _label(
                section_label.get("pretrain_quality"),
                name="section pretrain_quality",
                custom_id=custom_id,
            )
            math_reasoning = (
                _label(
                    section_label.get("math_reasoning"),
                    name="section math_reasoning",
                    custom_id=custom_id,
                )
                if source_family == "arxiv"
                else None
            )
            posttrain_suitability = (
                _label(
                    section_label.get("posttrain_suitability"),
                    name="section posttrain_suitability",
                    custom_id=custom_id,
                )
                if source_family == "arxiv"
                else None
            )
            confidence = _confidence(section_label.get("confidence"), custom_id=custom_id)
            model_input = (
                f"[SOURCE={source_family}] [SECTION_TYPE={section_type}] "
                f"[SECTION_TITLE={title}]\n{text}"
            )
            prepared.append(
                {
                    "schema_version": SCHEMA_VERSION,
                    "custom_id": custom_id,
                    "document_id": document_id,
                    "source_family": source_family,
                    "source_feed": source_feed,
                    "section_id": section_id,
                    "section_title": title,
                    "section_type": section_type,
                    "text": text,
                    "model_input": model_input,
                    "label_pretrain": pretrain_quality,
                    "label_math": math_reasoning,
                    "label_posttrain": posttrain_suitability,
                    "label_confidence": confidence,
                    "defect_flags": list(section_label.get("defect_flags") or []),
                    "label_rationale": str(section_label.get("rationale") or ""),
                    "document_pretrain": document_labels["pretrain_quality"],
                    "document_math": document_labels["math_reasoning"],
                    "document_posttrain": document_labels["posttrain_suitability"],
                    "evaluation_date": str(payload.get("evaluation_date") or ""),
                    "projection_version": str(
                        (payload.get("pipeline") or {}).get("projection_version") or ""
                    ),
                    "scoring_version": str(
                        (payload.get("pipeline") or {}).get("scoring_version") or ""
                    ),
                    "judge_model": str(meta.get("model") or ""),
                    "judge_response_id": str(meta.get("response_id") or ""),
                }
            )
            audit.emitted_sections += 1
            source_sections[source_feed] += 1
            label_distributions[f"{source_feed}:pretrain"][pretrain_quality] += 1
            if source_family == "arxiv":
                label_distributions["arxiv:math"][int(math_reasoning)] += 1
                label_distributions["arxiv:posttrain"][int(posttrain_suitability)] += 1

    assignments = _assign_document_splits(documents)
    split_documents: Counter[str] = Counter(assignments.values())
    split_sections: Counter[str] = Counter()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    with gzip.open(output_path, "wt", encoding="utf-8", compresslevel=6) as handle:
        for row in prepared:
            row["split"] = assignments[row["document_id"]]
            split_sections[row["split"]] += 1
            encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            handle.write(encoded)
            digest.update(encoded.encode())

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "created_at": datetime.now(UTC).isoformat(),
        "split_seed": SPLIT_SEED,
        "output": str(output_path),
        "uncompressed_sha256": digest.hexdigest(),
        "compressed_bytes": output_path.stat().st_size,
        "audit": audit.__dict__,
        "source_documents": dict(sorted(source_documents.items())),
        "source_sections": dict(sorted(source_sections.items())),
        "split_documents": dict(sorted(split_documents.items())),
        "split_sections": dict(sorted(split_sections.items())),
        "label_distributions": {
            name: {str(score): count for score, count in sorted(distribution.items())}
            for name, distribution in sorted(label_distributions.items())
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--request", action="append", type=Path, required=True)
    parser.add_argument("--responses", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    result = build(args.request, args.responses, args.output, args.manifest)
    print(json.dumps(result, indent=2, sort_keys=True))
