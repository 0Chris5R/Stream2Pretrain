from __future__ import annotations

import gzip
import json
from pathlib import Path

from scripts.build_classifier_training_data import build


def _request(custom_id: str, document_id: str, source_feed: str) -> dict:
    payload = {
        "evaluation_date": "2026-09-01",
        "document_id": document_id,
        "source_feed": source_feed,
        "pipeline": {"projection_version": "v2", "scoring_version": "v2"},
        "sections": [
            {
                "section_id": "section-1",
                "title": "Methods",
                "section_type": "methods",
                "text": "A complete technical section.",
            }
        ],
    }
    return {
        "custom_id": custom_id,
        "body": {
            "input": [
                {"role": "developer", "content": [{"type": "input_text", "text": "judge"}]},
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": json.dumps(payload)}],
                },
            ]
        },
    }


def _response(custom_id: str, document_id: str, *, arxiv: bool) -> dict:
    document_labels = {"pretrain_quality": 4, "confidence": 0.9, "rationale": "good"}
    section = {
        "section_id": "section-1",
        "pretrain_quality": 4,
        "confidence": 0.9,
        "defect_flags": ["none"],
        "rationale": "good",
    }
    if arxiv:
        document_labels.update(
            {
                "math_reasoning": 3,
                "posttrain_suitability": 5,
                "best_math_section_ids": ["section-1"],
                "best_posttrain_section_ids": ["section-1"],
            }
        )
        section.update({"math_reasoning": 3, "posttrain_suitability": 5})
    return {
        "custom_id": custom_id,
        "response_id": f"response-{custom_id}",
        "model": "gpt-5.6-luna",
        "output": {
            "document_id": document_id,
            "document_labels": document_labels,
            "sections": [section],
        },
    }


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def test_build_joins_labels_and_splits_only_by_document(tmp_path: Path) -> None:
    requests = tmp_path / "requests.jsonl"
    responses = tmp_path / "responses.jsonl"
    output = tmp_path / "labels.jsonl.gz"
    manifest_path = tmp_path / "manifest.json"
    request_rows = [
        _request(f"arxiv-{index}", f"doc-a-{index}", "arxiv-html-fetcher") for index in range(10)
    ] + [_request(f"hf-{index}", f"doc-h-{index}", "hf-models") for index in range(10)]
    response_rows = [
        _response(f"arxiv-{index}", f"doc-a-{index}", arxiv=True) for index in range(10)
    ] + [_response(f"hf-{index}", f"doc-h-{index}", arxiv=False) for index in range(10)]
    _write_jsonl(requests, request_rows)
    _write_jsonl(responses, response_rows)

    manifest = build([requests], responses, output, manifest_path)

    with gzip.open(output, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    assert len(rows) == 20
    assert manifest["audit"]["emitted_sections"] == 20
    assert {row["split"] for row in rows} == {"train", "test"}
    assert all(row["model_input"].endswith("A complete technical section.") for row in rows)
    arxiv = next(row for row in rows if row["source_family"] == "arxiv")
    assert arxiv["label_math"] == 3
    assert arxiv["label_posttrain"] == 5
    hf = next(row for row in rows if row["source_family"] == "hf")
    assert hf["label_math"] is None
    assert hf["label_posttrain"] is None


def test_build_discards_unrequested_and_duplicate_section_labels(tmp_path: Path) -> None:
    requests = tmp_path / "requests.jsonl"
    responses = tmp_path / "responses.jsonl"
    output = tmp_path / "labels.jsonl.gz"
    manifest_path = tmp_path / "manifest.json"
    request = _request("arxiv-1", "doc-1", "arxiv-html-fetcher")
    response = _response("arxiv-1", "mistyped-doc-1", arxiv=True)
    response["output"]["sections"].append(dict(response["output"]["sections"][0]))
    response["output"]["sections"].append(
        {
            **response["output"]["sections"][0],
            "section_id": "figure-1",
        }
    )
    _write_jsonl(requests, [request])
    _write_jsonl(responses, [response])

    manifest = build([requests], responses, output, manifest_path)

    with gzip.open(output, "rt", encoding="utf-8") as handle:
        assert list(handle) == []
    assert manifest["audit"]["mismatched_document_id"] == 1
    assert manifest["audit"]["duplicate_section_labels"] == 1
    assert manifest["audit"]["extra_section_labels"] == 1
