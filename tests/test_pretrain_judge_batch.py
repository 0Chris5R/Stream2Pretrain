from __future__ import annotations

import json
from pathlib import Path

from scripts import pretrain_judge_batch
from scripts.pretrain_judge_batch import (
    _auth_headers,
    batch_status,
    build_request,
    parse_sections,
    prepare,
    response_schema,
    wait_for_file,
)

ROOT = Path(__file__).parents[1]


def _row(*, source: str, doc_id: str = "sha256:" + "a" * 64) -> dict[str, object]:
    return {
        "doc_id": doc_id,
        "source_feed": source,
        "source_format": "html",
        "text": "# Useful title\n\n## Introduction\nMotivation and contribution.\n\n## Methods\nExact method.",
        "valid_from": "2026-09-01T00:00:00Z",
        "tokens": 20,
        "edu_score": 3.5,
        "projection_version": "scientific-body-v3",
        "scoring_version": "pretrain-content-v3",
    }


def test_parse_sections_preserves_complete_projection() -> None:
    title, sections = parse_sections(
        str(_row(source="arxiv-html-fetcher")["text"]), source="arxiv-html-fetcher"
    )
    assert title == "Useful title"
    assert [section.title for section in sections] == ["Introduction", "Methods"]
    assert [section.section_type for section in sections] == ["introduction", "methods"]
    assert "Motivation and contribution." in sections[0].text


def test_arxiv_request_contains_role_aware_labels_and_full_sections() -> None:
    request = build_request(
        _row(source="arxiv-html-fetcher"), index=1, evaluation_date="2026-09-01"
    )
    body = request["body"]
    developer_prompt = body["input"][0]["content"][0]["text"]
    payload = json.loads(body["input"][1]["content"][0]["text"])
    schema = body["text"]["format"]["schema"]
    assert "An abstract should" in developer_prompt
    assert "Evaluation date: 2026-09-01" in developer_prompt
    assert payload["complete_input"] is True
    assert "finepdfs_edu_v2" not in payload["pipeline"]
    assert "route" not in payload
    assert "license" not in payload
    assert "reject_reasons" not in payload
    assert [section["text"] for section in payload["sections"]] == [
        "Motivation and contribution.",
        "Exact method.",
    ]
    assert "math_reasoning" in schema["properties"]["sections"]["items"]["properties"]
    assert "posttrain_suitability" in schema["properties"]["document_labels"]["properties"]


def test_hf_request_has_only_pretrain_quality_labels() -> None:
    request = build_request(_row(source="hf-models"), index=2, evaluation_date="2026-09-01")
    schema = request["body"]["text"]["format"]["schema"]
    section_properties = schema["properties"]["sections"]["items"]["properties"]
    assert "pretrain_quality" in section_properties
    assert "math_reasoning" not in section_properties
    assert "posttrain_suitability" not in section_properties


def test_response_schema_is_strict_at_every_object_level() -> None:
    schema = response_schema("arxiv-html-fetcher")
    assert schema["additionalProperties"] is False
    assert schema["properties"]["document_labels"]["additionalProperties"] is False
    assert schema["properties"]["sections"]["items"]["additionalProperties"] is False


def test_prepare_writes_reproducible_manifest_and_requests(tmp_path: Path) -> None:
    input_path = tmp_path / "pool.jsonl"
    rows = [
        _row(source="arxiv-html-fetcher", doc_id="sha256:" + "a" * 64),
        _row(source="hf-datasets", doc_id="sha256:" + "b" * 64),
    ]
    input_path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    output_dir = tmp_path / "batch"
    manifest = prepare(input_path, output_dir, evaluation_date="2026-09-01")
    assert manifest["requests"] == 2
    assert manifest["source_counts"] == {"arxiv-html-fetcher": 1, "hf-datasets": 1}
    batch_path = output_dir / manifest["files"][0]["path"]
    requests = [json.loads(line) for line in batch_path.read_text().splitlines()]
    assert len(requests) == 2
    assert len({request["custom_id"] for request in requests}) == 2
    assert all(request["body"]["model"] == "gpt-5.6-luna" for request in requests)


def test_exporter_uses_exact_processed_full_text_pool_without_route_filtering() -> None:
    source = (ROOT / "scripts/export_pretrain_judge_pool.py").read_text()
    assert "LENGTH(TRIM(decision.text)) > 0" in source
    assert "PARTITION BY doc_id" in source
    assert "WHERE revision_rank = 1" in source
    assert "judge_selected_keys" in source
    assert "fetchmany(8)" in source
    assert "limit + 500" in source
    assert "route = 'pretrain'" not in source
    assert "risk_tier = 1" not in source
    assert "ARRAY_LENGTH(reject_reasons) = 0" not in source
    assert "PERMISSIVE_TRAINING_LICENSES" not in source


def test_label_workflow_isolates_historical_export_from_live_dashboard() -> None:
    workflow = (ROOT / ".github/workflows/deploy-main.yml").read_text()
    assert "OPENAI_PROJECT_ID: ${{ secrets.OPENAI_PROJECT_ID }}" in workflow
    assert "OPENAI_ORG_ID: ${{ secrets.OPENAI_ORG_ID }}" in workflow
    assert '"${OPENAI_PROJECT_ID:?Missing OPENAI_PROJECT_ID Actions secret}"' in workflow
    assert 'export_job="pretrain-judge-export-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"' in workflow
    assert '"app.kubernetes.io/component": "pretrain-judge-export"' in workflow
    assert '.name != "S2P_SERVING_INDEX_ENABLED"' in workflow
    assert 'select(.name != "serving-index")' in workflow
    assert 'limits: {cpu: "1", memory: "6Gi"}' in workflow
    assert '--for=create "pod"' in workflow
    assert 'kubectl -n stream2pretrain exec -i "$pod" -- env' in workflow
    assert '"$workload" == deployment/stream2pretrain-duckdb' in workflow
    assert "workload_timeout=600" in workflow


def test_batch_status_returns_only_safe_progress_fields(monkeypatch) -> None:
    monkeypatch.setattr(
        pretrain_judge_batch,
        "_api_request",
        lambda request: {
            "id": "batch_test",
            "status": "in_progress",
            "request_counts": {"total": 10, "completed": 4, "failed": 0},
            "created_at": 1,
            "in_progress_at": 2,
            "expires_at": 3,
            "metadata": {"secret": "not returned"},
        },
    )

    result = batch_status(["batch_test"], api_key="secret")

    assert result["batches"][0]["request_counts"]["completed"] == 4
    assert "metadata" not in result["batches"][0]


def test_wait_for_file_requires_processed_status(monkeypatch) -> None:
    statuses = iter([{"status": "uploaded"}, {"status": "processed", "id": "file_test"}])
    monkeypatch.setattr(
        pretrain_judge_batch,
        "retrieve_file",
        lambda file_id, api_key: next(statuses),
    )

    result = wait_for_file("file_test", api_key="secret", poll_seconds=0)

    assert result["status"] == "processed"


def test_auth_headers_keep_file_and_batch_requests_in_one_project(monkeypatch) -> None:
    monkeypatch.setenv("OPENAI_PROJECT_ID", "proj_stream2pretrain")
    monkeypatch.setenv("OPENAI_ORG_ID", "org_course")

    assert _auth_headers("secret") == {
        "Authorization": "Bearer secret",
        "OpenAI-Project": "proj_stream2pretrain",
        "OpenAI-Organization": "org_course",
    }
