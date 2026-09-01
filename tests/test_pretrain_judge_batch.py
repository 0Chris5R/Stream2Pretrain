from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from scripts import pretrain_judge_batch
from scripts.pretrain_judge_batch import (
    batch_status,
    build_request,
    parse_sections,
    prepare,
    response_schema,
    submit,
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
    assert "openai==3.6.0" in workflow
    assert "OPENAI_PROJECT_ID" not in workflow
    assert "OPENAI_ORG_ID" not in workflow
    assert 'export_job="pretrain-judge-export-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"' in workflow
    assert '"app.kubernetes.io/component": "pretrain-judge-export"' in workflow
    assert '.name != "S2P_SERVING_INDEX_ENABLED"' in workflow
    assert 'select(.name != "serving-index")' in workflow
    assert 'limits: {cpu: "1", memory: "6Gi"}' in workflow
    assert '--for=create "pod"' in workflow
    assert '"stream2pretrain.io/egress-class": "foundry-providers"' in workflow
    assert "nohup /tmp/run-pretrain-export.sh" in workflow
    assert "nohup /tmp/run-pretrain-prepare.sh" in workflow
    assert "nohup /tmp/run-pretrain-submit.sh" not in workflow
    assert "wait_remote_step" in workflow
    assert "split -b 1m -d -a 4" in workflow
    assert "pretrain-judge-chunks.sha256" in workflow
    assert 'name: pretrain-judge-jsonl-${{ github.run_id }}' in workflow
    assert "No OpenAI request" in workflow
    assert '"$workload" == deployment/stream2pretrain-duckdb' in workflow
    assert "workload_timeout=600" in workflow


def test_batch_status_returns_only_safe_progress_fields(monkeypatch) -> None:
    class Payload:
        def model_dump(self) -> dict[str, object]:
            return {"total": 10, "completed": 4, "failed": 0}

    batch = SimpleNamespace(
        id="batch_test",
        status="in_progress",
        request_counts=Payload(),
        created_at=1,
        in_progress_at=2,
        completed_at=None,
        expires_at=3,
        output_file_id=None,
        error_file_id=None,
        errors=None,
    )
    monkeypatch.setattr(
        pretrain_judge_batch,
        "_client",
        lambda api_key: SimpleNamespace(batches=SimpleNamespace(retrieve=lambda batch_id: batch)),
    )

    result = batch_status(["batch_test"], api_key="secret")

    assert result["batches"][0]["request_counts"]["completed"] == 4
    assert "metadata" not in result["batches"][0]


def test_submit_uses_official_file_and_batch_clients(tmp_path: Path, monkeypatch) -> None:
    input_path = tmp_path / "pool.jsonl"
    input_path.write_text(json.dumps(_row(source="hf-models")) + "\n")
    batch_dir = tmp_path / "batch"
    prepare(input_path, batch_dir, evaluation_date="2026-09-01")
    calls: list[tuple[str, object]] = []

    class Files:
        def create(self, *, file: object, purpose: str) -> SimpleNamespace:
            calls.append(("file", purpose))
            assert file is not None
            return SimpleNamespace(id="file_test")

    class Batches:
        def create(self, **kwargs: object) -> SimpleNamespace:
            calls.append(("batch", kwargs))
            return SimpleNamespace(id="batch_test", status="validating")

    monkeypatch.setattr(
        pretrain_judge_batch,
        "_client",
        lambda api_key: SimpleNamespace(files=Files(), batches=Batches()),
    )
    result = submit(batch_dir, tmp_path / "submitted.json", api_key="secret")

    assert calls[0] == ("file", "batch")
    assert calls[1][1]["input_file_id"] == "file_test"
    assert calls[1][1]["endpoint"] == "/v1/responses"
    assert result["batches"][0]["batch_id"] == "batch_test"
