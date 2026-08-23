"""Tests for :mod:`processor.seed.stack_edu_filter`."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from processor.seed import stack_edu_filter as se
from processor.seed.cursor import SeedCursor

ROOT = Path(__file__).resolve().parents[2]


def test_is_python_case_insensitive() -> None:
    assert se.is_python({"language": "Python"})
    assert se.is_python({"language": "python"})
    assert not se.is_python({"language": "JavaScript"})
    assert not se.is_python({"language": ""})
    assert not se.is_python({})


def test_is_ml_relevant_via_repo_allowlist() -> None:
    row = {"repository_name": "huggingface/transformers", "path": "src/foo.py"}
    assert se.is_ml_relevant(row)


def test_is_ml_relevant_via_path_keyword() -> None:
    row = {"path": "models/transformer/attention.py"}
    assert se.is_ml_relevant(row)


def test_is_ml_relevant_neither_matches() -> None:
    row = {"repository_name": "user/web", "path": "src/index.py"}
    assert not se.is_ml_relevant(row)


def test_stack_edu_filter_covers_every_live_ai_repository() -> None:
    values = yaml.safe_load(
        (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    )
    release_repos = set(values["sources"]["github"]["releases"]["repos"])

    assert release_repos <= se.ML_REPOS


def test_derive_valid_from_iso() -> None:
    row = {"commit_date": "2024-09-01T00:00:00Z"}
    assert se.derive_valid_from(row) == datetime(2024, 9, 1, tzinfo=UTC)


def test_derive_valid_from_falls_back_to_cutoff() -> None:
    row: dict[str, object] = {}
    assert se.derive_valid_from(row).year == 2024


def test_native_id_prefers_blob_id() -> None:
    row = {"blob_id": "abcdef0123", "path": "x.py"}
    assert se.native_id_for(row) == "abcdef0123"


def test_native_id_falls_back_to_path() -> None:
    row = {"path": "src/foo.py"}
    assert se.native_id_for(row) == "src/foo.py"


def test_license_for_per_file_spdx() -> None:
    spdx, source = se.license_for({"license": "MIT"})
    assert spdx == "MIT"
    assert source == "dataset_metadata"


def test_license_for_reads_stack_edu_detected_licence() -> None:
    spdx, source = se.license_for({"detected_licenses": ["Apache-2.0"]})
    assert spdx == "Apache-2.0"
    assert source == "dataset_metadata"


def test_license_for_quarantines_ambiguous_detected_licences() -> None:
    spdx, source = se.license_for({"detected_licenses": ["MIT", "Apache-2.0"]})
    assert spdx is None
    assert source == "unknown"


def test_license_for_does_not_substitute_dataset_wrapper() -> None:
    spdx, source = se.license_for({})
    assert spdx is None
    assert source == "unknown"


def test_to_seed_document_python_ml_repo() -> None:
    row = {
        "blob_id": "deadbeef",
        "language": "Python",
        "repository_name": "huggingface/transformers",
        "path": "src/transformers/training_args.py",
        "content": "import torch\n\ndef train(): ...\n",
        "commit_date": "2025-02-01T00:00:00Z",
        "license": "Apache-2.0",
    }
    doc = se.to_seed_document(row)
    assert doc is not None
    assert doc.repo_id == se.REPO_ID
    assert doc.native_id == "deadbeef"
    assert doc.source_format == "code"
    assert doc.spdx_license == "Apache-2.0"
    assert doc.url.startswith("https://github.com/huggingface/transformers/")
    assert "/blob/deadbeef/" in doc.url
    assert doc.license_resolver == "stack-edu-file-item-field"
    assert doc.license_evidence_revision == f"{se.DATASET_REVISION}:deadbeef"
    assert doc.license_evidence_scope == "file"
    assert doc.extraction_pipeline == "stack-edu-2024"
    assert doc.extra["language"] == "Python"


def test_metadata_only_row_defers_swh_body_fetch() -> None:
    row = {
        "blob_id": "a" * 40,
        "language": "Python",
        "repo_name": "huggingface/transformers",
        "path": "src/transformers/modeling.py",
        "detected_licenses": ["Apache-2.0"],
    }
    doc = se.to_seed_document(row)

    assert doc is not None
    assert doc.text == ""
    assert doc.body_loader is not None
    assert doc.spdx_license == "Apache-2.0"
    assert doc.license_evidence_revision == f"{se.DATASET_REVISION}:{'a' * 40}"


def test_metadata_only_row_materializes_exact_swh_blob(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requested: list[str] = []

    def fake_fetch(blob_id: str) -> str:
        requested.append(blob_id)
        return "def train():\n    return 1\n"

    monkeypatch.setattr(se, "fetch_software_heritage_blob", fake_fetch)
    row = {
        "blob_id": "b" * 40,
        "language": "Python",
        "repo_name": "huggingface/transformers",
        "path": "src/transformers/trainer.py",
        "detected_licenses": ["Apache-2.0"],
    }
    doc = se.to_seed_document(row)

    assert doc is not None
    materialized = doc.materialize_body()
    assert requested == ["b" * 40]
    assert materialized is not None
    assert materialized.text.startswith("def train")


def test_to_seed_document_drops_non_python() -> None:
    row = {"language": "JavaScript", "content": "x"}
    assert se.to_seed_document(row) is None


def test_to_seed_document_drops_off_topic_repo() -> None:
    row = {
        "language": "Python",
        "repository_name": "user/cookbook",
        "path": "recipe.py",
        "content": "x",
    }
    assert se.to_seed_document(row) is None


def test_iter_documents_with_cursor_and_max() -> None:
    rows = [
        {
            "blob_id": f"blob-{i:04d}",
            "language": "Python",
            "repository_name": "huggingface/transformers",
            "path": f"src/foo_{i}.py",
            "content": "code",
            "commit_date": "2024-09-01T00:00:00Z",
        }
        for i in range(5)
    ]
    cursor = SeedCursor(repo_id=se.REPO_ID)
    cursor.last_native_id = "blob-0001"
    out = list(se.iter_documents(cursor, rows=rows, max_docs=2))
    assert [d.native_id for d in out] == ["blob-0002", "blob-0003"]
