import hashlib
import importlib.util
from pathlib import Path, PurePosixPath
from zipfile import ZipFile

import yaml

from processor.content_policy import CONTENT_POLICY_GENERATION
from processor.foundry.config import FoundryConfig

ROOT = Path(__file__).resolve().parents[1]


def test_deploy_job_installs_uv_before_using_it() -> None:
    workflow = yaml.safe_load(
        (ROOT / ".github/workflows/deploy-main.yml").read_text(encoding="utf-8")
    )
    deploy_steps = workflow["jobs"]["deploy"]["steps"]
    setup_index = next(
        index
        for index, step in enumerate(deploy_steps)
        if step.get("uses") == "astral-sh/setup-uv@v9.0.0"
    )
    uv_run_indices = [
        index for index, step in enumerate(deploy_steps) if "uv run" in step.get("run", "")
    ]

    assert deploy_steps[setup_index]["with"]["version"] == "0.8.17"
    assert uv_run_indices
    assert setup_index < min(uv_run_indices)


def _packaging_module():
    path = ROOT / "scripts/package_submission.py"
    spec = importlib.util.spec_from_file_location("package_submission", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_runtime_scoring_generation_matches_helm_default() -> None:
    values = yaml.safe_load((ROOT / "charts/stream2pretrain/values.yaml").read_text())

    assert values["processor"]["curate"]["scoringVersion"] == CONTENT_POLICY_GENERATION


def test_documented_scoring_generation_matches_runtime_default() -> None:
    guide = (ROOT / "docs/SCORING_AND_ROUTING.md").read_text()

    assert f"default `{CONTENT_POLICY_GENERATION}`" in guide


def test_documented_foundry_prompt_version_matches_runtime_default() -> None:
    prompt_version = FoundryConfig().prompt_version
    foundry_guide = (ROOT / "docs/POSTTRAIN_FOUNDRY.md").read_text()
    reference = (ROOT / "docs/PIPELINE_IMPLEMENTATION_REFERENCE.md").read_text()

    assert f"`{prompt_version}`" in foundry_guide
    assert f"`{prompt_version}`" in reference


def test_submission_archive_is_reproducible(monkeypatch, tmp_path: Path) -> None:
    packaging = _packaging_module()
    files = [
        PurePosixPath("README.md"),
        PurePosixPath("docs/architecture.svg"),
        PurePosixPath("docs/screenshots/kubectl-pods.png"),
        PurePosixPath("docs/screenshots/platform-pods.png"),
        PurePosixPath("docs/screenshots/serving-output.png"),
        PurePosixPath("docs/screenshots/ui-dashboard.png"),
    ]
    for path in files:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.as_posix().encode())
    monkeypatch.setattr(packaging, "tracked_files", lambda _root: files)

    first = tmp_path / "first.zip"
    second = tmp_path / "second.zip"
    count, first_digest = packaging.build_archive(tmp_path, first)
    _, second_digest = packaging.build_archive(tmp_path, second)

    assert count == len(files)
    assert first_digest == second_digest
    assert first.read_bytes() == second.read_bytes()
    assert (
        first.with_suffix(".zip.sha256")
        .read_text()
        .startswith(hashlib.sha256(first.read_bytes()).hexdigest())
    )
    with ZipFile(first) as archive:
        names = archive.namelist()
        assert "Stream2Pretrain/SUBMISSION_MANIFEST.sha256" in names
        assert all(name.startswith("Stream2Pretrain/") for name in names)


def test_minio_is_a_declarative_storage_tier() -> None:
    helmfile = (ROOT / "helmfile.yaml").read_text()
    setup = (ROOT / "scripts" / "setup_dhbw_demo.sh").read_text()
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text()
    statefulset = (ROOT / "charts" / "minio" / "templates" / "statefulset.yaml").read_text()
    service = (ROOT / "charts" / "minio" / "templates" / "service.yaml").read_text()
    disruption_budget = (
        ROOT / "charts" / "minio" / "templates" / "poddisruptionbudget.yaml"
    ).read_text()
    bucket_job = (ROOT / "charts" / "minio" / "templates" / "buckets-job.yaml").read_text()

    assert "chart: ./charts/minio" in helmfile
    assert "labels: {tier: storage, component: object-storage}" in helmfile
    assert helmfile.count("- minio/minio") == 2
    assert "storage)" in setup
    assert "apply_tier storage" in setup
    assert "STORAGE_CHANGED" in workflow
    assert "MinIO storage reconciliation" in workflow
    assert "--selector name=minio" in workflow
    assert "rollout status statefulset/minio" in workflow
    assert "kind: StatefulSet" in statefulset
    assert "replicas: {{ .Values.replicas }}" in statefulset
    assert "volumeClaimTemplates:" in statefulset
    assert 'accessModes: ["ReadWriteOnce"]' in statefulset
    assert "persistentVolumeClaimRetentionPolicy:" in statefulset
    assert "clusterIP: None" in service
    assert "publishNotReadyAddresses: true" in service
    assert "kind: PodDisruptionBudget" in disruption_budget
    assert "MINIO_PROMETHEUS_AUTH_TYPE" in statefulset
    assert "mc mb --ignore-existing" in bucket_job


def test_core_only_profile_disables_only_foundry() -> None:
    values = yaml.safe_load(
        (ROOT / "infra/helmfile-values/stream2pretrain.core-only.yaml").read_text()
    )
    helmfile = (ROOT / "helmfile.yaml").read_text()
    setup = (ROOT / "scripts/setup_dhbw_demo.sh").read_text()
    configure = (ROOT / "scripts/configure_dhbw_secrets.sh").read_text()

    assert values == {"processor": {"foundry": {"enabled": False}}}
    assert 'env "S2P_CORE_ONLY"' in helmfile
    assert 'CORE_ONLY="${S2P_CORE_ONLY:-0}"' in setup
    assert 'CORE_ONLY="${S2P_CORE_ONLY:-0}"' in configure
