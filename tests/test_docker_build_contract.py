from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INGEST_PACKAGES = sorted(
    path.parent
    for path in (ROOT / "ingest").glob("*/pyproject.toml")
    if path.parent.name != "common"
)
LOCAL_PACKAGE_PATHS = {
    "stream2pretrain": ".",
    "stream2pretrain-ingest-common": "./ingest/common",
    "stream2pretrain-github-events": "./ingest/github_events",
}


def test_processor_ci_image_uses_an_immutable_dependency_base() -> None:
    full = (ROOT / "processor" / "Dockerfile").read_text(encoding="utf-8")
    app = (ROOT / "processor" / "Dockerfile.app").read_text(encoding="utf-8")
    model_app = (ROOT / "processor" / "Dockerfile.model-service.app").read_text(encoding="utf-8")
    entrypoint = (ROOT / "processor" / "container_entrypoint.sh").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    assert "AS runtime-base" in full
    assert "FROM runtime-base AS runtime" in full
    assert "ARG PROCESSOR_BASE_IMAGE=" in app
    assert "FROM ${PROCESSOR_BASE_IMAGE} AS runtime" in app
    assert "uv sync" not in app
    assert "apt-get" not in app
    assert 'ENTRYPOINT ["python", "-m", "processor.model_service"]' in model_app
    assert "COPY processor                  /app/processor" not in model_app
    assert "processor/model_service.py" in model_app
    assert "processor/operators/quality.py" in model_app
    assert "processor/Dockerfile.model-service.app" in workflow
    assert "S2P_INSTALL_EXTRA=${{ matrix.extra }}" in workflow
    assert "target: runtime-base" in workflow
    assert "dockerfile: processor/Dockerfile.app" in workflow
    assert "type=gha,scope=${{ matrix.image }}" in workflow
    assert 's2p-curator-model-service) module="processor.model_service"' in entrypoint


def test_processor_model_images_are_component_specific_and_immutable() -> None:
    dockerfile = (ROOT / "processor" / "Dockerfile").read_text(encoding="utf-8")
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    values = (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    curate_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-curate.yaml"
    ).read_text(encoding="utf-8")
    model_service_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-model-service.yaml"
    ).read_text(encoding="utf-8")
    fetcher_template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-fetcher.yaml"
    ).read_text(encoding="utf-8")

    assert "ARG S2P_MODEL_PROFILE=none" in dockerfile
    assert 'case "${S2P_MODEL_PROFILE}"' in dockerfile
    assert "processor-base-quality" in workflow
    assert "processor-base-embedding" in workflow
    assert "processor-base-kenlm" in workflow
    assert "processor-base-fetcher" in workflow
    assert "processor-quality-model" in workflow
    assert "processor-embedding-model" in workflow
    assert "processor-kenlm-model" in workflow
    assert "processor-fetcher-model" in workflow
    assert "image: stream2pretrain/processor-quality-model" in values
    assert "image: stream2pretrain/processor-embedding-model" in values
    assert "image: stream2pretrain/processor-kenlm-model" in values
    assert "image: stream2pretrain/processor-fetcher-model" in values
    assert "bootstrap: false" in values
    assert 'ternary "/models" "/opt/models" $externalModels' in curate_template
    assert "S2P_QUALITY_MODEL_SERVICE_URL" in curate_template
    assert "S2P_KENLM_MODEL_SERVICE_URL" in curate_template
    assert "S2P_EMBEDDING_MODEL_SERVICE_URL" in curate_template
    assert "kind: HorizontalPodAutoscaler" in model_service_template
    assert "requiredDuringSchedulingIgnoredDuringExecution" in model_service_template
    assert "preferredDuringSchedulingIgnoredDuringExecution" in model_service_template
    required_index = model_service_template.index(
        "requiredDuringSchedulingIgnoredDuringExecution"
    )
    preferred_index = model_service_template.index(
        "preferredDuringSchedulingIgnoredDuringExecution"
    )
    assert "$curatorComponent" not in model_service_template[
        required_index:preferred_index
    ]
    assert "$curatorComponent" in model_service_template[preferred_index:]
    assert "maxSurge: 0" in model_service_template
    assert fetcher_template.count("S2P_REQUIRE_REAL_MODELS") == 2


def test_curator_startup_probe_guards_slow_model_initialization() -> None:
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "processor-curate.yaml"
    ).read_text(encoding="utf-8")

    startup_probe = """          startupProbe:
            httpGet: {path: /healthz, port: metrics}
            periodSeconds: 10
            timeoutSeconds: 3
            failureThreshold: 90"""
    assert startup_probe in template
    assert template.index("startupProbe:") < template.index("livenessProbe:")
    assert "initialDelaySeconds: 60" not in template


def test_catalog_bootstrap_precedes_application_rollout() -> None:
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "job-polaris-bootstrap.yaml"
    ).read_text(encoding="utf-8")
    dockerfile = (ROOT / "processor" / "Dockerfile.app").read_text(encoding="utf-8")

    assert '"helm.sh/hook": pre-install,pre-upgrade' in template
    assert '"helm.sh/hook-weight": "-10"' in template
    assert "python -m processor.polaris_bootstrap" in template
    assert "--apply --register-missing --register-only" in template
    assert "activeDeadlineSeconds: 300" in template
    assert "from processor.polaris_bootstrap import main as bootstrap" in dockerfile


def test_github_tarball_scaler_uses_a_non_amplifying_job_topic() -> None:
    values = (ROOT / "charts" / "stream2pretrain" / "values.yaml").read_text(encoding="utf-8")
    template = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "ingest-github-tarball.yaml"
    ).read_text(encoding="utf-8")
    helper = (ROOT / "charts" / "stream2pretrain" / "templates" / "_helpers.tpl").read_text(
        encoding="utf-8"
    )

    assert "githubReleaseJobs: github.release.jobs" in values
    assert "- type: kafka" in template
    assert ".Values.redpanda.topics.githubReleaseJobs" in template
    assert "- type: prometheus" not in template
    assert "S2P_GITHUB_RELEASE_JOBS_TOPIC" in helper


def test_release_images_are_deployed_by_content_digest() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")
    helper = (ROOT / "charts" / "stream2pretrain" / "templates" / "_helpers.tpl").read_text(
        encoding="utf-8"
    )

    assert "image-pin:" in workflow
    assert "needs.image-pin.result == 'success'" in workflow
    assert "pin_component processor_quality_model processor-quality-model" in workflow
    assert "processor-quality-model@${IMAGE_DIGEST_PROCESSOR_QUALITY}" in workflow
    assert "ui.image=stream2pretrain/ui@${IMAGE_DIGEST_UI}" in workflow
    assert "minimum_rootfs_available=$((6 * 1024 * 1024 * 1024))" in workflow
    assert 'contains "@sha256:" .image' in helper
    assert 'printf "%s/%s" $ctx.Values.image.registry .image' in helper


def test_model_service_content_hash_ignores_unrelated_processor_code() -> None:
    workflow = (ROOT / ".github" / "workflows" / "deploy-main.yml").read_text(encoding="utf-8")

    model_input = (
        "inputs: .dockerignore pyproject.toml uv.lock tests/pyproject.toml "
        "ingest/*/pyproject.toml schemas processor/Dockerfile.model-service.app "
        "processor/__init__.py processor/common.py processor/model_service.py"
    )
    assert workflow.count(model_input) == 3
    assert (
        workflow.count(
            "inputs: .dockerignore pyproject.toml uv.lock tests/pyproject.toml "
            "ingest/*/pyproject.toml schemas processor\n"
        )
        == 0
    )


@pytest.mark.parametrize("package_dir", INGEST_PACKAGES, ids=lambda path: path.name)
def test_ingest_dockerfile_declares_an_isolated_local_install(package_dir: Path) -> None:
    metadata = tomllib.loads((package_dir / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata["project"]["dependencies"]
    dockerfile = (package_dir / "Dockerfile").read_text(encoding="utf-8")

    assert "uv pip install --system --no-cache --no-config --no-sources" in dockerfile
    for package_name, local_path in LOCAL_PACKAGE_PATHS.items():
        if package_name == "stream2pretrain" or any(
            dependency.split(" ", 1)[0] == package_name for dependency in dependencies
        ):
            assert f"-e {local_path}" in dockerfile
            if local_path not in {".", "./ingest/common"}:
                source_path = local_path.removeprefix("./")
                assert f"COPY {source_path} /src/{source_path}" in dockerfile
