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
