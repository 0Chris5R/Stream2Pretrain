"""Build a network-isolated, digest-pinned official-artifact oracle manifest."""

from __future__ import annotations

import argparse
import json
import shutil
import stat
import subprocess
from pathlib import Path
from typing import Any

from processor.foundry.util import canonical_json, sha256, stable_id
from schemas.foundry import OfficialArtifact, OracleRecipe


def tree_hash(path: Path) -> str:
    root = path.resolve()
    if not root.exists():
        raise ValueError(f"artifact path does not exist: {root}")
    if root.is_symlink():
        raise ValueError("artifact root may not be a symlink")
    files = (
        [root] if root.is_file() else sorted(value for value in root.rglob("*") if value.is_file())
    )
    manifest: list[dict[str, Any]] = []
    for value in files:
        if value.is_symlink():
            raise ValueError(f"artifact tree contains a symlink: {value}")
        relative = value.name if root.is_file() else value.relative_to(root).as_posix()
        manifest.append(
            {
                "path": relative,
                "executable": bool(value.stat().st_mode & stat.S_IXUSR),
                "hash": sha256(value.read_bytes()),
            }
        )
    if not manifest:
        raise ValueError("artifact tree contains no files")
    return sha256(manifest)


def build_oracle(
    *,
    context: str,
    artifact_path: str,
    containerfile: str,
    image_tag: str,
    source_uri: str,
    immutable_ref: str,
    kind: str,
    command: list[str],
    cpu_millis: int,
    memory_mib: int,
    timeout_seconds: int,
    build_memory: str,
    build_cpu_quota: int,
    expected_output_hash: str | None,
    output: str,
) -> OfficialArtifact:
    podman = shutil.which("podman")
    if not podman:
        raise RuntimeError("podman is required to build an official-artifact oracle")
    context_root = Path(context).resolve()
    artifact = Path(artifact_path).resolve()
    recipe_file = Path(containerfile).resolve()
    artifact.relative_to(context_root)
    recipe_file.relative_to(context_root)
    content_hash = tree_hash(artifact)
    build_inputs_hash = tree_hash(context_root)
    subprocess.run(
        [
            podman,
            "build",
            "--network=none",
            "--pull=never",
            "--jobs=1",
            f"--memory={build_memory}",
            f"--cpu-quota={build_cpu_quota}",
            "--tag",
            image_tag,
            "--file",
            str(recipe_file),
            str(context_root),
        ],
        check=True,
    )
    inspected = subprocess.run(
        [podman, "image", "inspect", image_tag, "--format", "{{.Digest}}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not inspected.startswith("sha256:") or len(inspected) != 71:
        raise RuntimeError("Podman did not return a content digest for the built image")
    build_recipe = canonical_json(
        {
            "builder": "podman-network-none-v1",
            "context_hash": build_inputs_hash,
            "artifact_hash": content_hash,
            "containerfile": recipe_file.relative_to(context_root).as_posix(),
            "containerfile_hash": sha256(recipe_file.read_bytes()),
            "image_digest": inspected,
        }
    ).decode("utf-8")
    artifact_record = OfficialArtifact(
        artifact_id=stable_id("official-artifact", source_uri, immutable_ref, content_hash),
        kind=kind,  # type: ignore[arg-type]
        source_uri=source_uri,
        immutable_ref=immutable_ref,
        content_hash=content_hash,
        build_recipe=build_recipe,
        oracle_recipe=OracleRecipe(
            oracle_id=stable_id("oracle", image_tag, inspected, content_hash),
            image=f"{image_tag}@{inspected}",
            embedded_artifact_hash=content_hash,
            command=command,
            cpu_millis=cpu_millis,
            memory_mib=memory_mib,
            timeout_seconds=timeout_seconds,
            expected_output_hash=expected_output_hash,
        ),
    )
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(canonical_json([artifact_record]) + b"\n")
    return artifact_record


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--artifact-path", required=True)
    parser.add_argument("--containerfile", required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--immutable-ref", required=True)
    parser.add_argument(
        "--kind", choices=("code", "data", "checkpoint", "supplement", "other"), required=True
    )
    parser.add_argument("--command-json", required=True)
    parser.add_argument("--cpu-millis", type=int, required=True)
    parser.add_argument("--memory-mib", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, required=True)
    parser.add_argument("--build-memory", required=True)
    parser.add_argument("--build-cpu-quota", type=int, required=True)
    parser.add_argument("--expected-output-hash")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    command = json.loads(args.command_json)
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(value, str) for value in command)
    ):
        parser.error("--command-json must be a non-empty JSON string array")
    artifact = build_oracle(
        context=args.context,
        artifact_path=args.artifact_path,
        containerfile=args.containerfile,
        image_tag=args.image_tag,
        source_uri=args.source_uri,
        immutable_ref=args.immutable_ref,
        kind=args.kind,
        command=command,
        cpu_millis=args.cpu_millis,
        memory_mib=args.memory_mib,
        timeout_seconds=args.timeout_seconds,
        build_memory=args.build_memory,
        build_cpu_quota=args.build_cpu_quota,
        expected_output_hash=args.expected_output_hash,
        output=args.output,
    )
    print(artifact.model_dump_json(indent=2))


if __name__ == "__main__":
    main()


__all__ = ["build_oracle", "main", "tree_hash"]
