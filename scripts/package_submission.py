"""Build a deterministic submission ZIP from the repository's tracked files."""

from __future__ import annotations

import argparse
import hashlib
import subprocess
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

ARCHIVE_ROOT = "Stream2Pretrain"
FIXED_TIMESTAMP = (2026, 1, 1, 0, 0, 0)
REQUIRED_FILES = {
    "README.md",
    "docs/architecture.svg",
    "docs/screenshots/ui-dashboard.png",
    "docs/screenshots/kubectl-pods.png",
    "docs/screenshots/platform-pods.png",
    "docs/screenshots/serving-output.png",
}
FORBIDDEN_PARTS = {
    ".git",
    ".next",
    ".venv",
    "__pycache__",
    "checkpoints",
    "node_modules",
    "source-corpus",
    "teacher-labels",
}
FORBIDDEN_SUFFIXES = {".ckpt", ".key", ".pem", ".pt", ".pth", ".safetensors"}


def tracked_files(root: Path) -> list[PurePosixPath]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=root,
        check=True,
        capture_output=True,
    )
    return sorted(PurePosixPath(raw.decode("utf-8")) for raw in result.stdout.split(b"\0") if raw)


def validate_files(root: Path, files: list[PurePosixPath]) -> None:
    names = {path.as_posix() for path in files}
    missing = sorted(REQUIRED_FILES - names)
    if missing:
        raise SystemExit(f"required submission files are not tracked: {', '.join(missing)}")

    unsafe: list[str] = []
    for path in files:
        disk_path = root / path
        if not disk_path.is_file() or disk_path.is_symlink():
            unsafe.append(path.as_posix())
            continue
        if (
            any(part in FORBIDDEN_PARTS for part in path.parts)
            or path.suffix.lower() in FORBIDDEN_SUFFIXES
            or path.name == ".env"
            or (path.name.startswith(".env.") and path.name != ".env.example")
        ):
            unsafe.append(path.as_posix())
    if unsafe:
        raise SystemExit(f"unsafe tracked submission files: {', '.join(sorted(set(unsafe)))}")


def zip_info(name: str, mode: int = 0o100644) -> ZipInfo:
    info = ZipInfo(name, FIXED_TIMESTAMP)
    info.compress_type = ZIP_DEFLATED
    info.create_system = 3
    info.external_attr = mode << 16
    return info


def build_archive(root: Path, output: Path) -> tuple[int, str]:
    files = tracked_files(root)
    validate_files(root, files)
    manifest_lines: list[str] = []
    output.parent.mkdir(parents=True, exist_ok=True)

    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path in files:
            source = root / path
            data = source.read_bytes()
            digest = hashlib.sha256(data).hexdigest()
            manifest_lines.append(f"{digest}  {path.as_posix()}")
            mode = 0o100755 if source.stat().st_mode & 0o111 else 0o100644
            archive.writestr(zip_info(f"{ARCHIVE_ROOT}/{path.as_posix()}", mode), data)

        manifest = ("\n".join(manifest_lines) + "\n").encode()
        archive.writestr(zip_info(f"{ARCHIVE_ROOT}/SUBMISSION_MANIFEST.sha256"), manifest)

    archive_digest = hashlib.sha256(output.read_bytes()).hexdigest()
    output.with_suffix(output.suffix + ".sha256").write_text(
        f"{archive_digest}  {output.name}\n", encoding="utf-8"
    )
    return len(files), archive_digest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("dist/Stream2Pretrain-submission.zip"),
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    output = args.output if args.output.is_absolute() else root / args.output
    count, digest = build_archive(root, output)
    print(f"Wrote {output} with {count} tracked files")
    print(f"SHA-256: {digest}")


if __name__ == "__main__":
    main()
