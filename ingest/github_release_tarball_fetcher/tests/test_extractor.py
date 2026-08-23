"""Unit tests for the streaming tarball extractor."""

from __future__ import annotations

import io
import tarfile

import pytest

from ingest.github_release_tarball_fetcher.extractor import (
    DEFAULT_ALLOWED_EXTENSIONS,
    DEFAULT_MAX_FILE_SIZE_BYTES,
    iter_tarball_files,
)

TOP_DIR = "huggingface-transformers-abcdef0"


def _build_tarball(files: dict[str, bytes]) -> bytes:
    """Build a gzipped tar archive that mirrors GitHub's tarball layout."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        # Top-level directory entry, like GitHub emits.
        dir_info = tarfile.TarInfo(name=TOP_DIR)
        dir_info.type = tarfile.DIRTYPE
        tf.addfile(dir_info)
        for path, data in files.items():
            info = tarfile.TarInfo(name=f"{TOP_DIR}/{path}")
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


def test_extractor_yields_python_and_markdown_files() -> None:
    payload = _build_tarball(
        {
            "src/foo.py": b"print('hello')\n\nprint('world')\n",
            "README.md": b"# Hello\n\nworld\n",
            "build/output.bin": b"\x00\x01\x02",  # binary; not allow-listed
            "scripts/run.sh": b"#!/usr/bin/env bash\n",  # not allow-listed
        }
    )
    out = list(iter_tarball_files(payload))
    paths = {f.path for f in out}
    assert paths == {"src/foo.py", "README.md"}
    py = next(f for f in out if f.path == "src/foo.py")
    assert py.language == "python"
    assert py.sloc == 2
    md = next(f for f in out if f.path == "README.md")
    assert md.language == "markdown"
    assert md.sloc == 2


def test_extractor_skips_oversized_files() -> None:
    big = b"a" * (DEFAULT_MAX_FILE_SIZE_BYTES + 1)
    payload = _build_tarball(
        {
            "small.py": b"x = 1\n",
            "huge.py": big,
        }
    )
    out = list(iter_tarball_files(payload))
    assert {f.path for f in out} == {"small.py"}


def test_extractor_skips_generated_vendor_and_undecodable_files() -> None:
    payload = _build_tarball(
        {
            "src/main.py": b"print('kept')\n",
            "dist/app.min.js": b"function x(){return 1}",
            "vendor/copied.py": b"print('vendor')\n",
            "src/not-utf8.py": b"value = '\xff'\n",
        }
    )

    assert {item.path for item in iter_tarball_files(payload)} == {"src/main.py"}


def test_extractor_respects_custom_allow_list() -> None:
    payload = _build_tarball(
        {
            "src/foo.py": b"x = 1\n",
            "src/bar.rs": b"fn main() {}\n",
        }
    )
    out = list(iter_tarball_files(payload, allowed_extensions=(".rs",)))
    assert {f.path for f in out} == {"src/bar.rs"}


def test_extractor_strips_top_level_dir() -> None:
    payload = _build_tarball({"deep/path/x.py": b"y = 2\n"})
    out = list(iter_tarball_files(payload))
    assert out[0].path == "deep/path/x.py"
    assert TOP_DIR not in out[0].path


def test_extractor_handles_empty_archive() -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz"):
        pass
    out = list(iter_tarball_files(buf.getvalue()))
    assert out == []


def test_extractor_rejects_zero_max_size() -> None:
    with pytest.raises(ValueError):
        list(iter_tarball_files(b"", max_file_size_bytes=0))


def test_extractor_skips_symlinks_and_dirs() -> None:
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        sym = tarfile.TarInfo(name=f"{TOP_DIR}/link.py")
        sym.type = tarfile.SYMTYPE
        sym.linkname = "src/foo.py"
        tf.addfile(sym)
        body = b"x = 1\n"
        info = tarfile.TarInfo(name=f"{TOP_DIR}/src/foo.py")
        info.size = len(body)
        tf.addfile(info, io.BytesIO(body))
    out = list(iter_tarball_files(buf.getvalue()))
    assert {f.path for f in out} == {"src/foo.py"}


def test_default_allow_list_includes_common_languages() -> None:
    for ext in (".py", ".rs", ".go", ".ts", ".cpp", ".cu", ".md"):
        assert ext in DEFAULT_ALLOWED_EXTENSIONS
