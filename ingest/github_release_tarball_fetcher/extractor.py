"""Stream-extract a GitHub release source tarball.

The tarball comes back from GitHub as a gzipped tar archive whose top-level
directory is ``<owner>-<repo>-<short-sha>/``. We use the stdlib ``tarfile`` in
streaming mode (``r|gz``) so the archive is never fully materialised in
memory, only one member at a time.

For every member that:

- is a regular file (no symlinks, devices, or directories),
- has an extension on the configured allow-list,
- is under the configured ``max_file_size_bytes``,

we yield a :class:`ExtractedFile` describing the file's path (with the
top-level repo prefix stripped), bytes, and a normalised lowercase language
label.
"""

from __future__ import annotations

import io
import tarfile
from collections.abc import Iterator
from dataclasses import dataclass

# Pygments is an optional fallback for files whose extension is on the allow
# list but is not in the static table below. Imported lazily inside
# ``_language_for`` to keep import cost low for the common path.
_LANG_BY_EXT: dict[str, str] = {
    ".py": "python",
    ".pyx": "python",
    ".pyi": "python",
    ".c": "c",
    ".h": "c",
    ".hh": "c++",
    ".hpp": "c++",
    ".cpp": "c++",
    ".cc": "c++",
    ".cxx": "c++",
    ".cu": "cuda",
    ".cuh": "cuda",
    ".rs": "rust",
    ".go": "go",
    ".js": "javascript",
    ".mjs": "javascript",
    ".cjs": "javascript",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".jsx": "javascript",
    ".java": "java",
    ".scala": "scala",
    ".kt": "kotlin",
    ".kts": "kotlin",
    ".swift": "swift",
    ".rb": "ruby",
    ".r": "r",
    ".jl": "julia",
    ".md": "markdown",
    ".rst": "restructuredtext",
    ".txt": "text",
    ".yaml": "yaml",
    ".yml": "yaml",
    ".toml": "toml",
    ".json": "json",
}

DEFAULT_ALLOWED_EXTENSIONS: tuple[str, ...] = tuple(sorted(_LANG_BY_EXT.keys()))
DEFAULT_MAX_FILE_SIZE_BYTES: int = 1 * 1024 * 1024  # 1 MiB


@dataclass(frozen=True)
class ExtractedFile:
    """A single source file extracted from a release tarball."""

    path: str
    """Path relative to the repo root (top-level tar prefix stripped)."""

    data: bytes
    """File bytes; bounded by ``max_file_size_bytes``."""

    language: str
    """Lowercase language label."""

    sloc: int
    """Source lines of code: count of newline-terminated lines that contain at
    least one non-whitespace character. Pure heuristic.
    """


def _strip_top_dir(name: str) -> str:
    """Strip the GitHub-injected ``<owner>-<repo>-<sha>/`` prefix.

    GitHub tarballs always wrap the repo in a single top-level directory.
    Names without a directory separator are returned unchanged.
    """
    idx = name.find("/")
    if idx < 0:
        return name
    return name[idx + 1 :]


def _language_for(path: str) -> str | None:
    """Return a lowercase language label for ``path`` or ``None`` if unknown.

    Falls back to ``pygments.lexers.get_lexer_for_filename`` when the static
    extension table does not match. The fallback is best-effort: a Pygments
    lookup miss returns ``None`` rather than raising.
    """
    lower = path.lower()
    for ext, lang in _LANG_BY_EXT.items():
        if lower.endswith(ext):
            return lang
    try:
        from pygments.lexers import get_lexer_for_filename
        from pygments.util import ClassNotFound
    except ImportError:
        return None
    try:
        lexer = get_lexer_for_filename(path)
    except ClassNotFound:
        return None
    name = (lexer.name or "").strip().lower()
    return name or None


def _count_sloc(data: bytes) -> int:
    """Count non-blank lines in ``data``; UTF-8 errors are tolerated."""
    try:
        text = data.decode("utf-8", errors="replace")
    except Exception:
        return 0
    return sum(1 for line in text.splitlines() if line.strip())


def _has_allowed_extension(path: str, allowed: tuple[str, ...]) -> bool:
    lower = path.lower()
    return any(lower.endswith(ext) for ext in allowed)


def iter_tarball_files(
    tar_bytes: bytes,
    *,
    allowed_extensions: tuple[str, ...] = DEFAULT_ALLOWED_EXTENSIONS,
    max_file_size_bytes: int = DEFAULT_MAX_FILE_SIZE_BYTES,
) -> Iterator[ExtractedFile]:
    """Stream-extract files from a gzipped tar archive.

    Parameters
    ----------
    tar_bytes:
        Raw ``tar.gz`` bytes as returned by the GitHub
        ``/repos/{o}/{r}/tarball/{tag}`` endpoint.
    allowed_extensions:
        Lowercase file extensions (with leading dot) to include. Files whose
        path does not end with one of these are skipped silently.
    max_file_size_bytes:
        Files whose tar header reports a larger size are skipped without
        being read into memory.

    Yields
    ------
    ExtractedFile
        One per file that passed every filter. The order of yielded files
        matches the tar archive's stored order.
    """
    if max_file_size_bytes <= 0:
        raise ValueError("max_file_size_bytes must be positive")
    if not allowed_extensions:
        return

    fileobj = io.BytesIO(tar_bytes)
    # ``r|gz`` is the streaming mode; ``r:gz`` would seek and require the full
    # archive in memory.
    with tarfile.open(fileobj=fileobj, mode="r|gz") as tf:
        for member in tf:
            if not member.isfile():
                continue
            if member.size > max_file_size_bytes:
                continue
            path = _strip_top_dir(member.name)
            if not path or path.endswith("/"):
                continue
            if not _has_allowed_extension(path, allowed_extensions):
                continue
            language = _language_for(path)
            if language is None:
                continue
            extracted = tf.extractfile(member)
            if extracted is None:
                continue
            data = extracted.read()
            if len(data) > max_file_size_bytes:
                # Defensive: tar header lied about size.
                continue
            yield ExtractedFile(
                path=path,
                data=data,
                language=language,
                sloc=_count_sloc(data),
            )


__all__ = [
    "DEFAULT_ALLOWED_EXTENSIONS",
    "DEFAULT_MAX_FILE_SIZE_BYTES",
    "ExtractedFile",
    "iter_tarball_files",
]
