"""Repository-local security lint for production readiness.

The scan is intentionally narrow and deterministic. It is not a replacement
for a hosted secret scanner, but it catches the production footguns this repo
has already hit: public admin CIDR defaults, pasted private keys, and real-
looking provider tokens. Example placeholders and test fixtures are allowed so
the gate can run on every developer machine without external services.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

TEXT_SUFFIXES = {
    ".Dockerfile",
    ".example",
    ".go",
    ".hcl",
    ".json",
    ".md",
    ".mjs",
    ".py",
    ".sh",
    ".tf",
    ".toml",
    ".ts",
    ".tsx",
    ".yaml",
    ".yml",
}

SKIP_PARTS = {
    ".git",
    ".mypy_cache",
    ".next",
    ".pytest_cache",
    ".ruff_cache",
    ".s2p-state",
    ".terraform",
    ".venv",
    "node_modules",
}

SECRET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private-key", re.compile(r"-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----")),
    ("github-token", re.compile(r"\b(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9_]{20,}\b")),
    ("github-fine-grained-token", re.compile(r"\bgithub_pat_[A-Za-z0-9_]{20,}\b")),
    ("huggingface-token", re.compile(r"\bhf_[A-Za-z0-9]{20,}\b")),
    ("aws-access-key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
)

PUBLIC_ADMIN_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"allowed_admin_cidrs\s*=\s*\[[^\]]*(?:0\.0\.0\.0/0|::/0)"),
    re.compile(
        r"variable\s+\"allowed_admin_cidrs\"[\s\S]{0,300}?default\s*=\s*\[[^\]]*(?:0\.0\.0\.0/0|::/0)"
    ),
)


@dataclass(frozen=True, slots=True)
class Finding:
    """One security scan finding."""

    path: Path
    line: int
    rule: str
    text: str

    def format(self, root: Path) -> str:
        rel = self.path.relative_to(root)
        return f"{rel}:{self.line}: {self.rule}: {self.text.strip()}"


def tracked_files(root: Path) -> list[Path]:
    """Return git-tracked files plus untracked source files relevant to this scan."""
    try:
        proc = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return [p for p in root.rglob("*") if p.is_file()]
    return [root / line for line in proc.stdout.splitlines() if line]


def should_scan(path: Path, root: Path) -> bool:
    """Whether ``path`` is a text file this gate owns."""
    try:
        rel = path.relative_to(root)
    except ValueError:
        return False
    if any(part in SKIP_PARTS for part in rel.parts):
        return False
    if path.name == "helmfile.yaml" or path.name.startswith("Dockerfile"):
        return True
    return path.suffix in TEXT_SUFFIXES


def is_allowed_example(line: str) -> bool:
    """Allow obvious placeholders and test-only credentials."""
    lowered = line.lower()
    return any(
        marker in lowered
        for marker in (
            "...",
            "_test",
            "example",
            "placeholder",
            "your-comment",
            "dummy",
        )
    )


def scan_text(path: Path, text: str) -> list[Finding]:
    """Scan one text blob."""
    findings: list[Finding] = []
    for idx, line in enumerate(text.splitlines(), start=1):
        for rule, pattern in SECRET_PATTERNS:
            if pattern.search(line) and not is_allowed_example(line):
                findings.append(Finding(path, idx, rule, line))
    for pattern in PUBLIC_ADMIN_PATTERNS:
        match = pattern.search(text)
        if match:
            line_no = text[: match.start()].count("\n") + 1
            snippet = text.splitlines()[line_no - 1] if text.splitlines() else ""
            findings.append(Finding(path, line_no, "public-admin-cidr", snippet))
    return findings


def scan_paths(paths: Iterable[Path], root: Path = ROOT) -> list[Finding]:
    """Scan paths and return all findings."""
    findings: list[Finding] = []
    for path in paths:
        if not path.is_file() or not should_scan(path, root):
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        findings.extend(scan_text(path, text))
    return sorted(findings, key=lambda f: (str(f.path), f.line, f.rule))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run Stream2Pretrain security lint.")
    parser.add_argument("paths", nargs="*", help="Optional paths to scan instead of git files.")
    args = parser.parse_args(argv)
    root = ROOT
    paths = [root / p for p in args.paths] if args.paths else tracked_files(root)
    findings = scan_paths(paths, root=root)
    if findings:
        for finding in findings:
            print(finding.format(root), file=sys.stderr)
        print(f"security scan failed: {len(findings)} finding(s)", file=sys.stderr)
        return 1
    print("security scan passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
