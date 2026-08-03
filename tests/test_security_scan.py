from __future__ import annotations

from pathlib import Path

from scripts.security_scan import scan_paths, scan_text, should_scan

GITHUB_TOKEN = "ghp_" + "1234567890abcdefghijklmnopqrstuv"
HF_TOKEN = "hf_" + "1234567890abcdefghijklmnopqrstuv"
AWS_KEY = "AKIA" + "1234567890ABCDEF"
PRIVATE_KEY_HEADER = "-----BEGIN " + "PRIVATE KEY-----"
PUBLIC_ADMIN_VAR = "allowed_admin_" + "cidrs"
PUBLIC_IPV4_ANY = "0.0.0.0" + "/0"


def test_scan_text_flags_real_looking_secrets() -> None:
    text = "\n".join(
        [
            f"GITHUB_TOKEN={GITHUB_TOKEN}",
            f"HF_TOKEN={HF_TOKEN}",
            f"AWS_ACCESS_KEY_ID={AWS_KEY}",
            PRIVATE_KEY_HEADER,
        ]
    )

    findings = scan_text(Path("secrets.env"), text)

    assert {f.rule for f in findings} == {
        "aws-access-key",
        "github-token",
        "huggingface-token",
        "private-key",
    }


def test_scan_text_allows_obvious_placeholders() -> None:
    text = "\n".join(
        [
            "GITHUB_TOKEN=ghp_example1234567890abcdefghijklmnop",
            "HF_TOKEN=hf_placeholder1234567890abcdefghijklm",
            "AWS_ACCESS_KEY_ID=AKIAEXAMPLE123456789",
            "# dummy -----BEGIN PRIVATE KEY----- fixture",
        ]
    )

    assert scan_text(Path("README.md"), text) == []


def test_scan_text_flags_public_admin_cidr_defaults() -> None:
    text = f'''variable "{PUBLIC_ADMIN_VAR}" {{
  type = list(string)
  default = ["{PUBLIC_IPV4_ANY}"]
}}
'''

    findings = scan_text(Path("variables.tf"), text)

    assert [(f.line, f.rule) for f in findings] == [(1, "public-admin-cidr")]


def test_scan_paths_skips_unowned_or_cache_files(tmp_path: Path) -> None:
    root = tmp_path
    cache_file = root / ".terraform" / "terraform.tfstate"
    cache_file.parent.mkdir()
    cache_file.write_text(f"token = {GITHUB_TOKEN}\n", encoding="utf-8")
    binaryish = root / "archive.bin"
    binaryish.write_text(f"token = {GITHUB_TOKEN}\n", encoding="utf-8")

    assert scan_paths([cache_file, binaryish], root=root) == []


def test_scan_paths_flags_supported_text_files(tmp_path: Path) -> None:
    root = tmp_path
    values = root / "values.yaml"
    values.write_text(f"token: {GITHUB_TOKEN}\n", encoding="utf-8")

    findings = scan_paths([values], root=root)

    assert len(findings) == 1
    assert findings[0].format(root) == f"values.yaml:1: github-token: token: {GITHUB_TOKEN}"


def test_should_scan_accepts_dockerfile_and_helmfile(tmp_path: Path) -> None:
    root = tmp_path
    dockerfile = root / "processor" / "Dockerfile"
    helmfile = root / "helmfile.yaml"
    dockerfile.parent.mkdir()
    dockerfile.touch()
    helmfile.touch()

    assert should_scan(dockerfile, root)
    assert should_scan(helmfile, root)
