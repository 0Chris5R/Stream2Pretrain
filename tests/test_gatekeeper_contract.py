"""Purpose-aware licence checks for the optional Gatekeeper policy."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_gatekeeper_allows_only_documented_transform_only_defaults() -> None:
    policy = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "gatekeeper-constraints.yaml"
    ).read_text(encoding="utf-8")

    assert 'spec.licenseDefault == "unknown"' not in policy
    for license_id in ("unknown", "arxiv-non-exclusive-distribution", "ODC-By-1.0"):
        assert f"- {license_id}" in policy
    assert "not contains_license(allowed, spec.licenseDefault)" in policy
