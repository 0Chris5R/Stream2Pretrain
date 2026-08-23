"""Purpose-aware licence checks for the optional Gatekeeper policy."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_gatekeeper_requires_item_level_license_resolution() -> None:
    policy = (
        ROOT / "charts" / "stream2pretrain" / "templates" / "gatekeeper-constraints.yaml"
    ).read_text(encoding="utf-8")

    for forbidden_default in ("unknown", "arxiv-non-exclusive-distribution", "ODC-By-1.0"):
        assert f"- {forbidden_default}" not in policy
    assert "- per-record" in policy
    assert "not contains_license(allowed, spec.licenseDefault)" in policy
