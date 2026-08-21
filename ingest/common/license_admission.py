"""Licence admission shared by every ingest component.

Every content format is fail-closed. Missing or unknown licences remain
quarantined before content retrieval, while explicitly allowlisted licences
are admitted and explicitly excluded licences remain quarantined.
"""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime

from ingest.common.hashing import canonical_url, doc_id_for_url
from schemas.license_admission import LicenseAdmissionDecision

PERMISSIVE_TRAINING_LICENSES = frozenset(
    {
        "CC-BY-4.0",
        "CC-BY-SA-4.0",
        "CC-BY-3.0",
        "CC-BY-SA-3.0",
        "CC0-1.0",
        "Apache-2.0",
        "MIT",
        "BSD-2-Clause",
        "BSD-3-Clause",
        "MPL-2.0",
        "ISC",
        "Unlicense",
    }
)

_CC_PATTERN = re.compile(
    r"creativecommons\.org/(?:licenses|publicdomain)/(by(?:-sa|-nc|-nc-sa|-nc-nd|-nd)?|zero)/(\d\.\d)",
    re.IGNORECASE,
)


def normalize_license(value: str | None) -> str:
    """Normalize source strings and licence URLs to stable SPDX identifiers."""
    if value is None or not value.strip():
        return "unknown"
    cleaned = value.strip().rstrip("/")
    lowered = cleaned.lower()
    if lowered in {"unknown", "none", "n/a", "null", "per-record"}:
        return "unknown"
    if "arxiv.org/licenses/nonexclusive-distrib" in lowered:
        return "arxiv-non-exclusive-distribution"
    match = _CC_PATTERN.search(lowered)
    if match:
        family, version = match.groups()
        if family == "zero":
            return f"CC0-{version}"
        return f"CC-{family.upper()}-{version}"
    aliases = {
        "apache 2.0": "Apache-2.0",
        "apache-2.0": "Apache-2.0",
        "mit": "MIT",
        "bsd-2-clause": "BSD-2-Clause",
        "bsd-3-clause": "BSD-3-Clause",
        "mpl-2.0": "MPL-2.0",
        "cc-by-4.0": "CC-BY-4.0",
        "cc-by-sa-4.0": "CC-BY-SA-4.0",
        "cc-by-3.0": "CC-BY-3.0",
        "cc-by-sa-3.0": "CC-BY-SA-3.0",
        "cc0-1.0": "CC0-1.0",
        "isc": "ISC",
        "unlicense": "Unlicense",
        # ODC-By licences the database structure, not necessarily the
        # copyright in each paper, page, or code file contained in it. Keep
        # the normalized identifier for an explicit quarantine reason, but
        # never admit content based on a dataset wrapper alone.
        "odc-by-1.0": "ODC-By-1.0",
    }
    return aliases.get(lowered, cleaned)


def is_training_permitted(value: str | None, *, source_format: str = "web") -> bool:
    """Return whether content may enter training under the allowlist policy.

    ``source_format`` remains part of the call contract for component
    compatibility, but admission is intentionally fail-closed for every
    content format.
    """
    normalized = normalize_license(value)
    return normalized in PERMISSIVE_TRAINING_LICENSES


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    decision: LicenseAdmissionDecision

    @property
    def admitted(self) -> bool:
        return self.decision.status == "admitted"

    @property
    def license_id(self) -> str:
        return self.decision.license_id


def decide_license_admission(
    *,
    source_url: str,
    source_feed: str,
    license_value: str | None,
    license_source: str,
    source_format: str = "web",
    trace_id: str | None = None,
    observed_at: datetime | None = None,
) -> AdmissionResult:
    """Build a deterministic decision before content fetch."""
    canon = canonical_url(source_url)
    doc_id = doc_id_for_url(canon)
    normalized = normalize_license(license_value)
    admitted = is_training_permitted(normalized, source_format=source_format)
    status = "admitted" if admitted else "quarantined"
    if admitted:
        reason = f"{normalized} is on the training allowlist"
    elif normalized == "unknown":
        reason = "machine-readable licence is missing"
    else:
        reason = f"{normalized} is not on the training allowlist"
    digest = hashlib.sha256(
        f"{doc_id}\0{source_feed}\0{source_format}\0{normalized}\0{license_source}\0{status}".encode()
    ).hexdigest()
    decision = LicenseAdmissionDecision(
        decision_id=f"sha256:{digest}",
        doc_id=doc_id,
        source_feed=source_feed,
        source_url=canon,  # type: ignore[arg-type]
        observed_at=observed_at or datetime.now(UTC),
        status=status,
        license_id=normalized,
        license_source=license_source,
        reason=reason,
        trace_id=trace_id or secrets.token_hex(16),
        content_fetch_started=False,
    )
    return AdmissionResult(decision=decision)


def effective_license(
    per_record: str | None,
    source_default: str | None,
) -> tuple[str, str]:
    """Prefer a per-record attestation; use only an explicit feed default."""
    normalized_record = normalize_license(per_record)
    if normalized_record != "unknown":
        return normalized_record, "rss_entry"
    normalized_default = normalize_license(source_default)
    if normalized_default != "unknown":
        return normalized_default, "manual_override"
    return "unknown", "unknown"
