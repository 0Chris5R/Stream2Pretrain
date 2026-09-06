"""Purpose-aware, item-scoped licence admission shared by every ingest path."""

from __future__ import annotations

import hashlib
import re
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime

from ingest.common.hashing import canonical_url, doc_id_for_url
from schemas.bronze import SourceFormat, TrainingUsage
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
        # Hugging Face's public-repository terms grant every Hub user broad
        # reuse rights in public repository content. This identifier applies
        # only to the exact-revision README card, never to model weights,
        # dataset rows, or other repository files.
        "HF-Public-Repository-Terms-2022-09-15",
    }
)

POSTTRAIN_TRANSFORM_LICENSES = frozenset(
    {
        "arxiv-non-exclusive-distribution",
        "CC-BY-NC-4.0",
        "CC-BY-NC-SA-4.0",
        "CC-BY-NC-3.0",
        "CC-BY-NC-SA-3.0",
    }
)

LICENSE_POLICY_REVISION = "license-policy-2026-08-25"

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
    if lowered in {
        "unknown",
        "none",
        "n/a",
        "null",
        "per-record",
        "noassertion",
        "other",
    }:
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
        "cc by 4.0": "CC-BY-4.0",
        "cc-by-sa-4.0": "CC-BY-SA-4.0",
        "cc by-sa 4.0": "CC-BY-SA-4.0",
        "cc by sa 4.0": "CC-BY-SA-4.0",
        "cc-by-nc-4.0": "CC-BY-NC-4.0",
        "cc by-nc 4.0": "CC-BY-NC-4.0",
        "cc by nc 4.0": "CC-BY-NC-4.0",
        "cc-by-nc-sa-4.0": "CC-BY-NC-SA-4.0",
        "cc by-nc-sa 4.0": "CC-BY-NC-SA-4.0",
        "cc by nc sa 4.0": "CC-BY-NC-SA-4.0",
        "cc-by-nd-4.0": "CC-BY-ND-4.0",
        "cc by-nd 4.0": "CC-BY-ND-4.0",
        "cc-by-nc-nd-4.0": "CC-BY-NC-ND-4.0",
        "cc by-nc-nd 4.0": "CC-BY-NC-ND-4.0",
        "cc-by-3.0": "CC-BY-3.0",
        "cc by 3.0": "CC-BY-3.0",
        "cc-by-sa-3.0": "CC-BY-SA-3.0",
        "cc by-sa 3.0": "CC-BY-SA-3.0",
        "cc-by-nc-3.0": "CC-BY-NC-3.0",
        "cc by-nc 3.0": "CC-BY-NC-3.0",
        "cc-by-nc-sa-3.0": "CC-BY-NC-SA-3.0",
        "cc by-nc-sa 3.0": "CC-BY-NC-SA-3.0",
        "cc0-1.0": "CC0-1.0",
        "isc": "ISC",
        "unlicense": "Unlicense",
        # ODC-By licences the database structure, not necessarily the
        # copyright in each contained paper, page, or code file. Keep the
        # normalized identifier so wrapper-only evidence can be restricted to
        # derived post-training rather than verbatim pretraining.
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


def is_posttrain_transform_permitted(value: str | None) -> bool:
    """Return whether a non-pretraining source may feed derived SFT/RL generation."""
    normalized = normalize_license(value)
    # Missing item rights and dataset-wrapper-only evidence are deliberately
    # retained only as grounding for derived post-training artifacts. They can
    # never enter a verbatim pretraining export. An explicit incompatible
    # licence such as an ND grant remains quarantined.
    return normalized in POSTTRAIN_TRANSFORM_LICENSES or normalized in {
        "unknown",
        "ODC-By-1.0",
    }


@dataclass(frozen=True, slots=True)
class AdmissionResult:
    decision: LicenseAdmissionDecision

    @property
    def admitted(self) -> bool:
        return self.decision.status == "admitted"

    @property
    def fetch_allowed(self) -> bool:
        return self.decision.status in {"admitted", "posttrain_transform_only"}

    @property
    def training_usage(self) -> TrainingUsage:
        return (
            "posttrain_transform_only"
            if self.decision.status == "posttrain_transform_only"
            else "pretrain_and_posttrain"
        )

    @property
    def license_id(self) -> str:
        return self.decision.license_id


def decide_license_admission(
    *,
    source_url: str,
    source_feed: str,
    license_value: str | None,
    license_source: str,
    source_format: SourceFormat = "web",
    trace_id: str | None = None,
    observed_at: datetime | None = None,
    resolver: str | None = None,
    evidence_url: str | None = None,
    evidence_revision: str | None = None,
    evidence_scope: str | None = None,
    document_id: str | None = None,
) -> AdmissionResult:
    """Build a deterministic decision before content fetch.

    ``document_id`` is reserved for source projections whose immutable object
    identity is known before the body fetch. Most sources retain the canonical
    URL-derived id; Hugging Face README projections bind it to the README Git
    blob so unrelated repository commits cannot create corpus revisions.
    """
    canon = canonical_url(source_url)
    doc_id = document_id or doc_id_for_url(canon)
    normalized = normalize_license(license_value)
    admitted = is_training_permitted(normalized, source_format=source_format)
    transform_only = not admitted and is_posttrain_transform_permitted(normalized)
    status = (
        "admitted" if admitted else "posttrain_transform_only" if transform_only else "quarantined"
    )
    if admitted:
        reason = f"{normalized} is on the training allowlist"
    elif normalized == "unknown":
        reason = (
            "item-level licence is unresolved; excluded from verbatim pretraining and "
            "allowed only for derived post-training generation"
        )
    elif normalized == "ODC-By-1.0":
        reason = (
            "dataset wrapper licence does not establish item rights; excluded from "
            "verbatim pretraining and allowed only for derived post-training generation"
        )
    elif transform_only:
        reason = (
            f"{normalized} is excluded from verbatim pretraining and allowed only "
            "for derived post-training generation"
        )
    else:
        reason = f"{normalized} is not on the training allowlist"
    resolved_at = observed_at or datetime.now(UTC)
    scope = evidence_scope or (
        "unknown"
        if normalized == "unknown"
        else "dataset_wrapper"
        if normalized == "ODC-By-1.0"
        else "item"
    )
    evidence_resolver = resolver or license_source
    digest = hashlib.sha256(
        (
            f"{doc_id}\0{source_feed}\0{source_format}\0{normalized}\0{license_source}\0"
            f"{status}\0{evidence_resolver}\0{evidence_url or ''}\0"
            f"{evidence_revision or ''}\0{scope}\0{LICENSE_POLICY_REVISION}"
        ).encode()
    ).hexdigest()
    decision = LicenseAdmissionDecision(
        decision_id=f"sha256:{digest}",
        doc_id=doc_id,
        source_feed=source_feed,
        source_url=canon,  # type: ignore[arg-type]
        source_format=source_format,
        observed_at=resolved_at,
        status=status,
        license_id=normalized,
        license_source=license_source,
        raw_license=license_value,
        normalized_license=normalized,
        resolver=evidence_resolver,
        evidence_url=evidence_url,  # type: ignore[arg-type]
        evidence_revision=evidence_revision,
        evidence_scope=scope,  # type: ignore[arg-type]
        policy_revision=LICENSE_POLICY_REVISION,
        resolved_at=resolved_at,
        reason=reason,
        trace_id=trace_id or secrets.token_hex(16),
        content_fetch_started=False,
    )
    return AdmissionResult(decision=decision)


def effective_license(
    per_record: str | None,
    source_default: str | None,
) -> tuple[str, str]:
    """Return only item-level feed evidence.

    Source defaults are configuration hints, never rights evidence for an
    individual training item. Source-wide terms require a dedicated, versioned
    resolver and must not flow through this compatibility helper.
    """
    normalized_record = normalize_license(per_record)
    if normalized_record != "unknown":
        return normalized_record, "rss_entry"
    del source_default
    return "unknown", "unknown"
