"""Validate durable live curation outcomes from inside the DuckDB API pod.

The deployment smoke test proves that one controlled message traverses Kafka.
This validator complements it with corpus-level invariants: real source data
must have reached durable decisions, licence admission must be fail-closed but
not reject everything, and accepted exports must retain model provenance.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from typing import Any

from ingest.common.license_admission import PERMISSIVE_TRAINING_LICENSES

DEFAULT_BASE_URL = "http://[::1]:8090"
_NON_PRODUCTION_SOURCE_MARKERS = ("cluster-smoke", "fixture", "local-", "test-")
_CRITICAL_REVISIONS = (
    "policy_revision",
    "scoring_version",
    "classifier_revision",
    "classifier_backend",
    "projection_version",
    "extraction_pipeline",
    "lang_detector_revision",
    "tokenizer_revision",
    "perplexity_scorer",
    "minhash_backend",
    "lsh_backend",
)


class ValidationError(RuntimeError):
    """A required live-corpus invariant was not satisfied."""


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValidationError(f"{field} must be a non-negative integer")
    return value


def validate_license_admissions(payload: dict[str, Any]) -> None:
    """Require both sides of the strict pre-fetch licence gate."""
    admitted = _integer(payload.get("admitted"), "license-admissions.admitted")
    quarantined = _integer(payload.get("quarantined"), "license-admissions.quarantined")
    if admitted == 0:
        raise ValidationError("the live licence gate has admitted no documents")
    if quarantined == 0:
        raise ValidationError("the live licence gate has quarantined no documents")

    by_license = payload.get("by_license")
    if not isinstance(by_license, list) or not by_license:
        raise ValidationError("the live licence ledger has no per-licence rows")
    permitted_seen = False
    rejected_seen = False
    for row in by_license:
        if not isinstance(row, dict):
            continue
        count = row.get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count <= 0:
            continue
        license_id = row.get("license_id")
        status = row.get("status")
        if status == "admitted" and license_id in PERMISSIVE_TRAINING_LICENSES:
            permitted_seen = True
        if status == "quarantined" and license_id not in PERMISSIVE_TRAINING_LICENSES:
            rejected_seen = True
    if not permitted_seen:
        raise ValidationError("the live licence ledger has no allowlisted admitted licence")
    if not rejected_seen:
        raise ValidationError("the live licence ledger has no fail-closed quarantine evidence")


def _is_production_source(name: str) -> bool:
    lowered = name.lower()
    return bool(name) and not any(marker in lowered for marker in _NON_PRODUCTION_SOURCE_MARKERS)


def validate_corpus_overview(payload: dict[str, Any]) -> None:
    """Require durable decisions, real accepted sources, and real rejections."""
    decisions = _integer(payload.get("durable_decisions"), "corpus-overview.durable_decisions")
    exports = _integer(
        payload.get("training_export_documents"),
        "corpus-overview.training_export_documents",
    )
    if decisions == 0:
        raise ValidationError("the durable curation decision table is empty")
    if exports == 0:
        raise ValidationError("the durable training export is empty")
    if exports > decisions:
        raise ValidationError("training exports exceed durable curation decisions")

    rejected = payload.get("rejected_by_reason")
    if not isinstance(rejected, dict) or not any(
        isinstance(value, int) and not isinstance(value, bool) and value > 0
        for value in rejected.values()
    ):
        raise ValidationError("the durable corpus has no recorded rejection reasons")

    per_source = payload.get("per_source_acceptance")
    if not isinstance(per_source, list) or not per_source:
        raise ValidationError("the durable corpus has no per-source acceptance rows")
    production_rows = [
        row
        for row in per_source
        if isinstance(row, dict) and _is_production_source(str(row.get("source", "")))
    ]
    if not production_rows:
        raise ValidationError("the durable corpus contains only fixture or smoke sources")
    if not any(
        isinstance(row.get("total"), int)
        and not isinstance(row.get("total"), bool)
        and row["total"] > 0
        and isinstance(row.get("accepted"), int)
        and not isinstance(row.get("accepted"), bool)
        and row["accepted"] > 0
        for row in production_rows
    ):
        raise ValidationError("every production source is rejected from the training corpus")


def validate_dataset_summary(payload: dict[str, Any]) -> None:
    """Require a non-empty strict export with complete model provenance."""
    documents = _integer(payload.get("documents"), "datasets-summary.documents")
    sources = _integer(payload.get("source_count"), "datasets-summary.source_count")
    if documents == 0 or sources == 0:
        raise ValidationError("the strict licence-filtered dataset selection is empty")

    selection = payload.get("selection")
    if not isinstance(selection, dict):
        raise ValidationError("the dataset summary has no selection contract")
    if selection.get("license_policy") != "strict_allowlist":
        raise ValidationError("the dataset export is not using the strict licence allowlist")
    if selection.get("fixtures_included") is not False:
        raise ValidationError("the production dataset export includes fixtures")

    manifest = payload.get("manifest")
    revisions = manifest.get("revisions") if isinstance(manifest, dict) else None
    if not isinstance(revisions, dict):
        raise ValidationError("the dataset export has no revision manifest")
    for field in _CRITICAL_REVISIONS:
        values = revisions.get(field)
        if not isinstance(values, list) or not any(
            isinstance(value, str)
            and value.strip()
            and "unknown" not in value.lower()
            and "unset" not in value.lower()
            for value in values
        ):
            raise ValidationError(f"the dataset export has no concrete {field}")


def _request_json(
    opener: urllib.request.OpenerDirector, base_url: str, path: str
) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/{path.lstrip('/')}",
        headers={"Accept": "application/json"},
    )
    try:
        with opener.open(request, timeout=30) as response:
            decoded = json.load(response)
    except urllib.error.HTTPError as exc:
        raise ValidationError(f"DuckDB API returned HTTP {exc.code} for {path}") from exc
    except (OSError, ValueError) as exc:
        raise ValidationError(f"DuckDB API request failed for {path}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ValidationError(f"DuckDB API returned a non-object response for {path}")
    return decoded


def main() -> int:
    base_url = os.environ.get("S2P_DUCKDB_LOCAL_URL", DEFAULT_BASE_URL)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    now = datetime.now(UTC).isoformat()
    query = urllib.parse.urlencode(
        [
            ("date_from", "2000-01-01T00:00:00Z"),
            ("date_to", now),
            ("route", "pretrain"),
            ("route", "broad_pretraining"),
            ("route", "posttrain_candidate"),
            ("route", "reasoning_candidate"),
            ("include_structured", "true"),
        ]
    )
    try:
        health = _request_json(opener, base_url, "/healthz")
        if health.get("status") != "ok":
            raise ValidationError("DuckDB API is not healthy")
        admissions = _request_json(opener, base_url, "/license-admissions?recent_limit=100")
        overview = _request_json(opener, base_url, "/corpus-overview")
        dataset = _request_json(opener, base_url, f"/datasets/summary?{query}")
        validate_license_admissions(admissions)
        validate_corpus_overview(overview)
        validate_dataset_summary(dataset)
        print(
            json.dumps(
                {
                    "license_admissions": {
                        "admitted": admissions["admitted"],
                        "quarantined": admissions["quarantined"],
                    },
                    "corpus": {
                        "durable_decisions": overview["durable_decisions"],
                        "training_export_documents": overview["training_export_documents"],
                        "production_sources": [
                            row
                            for row in overview["per_source_acceptance"]
                            if _is_production_source(str(row.get("source", "")))
                        ],
                    },
                    "dataset": {
                        "documents": dataset["documents"],
                        "source_count": dataset["source_count"],
                        "revisions": dataset["manifest"]["revisions"],
                    },
                },
                sort_keys=True,
            )
        )
    except ValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
