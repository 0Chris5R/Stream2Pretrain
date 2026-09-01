"""Stream the unique durable processed full-text pool for judge labeling.

This script is designed to be piped into the already-running DuckDB pod with
``python -``. It reads only the latest durable decision for each document. No
route, licence, risk, rejection, or existing-classifier predicate is applied:
this is exactly the full-text population from which the replacement learned
classifiers are trained. Requested limits are maxima and rows are never
duplicated to reach a target count.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Iterator
from typing import Any

from processor.duckdb_api import DuckDBQueryService


def _bounded_limit(name: str, default: int) -> int:
    value = int(os.environ.get(name, str(default)))
    if value < 1 or value > 10_000:
        raise ValueError(f"{name} must be between 1 and 10000")
    return value


_OUTPUT_COLUMNS = (
    "doc_id",
    "text",
    "source_feed",
    "source_format",
    "valid_from",
    "route",
    "tokens",
    "quality_score",
    "edu_score",
    "structural_quality_score",
    "reasoning_score",
    "scoring_version",
    "classifier_revision",
    "projection_version",
    "license",
)


def _selected_keys(
    service: DuckDBQueryService, *, sources: tuple[str, ...], limit: int
) -> list[dict[str, Any]]:
    quoted_sources = ", ".join("'" + value.replace("'", "''") + "'" for value in sources)
    # A document can have several decisions after a policy deployment. Select
    # the latest decision so each full-text document appears exactly once.
    # Keep multi-megabyte text out of this ranking and hash sort. Sorting text
    # exceeded the serving pod's memory limit even though the sample is small.
    sql = f"""
    WITH ranked AS (
      SELECT
        doc_id, scoring_version, classifier_revision, policy_revision, trace_id,
        ROW_NUMBER() OVER (
          PARTITION BY doc_id
          ORDER BY valid_from DESC, scoring_version DESC, policy_revision DESC
        ) AS revision_rank
      FROM decisions
      WHERE source_feed IN ({quoted_sources})
    )
    SELECT
      doc_id, scoring_version, classifier_revision, policy_revision, trace_id
    FROM ranked
    WHERE revision_rank = 1
    ORDER BY HASH(doc_id)
    LIMIT ?
    """
    return service._rows(sql, [limit], relation=service._decisions)


def _stream_rows(
    service: DuckDBQueryService, *, sources: tuple[str, ...], limit: int
) -> Iterator[dict[str, Any]]:
    """Join selected identities back to bodies without buffering the bodies."""
    # A small identity-only buffer replaces selected latest decisions whose
    # projection is empty. The join stays streaming and stops at the requested
    # count; no body participates in the ranking or hash sort.
    keys = _selected_keys(service, sources=sources, limit=min(10_000, limit + 500))
    connection = service._conn
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE judge_selected_keys (
          ordinal INTEGER,
          doc_id VARCHAR,
          scoring_version VARCHAR,
          classifier_revision VARCHAR,
          policy_revision VARCHAR,
          trace_id VARCHAR
        )
        """
    )
    connection.executemany(  # type: ignore[attr-defined]
        "INSERT INTO judge_selected_keys VALUES (?, ?, ?, ?, ?, ?)",
        [
            (
                ordinal,
                row["doc_id"],
                row["scoring_version"],
                row["classifier_revision"],
                row["policy_revision"],
                row["trace_id"],
            )
            for ordinal, row in enumerate(keys)
        ],
    )
    cursor = connection.execute(
        f"""
        SELECT
          decision.doc_id, decision.text, decision.source_feed,
          decision.source_format, CAST(decision.valid_from AS VARCHAR),
          decision.route, decision.tokens, decision.quality_score,
          decision.edu_score, decision.structural_quality_score,
          decision.reasoning_score, decision.scoring_version,
          decision.classifier_revision, decision.projection_version,
          COALESCE(decision.spdx_license, decision.license) AS license
        FROM {service._decisions} AS decision
        INNER JOIN judge_selected_keys AS selected
          ON decision.doc_id = selected.doc_id
         AND decision.scoring_version = selected.scoring_version
         AND decision.classifier_revision = selected.classifier_revision
         AND decision.policy_revision = selected.policy_revision
         AND decision.trace_id = selected.trace_id
        WHERE LENGTH(TRIM(decision.text)) > 0
        """
    )
    emitted = 0
    while rows := cursor.fetchmany(8):  # type: ignore[attr-defined]
        for values in rows:
            yield dict(zip(_OUTPUT_COLUMNS, values, strict=True))
            emitted += 1
            if emitted >= limit:
                return


def main() -> None:
    service = DuckDBQueryService.from_env()
    arxiv_limit = _bounded_limit("S2P_JUDGE_ARXIV_LIMIT", 3_000)
    hf_limit = _bounded_limit("S2P_JUDGE_HF_LIMIT", 5_000)
    for sources, limit in (
        (("arxiv-html-fetcher",), arxiv_limit),
        (("hf-models", "hf-datasets"), hf_limit),
    ):
        for row in _stream_rows(service, sources=sources, limit=limit):
            sys.stdout.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


if __name__ == "__main__":
    main()
