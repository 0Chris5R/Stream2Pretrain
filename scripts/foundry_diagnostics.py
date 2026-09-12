"""Print a compact, read-only diagnosis of the durable post-training store."""

from __future__ import annotations

import json
import os
from collections import Counter
from pathlib import Path
from typing import Any

from processor.foundry.database import (
    connect_database,
    coordination_database_target,
    database_dialect,
)


def _json(value: bytes | str) -> dict[str, Any]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise TypeError("foundry JSON record must be an object")
    return parsed


def main() -> None:
    state_dir = Path(os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry"))
    target = coordination_database_target(str(state_dir), "control.sqlite3")
    connection = connect_database(target, read_only=True)
    backend = database_dialect(connection)
    database_bytes = Path(target).stat().st_size if backend == "sqlite" else None

    jobs = [
        dict(row)
        for row in connection.execute(
            """
            SELECT job_id,paper_id,doc_id,state,reason,received_at,updated_at
            FROM jobs ORDER BY updated_at DESC LIMIT 100
            """
        ).fetchall()
    ]
    artifacts = []
    for row in connection.execute(
        "SELECT artifact_json FROM artifacts ORDER BY created_at DESC LIMIT 500"
    ).fetchall():
        artifact = _json(row["artifact_json"])
        validation = artifact.get("validation")
        artifacts.append(
            {
                key: artifact.get(key)
                for key in (
                    "artifact_id",
                    "job_id",
                    "task_id",
                    "family",
                    "kind",
                    "pool",
                    "dataset_split",
                    "status",
                    "created_at",
                )
            }
            | {"validation": validation}
        )

    queue = {
        str(row["state"]): int(row["count"])
        for row in connection.execute(
            "SELECT state,COUNT(*) AS count FROM candidate_queue GROUP BY state"
        ).fetchall()
    }
    verifier_attempts = []
    for row in connection.execute(
        """
        SELECT job_id,call_key,response_json,created_at
        FROM provider_results
        WHERE call_key LIKE 'verifier_%'
        ORDER BY created_at DESC LIMIT 100
        """
    ).fetchall():
        verifier_attempts.append(
            {
                "job_id": str(row["job_id"]),
                "call_key": str(row["call_key"]),
                "created_at": str(row["created_at"]),
                "response": _json(row["response_json"]),
            }
        )
    stream_checkpoints = []
    for row in connection.execute(
        """
        SELECT job_id,call_key,attempt,partial_text,updated_at
        FROM stream_checkpoints
        ORDER BY updated_at DESC LIMIT 20
        """
    ).fetchall():
        partial = bytes(row["partial_text"]).decode("utf-8", errors="replace")
        stream_checkpoints.append(
            {
                "job_id": str(row["job_id"]),
                "call_key": str(row["call_key"]),
                "attempt": int(row["attempt"]),
                "partial_characters": len(partial),
                "starts_with": partial[:400],
                "ends_with": partial[-400:],
                "updated_at": str(row["updated_at"]),
            }
        )
    artifact_counts = Counter(f"{artifact['kind']}:{artifact['status']}" for artifact in artifacts)
    payload = {
        "backend": backend,
        "database_bytes": database_bytes,
        "job_counts": dict(Counter(str(job["state"]) for job in jobs)),
        "artifact_counts": dict(sorted(artifact_counts.items())),
        "candidate_queue": queue,
        "daily_runs": [
            dict(row)
            for row in connection.execute("SELECT * FROM daily_runs ORDER BY run_date DESC LIMIT 3")
        ],
        "queue_evidence": [
            dict(row)
            for row in connection.execute(
                "SELECT state, COUNT(*) AS candidates, "
                "COUNT(scientific_payload) AS cached_evidence, "
                "MIN(enqueued_at) AS oldest, MAX(enqueued_at) AS newest "
                "FROM candidate_queue GROUP BY state"
            )
        ],
        "stream_checkpoints": stream_checkpoints,
        "verifier_attempts": verifier_attempts,
        "jobs": jobs,
        "artifacts": artifacts,
    }
    connection.close()
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
