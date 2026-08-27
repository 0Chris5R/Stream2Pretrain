"""Print a compact, read-only diagnosis of the durable post-training store."""

from __future__ import annotations

import json
import os
import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any


def _json(value: bytes | str) -> dict[str, Any]:
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise TypeError("foundry JSON record must be an object")
    return parsed


def main() -> None:
    state_dir = Path(os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry"))
    database = state_dir / "control.sqlite3"
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row

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
    daily_runs = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM daily_runs ORDER BY run_date DESC LIMIT 5"
        ).fetchall()
    ]
    latest_run_candidates = [
        dict(row)
        for row in connection.execute(
            """
            SELECT run_date,rank,doc_id FROM daily_run_candidates
            WHERE run_date=(SELECT MAX(run_date) FROM daily_run_candidates)
            ORDER BY rank
            """
        ).fetchall()
    ]
    manual_runs = [
        dict(row)
        for row in connection.execute(
            "SELECT * FROM manual_runs ORDER BY requested_at DESC LIMIT 5"
        ).fetchall()
    ]
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
        "database_bytes": database.stat().st_size,
        "job_counts": dict(Counter(str(job["state"]) for job in jobs)),
        "artifact_counts": dict(sorted(artifact_counts.items())),
        "candidate_queue": queue,
        "daily_candidate_limit": int(os.environ.get("S2P_FOUNDRY_DAILY_CANDIDATE_LIMIT", "20")),
        "daily_not_before_utc": os.environ.get("S2P_FOUNDRY_DAILY_NOT_BEFORE_UTC"),
        "daily_run_hour_utc": int(os.environ.get("S2P_FOUNDRY_DAILY_RUN_HOUR_UTC", "0")),
        "daily_runs": daily_runs,
        "latest_daily_run_candidates": latest_run_candidates,
        "manual_runs": manual_runs,
        "stream_checkpoints": stream_checkpoints,
        "verifier_attempts": verifier_attempts,
        "jobs": jobs,
        "artifacts": artifacts,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
