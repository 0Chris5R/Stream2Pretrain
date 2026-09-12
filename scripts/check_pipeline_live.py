"""Small read-only cloud status check, run inside the named service container."""

from __future__ import annotations

import json
import os
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote

from processor.foundry.database import (
    connect_database,
    coordination_database_target,
    database_dialect,
)


def _foundry_status() -> dict[str, object]:
    state_dir = os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry")
    target = coordination_database_target(state_dir, "control.sqlite3")
    connection = connect_database(target, read_only=True)
    backend = database_dialect(connection)
    try:
        return {
            "backend": backend,
            "database_bytes": Path(target).stat().st_size if backend == "sqlite" else None,
            "queue": [
                dict(row)
                for row in connection.execute(
                    "SELECT state,COUNT(*) AS count,"
                    "COUNT(scientific_payload) AS retained_evidence "
                    "FROM candidate_queue GROUP BY state"
                )
            ],
            "recent_events": [
                json.loads(row["event_json"])
                for row in connection.execute(
                    "SELECT event_json FROM events ORDER BY occurred_at DESC LIMIT 12"
                )
            ],
            "stream_progress": [
                dict(row)
                for row in connection.execute(
                    "SELECT job_id,call_key,attempt,LENGTH(partial_text) AS characters,updated_at "
                    "FROM stream_checkpoints ORDER BY updated_at DESC LIMIT 3"
                )
            ],
        }
    finally:
        connection.close()


def main() -> None:
    role = sys.argv[1]
    if role == "foundry":
        print(json.dumps(_foundry_status()))
        return
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    paths = ["/readyz", "/corpus-overview"] if role == "duckdb" else ["/healthz", "/metrics"]
    if role == "duckdb":
        paths.append("/as-of?ts=" + quote(datetime.now(UTC).isoformat()))
    port = 8090 if role == "duckdb" else 9090
    for path in paths:
        with opener.open(f"http://[::1]:{port}{path}", timeout=30) as response:
            body = response.read().decode()
        if path == "/metrics":
            body = "\n".join(
                line
                for line in body.splitlines()
                if line.startswith(("s2p_processor_", "s2p_curator_model_endpoints"))
            )
        print(f"{role} {path}: {body}")


if __name__ == "__main__":
    main()
