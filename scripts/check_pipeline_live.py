"""Small read-only cloud status check, run inside the named service container."""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote


def main() -> None:
    role = sys.argv[1]
    if role == "foundry":
        path = (
            Path(os.environ.get("S2P_FOUNDRY_STATE_DIR", "/var/lib/s2p/foundry"))
            / "control.sqlite3"
        )
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as conn:
            conn.row_factory = sqlite3.Row
            print(
                json.dumps(
                    {
                        "queue": [
                            dict(row)
                            for row in conn.execute(
                                "SELECT state,COUNT(*) AS count,SUM(scientific_payload IS NOT NULL) AS retained_evidence FROM candidate_queue GROUP BY state"
                            )
                        ],
                        "recent_events": [
                            json.loads(row[0])
                            for row in conn.execute(
                                "SELECT event_json FROM events ORDER BY occurred_at DESC LIMIT 12"
                            )
                        ],
                        "stream_progress": [
                            dict(row)
                            for row in conn.execute(
                                "SELECT job_id,call_key,attempt,LENGTH(partial_text) AS characters,updated_at "
                                "FROM stream_checkpoints ORDER BY updated_at DESC LIMIT 3"
                            )
                        ],
                    }
                )
            )
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
