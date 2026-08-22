"""Print process metrics and bounded Bytewax recovery metadata."""

from __future__ import annotations

import json
import sqlite3
import urllib.request
from pathlib import Path


def main() -> None:
    try:
        metrics = urllib.request.urlopen("http://127.0.0.1:9090/metrics", timeout=5).read()
        print(metrics.decode())
    except Exception as exc:
        print(json.dumps({"metrics_error": str(exc)}))

    for path in Path("/var/lib/s2p").rglob("*.sqlite3"):
        print(json.dumps({"recovery_db": str(path), "bytes": path.stat().st_size}))
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2)
        try:
            tables = [
                row[0]
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
                )
            ]
            for table in tables:
                count = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
                print(json.dumps({"table": table, "columns": columns, "rows": count}))
                for row in connection.execute(f'SELECT * FROM "{table}" LIMIT 5'):
                    print(repr(row)[:1000])
        finally:
            connection.close()


if __name__ == "__main__":
    main()
