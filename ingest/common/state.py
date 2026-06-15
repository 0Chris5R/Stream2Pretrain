"""Polling state persisted to disk.

The CronJob entrypoints need to remember per-feed cursors across runs:

- RSS / Atom: the ``ETag`` and ``Last-Modified`` headers seen in the last 200
- OAI-PMH: the ``from`` timestamp + outstanding resumption token (if any)
- HF Hub: the maximum ``lastModified`` seen so far

In production this state lives on a PVC mounted at ``/var/lib/s2p-state``. In
dev tests we point at a tmp dir.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


class FeedStateStore:
    """Tiny JSON-on-disk key/value store, one file per feed."""

    def __init__(self, root: str | Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def _path_for(self, feed_name: str) -> Path:
        # Keep file names POSIX-safe.
        safe = feed_name.replace("/", "_").replace(" ", "_")
        return self._root / f"{safe}.json"

    def get(self, feed_name: str) -> dict[str, Any]:
        p = self._path_for(feed_name)
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}

    def put(self, feed_name: str, state: dict[str, Any]) -> None:
        p = self._path_for(feed_name)
        tmp = p.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(state, sort_keys=True, default=str), encoding="utf-8")
        tmp.replace(p)
