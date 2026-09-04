"""Durable completed section scores, keyed by exact input and model revision."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Sequence
from dataclasses import asdict
from typing import Any

from processor.operators.quality import QualityScore


class CachedQualityScorer:
    def __init__(self, scorer: Any, path: str) -> None:
        self._scorer = scorer
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS scores "
            "(key TEXT PRIMARY KEY, value TEXT NOT NULL, created INTEGER NOT NULL)"
        )
        self._db.commit()
        self._writes = 0

    @property
    def revision(self) -> str:
        return self._scorer.revision

    @property
    def backend(self) -> str:
        return self._scorer.backend

    def _key(self, text: str) -> str:
        return hashlib.sha256((self.revision + "\0" + text).encode()).hexdigest()

    def score(self, text: str) -> QualityScore:
        return self.score_many([text])[0]

    def score_many(self, texts: Sequence[str]) -> list[QualityScore]:
        values: dict[str, QualityScore] = {}
        missing: dict[str, str] = {}
        keys = [self._key(text) for text in texts]
        with self._lock:
            for key, text in zip(keys, texts, strict=True):
                row = self._db.execute("SELECT value FROM scores WHERE key=?", (key,)).fetchone()
                if row is None:
                    missing[key] = text
                else:
                    payload = json.loads(row[0])
                    payload["probabilities"] = tuple(payload.get("probabilities", ()))
                    values[key] = QualityScore(**payload)
        if missing:
            score_many = getattr(self._scorer, "score_many", None)
            results = (
                score_many(list(missing.values()))
                if callable(score_many)
                else [self._scorer.score(text) for text in missing.values()]
            )
            pairs = list(zip(missing, results, strict=True))
            with self._lock, self._db:
                self._db.executemany(
                    "INSERT OR REPLACE INTO scores VALUES (?, ?, unixepoch())",
                    [(key, json.dumps(asdict(score))) for key, score in pairs],
                )
                self._writes += len(pairs)
                if self._writes >= 1000:
                    self._db.execute(
                        "DELETE FROM scores WHERE key IN (SELECT key FROM scores "
                        "ORDER BY created DESC, key LIMIT -1 OFFSET 100000)"
                    )
                    self._writes = 0
            values.update(pairs)
        return [values[key] for key in keys]

    def close(self) -> None:
        with self._lock:
            self._db.close()
