"""Bounded, idempotent inference jobs: HTTP lifetimes never bound model work."""

from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any


class InferenceJobs:
    def __init__(self, *, capacity: int = 128, retention_seconds: float = 600) -> None:
        self._capacity = capacity
        self._retention = retention_seconds
        self._lock = threading.Lock()
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="quality-job")
        self._jobs: dict[str, tuple[float, Future[dict[str, Any]]]] = {}

    def submit(self, identity: bytes, work: Callable[[], dict[str, Any]]) -> str:
        key = hashlib.sha256(identity).hexdigest()
        with self._lock:
            now = time.monotonic()
            for old_key, (created, future) in list(self._jobs.items()):
                if future.done() and (
                    now - created > self._retention or future.exception() is not None
                ):
                    del self._jobs[old_key]
            if key not in self._jobs:
                if len(self._jobs) >= self._capacity:
                    completed = [k for k, (_, f) in self._jobs.items() if f.done()]
                    if not completed:
                        raise RuntimeError("inference job queue is full")
                    del self._jobs[completed[0]]
                self._jobs[key] = (now, self._executor.submit(work))
            return key

    def result(self, key: str, *, wait_seconds: float = 20) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(key)
        if job is None:
            raise KeyError(key)
        future = job[1]
        try:
            return future.result(timeout=wait_seconds)
        except TimeoutError:
            if future.done():
                raise
            return None

    def close(self) -> None:
        self._executor.shutdown(wait=True)
