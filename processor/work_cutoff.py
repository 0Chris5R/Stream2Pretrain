"""Age admission for unfinished pretraining work, independent of corpus retention."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

from processor.metrics import ProcessorMetrics


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class WorkCutoff:
    max_age_seconds: int = 86400
    clock: Callable[[], datetime] = _now

    def __post_init__(self) -> None:
        if self.max_age_seconds <= 0:
            raise ValueError("pretraining work cutoff must be positive")

    @classmethod
    def from_env(cls) -> WorkCutoff:
        return cls(
            max_age_seconds=int(os.environ.get("S2P_PRETRAIN_MAX_WORK_AGE_SECONDS", "86400"))
        )

    def expired(
        self,
        fetched_at: datetime | None,
        *,
        stage: str,
        source_feed: str,
        metrics: ProcessorMetrics | None = None,
    ) -> bool:
        reason: str | None = None
        if fetched_at is None:
            reason = "missing_intake_timestamp"
        else:
            intake = fetched_at.replace(tzinfo=UTC) if fetched_at.tzinfo is None else fetched_at
            if (self.clock() - intake).total_seconds() >= self.max_age_seconds:
                reason = "age_exceeded"
        if reason is None:
            return False
        if metrics is not None:
            metrics.record_work_expired(stage=stage, source_feed=source_feed, reason=reason)
        return True
