"""Transactional provider quota reservation and reconciliation."""

from __future__ import annotations

import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

from processor.foundry.config import ProviderConfig
from schemas.foundry import ProviderTrace, QuotaState


class QuotaExceededError(RuntimeError):
    def __init__(
        self,
        *,
        provider: str,
        window: str,
        resource: str,
        usable_limit: int,
    ) -> None:
        self.provider = provider
        self.window = window
        self.resource = resource
        self.usable_limit = usable_limit
        super().__init__(f"{provider} {window} {resource} limit would exceed {usable_limit}")


@dataclass(frozen=True, slots=True)
class Reservation:
    reservation_id: str
    provider: str
    requests: int
    input_tokens: int
    output_tokens: int
    minute_start: datetime
    day_start: datetime


class QuotaLedger:
    """Single-writer SQLite ledger used by the foundry StatefulSet."""

    def __init__(
        self,
        path: str,
        providers: dict[str, ProviderConfig],
    ) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._providers = providers
        self._lock = threading.Lock()
        self._conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=FULL;
            CREATE TABLE IF NOT EXISTS quota_windows (
              provider TEXT NOT NULL,
              window_kind TEXT NOT NULL,
              window_start TEXT NOT NULL,
              requests_used INTEGER NOT NULL DEFAULT 0,
              input_used INTEGER NOT NULL DEFAULT 0,
              output_used INTEGER NOT NULL DEFAULT 0,
              requests_reserved INTEGER NOT NULL DEFAULT 0,
              input_reserved INTEGER NOT NULL DEFAULT 0,
              output_reserved INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(provider, window_kind, window_start)
            );
            CREATE TABLE IF NOT EXISTS quota_reservations (
              reservation_id TEXT PRIMARY KEY,
              provider TEXT NOT NULL,
              requests INTEGER NOT NULL,
              input_tokens INTEGER NOT NULL,
              output_tokens INTEGER NOT NULL,
              minute_start TEXT NOT NULL,
              day_start TEXT NOT NULL,
              state TEXT NOT NULL,
              created_at TEXT NOT NULL,
              reconciled_at TEXT
            );
            """
        )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def reconcile_abandoned_reservations(self) -> int:
        """Conservatively charge reservations left open by a prior worker process."""
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    "SELECT * FROM quota_reservations WHERE state='reserved'"
                ).fetchall()
                for row in rows:
                    for kind, start in (
                        ("minute", row["minute_start"]),
                        ("day", row["day_start"]),
                    ):
                        self._conn.execute(
                            """
                            UPDATE quota_windows SET
                              requests_reserved=MAX(0, requests_reserved - ?),
                              input_reserved=MAX(0, input_reserved - ?),
                              output_reserved=MAX(0, output_reserved - ?),
                              requests_used=requests_used + ?,
                              input_used=input_used + ?,
                              output_used=output_used + ?
                            WHERE provider=? AND window_kind=? AND window_start=?
                            """,
                            (
                                row["requests"],
                                row["input_tokens"],
                                row["output_tokens"],
                                row["requests"],
                                row["input_tokens"],
                                row["output_tokens"],
                                row["provider"],
                                kind,
                                start,
                            ),
                        )
                    self._conn.execute(
                        """
                        UPDATE quota_reservations
                        SET state='reconciled_after_restart',reconciled_at=?
                        WHERE reservation_id=?
                        """,
                        (now, row["reservation_id"]),
                    )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return len(rows)

    def reserve(
        self,
        provider: str,
        *,
        input_tokens: int,
        output_tokens: int,
        requests: int = 1,
        now: datetime | None = None,
    ) -> Reservation:
        config = self._providers[provider]
        current = (now or datetime.now(UTC)).astimezone(UTC)
        minute_start = current.replace(second=0, microsecond=0)
        day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        reservation = Reservation(
            reservation_id=f"quota:{uuid.uuid4()}",
            provider=provider,
            requests=requests,
            input_tokens=max(0, input_tokens),
            output_tokens=max(0, output_tokens),
            minute_start=minute_start,
            day_start=day_start,
        )
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                minute = self._window(provider, "minute", minute_start)
                day = self._window(provider, "day", day_start)
                self._assert_capacity(config, minute, reservation, "minute")
                self._assert_capacity(config, day, reservation, "day")
                for kind, start in (("minute", minute_start), ("day", day_start)):
                    self._conn.execute(
                        """
                        UPDATE quota_windows SET
                          requests_reserved=requests_reserved + ?,
                          input_reserved=input_reserved + ?,
                          output_reserved=output_reserved + ?
                        WHERE provider=? AND window_kind=? AND window_start=?
                        """,
                        (
                            requests,
                            reservation.input_tokens,
                            reservation.output_tokens,
                            provider,
                            kind,
                            start.isoformat(),
                        ),
                    )
                self._conn.execute(
                    """
                    INSERT INTO quota_reservations VALUES (?, ?, ?, ?, ?, ?, ?, 'reserved', ?, NULL)
                    """,
                    (
                        reservation.reservation_id,
                        provider,
                        requests,
                        reservation.input_tokens,
                        reservation.output_tokens,
                        minute_start.isoformat(),
                        day_start.isoformat(),
                        current.isoformat(),
                    ),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return reservation

    def reconcile(self, reservation: Reservation, trace: ProviderTrace | None) -> None:
        actual_requests = trace.request_attempts if trace is not None else reservation.requests
        actual_input = (
            trace.input_tokens
            if trace is not None and trace.input_tokens > 0
            else reservation.input_tokens
        )
        actual_output = (
            trace.output_tokens
            if trace is not None and trace.output_tokens > 0
            else reservation.output_tokens
        )
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT state FROM quota_reservations WHERE reservation_id=?",
                    (reservation.reservation_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(reservation.reservation_id)
                if row["state"] != "reserved":
                    self._conn.rollback()
                    return
                for kind, start in (
                    ("minute", reservation.minute_start),
                    ("day", reservation.day_start),
                ):
                    self._conn.execute(
                        """
                        UPDATE quota_windows SET
                          requests_reserved=MAX(0, requests_reserved - ?),
                          input_reserved=MAX(0, input_reserved - ?),
                          output_reserved=MAX(0, output_reserved - ?),
                          requests_used=requests_used + ?,
                          input_used=input_used + ?,
                          output_used=output_used + ?
                        WHERE provider=? AND window_kind=? AND window_start=?
                        """,
                        (
                            reservation.requests,
                            reservation.input_tokens,
                            reservation.output_tokens,
                            actual_requests,
                            actual_input,
                            actual_output,
                            reservation.provider,
                            kind,
                            start.isoformat(),
                        ),
                    )
                self._conn.execute(
                    "UPDATE quota_reservations SET state='reconciled', reconciled_at=? WHERE reservation_id=?",
                    (now, reservation.reservation_id),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def states(self, now: datetime | None = None) -> list[QuotaState]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        minute_start = current.replace(second=0, microsecond=0)
        day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
        result: list[QuotaState] = []
        with self._lock:
            for provider, config in self._providers.items():
                for kind, start, reset in (
                    ("minute", minute_start, minute_start + timedelta(minutes=1)),
                    ("day", day_start, day_start + timedelta(days=1)),
                ):
                    row = self._window(provider, kind, start)
                    req_limit, in_limit, out_limit = self._limits(config, kind)
                    result.append(
                        QuotaState(
                            provider=provider,  # type: ignore[arg-type]
                            window=kind,  # type: ignore[arg-type]
                            observed_requests_used=int(row["requests_used"]),
                            observed_input_used=int(row["input_used"]),
                            observed_output_used=int(row["output_used"]),
                            locally_reserved_requests=int(row["requests_reserved"]),
                            locally_reserved_input=int(row["input_reserved"]),
                            locally_reserved_output=int(row["output_reserved"]),
                            estimated_remaining_requests=_remaining(
                                req_limit,
                                int(row["requests_used"]) + int(row["requests_reserved"]),
                            ),
                            estimated_remaining_input=_remaining(
                                in_limit,
                                int(row["input_used"]) + int(row["input_reserved"]),
                            ),
                            estimated_remaining_output=_remaining(
                                out_limit,
                                int(row["output_used"]) + int(row["output_reserved"]),
                            ),
                            reset_at=reset,
                            confidence="local_exact",
                        )
                    )
        return result

    def _window(self, provider: str, kind: str, start: datetime) -> sqlite3.Row:
        self._conn.execute(
            "INSERT OR IGNORE INTO quota_windows(provider, window_kind, window_start) VALUES (?, ?, ?)",
            (provider, kind, start.isoformat()),
        )
        row = self._conn.execute(
            "SELECT * FROM quota_windows WHERE provider=? AND window_kind=? AND window_start=?",
            (provider, kind, start.isoformat()),
        ).fetchone()
        assert row is not None
        return cast(sqlite3.Row, row)

    def _assert_capacity(
        self,
        config: ProviderConfig,
        row: sqlite3.Row,
        reservation: Reservation,
        kind: str,
    ) -> None:
        limits = self._limits(config, kind)
        used = (
            int(row["requests_used"]) + int(row["requests_reserved"]) + reservation.requests,
            int(row["input_used"]) + int(row["input_reserved"]) + reservation.input_tokens,
            int(row["output_used"]) + int(row["output_reserved"]) + reservation.output_tokens,
        )
        for name, value, limit in zip(("requests", "input", "output"), used, limits, strict=True):
            if limit is None:
                continue
            if value > limit:
                raise QuotaExceededError(
                    provider=config.name,
                    window=kind,
                    resource=name,
                    usable_limit=limit,
                )

    @staticmethod
    def _limits(config: ProviderConfig, kind: str) -> tuple[int | None, int | None, int | None]:
        if kind == "minute":
            return config.minute_requests, config.minute_input_tokens, config.minute_output_tokens
        return config.daily_requests, config.daily_input_tokens, config.daily_output_tokens


def _remaining(limit: int | None, used: int) -> int | None:
    return None if limit is None else max(0, limit - used)


__all__ = ["QuotaExceededError", "QuotaLedger", "Reservation"]
