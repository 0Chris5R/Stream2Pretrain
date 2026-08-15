"""MinIO-backed cursor store for the seed-loader Job.

The seed loader is a one-shot Bytewax Job; we still want it to be
deterministic on rerun so the demo can interrupt + restart without
double-ingesting. The cursor file at ``s3://state/seed-loader/<repo_id>.cursor.json``
holds a small JSON payload with per-namespace last-seen native ids and a
row counter:

    {
      "repo_id": "allenai/peS2o",
      "last_native_id": "S2_arxiv_2024-09:9999",
      "namespaces": {"S2_arxiv_2024-09": "9999"},
      "rows_emitted": 12345,
      "updated_at": "2026-06-15T12:00:00+00:00"
    }

On startup each per-source loader reads the cursor (or :class:`SeedCursor`
zero-value if none) and skips rows whose native id sorts <= the cursor's
last-seen id **within the same namespace**. The namespace is the prefix
before the first ``:`` in the native id (or the empty string when no ``:``
appears); this lets a single cursor file cover loaders whose stream
interleaves multiple sub-feeds:

- RedPajama-arxiv alternates between arXiv ids (``2304.12345``, namespace
  ``""``) and surrogate hash ids (``sha:0e1f...``, namespace ``"sha"``).
- Wayback uses ``<feed_name>:<timestamp>`` (one namespace per feed:
  ``rss-arxiv-cs-cl``, ``bair-blog``, ...).

Earlier versions kept a single ``last_native_id`` and skipped via plain
lex compare. That silently dropped every later row whose id sorted below
the most recent cross-namespace native id (e.g. all ``2304.*`` after a
``sha:*`` row, or every blog feed after the first arXiv RSS feed). The
namespace partitioning fixes that while staying compatible with old
single-id cursor files (``last_native_id`` is still written + read).

The store is intentionally tiny and synchronous; we are writing one JSON
object every N rows, not gigabyte payloads. Read failures are non-fatal:
the loader degrades to "cursor zero" and starts from the top.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    pass


def _namespace_of(native_id: str) -> str:
    """Return the prefix before the first ``:`` (empty string if none).

    The empty namespace covers ids that have no logical sub-stream
    (e.g. plain arXiv ids in RedPajama, plain peS2o ids); ``"sha"`` covers
    surrogate hash fallbacks; ``"<feed_name>"`` covers Wayback per-feed
    streams; ``"<dataset_subset>"`` covers peS2o-style ``S2_arxiv_2024-09``
    subset prefixes.
    """
    head, sep, _ = native_id.partition(":")
    return head if sep else ""


@dataclass(slots=True)
class SeedCursor:
    """Per-component, per-namespace progress marker.

    A zero-value :class:`SeedCursor` means "no progress yet" and the loader
    starts from the top of the stream. The :meth:`should_skip` predicate is
    used by each per-source iterator to skip already-emitted rows.
    """

    repo_id: str
    last_native_id: str = ""
    rows_emitted: int = 0
    updated_at: datetime = field(default_factory=lambda: datetime(1970, 1, 1, tzinfo=UTC))
    namespaces: dict[str, str] = field(default_factory=dict)
    """Per-namespace last-seen suffix.

    Key: the namespace prefix returned by :func:`_namespace_of`.
    Value: the suffix portion of the most recent native id seen in that
    namespace. Compared lexicographically against incoming suffixes, so
    components must still zero-pad inside a single namespace if their
    natural order is integer. The empty key holds ids without a ``:``
    separator.
    """

    def should_skip(self, native_id: str) -> bool:
        """Skip rows whose ``native_id`` was already emitted.

        Comparison is lexicographic on the suffix **within the namespace**
        (everything before the first ``:`` is the namespace key; everything
        after, or the whole id if there is no ``:``, is the suffix).
        Components whose natural order is not lexicographic inside a
        namespace must zero-pad in their loader before calling this.
        """
        ns = _namespace_of(native_id)
        suffix = native_id[len(ns) + 1 :] if ns else native_id
        last = self.namespaces.get(ns, "")
        if not last and self.last_native_id:
            legacy_ns = _namespace_of(self.last_native_id)
            if legacy_ns == ns:
                last = (
                    self.last_native_id[len(legacy_ns) + 1 :] if legacy_ns else self.last_native_id
                )
        if not last:
            return False
        return suffix <= last

    def advance(self, native_id: str) -> None:
        """Advance the cursor for ``native_id``'s namespace and bump rows."""
        ns = _namespace_of(native_id)
        suffix = native_id[len(ns) + 1 :] if ns else native_id
        self.namespaces[ns] = suffix
        # Maintained for backward compatibility with any consumer reading
        # the legacy ``last_native_id`` field; it is no longer used by
        # should_skip.
        self.last_native_id = native_id
        self.rows_emitted += 1
        self.updated_at = datetime.now(tz=UTC)

    def to_json(self) -> bytes:
        """Serialize to canonical JSON bytes."""
        payload = {
            "repo_id": self.repo_id,
            "last_native_id": self.last_native_id,
            "namespaces": dict(sorted(self.namespaces.items())),
            "rows_emitted": self.rows_emitted,
            "updated_at": self.updated_at.isoformat(),
        }
        return json.dumps(payload, sort_keys=True).encode("utf-8")

    @classmethod
    def from_json(cls, payload: bytes | str, *, repo_id: str) -> SeedCursor:
        """Parse a JSON payload into a :class:`SeedCursor`.

        Tolerates a missing or malformed ``updated_at`` by falling back to
        the unix epoch; the caller's reconciliation is "if the cursor is
        unreadable, start over". Older cursor files that predate the
        per-namespace map are migrated transparently: their flat
        ``last_native_id`` is split into ``(namespace, suffix)`` so the new
        skip predicate keeps the previous behaviour for that one namespace.
        """
        text = payload.decode("utf-8") if isinstance(payload, (bytes, bytearray)) else payload
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            return cls(repo_id=repo_id)
        if not isinstance(obj, dict):
            return cls(repo_id=repo_id)
        updated_raw = obj.get("updated_at")
        try:
            updated_at = (
                datetime.fromisoformat(updated_raw)
                if isinstance(updated_raw, str)
                else datetime(1970, 1, 1, tzinfo=UTC)
            )
        except ValueError:
            updated_at = datetime(1970, 1, 1, tzinfo=UTC)
        if updated_at.tzinfo is None:
            updated_at = updated_at.replace(tzinfo=UTC)
        ns_obj = obj.get("namespaces")
        namespaces: dict[str, str] = {}
        if isinstance(ns_obj, dict):
            for k, v in ns_obj.items():
                if isinstance(k, str) and isinstance(v, str):
                    namespaces[k] = v
        last_id = str(obj.get("last_native_id", ""))
        if last_id and not namespaces:
            # Migrate legacy cursor files (pre-namespace) into the new shape.
            ns = _namespace_of(last_id)
            suffix = last_id[len(ns) + 1 :] if ns else last_id
            namespaces[ns] = suffix
        return cls(
            repo_id=str(obj.get("repo_id", repo_id)),
            last_native_id=last_id,
            rows_emitted=int(obj.get("rows_emitted", 0) or 0),
            updated_at=updated_at,
            namespaces=namespaces,
        )


class CursorStore:
    """Thin MinIO key/value wrapper.

    The store reads and writes to ``s3://<state_bucket>/<prefix>/<repo_safe>.cursor.json``
    where ``repo_safe`` is ``repo_id`` with ``/`` replaced by ``__``. Any
    boto3 client (real MinIO, moto, in-memory stub) works.
    """

    def __init__(
        self,
        s3_client: Any,
        *,
        bucket: str = "state",
        prefix: str = "seed-loader",
    ) -> None:
        self._s3 = s3_client
        self._bucket = bucket
        self._prefix = prefix.strip("/")

    @staticmethod
    def _safe(repo_id: str) -> str:
        """``allenai/peS2o`` -> ``allenai__peS2o``."""
        return repo_id.replace("/", "__")

    def key_for(self, repo_id: str) -> str:
        """Object key the cursor for ``repo_id`` lives at."""
        return f"{self._prefix}/{self._safe(repo_id)}.cursor.json"

    def load(self, repo_id: str) -> SeedCursor:
        """Read the cursor for ``repo_id``; missing object returns zero."""
        key = self.key_for(repo_id)
        try:
            resp = self._s3.get_object(Bucket=self._bucket, Key=key)
        except Exception:
            # Includes botocore.exceptions.ClientError(NoSuchKey) and any
            # transient endpoint failure. We never want a missing cursor to
            # crash the Job; a fresh cursor is a valid state.
            return SeedCursor(repo_id=repo_id)
        body = resp.get("Body")
        if body is None:
            return SeedCursor(repo_id=repo_id)
        try:
            data = body.read()
        except Exception:
            return SeedCursor(repo_id=repo_id)
        return SeedCursor.from_json(data, repo_id=repo_id)

    def save(self, cursor: SeedCursor) -> None:
        """Persist the cursor for ``cursor.repo_id``.

        Writes are idempotent on identical content; we do not gate on
        version.
        """
        key = self.key_for(cursor.repo_id)
        self._s3.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=cursor.to_json(),
            ContentType="application/json",
        )


__all__ = ["CursorStore", "SeedCursor"]
