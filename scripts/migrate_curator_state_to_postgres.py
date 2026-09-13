"""Migrate the quiesced curator's local SQLite and LSH state into PostgreSQL.

The curator's decision cache is part of its at-least-once boundary. Its
near-duplicate state is equally durable: an incomplete or ambiguous import
would permit a document that the previous curator rejected. This command is
therefore deliberately fail-closed. It snapshots the local state, validates
the complete logical LSH index, and only imports into an empty PostgreSQL
target or verifies a byte-for-byte equivalent earlier import.
"""

from __future__ import annotations

import argparse
import errno
import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from processor.foundry.database import PostgresConnection, connect_database
from processor.operators.minhash import MinHasher, MinHashSignature

CURATOR_SCHEMA_LOCK_NAME = "stream2pretrain-curator-state-migration-v1"


@dataclass(frozen=True, slots=True)
class DecisionRow:
    cache_key: str
    payload: bytes
    trainable: bool


@dataclass(frozen=True, slots=True)
class DecisionLshEvidence:
    doc_id: str
    text: str
    generation: str
    cluster_id: str | None
    near_duplicate: bool
    signature_backend: str
    num_perms: int


@dataclass(frozen=True, slots=True)
class LshClusterRow:
    generation: str
    cluster_id: str
    anchor_doc_id: str
    signature: bytes
    signature_backend: str = ""
    num_perms: int = 0


@dataclass(frozen=True, slots=True)
class LshBandRow:
    generation: str
    cluster_key: str
    cluster_id: str
    signature_backend: str = ""
    num_perms: int = 0


@dataclass(frozen=True, slots=True)
class LegacyLshBandRow:
    generation: str
    cluster_key: str
    cluster_id: str
    signature_backend: str
    num_perms: int


@dataclass(frozen=True, slots=True)
class CuratorState:
    decisions: tuple[DecisionRow, ...]
    clusters: tuple[LshClusterRow, ...]
    bands: tuple[LshBandRow, ...]
    legacy_bands: tuple[LegacyLshBandRow, ...] = ()


@dataclass(frozen=True, slots=True)
class LshSource:
    generation: str
    backend: Literal["leveldb", "sqlitedict"]
    path: Path
    relative_path: Path


@dataclass(slots=True)
class _LshMaps:
    cluster_keys: dict[str, str]
    anchors: dict[str, str]
    signatures: dict[str, bytes]


def _update_digest(digest: Any, value: Any) -> None:
    if value is None:
        tag = b"null"
        payload = b""
    elif isinstance(value, (bytes, bytearray, memoryview)):
        tag = b"bytes"
        payload = bytes(value)
    elif isinstance(value, bool):
        tag = b"bool"
        payload = b"true" if value else b"false"
    elif isinstance(value, int):
        tag = b"int"
        payload = str(value).encode("ascii")
    elif isinstance(value, str):
        tag = b"text"
        payload = value.encode("utf-8")
    else:
        raise TypeError(f"unsupported state value type: {type(value).__name__}")
    digest.update(len(tag).to_bytes(1, "big"))
    digest.update(tag)
    digest.update(len(payload).to_bytes(8, "big"))
    digest.update(payload)


def _fingerprint_rows(rows: Iterable[Sequence[Any]]) -> dict[str, int | str]:
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        digest.update(b"row")
        for value in row:
            _update_digest(digest, value)
        count += 1
    return {"rows": count, "sha256": digest.hexdigest()}


def fingerprints(state: CuratorState) -> dict[str, dict[str, int | str]]:
    """Return deterministic logical fingerprints, excluding target timestamps."""
    return {
        "curator_decisions": _fingerprint_rows(
            (row.cache_key, row.payload, row.trainable) for row in state.decisions
        ),
        "curator_lsh_clusters": _fingerprint_rows(
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_id,
                row.anchor_doc_id,
                row.signature,
            )
            for row in state.clusters
        ),
        "curator_lsh_bands": _fingerprint_rows(
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_key,
                row.cluster_id,
            )
            for row in state.bands
        ),
        "curator_lsh_legacy_bands": _fingerprint_rows(
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_key,
                row.cluster_id,
            )
            for row in state.legacy_bands
        ),
    }


def source_fingerprints(
    decision_snapshot: Path,
    lsh_state: CuratorState,
) -> dict[str, dict[str, int | str]]:
    """Fingerprint the large SQLite cache by streaming it in deterministic order."""
    return {
        "curator_decisions": _fingerprint_rows(
            (row.cache_key, row.payload, row.trainable) for row in iter_decisions(decision_snapshot)
        ),
        **{
            name: value
            for name, value in fingerprints(
                CuratorState(
                    decisions=(),
                    clusters=lsh_state.clusters,
                    bands=lsh_state.bands,
                    legacy_bands=lsh_state.legacy_bands,
                )
            ).items()
            if name != "curator_decisions"
        },
    }


def _snapshot_sqlite(source: Path, destination: Path) -> None:
    if not source.is_file():
        raise FileNotFoundError(source)
    source_connection = sqlite3.connect(f"{source.resolve().as_uri()}?mode=ro", uri=True)
    destination_connection = sqlite3.connect(destination)
    try:
        source_connection.backup(destination_connection)
    finally:
        destination_connection.close()
        source_connection.close()


def _sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _file_hashes(root: Path) -> dict[str, str]:
    if root.is_file():
        return {root.name: _sha256_file(root)}
    return {
        str(path.relative_to(root)): _sha256_file(path)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def _logical_size(path: Path) -> int:
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _sqlite_source_size(path: Path) -> int:
    return sum(
        candidate.stat().st_size
        for candidate in (path, Path(f"{path}-wal"), Path(f"{path}-shm"))
        if candidate.is_file()
    )


def _existing_parent(path: Path) -> Path:
    parent = path.parent.resolve()
    while not parent.exists():
        if parent == parent.parent:
            raise FileNotFoundError(path.parent)
        parent = parent.parent
    return parent


def _ensure_snapshot_capacity(
    decision_source: Path,
    lsh_sources: Iterable[LshSource],
    snapshot_dir: Path,
) -> None:
    required_bytes = _sqlite_source_size(decision_source) + sum(
        _logical_size(source.path) for source in lsh_sources
    )
    available_bytes = shutil.disk_usage(_existing_parent(snapshot_dir)).free
    if available_bytes < required_bytes:
        raise RuntimeError(
            "insufficient free space for the curator migration snapshot: "
            f"required_bytes={required_bytes}, available_bytes={available_bytes}"
        )


@contextmanager
def _quiesced_leveldb_source(path: Path) -> Iterator[None]:
    lock_path = path / "LOCK"
    if not lock_path.is_file():
        raise RuntimeError(f"LevelDB lock file is missing: {lock_path}")
    with lock_path.open("rb") as lock_file:
        try:
            fcntl.lockf(lock_file.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno in (errno.EACCES, errno.EAGAIN):
                raise RuntimeError(
                    f"LevelDB source is still locked by a running process: {path}"
                ) from exc
            raise
        yield


def _select_backend(
    directory: Path,
    requested: Literal["leveldb", "sqlitedict"] | None,
) -> Literal["leveldb", "sqlitedict"]:
    candidates: list[Literal["leveldb", "sqlitedict"]] = []
    if (directory / "leveldb").is_dir():
        candidates.append("leveldb")
    if (directory / "lshbloom.sqlite").is_file():
        candidates.append("sqlitedict")
    if len(candidates) == 1:
        return candidates[0]
    if requested is not None and requested in candidates:
        return requested
    if len(candidates) != 1:
        found = ", ".join(candidates) or "none"
        raise RuntimeError(
            f"cannot determine the authoritative LSH backend in {directory}: {found}. "
            "Pass --lsh-backend after inspecting the quiesced source."
        )
    raise AssertionError("unreachable")


def discover_lsh_sources(
    state_dir: Path,
    *,
    legacy_generation: str | None,
    requested_backend: Literal["leveldb", "sqlitedict"] | None,
) -> tuple[LshSource, ...]:
    """Find old bare and generation-scoped LSH indexes without guessing a generation."""
    root = state_dir / "lshbloom"
    if not root.is_dir():
        raise FileNotFoundError(root)
    sources: list[LshSource] = []
    root_has_database = (root / "leveldb").is_dir() or (root / "lshbloom.sqlite").is_file()
    if root_has_database:
        if not legacy_generation:
            raise RuntimeError(
                "bare legacy lshbloom state has no generation marker. "
                "Pass the recorded legacy S2P_SCORING_VERSION."
            )
        backend = _select_backend(root, requested_backend)
        path = root / ("leveldb" if backend == "leveldb" else "lshbloom.sqlite")
        sources.append(LshSource(legacy_generation, backend, path, path.relative_to(state_dir)))
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if not ((child / "leveldb").is_dir() or (child / "lshbloom.sqlite").is_file()):
            continue
        backend = _select_backend(child, requested_backend)
        path = child / ("leveldb" if backend == "leveldb" else "lshbloom.sqlite")
        sources.append(LshSource(child.name, backend, path, path.relative_to(state_dir)))
    if not sources:
        raise RuntimeError(f"no recognized LSH state exists below {root}")
    return tuple(sources)


def snapshot_sources(
    state_dir: Path,
    snapshot_dir: Path,
    *,
    legacy_generation: str | None,
    requested_backend: Literal["leveldb", "sqlitedict"] | None,
) -> tuple[Path, tuple[LshSource, ...], dict[str, Any]]:
    """Create a self-contained immutable migration snapshot.

    LevelDB has no SQLite-style backup API. The source must already be
    quiesced, then its whole directory is copied before it is opened. SQLite
    state uses the backup API so WAL-resident commits are included.
    """
    decisions_source = state_dir / "decision-cache.sqlite3"
    lsh_sources = discover_lsh_sources(
        state_dir,
        legacy_generation=legacy_generation,
        requested_backend=requested_backend,
    )
    _ensure_snapshot_capacity(decisions_source, lsh_sources, snapshot_dir)
    decisions_snapshot = snapshot_dir / "decision-cache.sqlite3"
    with ExitStack() as source_locks:
        for source in lsh_sources:
            if source.backend == "leveldb":
                source_locks.enter_context(_quiesced_leveldb_source(source.path))
        snapshot_dir.mkdir(parents=True, exist_ok=False)
        _snapshot_sqlite(decisions_source, decisions_snapshot)
        source_hashes: dict[str, Any] = {
            "decision-cache.sqlite3": _sha256_file(decisions_snapshot),
            "lsh": {},
        }
        copied_sources: list[LshSource] = []
        for source in lsh_sources:
            destination = snapshot_dir / source.relative_path
            destination.parent.mkdir(parents=True, exist_ok=True)
            if source.backend == "leveldb":
                shutil.copytree(source.path, destination)
            else:
                _snapshot_sqlite(source.path, destination)
            copied_sources.append(
                LshSource(
                    generation=source.generation,
                    backend=source.backend,
                    path=destination,
                    relative_path=source.relative_path,
                )
            )
            source_hashes["lsh"][str(source.relative_path)] = {
                "backend": source.backend,
                "files": _file_hashes(destination),
            }
    return decisions_snapshot, tuple(copied_sources), source_hashes


def reuse_snapshot(
    snapshot_dir: Path,
    *,
    legacy_generation: str | None,
    requested_backend: Literal["leveldb", "sqlitedict"] | None,
) -> tuple[Path, tuple[LshSource, ...], dict[str, Any]]:
    """Open a complete retained snapshot after a validation-only failure."""
    if (snapshot_dir / "migration-manifest.json").exists():
        raise RuntimeError("a completed migration manifest already exists in the snapshot")
    decisions_snapshot = snapshot_dir / "decision-cache.sqlite3"
    if not decisions_snapshot.is_file():
        raise FileNotFoundError(decisions_snapshot)
    lsh_sources = discover_lsh_sources(
        snapshot_dir,
        legacy_generation=legacy_generation,
        requested_backend=requested_backend,
    )
    source_hashes: dict[str, Any] = {
        "decision-cache.sqlite3": _sha256_file(decisions_snapshot),
        "lsh": {},
    }
    for source in lsh_sources:
        source_hashes["lsh"][str(source.relative_path)] = {
            "backend": source.backend,
            "files": _file_hashes(source.path),
        }
    return decisions_snapshot, lsh_sources, source_hashes


def _decode_ascii(value: bytes, *, label: str) -> str:
    try:
        decoded = value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{label} must be ASCII") from exc
    if not decoded:
        raise RuntimeError(f"{label} must not be empty")
    return decoded


def _decode_utf8(value: bytes, *, label: str) -> str:
    try:
        decoded = value.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"{label} must be UTF-8") from exc
    if not decoded:
        raise RuntimeError(f"{label} must not be empty")
    return decoded


def _merge_exact(target: dict[str, Any], key: str, value: Any, *, label: str) -> None:
    existing = target.get(key)
    if existing is not None and existing != value:
        raise RuntimeError(f"conflicting {label} values for {key}")
    target[key] = value


def _json_string_map(value: bytes, *, label: str) -> dict[str, str]:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} is not valid JSON") from exc
    if not isinstance(parsed, dict) or not all(
        isinstance(key, str) and key and isinstance(item, str) and item
        for key, item in parsed.items()
    ):
        raise RuntimeError(f"{label} must be a non-empty string map")
    return parsed


def _collect_lsh_records(records: Iterable[tuple[bytes, bytes]]) -> _LshMaps:
    cluster_keys: dict[str, str] = {}
    anchors: dict[str, str] = {}
    signatures: dict[str, bytes] = {}
    for raw_key, raw_value in records:
        key = _decode_ascii(bytes(raw_key), label="LSH key")
        value = bytes(raw_value)
        if key == "__clusters__":
            for cluster_key, cluster_id in _json_string_map(value, label=key).items():
                _merge_exact(cluster_keys, cluster_key, cluster_id, label="cluster key")
        elif key == "__cluster_anchors__":
            for cluster_id, doc_id in _json_string_map(value, label=key).items():
                _merge_exact(anchors, cluster_id, doc_id, label="anchor")
        elif key == "__counter__" or key.startswith("band:"):
            # Bloom bitmaps and the old sequence counter are only local
            # acceleration state. PostgreSQL uses the exact cluster-key rows.
            continue
        elif key.startswith("cluster:"):
            cluster_key = key.removeprefix("cluster:")
            _merge_exact(
                cluster_keys,
                _decode_ascii(cluster_key.encode("ascii"), label="cluster key"),
                _decode_ascii(value, label=f"cluster id for {cluster_key}"),
                label="cluster key",
            )
        elif key.startswith("anchor:"):
            cluster_id = key.removeprefix("anchor:")
            _merge_exact(
                anchors,
                _decode_ascii(cluster_id.encode("ascii"), label="cluster id"),
                _decode_utf8(value, label=f"anchor for {cluster_id}"),
                label="anchor",
            )
        elif key.startswith("signature:"):
            cluster_id = key.removeprefix("signature:")
            if not value:
                raise RuntimeError(f"signature for {cluster_id} is empty")
            _merge_exact(
                signatures,
                _decode_ascii(cluster_id.encode("ascii"), label="cluster id"),
                value,
                label="signature",
            )
        else:
            raise RuntimeError(f"unrecognized durable LSH key: {key}")

    return _LshMaps(cluster_keys, anchors, signatures)


def _merge_lsh_maps(target: _LshMaps, source: _LshMaps) -> None:
    for cluster_key, cluster_id in source.cluster_keys.items():
        _merge_exact(target.cluster_keys, cluster_key, cluster_id, label="cluster key")
    for cluster_id, anchor_doc_id in source.anchors.items():
        _merge_exact(target.anchors, cluster_id, anchor_doc_id, label="anchor")
    for cluster_id, signature in source.signatures.items():
        _merge_exact(target.signatures, cluster_id, signature, label="signature")


def _lsh_rows(
    state: _LshMaps,
    *,
    generation: str,
    allow_incomplete: bool = False,
) -> tuple[tuple[LshClusterRow, ...], tuple[LshBandRow, ...]]:
    cluster_keys = state.cluster_keys
    anchors = state.anchors
    signatures = state.signatures

    referenced_clusters = set(cluster_keys.values())
    if set(anchors) - referenced_clusters or set(signatures) - referenced_clusters:
        orphan_anchors = sorted(set(anchors) - referenced_clusters)
        orphan_signatures = sorted(set(signatures) - referenced_clusters)
        details = {
            "orphan_anchors": orphan_anchors[:20],
            "orphan_anchor_count": len(orphan_anchors),
            "orphan_signatures": orphan_signatures[:20],
            "orphan_signature_count": len(orphan_signatures),
        }
        raise RuntimeError(f"orphaned local LSH state: {json.dumps(details, sort_keys=True)}")
    if not allow_incomplete and (
        set(anchors) != referenced_clusters or set(signatures) != referenced_clusters
    ):
        missing_anchors = sorted(referenced_clusters - set(anchors))
        missing_signatures = sorted(referenced_clusters - set(signatures))
        details = {
            "missing_anchors": missing_anchors[:20],
            "missing_anchor_count": len(missing_anchors),
            "missing_signatures": missing_signatures[:20],
            "missing_signature_count": len(missing_signatures),
        }
        raise RuntimeError(f"incomplete local LSH state: {json.dumps(details, sort_keys=True)}")
    clusters = tuple(
        LshClusterRow(
            generation,
            cluster_id,
            anchors.get(cluster_id, ""),
            signatures.get(cluster_id, b""),
        )
        for cluster_id in sorted(referenced_clusters)
    )
    bands = tuple(
        LshBandRow(generation, cluster_key, cluster_id)
        for cluster_key, cluster_id in sorted(cluster_keys.items())
    )
    return clusters, bands


def parse_lsh_records(
    records: Iterable[tuple[bytes, bytes]],
    *,
    generation: str,
) -> tuple[tuple[LshClusterRow, ...], tuple[LshBandRow, ...]]:
    """Convert every supported local LSH encoding into the exact SQL rows."""
    return _lsh_rows(_collect_lsh_records(records), generation=generation)


def _iter_leveldb(path: Path) -> Iterator[tuple[bytes, bytes]]:
    try:
        import plyvel  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("LevelDB migration requires the processor runtime image") from exc
    database = plyvel.DB(str(path), create_if_missing=False)
    try:
        yield from ((bytes(key), bytes(value)) for key, value in database.iterator())
    finally:
        database.close()


def _iter_sqlitedict(path: Path) -> Iterator[tuple[bytes, bytes]]:
    try:
        from sqlitedict import SqliteDict  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError("SQLiteDict migration requires the processor runtime image") from exc
    database = SqliteDict(filename=str(path), flag="r", autocommit=False)
    try:
        for key, value in database.items():
            if not isinstance(key, str):
                raise RuntimeError("SQLiteDict LSH key is not text")
            encoded = value.encode("latin1") if isinstance(value, str) else bytes(value)
            yield key.encode("ascii"), encoded
    finally:
        database.close()


def _read_lsh_source(source: LshSource) -> _LshMaps:
    records: Iterable[tuple[bytes, bytes]]
    if source.backend == "leveldb":
        records = _iter_leveldb(source.path)
    else:
        records = _iter_sqlitedict(source.path)
    return _collect_lsh_records(records)


def iter_decisions(path: Path) -> Iterator[DecisionRow]:
    """Read decision rows in key order without loading the cache into memory."""
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        for cache_key, payload, trainable in connection.execute(
            "SELECT cache_key, payload, trainable FROM decisions ORDER BY cache_key"
        ):
            if not isinstance(cache_key, str) or not cache_key:
                raise RuntimeError("decision cache key is invalid")
            if trainable not in (0, 1, False, True):
                raise RuntimeError(f"decision cache trainable value is invalid: {cache_key}")
            yield DecisionRow(cache_key, bytes(payload), bool(trainable))
    finally:
        connection.close()


def _read_decisions(path: Path) -> tuple[DecisionRow, ...]:
    """Materialize a decision cache only for focused offline unit tests."""
    return tuple(iter_decisions(path))


def read_source_state(
    decision_snapshot: Path,
    lsh_sources: Iterable[LshSource],
) -> CuratorState:
    lsh_state = read_lsh_state(lsh_sources)
    return CuratorState(
        decisions=_read_decisions(decision_snapshot),
        clusters=lsh_state.clusters,
        bands=lsh_state.bands,
    )


def read_lsh_state(
    lsh_sources: Iterable[LshSource],
    *,
    allow_incomplete: bool = False,
) -> CuratorState:
    """Read compact local LSH state without materializing the decision cache."""
    by_generation: dict[str, _LshMaps] = {}
    for source in lsh_sources:
        merged = by_generation.setdefault(source.generation, _LshMaps({}, {}, {}))
        _merge_lsh_maps(merged, _read_lsh_source(source))

    clusters: list[LshClusterRow] = []
    bands: list[LshBandRow] = []
    for generation, state in sorted(by_generation.items()):
        source_clusters, source_bands = _lsh_rows(
            state,
            generation=generation,
            allow_incomplete=allow_incomplete,
        )
        clusters.extend(source_clusters)
        bands.extend(source_bands)
    return CuratorState(
        decisions=(),
        clusters=tuple(sorted(clusters, key=lambda row: (row.generation, row.cluster_id))),
        bands=tuple(sorted(bands, key=lambda row: (row.generation, row.cluster_key))),
    )


def _decision_lsh_evidence(row: DecisionRow) -> DecisionLshEvidence:
    try:
        payload = json.loads(row.payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"decision payload is not valid JSON: {row.cache_key}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"decision payload is not a JSON object: {row.cache_key}")

    doc_id = payload.get("doc_id")
    text = payload.get("text")
    generation = payload.get("scoring_version")
    cluster_id = payload.get("near_dup_cluster_id")
    near_duplicate = payload.get("near_duplicate", False)
    signature_backend = payload.get("minhash_backend", "unknown")
    num_perms = payload.get("minhash_num_perms", 0)
    if not isinstance(doc_id, str) or not doc_id:
        raise RuntimeError(f"decision payload has no document ID: {row.cache_key}")
    if not isinstance(text, str):
        raise RuntimeError(f"decision payload has no retained text: {row.cache_key}")
    if not isinstance(generation, str) or not generation:
        raise RuntimeError(f"decision payload has no scoring generation: {row.cache_key}")
    if cluster_id is not None and (not isinstance(cluster_id, str) or not cluster_id):
        raise RuntimeError(f"decision payload has an invalid cluster ID: {row.cache_key}")
    if not isinstance(near_duplicate, bool):
        raise RuntimeError(f"decision payload has an invalid duplicate flag: {row.cache_key}")
    if not isinstance(signature_backend, str) or not signature_backend:
        raise RuntimeError(f"decision payload has an invalid MinHash backend: {row.cache_key}")
    if isinstance(num_perms, bool) or not isinstance(num_perms, int) or num_perms < 0:
        raise RuntimeError(f"decision payload has an invalid permutation count: {row.cache_key}")
    return DecisionLshEvidence(
        doc_id=doc_id,
        text=text,
        generation=generation,
        cluster_id=cluster_id,
        near_duplicate=near_duplicate,
        signature_backend=signature_backend,
        num_perms=num_perms,
    )


def _validate_anchor_minhash_contract(
    state: CuratorState,
    *,
    expected_backend: str,
    expected_permutations: int,
    decisions: Iterable[DecisionRow] | None = None,
    minhasher: MinHasher | None = None,
) -> CuratorState:
    """Prove or reconstruct every anchor from the durable decision record."""
    generations: dict[str, list[LshClusterRow]] = {}
    for cluster in state.clusters:
        generations.setdefault(cluster.generation, []).append(cluster)
    legacy_generations: set[str] = set()
    for generation, clusters in generations.items():
        missing_signatures = [cluster for cluster in clusters if not cluster.signature]
        if missing_signatures and len(missing_signatures) != len(clusters):
            raise RuntimeError(
                f"one LSH generation mixes signature-aware and band-only clusters: {generation}"
            )
        if missing_signatures:
            legacy_generations.add(generation)

    metadata: dict[tuple[str, str], set[tuple[str, int]]] = {}
    generation_contracts: dict[str, set[tuple[str, int]]] = {}
    cluster_keys = {(row.generation, row.cluster_id) for row in state.clusters}
    band_keys_by_cluster: dict[tuple[str, str], set[str]] = {}
    for band in state.bands:
        band_keys_by_cluster.setdefault((band.generation, band.cluster_id), set()).add(
            band.cluster_key
        )
    anchor_evidence: dict[tuple[str, str], set[tuple[str, str, int, bytes]]] = {}
    for row in decisions if decisions is not None else state.decisions:
        evidence_row = _decision_lsh_evidence(row)
        generation = evidence_row.generation
        doc_id = evidence_row.doc_id
        backend = evidence_row.signature_backend
        num_perms = evidence_row.num_perms
        metadata.setdefault((generation, doc_id), set()).add((backend, num_perms))
        generation_contracts.setdefault(generation, set()).add((backend, num_perms))
        cluster_id = evidence_row.cluster_id
        cluster_key = (generation, str(cluster_id))
        if (
            cluster_id is None
            or evidence_row.near_duplicate
            or cluster_key not in cluster_keys
            or generation in legacy_generations
        ):
            continue
        signature = b""
        if minhasher is not None:
            computed = minhasher.signature(evidence_row.text)
            if (backend, num_perms) != (computed.backend, computed.num_perms):
                raise RuntimeError(
                    "anchor decision MinHash provenance does not match recomputation: "
                    f"{generation}/{cluster_id}"
                )
            signature = computed.digest
        anchor_evidence.setdefault(cluster_key, set()).add((doc_id, backend, num_perms, signature))

    contracts: dict[tuple[str, str], tuple[str, int]] = {}
    legacy_contracts: dict[str, tuple[str, int]] = {}
    for generation in sorted(legacy_generations):
        contract = generation_contracts.get(generation)
        if contract != {(expected_backend, expected_permutations)}:
            raise RuntimeError(
                "band-only LSH generation MinHash contract cannot be proven compatible: "
                f"{generation}"
            )
        legacy_contracts[generation] = next(iter(contract))

    reconstructed_clusters: list[LshClusterRow] = []
    for cluster in state.clusters:
        if cluster.generation in legacy_generations:
            continue
        cluster_key = (cluster.generation, cluster.cluster_id)
        evidence = anchor_evidence.get(cluster_key, set())
        anchor_doc_id = cluster.anchor_doc_id
        signature = cluster.signature
        candidates = {
            candidate
            for candidate in evidence
            if (not anchor_doc_id or candidate[0] == anchor_doc_id)
            and (not signature or not candidate[3] or candidate[3] == signature)
        }
        if not signature and minhasher is not None:
            actual_band_keys = band_keys_by_cluster.get(cluster_key, set())
            candidates = {
                candidate
                for candidate in candidates
                if candidate[3]
                and actual_band_keys
                <= _cluster_keys_for_signature(
                    MinHashSignature(
                        digest=candidate[3],
                        backend=candidate[1],
                        num_perms=candidate[2],
                    )
                )
            }
        if evidence and not candidates:
            raise RuntimeError(
                "no non-duplicate decision signature matches the persisted LSH cluster: "
                f"{cluster.generation}/{cluster.cluster_id}"
            )
        if len(candidates) > 1:
            raise RuntimeError(
                "multiple distinct decision signatures match the LSH cluster anchor: "
                f"{cluster.generation}/{cluster.cluster_id}"
            )
        if candidates:
            evidence_doc_id, _, _, evidence_signature = next(iter(candidates))
            anchor_doc_id = evidence_doc_id
            signature = signature or evidence_signature
        if not anchor_doc_id:
            raise RuntimeError(
                "LSH anchor cannot be reconstructed from a non-duplicate decision: "
                f"{cluster.generation}/{cluster.cluster_id}"
            )
        if len(signature) != expected_permutations * 4:
            raise RuntimeError(
                f"anchor signature length does not match target permutations: {cluster.cluster_id}"
            )
        contract = metadata.get(
            (cluster.generation, anchor_doc_id),
            generation_contracts.get(cluster.generation),
        )
        if contract != {(expected_backend, expected_permutations)}:
            raise RuntimeError(
                "anchor MinHash backend or permutation count cannot be proven compatible: "
                f"{cluster.cluster_id}"
            )
        contracts[(cluster.generation, cluster.cluster_id)] = next(iter(contract))
        reconstructed_clusters.append(
            LshClusterRow(
                generation=cluster.generation,
                cluster_id=cluster.cluster_id,
                anchor_doc_id=anchor_doc_id,
                signature=signature,
                signature_backend=contracts[(cluster.generation, cluster.cluster_id)][0],
                num_perms=contracts[(cluster.generation, cluster.cluster_id)][1],
            )
        )
    clusters = tuple(reconstructed_clusters)
    bands = tuple(
        LshBandRow(
            generation=band.generation,
            cluster_key=band.cluster_key,
            cluster_id=band.cluster_id,
            signature_backend=contracts[(band.generation, band.cluster_id)][0],
            num_perms=contracts[(band.generation, band.cluster_id)][1],
        )
        for band in state.bands
        if band.generation not in legacy_generations
    )
    legacy_bands = tuple(
        LegacyLshBandRow(
            generation=band.generation,
            cluster_key=band.cluster_key,
            cluster_id=band.cluster_id,
            signature_backend=legacy_contracts[band.generation][0],
            num_perms=legacy_contracts[band.generation][1],
        )
        for band in state.bands
        if band.generation in legacy_generations
    )

    expected_keys_by_cluster = {
        (cluster.generation, cluster.cluster_id): _cluster_keys_for_signature(
            MinHashSignature(
                digest=cluster.signature,
                backend=cluster.signature_backend,
                num_perms=cluster.num_perms,
            )
        )
        for cluster in clusters
    }
    for band in bands:
        expected_keys = expected_keys_by_cluster[(band.generation, band.cluster_id)]
        if band.cluster_key not in expected_keys:
            raise RuntimeError(
                "persisted LSH band ownership conflicts with the anchor signature: "
                f"{band.generation}/{band.cluster_id}/{band.cluster_key}"
            )
    return CuratorState(
        decisions=state.decisions,
        clusters=clusters,
        bands=bands,
        legacy_bands=legacy_bands,
    )


def _cluster_keys_for_signature(signature: MinHashSignature) -> set[str]:
    return {
        f"{index:04d}:" + hashlib.blake2b(value, digest_size=8, person=b"s2pck").hexdigest()
        for index, value in enumerate(signature.band_keys())
    }


def _initialize_target_schema(connection: Any) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS curator_decisions (
          cache_key TEXT PRIMARY KEY,
          payload BYTEA NOT NULL,
          trainable BOOLEAN NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS curator_lsh_legacy_bands (
          generation TEXT NOT NULL,
          signature_backend TEXT NOT NULL,
          num_perms INTEGER NOT NULL CHECK (num_perms > 0),
          cluster_key TEXT NOT NULL,
          cluster_id TEXT NOT NULL,
          PRIMARY KEY (generation, signature_backend, num_perms, cluster_key)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS curator_lsh_clusters (
          generation TEXT NOT NULL,
          signature_backend TEXT NOT NULL,
          num_perms INTEGER NOT NULL CHECK (num_perms > 0),
          cluster_id TEXT NOT NULL,
          anchor_doc_id TEXT NOT NULL,
          signature BYTEA NOT NULL CHECK (octet_length(signature) = num_perms * 4),
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
          PRIMARY KEY (generation, signature_backend, num_perms, cluster_id)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS curator_lsh_bands (
          generation TEXT NOT NULL,
          signature_backend TEXT NOT NULL,
          num_perms INTEGER NOT NULL,
          cluster_key TEXT NOT NULL,
          cluster_id TEXT NOT NULL,
          PRIMARY KEY (generation, signature_backend, num_perms, cluster_key),
          FOREIGN KEY (generation, signature_backend, num_perms, cluster_id)
            REFERENCES curator_lsh_clusters(
              generation, signature_backend, num_perms, cluster_id
            )
        )
        """
    )


_LEGACY_CLUSTER_COLUMNS = {
    "generation",
    "cluster_id",
    "anchor_doc_id",
    "signature",
    "created_at",
}
_CURRENT_CLUSTER_COLUMNS = _LEGACY_CLUSTER_COLUMNS | {"signature_backend", "num_perms"}
_LEGACY_BAND_COLUMNS = {"generation", "cluster_key", "cluster_id"}
_CURRENT_BAND_COLUMNS = _LEGACY_BAND_COLUMNS | {"signature_backend", "num_perms"}


def _table_columns(connection: Any, table: str) -> set[str]:
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = ?
        ORDER BY ordinal_position
        """,
        (table,),
    ).fetchall()
    return {str(row["column_name"]) for row in rows}


def _lsh_schema_version(connection: Any) -> Literal["absent", "legacy", "current"]:
    cluster_columns = _table_columns(connection, "curator_lsh_clusters")
    band_columns = _table_columns(connection, "curator_lsh_bands")
    if not cluster_columns and not band_columns:
        return "absent"
    if cluster_columns >= _CURRENT_CLUSTER_COLUMNS and band_columns >= _CURRENT_BAND_COLUMNS:
        return "current"
    if (
        cluster_columns >= _LEGACY_CLUSTER_COLUMNS
        and not {"signature_backend", "num_perms"} & cluster_columns
        and band_columns >= _LEGACY_BAND_COLUMNS
        and not {"signature_backend", "num_perms"} & band_columns
    ):
        return "legacy"
    raise RuntimeError("PostgreSQL curator LSH tables have an unsupported mixed schema")


def _legacy_source_fingerprints(
    decision_snapshot: Path,
    lsh_state: CuratorState,
) -> dict[str, dict[str, int | str]]:
    return {
        "curator_decisions": _fingerprint_rows(
            (row.cache_key, row.payload, row.trainable) for row in iter_decisions(decision_snapshot)
        ),
        "curator_lsh_clusters": _fingerprint_rows(
            (row.generation, row.cluster_id, row.anchor_doc_id, row.signature)
            for row in lsh_state.clusters
        ),
        "curator_lsh_bands": _fingerprint_rows(
            (row.generation, row.cluster_key, row.cluster_id) for row in lsh_state.bands
        ),
    }


def _legacy_target_fingerprints(connection: PostgresConnection) -> dict[str, dict[str, int | str]]:
    return {
        "curator_decisions": _fingerprint_rows(
            (str(row["cache_key"]), bytes(row["payload"]), bool(row["trainable"]))
            for row in connection.stream_rows(
                "SELECT cache_key, payload, trainable FROM curator_decisions ORDER BY cache_key"
            )
        ),
        "curator_lsh_clusters": _fingerprint_rows(
            (
                str(row["generation"]),
                str(row["cluster_id"]),
                str(row["anchor_doc_id"]),
                bytes(row["signature"]),
            )
            for row in connection.stream_rows(
                """
                SELECT generation, cluster_id, anchor_doc_id, signature
                FROM curator_lsh_clusters
                ORDER BY generation, cluster_id
                """
            )
        ),
        "curator_lsh_bands": _fingerprint_rows(
            (str(row["generation"]), str(row["cluster_key"]), str(row["cluster_id"]))
            for row in connection.stream_rows(
                """
                SELECT generation, cluster_key, cluster_id
                FROM curator_lsh_bands
                ORDER BY generation, cluster_key
                """
            )
        ),
    }


def _fingerprints_are_empty(values: dict[str, dict[str, int | str]]) -> bool:
    return all(int(fingerprint["rows"]) == 0 for fingerprint in values.values())


def _copy_lsh_state(lsh_state: CuratorState, target: Any) -> None:
    target.copy_records(
        "curator_lsh_clusters",
        (
            "generation",
            "signature_backend",
            "num_perms",
            "cluster_id",
            "anchor_doc_id",
            "signature",
        ),
        (
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_id,
                row.anchor_doc_id,
                row.signature,
            )
            for row in lsh_state.clusters
        ),
    )
    target.copy_records(
        "curator_lsh_legacy_bands",
        ("generation", "signature_backend", "num_perms", "cluster_key", "cluster_id"),
        (
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_key,
                row.cluster_id,
            )
            for row in lsh_state.legacy_bands
        ),
    )
    target.copy_records(
        "curator_lsh_bands",
        ("generation", "signature_backend", "num_perms", "cluster_key", "cluster_id"),
        (
            (
                row.generation,
                row.signature_backend,
                row.num_perms,
                row.cluster_key,
                row.cluster_id,
            )
            for row in lsh_state.bands
        ),
    )


def _prepare_target_schema(
    connection: PostgresConnection,
    decision_snapshot: Path,
    lsh_state: CuratorState,
) -> bool:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS curator_decisions (
          cache_key TEXT PRIMARY KEY,
          payload BYTEA NOT NULL,
          trainable BOOLEAN NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    schema_version = _lsh_schema_version(connection)
    if schema_version != "legacy":
        _initialize_target_schema(connection)
        return False

    connection.execute(
        "LOCK TABLE curator_decisions,curator_lsh_clusters,curator_lsh_bands "
        "IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    observed = _legacy_target_fingerprints(connection)
    expected = _legacy_source_fingerprints(decision_snapshot, lsh_state)
    if observed != expected and not _fingerprints_are_empty(observed):
        raise RuntimeError(
            "legacy PostgreSQL curator tables are not empty and do not match the source"
        )
    matching_legacy_state = observed == expected
    connection.execute("DROP TABLE curator_lsh_bands")
    connection.execute("DROP TABLE curator_lsh_clusters")
    _initialize_target_schema(connection)
    if matching_legacy_state:
        _copy_lsh_state(lsh_state, connection)
    return True


def target_fingerprints(connection: PostgresConnection) -> dict[str, dict[str, int | str]]:
    """Fingerprint all target state through server cursors, never a 6.9 GB tuple."""
    return {
        "curator_decisions": _fingerprint_rows(
            (str(row["cache_key"]), bytes(row["payload"]), bool(row["trainable"]))
            for row in connection.stream_rows(
                "SELECT cache_key, payload, trainable FROM curator_decisions ORDER BY cache_key"
            )
        ),
        "curator_lsh_clusters": _fingerprint_rows(
            (
                str(row["generation"]),
                str(row["signature_backend"]),
                int(row["num_perms"]),
                str(row["cluster_id"]),
                str(row["anchor_doc_id"]),
                bytes(row["signature"]),
            )
            for row in connection.stream_rows(
                """
                SELECT generation, signature_backend, num_perms, cluster_id, anchor_doc_id, signature
                FROM curator_lsh_clusters
                ORDER BY generation, signature_backend, num_perms, cluster_id
                """
            )
        ),
        "curator_lsh_bands": _fingerprint_rows(
            (
                str(row["generation"]),
                str(row["signature_backend"]),
                int(row["num_perms"]),
                str(row["cluster_key"]),
                str(row["cluster_id"]),
            )
            for row in connection.stream_rows(
                """
                SELECT generation, signature_backend, num_perms, cluster_key, cluster_id
                FROM curator_lsh_bands
                ORDER BY generation, signature_backend, num_perms, cluster_key
                """
            )
        ),
        "curator_lsh_legacy_bands": _fingerprint_rows(
            (
                str(row["generation"]),
                str(row["signature_backend"]),
                int(row["num_perms"]),
                str(row["cluster_key"]),
                str(row["cluster_id"]),
            )
            for row in connection.stream_rows(
                """
                SELECT generation, signature_backend, num_perms, cluster_key, cluster_id
                FROM curator_lsh_legacy_bands
                ORDER BY generation, signature_backend, num_perms, cluster_key
                """
            )
        ),
    }


def _target_is_pristine(connection: Any) -> bool:
    return all(
        int(connection.execute(f"SELECT COUNT(*) AS count FROM {table}").fetchone()["count"]) == 0
        for table in (
            "curator_decisions",
            "curator_lsh_clusters",
            "curator_lsh_bands",
            "curator_lsh_legacy_bands",
        )
    )


def copy_state(
    decision_snapshot: Path,
    lsh_state: CuratorState,
    target: Any,
) -> None:
    """COPY every table inside one transaction without a per-row network round trip."""
    target.copy_records(
        "curator_decisions",
        ("cache_key", "payload", "trainable"),
        ((row.cache_key, row.payload, row.trainable) for row in iter_decisions(decision_snapshot)),
    )
    _copy_lsh_state(lsh_state, target)


def import_verified_state(
    decision_snapshot: Path,
    lsh_state: CuratorState,
    target: Any,
    expected_fingerprints: dict[str, dict[str, int | str]],
) -> str:
    """Import once, or verify an identical prior import, under one lock."""
    target.execute("BEGIN")
    try:
        target.execute(
            "SELECT pg_advisory_xact_lock(hashtext(?))",
            (CURATOR_SCHEMA_LOCK_NAME,),
        )
        migrated_legacy_schema = _prepare_target_schema(target, decision_snapshot, lsh_state)
        target.execute(
            "LOCK TABLE curator_decisions,curator_lsh_clusters,curator_lsh_bands,"
            "curator_lsh_legacy_bands "
            "IN ACCESS EXCLUSIVE MODE NOWAIT"
        )
        existing_fingerprints = target_fingerprints(target)
        if existing_fingerprints == expected_fingerprints:
            mode = "migrated" if migrated_legacy_schema else "verified-existing"
        else:
            if not _target_is_pristine(target):
                raise RuntimeError(
                    "PostgreSQL curator tables are not empty and do not match the source"
                )
            copy_state(decision_snapshot, lsh_state, target)
            if target_fingerprints(target) != expected_fingerprints:
                raise RuntimeError("post-import curator state verification failed")
            mode = "migrated"
        target.commit()
        return mode
    except Exception:
        target.rollback()
        raise


def migrate(
    state_dir: Path,
    snapshot_dir: Path,
    database_url: str,
    *,
    legacy_generation: str | None = None,
    requested_backend: Literal["leveldb", "sqlitedict"] | None = None,
    reuse_existing_snapshot: bool = False,
) -> dict[str, Any]:
    if not database_url.startswith(("postgresql://", "postgres://")):
        raise ValueError("S2P_COORDINATION_DATABASE_URL must be a PostgreSQL URL")
    if reuse_existing_snapshot:
        decision_snapshot, lsh_sources, source_hashes = reuse_snapshot(
            snapshot_dir,
            legacy_generation=legacy_generation,
            requested_backend=requested_backend,
        )
    else:
        decision_snapshot, lsh_sources, source_hashes = snapshot_sources(
            state_dir,
            snapshot_dir,
            legacy_generation=legacy_generation,
            requested_backend=requested_backend,
        )
    source = read_lsh_state(lsh_sources, allow_incomplete=True)
    target_minhash = MinHasher()
    source = _validate_anchor_minhash_contract(
        source,
        expected_backend=target_minhash.backend,
        expected_permutations=target_minhash.num_perms,
        decisions=iter_decisions(decision_snapshot),
        minhasher=target_minhash,
    )
    expected_fingerprints = source_fingerprints(decision_snapshot, source)
    target = connect_database(database_url)
    if not isinstance(target, PostgresConnection):
        target.close()
        raise AssertionError("PostgreSQL URL did not create a PostgreSQL coordination connection")
    try:
        mode = import_verified_state(
            decision_snapshot,
            source,
            target,
            expected_fingerprints,
        )
    finally:
        target.close()
    return {
        "format": "stream2pretrain-curator-state-migration-v1",
        "mode": mode,
        "source_claim": "checkpoint-stream2pretrain-processor-curate-0",
        "source_files": source_hashes,
        "generations": [source.generation for source in lsh_sources],
        "minhash": {
            "backend": target_minhash.backend,
            "num_perms": target_minhash.num_perms,
        },
        "tables": expected_fingerprints,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state-dir", type=Path, required=True)
    parser.add_argument("--snapshot-dir", type=Path, required=True)
    parser.add_argument(
        "--legacy-generation",
        help="recorded S2P_SCORING_VERSION for a bare legacy lshbloom directory",
    )
    parser.add_argument(
        "--lsh-backend",
        choices=("leveldb", "sqlitedict"),
        help="required if both durable local LSH backends are present",
    )
    parser.add_argument(
        "--reuse-existing-snapshot",
        action="store_true",
        help="resume from a retained snapshot that has no completed migration manifest",
    )
    args = parser.parse_args()
    database_url = os.environ.get("S2P_COORDINATION_DATABASE_URL", "").strip()
    manifest = migrate(
        args.state_dir,
        args.snapshot_dir,
        database_url,
        legacy_generation=args.legacy_generation,
        requested_backend=args.lsh_backend,
        reuse_existing_snapshot=args.reuse_existing_snapshot,
    )
    manifest_bytes = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
    manifest_path = args.snapshot_dir / "migration-manifest.json"
    manifest_path.write_bytes(manifest_bytes)
    print(
        json.dumps(
            {
                "manifest_path": str(manifest_path),
                "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                "mode": manifest["mode"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
