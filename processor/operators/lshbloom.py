"""Band-partitioned Bloom filter for streaming MinHash near-deduplication.

Implements the LSHBloom design from arXiv 2411.04257: each LSH band gets
its own Bloom filter, indexed in a key-value store. Band collisions produce
candidates; the stored anchor MinHash then confirms similarity. Requiring all
bands to collide only detects virtually identical text and previously allowed
large numbers of lightly edited repository cards through.

State backend
-------------
Primary: ``plyvel`` (LevelDB) - constant memory, append-only, fast on
small VMs. Falls back to ``sqlitedict`` if plyvel is unavailable in the
container (its build chain pulls in libleveldb-dev which is not always
worth the image size). Cluster keys are persisted incrementally in one
transaction per document. Older releases rewrote the complete cluster map
and every multi-megabyte Bloom band for every document, making write work
grow quadratically with corpus size. The fallback is documented in the
operator's ``backend`` field for forensic replay.

This operator is the stateful core of Stream2Pretrain's near-dup pass. It
is deterministic: given the same insertion order it produces the same
cluster assignment, which keeps replayed deduplication deterministic
work across pipeline restarts.
"""

from __future__ import annotations

import hashlib
import os
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from processor.operators.minhash import MinHashSignature

if TYPE_CHECKING:
    pass


@dataclass(frozen=True, slots=True)
class NearDupResult:
    """Outcome of a single :meth:`LSHBloomIndex.observe` call."""

    is_near_duplicate: bool
    cluster_id: str | None


class _BitArray:
    """In-memory bit array used inside each band's Bloom filter."""

    __slots__ = ("_bits", "_data")

    def __init__(self, num_bits: int) -> None:
        self._bits = num_bits
        self._data = bytearray((num_bits + 7) // 8)

    def set(self, idx: int) -> None:
        self._data[idx >> 3] |= 1 << (idx & 7)

    def get(self, idx: int) -> bool:
        return bool(self._data[idx >> 3] & (1 << (idx & 7)))

    @property
    def num_bits(self) -> int:
        return self._bits

    def to_bytes(self) -> bytes:
        return bytes(self._data)

    @classmethod
    def from_bytes(cls, payload: bytes, num_bits: int) -> _BitArray:
        ba = cls(num_bits)
        ba._data = bytearray(payload)
        return ba


class LSHBloomIndex:
    """Band-partitioned Bloom near-dup index with pluggable durable state.

    Parameters
    ----------
    num_bands
        How many LSH bands the signature is split into. Must divide the
        signature's ``num_perms``.
    bits_per_band
        Bit-array size per band's Bloom. With Stream2Pretrain default of
        2**24 bits and three hashes, false positive rate at 5M docs is
        ``needs-measurement``; defaults are FineWeb-comparable.
    num_hashes
        How many independent hash functions to apply per band.
    state_dir
        Directory for the durable state. ``None`` means in-memory only -
        useful for tests; production always sets this.
    """

    def __init__(
        self,
        *,
        num_bands: int = 28,
        bits_per_band: int = 1 << 24,
        num_hashes: int = 3,
        state_dir: str | Path | None = None,
        backend: str | None = None,
        similarity_threshold: float = 0.80,
    ) -> None:
        self._num_bands = num_bands
        self._bits_per_band = bits_per_band
        self._num_hashes = num_hashes
        if not 0.0 < similarity_threshold <= 1.0:
            raise ValueError("similarity_threshold must be in (0, 1]")
        self._similarity_threshold = similarity_threshold
        self._state_path = Path(state_dir) if state_dir else None
        self._cluster_counter = 0
        self._memory_bands: list[_BitArray] = [_BitArray(bits_per_band) for _ in range(num_bands)]
        # These maps contain only the legacy monolithic snapshot, if present.
        # New entries remain in the KV backend and are never accumulated into
        # another unbounded JSON value.
        self._cluster_map: dict[str, str] = {}
        self._cluster_anchors: dict[str, str] = {}
        self._memory_signatures: dict[str, bytes] = {}
        self._dirty_bands: set[int] = set()
        self._observations_since_bloom_checkpoint = 0
        self._bloom_checkpoint_interval = max(
            1, int(os.environ.get("S2P_LSH_BLOOM_CHECKPOINT_INTERVAL", "2048"))
        )
        self._db, self._backend = self._open_state(backend)

    @property
    def num_bands(self) -> int:
        return self._num_bands

    @property
    def backend(self) -> str:
        """Identifier persisted into provenance for forensic replay."""
        return self._backend

    def _open_state(self, requested: str | None) -> tuple[object | None, str]:
        if self._state_path is None:
            return None, "memory"
        self._state_path.mkdir(parents=True, exist_ok=True)
        if requested in (None, "plyvel"):
            try:
                import plyvel  # type: ignore[import-untyped]

                db = plyvel.DB(str(self._state_path / "leveldb"), create_if_missing=True)
                self._restore_from(db)
                return db, "plyvel"
            except Exception:
                if requested == "plyvel":
                    raise
        if requested in (None, "sqlitedict"):
            try:
                from sqlitedict import SqliteDict  # type: ignore[import-untyped]

                db = SqliteDict(
                    filename=str(self._state_path / "lshbloom.sqlite"),
                    autocommit=False,
                    journal_mode="WAL",
                )
                self._restore_from(db)
                return db, "sqlitedict"
            except Exception:
                if requested == "sqlitedict":
                    raise
        return None, "memory"

    def _restore_from(self, db: object) -> None:
        """Hydrate band bitmaps + cluster map from durable state."""
        for i in range(self._num_bands):
            key = self._band_key(i)
            blob = self._db_get(db, key)
            if blob is not None:
                self._memory_bands[i] = _BitArray.from_bytes(blob, self._bits_per_band)
        cluster_blob = self._db_get(db, b"__clusters__")
        anchors_blob = self._db_get(db, b"__cluster_anchors__")
        counter_blob = self._db_get(db, b"__counter__")
        if cluster_blob:
            import orjson

            self._cluster_map = orjson.loads(cluster_blob)
        if anchors_blob:
            import orjson

            self._cluster_anchors = orjson.loads(anchors_blob)
        if counter_blob:
            self._cluster_counter = int(counter_blob.decode("ascii"))

    @staticmethod
    def _db_get(db: object, key: bytes) -> bytes | None:
        if hasattr(db, "put"):
            # plyvel uses byte keys and values.
            res = db.get(key)  # type: ignore[union-attr]
            return bytes(res) if res is not None else None
        try:
            # sqlitedict uses str keys.
            v = db[key.decode("ascii")]  # type: ignore[index]
            if isinstance(v, str):
                return v.encode("latin1")
            return bytes(v)
        except KeyError:
            return None

    def _db_put_many(self, items: Iterable[tuple[bytes, bytes]]) -> None:
        """Persist one document's state as one backend transaction."""
        if self._db is None:
            return
        materialized = list(items)
        if hasattr(self._db, "write_batch"):
            with self._db.write_batch(transaction=True) as batch:  # type: ignore[union-attr]
                for key, value in materialized:
                    batch.put(key, value)
            return
        for key, value in materialized:
            self._db[key.decode("ascii")] = value  # type: ignore[index]
        if hasattr(self._db, "commit"):
            self._db.commit()  # type: ignore[union-attr]

    @staticmethod
    def _band_key(band_idx: int) -> bytes:
        return f"band:{band_idx:04d}".encode("ascii")

    @staticmethod
    def _cluster_state_key(cluster_key: str) -> bytes:
        return f"cluster:{cluster_key}".encode("ascii")

    @staticmethod
    def _anchor_state_key(cluster_id: str) -> bytes:
        return f"anchor:{cluster_id}".encode("ascii")

    def _lookup_cluster(self, cluster_key: str) -> str | None:
        legacy = self._cluster_map.get(cluster_key)
        if legacy is not None:
            return legacy
        if self._db is None:
            return None
        value = self._db_get(self._db, self._cluster_state_key(cluster_key))
        return value.decode("ascii") if value is not None else None

    def _lookup_anchor(self, cluster_id: str) -> str | None:
        legacy = self._cluster_anchors.get(cluster_id)
        if legacy is not None:
            return legacy
        if self._db is None:
            return None
        value = self._db_get(self._db, self._anchor_state_key(cluster_id))
        return value.decode("utf-8") if value is not None else None

    def _lookup_anchor_signature(self, cluster_id: str) -> bytes | None:
        if self._db is None:
            return getattr(self, "_memory_signatures", {}).get(cluster_id)
        return self._db_get(self._db, self._signature_state_key(cluster_id))

    def _checkpoint_dirty_bands(self, *, force: bool = False) -> None:
        """Amortize large Bloom bitmap writes across many observations.

        Exact cluster-key rows are the authoritative crash-consistent index.
        The bitmaps are a compact acceleration/provenance structure and may
        lag until this bounded checkpoint; they are never used to skip an
        authoritative KV lookup.
        """
        if not self._dirty_bands:
            return
        if (
            not force
            and self._observations_since_bloom_checkpoint < self._bloom_checkpoint_interval
        ):
            return
        self._db_put_many(
            (self._band_key(index), self._memory_bands[index].to_bytes())
            for index in sorted(self._dirty_bands)
        )
        self._dirty_bands.clear()
        self._observations_since_bloom_checkpoint = 0

    def observe(self, doc_id: str, sig: MinHashSignature) -> NearDupResult:
        """Insert a doc's signature; report whether it is a near-duplicate.

        The first occurrence in a (band, value) cell is the cluster anchor
        and gets a fresh ``cluster_id``. Subsequent collisions report the
        same cluster id.
        """
        bands = sig.band_keys(self._num_bands)
        cluster_keys = [self._cluster_key(i, b) for i, b in enumerate(bands)]
        existing = self._probe_cluster_keys(doc_id, sig, cluster_keys)
        if existing is not None:
            return existing

        # No candidate passed verification. Register a new anchor and fill
        # every still-empty band bucket. Existing buckets remain attached to
        # their first anchor; the other bands make the new cluster discoverable.
        cluster_id = self._mint_cluster_id(doc_id)
        writes: list[tuple[bytes, bytes]] = []
        writes.append((self._anchor_state_key(cluster_id), doc_id.encode("utf-8")))
        writes.append((self._signature_state_key(cluster_id), sig.digest))
        for i, ck in enumerate(cluster_keys):
            for h in self._hashes(ck.encode("utf-8")):
                self._memory_bands[i].set(h)
            self._dirty_bands.add(i)
            if self._lookup_cluster(ck) is None:
                writes.append((self._cluster_state_key(ck), cluster_id.encode("ascii")))
        writes.append((b"__counter__", str(self._cluster_counter).encode("ascii")))
        self._db_put_many(writes)
        # An in-memory test backend still needs current-process lookup state.
        if self._db is None:
            self._cluster_anchors.setdefault(cluster_id, doc_id)
            self._memory_signatures.setdefault(cluster_id, sig.digest)
            for ck in cluster_keys:
                self._cluster_map.setdefault(ck, cluster_id)
        self._observations_since_bloom_checkpoint += 1
        self._checkpoint_dirty_bands()
        return NearDupResult(is_near_duplicate=False, cluster_id=cluster_id)

    def probe(self, doc_id: str, sig: MinHashSignature) -> NearDupResult:
        """Check existing anchors without mutating durable deduplication state."""
        cluster_keys = [
            self._cluster_key(index, band)
            for index, band in enumerate(sig.band_keys(self._num_bands))
        ]
        existing = self._probe_cluster_keys(doc_id, sig, cluster_keys)
        return existing or NearDupResult(is_near_duplicate=False, cluster_id=None)

    def _probe_cluster_keys(
        self,
        doc_id: str,
        sig: MinHashSignature,
        cluster_keys: list[str],
    ) -> NearDupResult | None:
        candidate_clusters: list[str] = []
        for ck in cluster_keys:
            cid = self._lookup_cluster(ck)
            if cid is not None and cid not in candidate_clusters:
                candidate_clusters.append(cid)
        for cluster_id in candidate_clusters:
            anchor_doc_id = self._lookup_anchor(cluster_id)
            anchor_signature = self._lookup_anchor_signature(cluster_id)
            # At-least-once delivery replays the same document after its
            # curation completed but before the source offset committed.
            if anchor_doc_id == doc_id:
                return NearDupResult(is_near_duplicate=False, cluster_id=cluster_id)
            if (
                anchor_signature is not None
                and _signature_similarity(sig.digest, anchor_signature)
                >= self._similarity_threshold
            ):
                return NearDupResult(is_near_duplicate=True, cluster_id=cluster_id)
        return None

    def _mint_cluster_id(self, doc_id: str) -> str:
        self._cluster_counter += 1
        return f"cl-{self._cluster_counter:08d}-{doc_id[7:15]}"

    def _hashes(self, payload: bytes) -> Iterable[int]:
        for k in range(self._num_hashes):
            digest = hashlib.blake2b(payload, digest_size=8, person=b"s2plshbl").digest()
            v = int.from_bytes(digest, "little", signed=False) ^ (k * 0x9E3779B97F4A7C15)
            yield v % self._bits_per_band

    @staticmethod
    def _cluster_key(band_idx: int, band_bytes: bytes) -> str:
        h = hashlib.blake2b(band_bytes, digest_size=8, person=b"s2pck").hexdigest()
        return f"{band_idx:04d}:{h}"

    @staticmethod
    def _signature_state_key(cluster_id: str) -> bytes:
        return f"signature:{cluster_id}".encode("ascii")

    def close(self) -> None:
        """Flush durable state and release file handles."""
        if self._db is None:
            return
        self._checkpoint_dirty_bands(force=True)
        try:
            if hasattr(self._db, "close"):
                self._db.close()  # type: ignore[union-attr]
        except Exception:
            pass

    def __enter__(self) -> LSHBloomIndex:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def memory_index() -> LSHBloomIndex:
    """Convenience: in-memory index for unit tests."""
    return LSHBloomIndex(state_dir=None)


def _signature_similarity(left: bytes, right: bytes) -> float:
    """Estimate Jaccard similarity from equal MinHash permutation values."""
    if len(left) != len(right) or not left or len(left) % 4:
        return 0.0
    permutations = len(left) // 4
    equal = sum(
        left[offset : offset + 4] == right[offset : offset + 4] for offset in range(0, len(left), 4)
    )
    return equal / permutations


def from_env() -> LSHBloomIndex:
    """Build an LSHBloomIndex from process environment variables."""
    return LSHBloomIndex(state_dir=os.environ.get("S2P_STATE_DIR", "/var/lib/s2p") + "/lshbloom")
