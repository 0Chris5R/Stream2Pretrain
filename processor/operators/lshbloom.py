"""Band-partitioned Bloom filter for streaming MinHash near-deduplication.

Implements the LSHBloom design from arXiv 2411.04257: each LSH band gets
its own Bloom filter, indexed in a key-value store. Inserting a new
signature is "for each band, hash the band-key into the corresponding
Bloom; if all bands report seen, declare a duplicate cluster".

State backend
-------------
Primary: ``plyvel`` (LevelDB) - constant memory, append-only, fast on
small VMs. Falls back to ``sqlitedict`` if plyvel is unavailable in the
container (its build chain pulls in libleveldb-dev which is not always
worth the image size). The fallback is documented in the operator's
``backend`` field for forensic replay.

This operator is the stateful core of Stream2Pretrain's near-dup pass. It
is deterministic: given the same insertion order it produces the same
cluster assignment, which is what makes the contamination-bisect feature
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
    ) -> None:
        self._num_bands = num_bands
        self._bits_per_band = bits_per_band
        self._num_hashes = num_hashes
        self._state_path = Path(state_dir) if state_dir else None
        self._cluster_counter = 0
        self._memory_bands: list[_BitArray] = [_BitArray(bits_per_band) for _ in range(num_bands)]
        self._cluster_map: dict[str, str] = {}
        self._cluster_anchors: dict[str, str] = {}
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
                    autocommit=True,
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
        if hasattr(db, "get") and not hasattr(db, "execute"):
            # plyvel / dict-like with bytes
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

    @staticmethod
    def _db_put(db: object, key: bytes, value: bytes) -> None:
        if hasattr(db, "put"):
            db.put(key, value)  # plyvel  # type: ignore[union-attr]
        else:
            db[key.decode("ascii")] = value  # type: ignore[index]

    @staticmethod
    def _band_key(band_idx: int) -> bytes:
        return f"band:{band_idx:04d}".encode("ascii")

    def _persist_band(self, band_idx: int) -> None:
        if self._db is None:
            return
        self._db_put(self._db, self._band_key(band_idx), self._memory_bands[band_idx].to_bytes())

    def _persist_cluster_map(self) -> None:
        if self._db is None:
            return
        import orjson

        self._db_put(self._db, b"__clusters__", orjson.dumps(self._cluster_map))
        self._db_put(
            self._db,
            b"__cluster_anchors__",
            orjson.dumps(self._cluster_anchors),
        )
        self._db_put(self._db, b"__counter__", str(self._cluster_counter).encode("ascii"))

    def observe(self, doc_id: str, sig: MinHashSignature) -> NearDupResult:
        """Insert a doc's signature; report whether it is a near-duplicate.

        The first occurrence in a (band, value) cell is the cluster anchor
        and gets a fresh ``cluster_id``. Subsequent collisions report the
        same cluster id.
        """
        bands = sig.band_keys(self._num_bands)
        cluster_keys = [self._cluster_key(i, b) for i, b in enumerate(bands)]
        # Determine if every band already has a hit -> near duplicate.
        all_seen = True
        existing_cluster: str | None = None
        for ck in cluster_keys:
            cid = self._cluster_map.get(ck)
            if cid is None:
                all_seen = False
                break
            existing_cluster = existing_cluster or cid
        if all_seen and existing_cluster is not None:
            # At-least-once Kafka delivery can replay the same document after
            # its expensive curation completed but before the source offset
            # checkpoint committed. A document is never a near-duplicate of
            # itself; only a different doc_id colliding with the anchor is.
            if self._cluster_anchors.get(existing_cluster) == doc_id:
                return NearDupResult(is_near_duplicate=False, cluster_id=existing_cluster)
            return NearDupResult(is_near_duplicate=True, cluster_id=existing_cluster)
        # Otherwise: register doc as the anchor of a new cluster (if no
        # band claimed one) and update the band Blooms.
        cluster_id = existing_cluster or self._mint_cluster_id(doc_id)
        self._cluster_anchors.setdefault(cluster_id, doc_id)
        for i, ck in enumerate(cluster_keys):
            for h in self._hashes(ck.encode("utf-8")):
                self._memory_bands[i].set(h)
            self._cluster_map.setdefault(ck, cluster_id)
            self._persist_band(i)
        self._persist_cluster_map()
        return NearDupResult(is_near_duplicate=False, cluster_id=cluster_id)

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

    def close(self) -> None:
        """Flush durable state and release file handles."""
        if self._db is None:
            return
        self._persist_cluster_map()
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


def from_env() -> LSHBloomIndex:
    """Build an LSHBloomIndex from process environment variables."""
    return LSHBloomIndex(state_dir=os.environ.get("S2P_STATE_DIR", "/var/lib/s2p") + "/lshbloom")
