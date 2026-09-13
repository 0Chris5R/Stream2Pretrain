"""Focused contracts for the fail-closed curator state migration."""

from __future__ import annotations

import errno
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.migrate_curator_state_to_postgres as migration
from processor.operators.minhash import MinHashSignature
from scripts.migrate_curator_state_to_postgres import (
    CuratorState,
    DecisionRow,
    LegacyLshBandRow,
    LshBandRow,
    LshClusterRow,
    LshSource,
    _ensure_snapshot_capacity,
    _LshMaps,
    _quiesced_leveldb_source,
    _validate_anchor_minhash_contract,
    fingerprints,
    import_verified_state,
    parse_lsh_records,
    read_lsh_state,
    reuse_snapshot,
)


def test_parse_lsh_records_maps_legacy_snapshot_and_incremental_keys() -> None:
    clusters, bands = parse_lsh_records(
        (
            (
                b"__clusters__",
                json.dumps({"0000:alpha": "cl-00000001-anchor"}).encode(),
            ),
            (
                b"__cluster_anchors__",
                json.dumps({"cl-00000001-anchor": "sha256:anchor"}).encode(),
            ),
            (b"cluster:0000:alpha", b"cl-00000001-anchor"),
            (b"anchor:cl-00000001-anchor", b"sha256:anchor"),
            (b"signature:cl-00000001-anchor", b"\x01\x02\x03\x04"),
            (b"band:0000", b"bloom-acceleration-only"),
            (b"__counter__", b"1"),
        ),
        generation="pretrain-content-v4",
    )

    assert clusters == (
        LshClusterRow(
            "pretrain-content-v4",
            "cl-00000001-anchor",
            "sha256:anchor",
            b"\x01\x02\x03\x04",
        ),
    )
    assert bands == (LshBandRow("pretrain-content-v4", "0000:alpha", "cl-00000001-anchor"),)


def test_parse_lsh_records_refuses_a_cluster_without_a_signature() -> None:
    with pytest.raises(RuntimeError, match="missing_signatures"):
        parse_lsh_records(
            (
                (b"cluster:0000:alpha", b"cl-00000001-anchor"),
                (b"anchor:cl-00000001-anchor", b"sha256:anchor"),
            ),
            generation="pretrain-content-v4",
        )


def test_parse_lsh_records_refuses_conflicting_encodings() -> None:
    with pytest.raises(RuntimeError, match="conflicting cluster key"):
        parse_lsh_records(
            (
                (b"__clusters__", b'{"0000:alpha":"cl-one"}'),
                (b"cluster:0000:alpha", b"cl-two"),
            ),
            generation="pretrain-content-v4",
        )


def test_fingerprints_do_not_depend_on_target_created_at() -> None:
    state = CuratorState(
        decisions=(DecisionRow("recipe", b"decision", True),),
        clusters=(LshClusterRow("v1", "cl-one", "sha256:one", b"\x01\x02\x03\x04"),),
        bands=(LshBandRow("v1", "0000:alpha", "cl-one"),),
    )

    assert fingerprints(state) == fingerprints(state)
    assert fingerprints(state)["curator_decisions"]["rows"] == 1
    assert fingerprints(state)["curator_lsh_clusters"]["rows"] == 1
    assert fingerprints(state)["curator_lsh_bands"]["rows"] == 1


def test_anchor_contract_refuses_unproven_or_incompatible_minhash() -> None:
    state = CuratorState(
        decisions=(
            DecisionRow(
                "recipe",
                json.dumps(
                    {
                        "doc_id": "sha256:one",
                        "text": "anchor text",
                        "scoring_version": "v1",
                        "near_dup_cluster_id": "cl-one",
                        "near_duplicate": False,
                        "minhash_backend": "datasketch",
                        "minhash_num_perms": 112,
                    }
                ).encode(),
                True,
            ),
        ),
        clusters=(LshClusterRow("v1", "cl-one", "sha256:one", b"\x00" * 448),),
        bands=(LshBandRow("v1", "0000:alpha", "cl-one"),),
    )

    with pytest.raises(RuntimeError, match="cannot be proven compatible"):
        _validate_anchor_minhash_contract(
            state,
            expected_backend="rensa",
            expected_permutations=112,
        )


def test_anchor_contract_requires_matching_scoring_generation() -> None:
    payload = json.dumps(
        {
            "doc_id": "sha256:one",
            "text": "anchor text",
            "scoring_version": "v2",
            "near_dup_cluster_id": "cl-one",
            "near_duplicate": False,
            "minhash_backend": "rensa",
            "minhash_num_perms": 1,
        }
    ).encode()
    state = CuratorState(
        decisions=(DecisionRow("recipe", payload, True),),
        clusters=(LshClusterRow("v1", "cl-one", "sha256:one", b"\x00" * 4),),
        bands=(LshBandRow("v1", "0000:alpha", "cl-one"),),
    )

    with pytest.raises(RuntimeError, match="cannot be proven compatible"):
        _validate_anchor_minhash_contract(
            state,
            expected_backend="rensa",
            expected_permutations=1,
        )


def test_anchor_contract_uses_generation_contract_for_complete_lsh_state() -> None:
    signature = MinHashSignature(digest=b"\x01" * 448, num_perms=112, backend="rensa")
    first_band = signature.band_keys()[0]
    cluster_key = (
        "0000:"
        + migration.hashlib.blake2b(
            first_band,
            digest_size=8,
            person=b"s2pck",
        ).hexdigest()
    )
    payload = json.dumps(
        {
            "doc_id": "sha256:another-document",
            "text": "another document",
            "scoring_version": "v1",
            "near_dup_cluster_id": None,
            "near_duplicate": False,
            "minhash_backend": "rensa",
            "minhash_num_perms": 112,
        }
    ).encode()
    state = CuratorState(
        decisions=(DecisionRow("recipe", payload, True),),
        clusters=(
            LshClusterRow(
                "v1",
                "cl-one",
                "sha256:missing-decision",
                signature.digest,
            ),
        ),
        bands=(LshBandRow("v1", cluster_key, "cl-one"),),
    )

    validated = _validate_anchor_minhash_contract(
        state,
        expected_backend="rensa",
        expected_permutations=112,
    )

    assert validated.clusters[0].signature_backend == "rensa"
    assert validated.clusters[0].num_perms == 112


def test_anchor_contract_preserves_band_only_generation() -> None:
    payload = json.dumps(
        {
            "doc_id": "sha256:known",
            "text": "known decision",
            "scoring_version": "v1",
            "near_dup_cluster_id": "cl-known",
            "near_duplicate": False,
            "minhash_backend": "rensa",
            "minhash_num_perms": 112,
        }
    ).encode()
    state = CuratorState(
        decisions=(DecisionRow("recipe", payload, True),),
        clusters=(LshClusterRow("v1", "cl-missing", "", b""),),
        bands=(LshBandRow("v1", "0000:legacy", "cl-missing"),),
    )

    validated = _validate_anchor_minhash_contract(
        state,
        expected_backend="rensa",
        expected_permutations=112,
    )

    assert validated.clusters == ()
    assert validated.bands == ()
    assert validated.legacy_bands == (
        LegacyLshBandRow("v1", "0000:legacy", "cl-missing", "rensa", 112),
    )


def test_anchor_contract_does_not_invent_legacy_signature_from_decision() -> None:
    signature = MinHashSignature(digest=b"\x01" * 448, num_perms=112, backend="rensa")
    first_band = signature.band_keys()[0]
    cluster_key = (
        "0000:"
        + migration.hashlib.blake2b(
            first_band,
            digest_size=8,
            person=b"s2pck",
        ).hexdigest()
    )
    state = CuratorState(
        decisions=(),
        clusters=(LshClusterRow("v1", "cl-one", "", b""),),
        bands=(LshBandRow("v1", cluster_key, "cl-one"),),
    )
    payload = json.dumps(
        {
            "doc_id": "sha256:one",
            "text": "anchor text",
            "scoring_version": "v1",
            "near_dup_cluster_id": "cl-one",
            "near_duplicate": False,
            "minhash_backend": "rensa",
            "minhash_num_perms": 112,
        }
    ).encode()
    minhasher = SimpleNamespace(
        signature=lambda text: signature if text == "anchor text" else None,
    )

    reconstructed = _validate_anchor_minhash_contract(
        state,
        expected_backend="rensa",
        expected_permutations=112,
        decisions=(DecisionRow("recipe", payload, True),),
        minhasher=minhasher,  # type: ignore[arg-type]
    )

    assert reconstructed.clusters == ()
    assert reconstructed.bands == ()
    assert reconstructed.legacy_bands == (
        LegacyLshBandRow("v1", cluster_key, "cl-one", "rensa", 112),
    )


def test_anchor_contract_accepts_reprocessed_anchor_with_same_signature() -> None:
    signature = MinHashSignature(digest=b"\x01" * 448, num_perms=112, backend="rensa")
    first_band = signature.band_keys()[0]
    cluster_key = (
        "0000:"
        + migration.hashlib.blake2b(
            first_band,
            digest_size=8,
            person=b"s2pck",
        ).hexdigest()
    )
    state = CuratorState(
        decisions=(),
        clusters=(
            LshClusterRow(
                "v1",
                "cl-one",
                "sha256:one",
                signature.digest,
            ),
        ),
        bands=(LshBandRow("v1", cluster_key, "cl-one"),),
    )

    def payload(text: str) -> bytes:
        return json.dumps(
            {
                "doc_id": "sha256:one",
                "text": text,
                "scoring_version": "v1",
                "near_dup_cluster_id": "cl-one",
                "near_duplicate": False,
                "minhash_backend": "rensa",
                "minhash_num_perms": 112,
            }
        ).encode()

    minhasher = SimpleNamespace(signature=lambda _text: signature)

    reconstructed = _validate_anchor_minhash_contract(
        state,
        expected_backend="rensa",
        expected_permutations=112,
        decisions=(
            DecisionRow("one", payload("anchor text"), True),
            DecisionRow("two", payload("anchor text with punctuation!"), False),
        ),
        minhasher=minhasher,  # type: ignore[arg-type]
    )

    assert reconstructed.clusters[0].signature == signature.digest


def test_anchor_contract_refuses_reconstructed_signature_with_wrong_band() -> None:
    signature = MinHashSignature(digest=b"\x01" * 448, num_perms=112, backend="rensa")
    state = CuratorState(
        decisions=(),
        clusters=(
            LshClusterRow(
                "v1",
                "cl-one",
                "sha256:one",
                signature.digest,
            ),
        ),
        bands=(LshBandRow("v1", "0000:not-the-anchor-band", "cl-one"),),
    )
    payload = json.dumps(
        {
            "doc_id": "sha256:one",
            "text": "anchor text",
            "scoring_version": "v1",
            "near_dup_cluster_id": "cl-one",
            "near_duplicate": False,
            "minhash_backend": "rensa",
            "minhash_num_perms": 112,
        }
    ).encode()
    minhasher = SimpleNamespace(signature=lambda _text: signature)

    with pytest.raises(RuntimeError, match="band ownership conflicts"):
        _validate_anchor_minhash_contract(
            state,
            expected_backend="rensa",
            expected_permutations=112,
            decisions=(DecisionRow("recipe", payload, True),),
            minhasher=minhasher,  # type: ignore[arg-type]
        )


def test_snapshot_capacity_refuses_insufficient_destination_space(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    decision = tmp_path / "decision-cache.sqlite3"
    decision.write_bytes(b"1234")
    leveldb = tmp_path / "leveldb"
    leveldb.mkdir()
    (leveldb / "data").write_bytes(b"5678")
    source = LshSource("v1", "leveldb", leveldb, Path("lshbloom/v1/leveldb"))
    monkeypatch.setattr(
        migration.shutil,
        "disk_usage",
        lambda _path: SimpleNamespace(free=7),
    )

    with pytest.raises(RuntimeError, match="required_bytes=8, available_bytes=7"):
        _ensure_snapshot_capacity(decision, (source,), tmp_path / "snapshot")


def test_leveldb_snapshot_refuses_a_locked_source(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "leveldb"
    source.mkdir()
    (source / "LOCK").touch()

    def locked(*_args: object) -> None:
        raise BlockingIOError(errno.EAGAIN, "locked")

    monkeypatch.setattr(migration.fcntl, "lockf", locked)

    with (
        pytest.raises(RuntimeError, match="still locked"),
        _quiesced_leveldb_source(source),
    ):
        pytest.fail("locked LevelDB must not be copied")


def test_reuse_snapshot_loads_retained_sources(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    (snapshot / "decision-cache.sqlite3").write_bytes(b"decisions")
    lsh = snapshot / "lshbloom" / "v1"
    lsh.mkdir(parents=True)
    (lsh / "lshbloom.sqlite").write_bytes(b"lsh")

    decision, sources, hashes = reuse_snapshot(
        snapshot,
        legacy_generation=None,
        requested_backend="sqlitedict",
    )

    assert decision == snapshot / "decision-cache.sqlite3"
    assert sources == (
        LshSource(
            "v1",
            "sqlitedict",
            lsh / "lshbloom.sqlite",
            Path("lshbloom/v1/lshbloom.sqlite"),
        ),
    )
    assert hashes["decision-cache.sqlite3"] == migration._sha256_file(decision)


def test_reuse_snapshot_refuses_completed_manifest(tmp_path: Path) -> None:
    snapshot = tmp_path / "snapshot"
    snapshot.mkdir()
    (snapshot / "migration-manifest.json").write_text("{}")

    with pytest.raises(RuntimeError, match="completed migration manifest"):
        reuse_snapshot(
            snapshot,
            legacy_generation=None,
            requested_backend="leveldb",
        )


def test_read_lsh_state_unions_matching_sources_for_one_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = (
        LshSource("v1", "sqlitedict", Path("legacy.sqlite"), Path("legacy.sqlite")),
        LshSource("v1", "leveldb", Path("leveldb"), Path("v1/leveldb")),
    )
    source_maps = {
        Path("legacy.sqlite"): _LshMaps(
            {"0000:alpha": "cl-one"},
            {"cl-one": "sha256:one"},
            {},
        ),
        Path("leveldb"): _LshMaps(
            {"0000:alpha": "cl-one"},
            {"cl-one": "sha256:one"},
            {"cl-one": b"\x00" * 448},
        ),
    }
    monkeypatch.setattr(migration, "_read_lsh_source", lambda source: source_maps[source.path])

    state = read_lsh_state(sources)

    assert len(state.clusters) == 1
    assert len(state.bands) == 1
    assert state.clusters[0].signature == b"\x00" * 448


def test_read_lsh_state_refuses_conflicting_sources_for_one_generation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sources = (
        LshSource("v1", "sqlitedict", Path("one"), Path("one")),
        LshSource("v1", "leveldb", Path("two"), Path("two")),
    )
    source_maps = {
        Path("one"): _LshMaps({"0000:alpha": "cl-one"}, {}, {}),
        Path("two"): _LshMaps({"0000:alpha": "cl-two"}, {}, {}),
    }
    monkeypatch.setattr(migration, "_read_lsh_source", lambda source: source_maps[source.path])

    with pytest.raises(RuntimeError, match="conflicting cluster key"):
        read_lsh_state(sources)


class _TransactionTarget:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.committed = False
        self.rolled_back = False

    def execute(self, sql: str, parameters: object = ()) -> object:
        self.executed.append(sql)
        return object()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True


def test_prepare_target_schema_rebuilds_only_matching_legacy_lsh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _TransactionTarget()
    legacy_fingerprints = {
        "curator_decisions": {"rows": 1, "sha256": "decisions"},
        "curator_lsh_clusters": {"rows": 1, "sha256": "clusters"},
        "curator_lsh_bands": {"rows": 1, "sha256": "bands"},
    }
    initialized: list[bool] = []
    copied: list[bool] = []
    monkeypatch.setattr(migration, "_lsh_schema_version", lambda _target: "legacy")
    monkeypatch.setattr(
        migration,
        "_legacy_target_fingerprints",
        lambda _target: legacy_fingerprints,
    )
    monkeypatch.setattr(
        migration,
        "_legacy_source_fingerprints",
        lambda *_args: legacy_fingerprints,
    )
    monkeypatch.setattr(
        migration,
        "_initialize_target_schema",
        lambda _target: initialized.append(True),
    )
    monkeypatch.setattr(
        migration,
        "_copy_lsh_state",
        lambda *_args: copied.append(True),
    )

    migrated = migration._prepare_target_schema(
        target,
        Path("decisions.sqlite3"),
        CuratorState((), (), ()),
    )

    assert migrated is True
    assert any(statement == "DROP TABLE curator_lsh_bands" for statement in target.executed)
    assert any(statement == "DROP TABLE curator_lsh_clusters" for statement in target.executed)
    assert initialized == [True]
    assert copied == [True]


def test_import_verified_state_is_idempotent_without_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _TransactionTarget()
    expected = fingerprints(CuratorState((), (), ()))
    monkeypatch.setattr(migration, "target_fingerprints", lambda _target: expected)
    monkeypatch.setattr(migration, "_prepare_target_schema", lambda *_args: False)
    monkeypatch.setattr(
        migration,
        "copy_state",
        lambda *_args: pytest.fail("verified state must not be copied again"),
    )

    mode = import_verified_state(
        Path("decisions.sqlite3"), CuratorState((), (), ()), target, expected
    )

    assert mode == "verified-existing"
    assert target.committed is True
    assert target.rolled_back is False


def test_import_verified_state_rolls_back_a_fingerprint_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = _TransactionTarget()
    expected = fingerprints(CuratorState((), (), ()))
    observed = iter(
        (
            {**expected, "curator_decisions": {"rows": 1, "sha256": "before"}},
            {**expected, "curator_decisions": {"rows": 1, "sha256": "after"}},
        )
    )
    monkeypatch.setattr(migration, "target_fingerprints", lambda _target: next(observed))
    monkeypatch.setattr(migration, "_prepare_target_schema", lambda *_args: False)
    monkeypatch.setattr(migration, "_target_is_pristine", lambda _target: True)
    monkeypatch.setattr(migration, "copy_state", lambda *_args: None)

    with pytest.raises(RuntimeError, match="post-import"):
        import_verified_state(
            Path("decisions.sqlite3"),
            CuratorState((), (), ()),
            target,
            expected,
        )

    assert target.committed is False
    assert target.rolled_back is True
