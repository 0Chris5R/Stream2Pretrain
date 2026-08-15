"""Tests for :mod:`processor.decon_api`."""

from __future__ import annotations

from datetime import UTC, datetime

from processor import common
from processor.decon_api import AttestationStore, _wire_attestation
from schemas.decon import DeconAttestation


class _Body:
    def __init__(self, data: bytes) -> None:
        self._data = data

    def read(self) -> bytes:
        return self._data


class _FakeS3:
    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        last_modified: dict[str, datetime] | None = None,
    ) -> None:
        self.objects = objects
        self.last_modified = last_modified or {}

    def get_object(self, **kwargs: str) -> dict[str, _Body]:
        assert kwargs["Bucket"] == "s2p-decon"
        key = kwargs["Key"]
        if key not in self.objects:
            raise KeyError(key)
        return {"Body": _Body(self.objects[key])}

    def list_objects_v2(self, **kwargs: str) -> dict[str, list[dict[str, object]]]:
        assert kwargs["Bucket"] == "s2p-decon"
        prefix = kwargs["Prefix"]
        return {
            "Contents": [
                {"Key": key, "LastModified": self.last_modified.get(key)}
                for key in self.objects
                if key.startswith(prefix)
            ]
        }


def _attestation(snapshot_id: int) -> DeconAttestation:
    return DeconAttestation(
        snapshot_id=snapshot_id,
        committed_at=datetime(2026, 6, 15, tzinfo=UTC),
        benchmark_set_version="v-test",
        benchmarks=["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"],
        tokens_scanned=10,
        tokens_flagged=0,
        rejected_doc_hashes=[],
        per_benchmark_hits={"MMLU": 0, "GSM8K": 0, "HumanEval": 0, "MATH": 0, "GPQA": 0},
        signature="sig",
        signer_cert="cert",
    )


def test_store_get_uses_canonical_attestation_key() -> None:
    record = _attestation(42)
    key = "decon/v-test/00000000000000000042.json"
    store = AttestationStore(
        s3_client=_FakeS3({key: common.decon_dumps(record)}),
        bucket="s2p-decon",
        benchmark_set_version="v-test",
    )

    assert store.key_for_snapshot(42) == key
    assert store.get(42) == record
    assert store.get(43) is None


def test_store_list_returns_newest_object_first_limited() -> None:
    objects = {
        f"decon/v-test/{i:020d}.json": common.decon_dumps(_attestation(i)) for i in range(1, 4)
    }
    timestamps = {
        "decon/v-test/00000000000000000001.json": datetime(2026, 6, 15, 12, 3, tzinfo=UTC),
        "decon/v-test/00000000000000000002.json": datetime(2026, 6, 15, 12, 1, tzinfo=UTC),
        "decon/v-test/00000000000000000003.json": datetime(2026, 6, 15, 12, 2, tzinfo=UTC),
    }
    store = AttestationStore(
        s3_client=_FakeS3(objects, last_modified=timestamps),
        bucket="s2p-decon",
        benchmark_set_version="v-test",
    )

    assert [r.snapshot_id for r in store.list(2)] == [1, 3]


def test_wire_snapshot_id_is_lossless_for_javascript() -> None:
    record = _attestation(9_223_372_036_854_775_000)

    payload = _wire_attestation(record)

    assert payload["snapshot_id"] == "9223372036854775000"
