"""Tests for :mod:`processor.fetcher`."""

from __future__ import annotations

import gzip
from typing import Any

from processor.fetcher import (
    FetcherState,
    fetch_raw_bytes,
    normalize,
    process_bronze_payload,
    serialize_for_kafka,
)
from processor.operators.extract import ResiliparseExtractor
from processor.operators.langid import LangIdentifier
from processor.operators.minhash import MinHasher
from processor.operators.validity import ValidityEnricher
from schemas.bronze import BronzeRecord


class _FakeS3:
    def __init__(self, payload: bytes, *, gzip_encoded: bool = True) -> None:
        self._payload = gzip.compress(payload) if gzip_encoded else payload
        self._gzip = gzip_encoded
        self.calls: list[tuple[str, str]] = []

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        self.calls.append((Bucket, Key))

        class _Body:
            def __init__(self, payload: bytes) -> None:
                self._p = payload

            def read(self) -> bytes:
                return self._p

        headers = {"Body": _Body(self._payload)}
        if self._gzip:
            headers["ContentEncoding"] = "gzip"
        return headers


def _state(s3: _FakeS3, *, bucket: str = "bronze") -> FetcherState:
    return FetcherState(
        extractor=ResiliparseExtractor(),
        lang_id=LangIdentifier(),
        minhasher=MinHasher(num_perms=64),
        validity=ValidityEnricher(),
        s3=s3,
        bucket=bucket,
    )


def test_fetch_raw_bytes_decompresses_gzip(bronze_record: BronzeRecord) -> None:
    s3 = _FakeS3(b"<html><body><p>hello world from the test</p></body></html>")
    state = _state(s3)
    raw = fetch_raw_bytes(state, bronze_record)
    assert b"hello world" in raw
    assert s3.calls and s3.calls[0][0] == "bronze"


def test_normalize_returns_silver(bronze_record: BronzeRecord) -> None:
    html = (
        b"<html><head><title>Streaming Curator</title></head><body>"
        b"<p>The Stream2Pretrain pipeline curates documents into training shards.</p>"
        b"<p>This is a sufficiently long paragraph to drive language identification.</p>"
        b"</body></html>"
    )
    state = _state(_FakeS3(b""))
    silver = normalize(state, bronze_record, html)
    assert silver is not None
    assert silver.doc_id == bronze_record.doc_id
    assert silver.text
    assert silver.valid_from_source in {
        "http_last_modified",
        "schema_org_date_published",
        "fetched_at",
        "license_effective_date",
        "wayback_first_seen",
    }


def test_normalize_code_bronze_decodes_plain_text(bronze_record: BronzeRecord) -> None:
    state = _state(_FakeS3(b""))
    code_bronze = bronze_record.model_copy(
        update={
            "url": "https://github.com/org/repo/blob/v1/src/foo.py",
            "source_format": "code",
            "extraction_pipeline": "github-release-tarball-2026-06",
            "spdx_license": "Apache-2.0",
            "spdx_license_source": "github_api",
        }
    )
    silver = normalize(state, code_bronze, b"def fit_model(x):\n    return x\n")
    assert silver is not None
    assert silver.source_format == "code"
    assert silver.extraction_pipeline == "github-release-tarball-2026-06"
    assert silver.spdx_license == "Apache-2.0"
    assert "def fit_model" in silver.text


def test_normalize_returns_none_on_empty_html(bronze_record: BronzeRecord) -> None:
    state = _state(_FakeS3(b""))
    assert normalize(state, bronze_record, b"") is None


def test_missing_non_code_license_is_processed(
    bronze_record: BronzeRecord,
) -> None:
    s3 = _FakeS3(b"<html><body><p>This is an English document with enough readable content for extraction.</p></body></html>")
    state = _state(s3)
    unlicensed = bronze_record.model_copy(
        update={"spdx_license": None, "spdx_license_source": "unknown"}
    )

    assert process_bronze_payload(state, unlicensed.model_dump_json().encode()) is not None
    assert len(s3.calls) == 1


def test_missing_code_license_stops_before_minio_and_processing(
    bronze_record: BronzeRecord,
) -> None:
    s3 = _FakeS3(b"must not be read")
    state = _state(s3)
    unlicensed = bronze_record.model_copy(
        update={
            "source_format": "code",
            "spdx_license": None,
            "spdx_license_source": "unknown",
        }
    )

    assert process_bronze_payload(state, unlicensed.model_dump_json().encode()) is None
    assert s3.calls == []


def test_serialize_for_kafka_roundtrip(silver_record: Any) -> None:
    key, value = serialize_for_kafka(silver_record)
    assert key == silver_record.doc_id.encode("utf-8")
    assert value
    # Ensure the serialised payload is JSON-decodable.
    import orjson

    decoded = orjson.loads(value)
    assert decoded["doc_id"] == silver_record.doc_id
