"""Tests for :mod:`processor.fetcher`."""

from __future__ import annotations

import gzip
from typing import Any

import pytest

from processor import common
from processor.fetcher import (
    FetcherState,
    fetch_raw_bytes,
    fetcher_input_topics,
    native_consumer_config,
    normalize,
    process_bronze_payload,
    run_native_fetcher,
    serialize_for_kafka,
    uses_scientific_extraction,
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

            def read(self, amount: int | None = None) -> bytes:
                return self._p if amount is None else self._p[:amount]

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


def test_scientific_extraction_is_source_aware(bronze_record: BronzeRecord) -> None:
    blog = bronze_record.model_copy(
        update={
            "source_format": "html",
            "source_feed": "rss-openai-news",
            "extraction_pipeline": "resiliparse-html-v1",
        }
    )
    paper = blog.model_copy(
        update={
            "source_feed": "arxiv-html",
            "extraction_pipeline": "arxiv-html-2026-06",
        }
    )
    review = blog.model_copy(
        update={
            "source_format": "review",
            "source_feed": "openreview-live",
        }
    )

    assert uses_scientific_extraction(blog) is False
    assert uses_scientific_extraction(paper) is True
    assert uses_scientific_extraction(review) is False


def test_fetch_raw_bytes_rejects_oversized_stored_object(
    bronze_record: BronzeRecord, monkeypatch: Any
) -> None:
    monkeypatch.setenv("S2P_MAX_RAW_OBJECT_BYTES", "16")
    s3 = _FakeS3(b"x" * 64, gzip_encoded=False)
    state = _state(s3)

    assert fetch_raw_bytes(state, bronze_record) == b""


def test_fetch_raw_bytes_rejects_oversized_gzip_expansion(
    bronze_record: BronzeRecord, monkeypatch: Any
) -> None:
    monkeypatch.setenv("S2P_MAX_RAW_OBJECT_BYTES", "1024")
    monkeypatch.setenv("S2P_MAX_EXPANDED_OBJECT_BYTES", "32")
    s3 = _FakeS3(b"x" * 128)
    state = _state(s3)

    assert fetch_raw_bytes(state, bronze_record) == b""


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


@pytest.mark.parametrize("source_format", ["latex", "markdown"])
def test_normalize_scientific_text_skips_html_extraction(
    bronze_record: BronzeRecord, source_format: str
) -> None:
    state = _state(_FakeS3(b""))
    scientific_bronze = bronze_record.model_copy(
        update={
            "url": "https://openreview.net/pdf?id=paper1",
            "source_format": source_format,
            "source_feed": "openreview-backfill",
            "extraction_pipeline": "reviewarena-ocr-markdown-v1",
            "spdx_license": "unknown",
            "spdx_license_source": "unknown",
            "training_usage": "posttrain_transform_only",
        }
    )
    payload = b"# A scientific paper\n\nWe derive the objective $L(theta)$ and evaluate it."

    silver = normalize(state, scientific_bronze, payload)

    assert silver is not None
    assert silver.source_format == source_format
    assert "derive the objective" in silver.text
    assert silver.extracted_with == "reviewarena-ocr-markdown-v1"


def test_normalize_structured_metadata_extracts_human_text(bronze_record: BronzeRecord) -> None:
    state = _state(_FakeS3(b""))
    metadata_bronze = bronze_record.model_copy(
        update={
            "source_format": "metadata",
            "extraction_pipeline": "hf-api-json-v1",
            "spdx_license": "Apache-2.0",
            "spdx_license_source": "dataset_metadata",
        }
    )
    payload = (
        b'{"id":"org/model","title":"Research model",'
        b'"description":"A model trained for scientific retrieval",'
        b'"url":"https://example.invalid/model"}'
    )

    silver = normalize(state, metadata_bronze, payload)

    assert silver is not None
    assert silver.title == "Research model"
    assert "scientific retrieval" in silver.text
    assert "https://example.invalid" not in silver.text
    assert silver.extracted_with == "hf-api-json-v1"


def test_normalize_returns_none_on_empty_html(bronze_record: BronzeRecord) -> None:
    state = _state(_FakeS3(b""))
    assert normalize(state, bronze_record, b"") is None


def test_missing_non_code_license_stops_before_minio_and_processing(
    bronze_record: BronzeRecord,
) -> None:
    s3 = _FakeS3(
        b"<html><body><p>This is an English document with enough readable content for extraction.</p></body></html>"
    )
    state = _state(s3)
    unlicensed = bronze_record.model_copy(
        update={"spdx_license": None, "spdx_license_source": "unknown"}
    )

    assert process_bronze_payload(state, unlicensed.model_dump_json().encode()) is None
    assert s3.calls == []


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


class _FakeMessage:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def error(self) -> None:
        return None

    def value(self) -> bytes:
        return self._payload

    def topic(self) -> str:
        return "raw.fetched"

    def partition(self) -> int:
        return 2

    def offset(self) -> int:
        return 7


class _FakeConsumer:
    def __init__(self, config: dict[str, object], payload: bytes) -> None:
        self.config = config
        self._message = _FakeMessage(payload)
        self.topics: list[str] = []
        self.commits: list[tuple[list[Any], bool]] = []
        self.closed = False

    def subscribe(self, topics: list[str], *, on_revoke: Any = None) -> None:
        self.topics = topics
        self.on_revoke = on_revoke

    def poll(self, _timeout: float) -> _FakeMessage | None:
        message, self._message = self._message, None
        return message

    def commit(self, *, offsets: list[Any], asynchronous: bool) -> None:
        self.commits.append((offsets, asynchronous))

    def close(self) -> None:
        self.closed = True


class _FakeProducer:
    def __init__(self, config: dict[str, object], *, delivery_error: object = None) -> None:
        self.config = config
        self.delivery_error = delivery_error
        self.records: list[dict[str, Any]] = []

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.records.append({"topic": topic, **kwargs})
        callback = kwargs.get("on_delivery")
        if callback is not None:
            callback(self.delivery_error, object())

    def poll(self, _timeout: float) -> None:
        return None

    def flush(self, _timeout: float) -> int:
        return 0


def test_native_fetcher_commits_only_after_output_delivery(
    bronze_record: BronzeRecord,
    silver_record: Any,
    monkeypatch: Any,
) -> None:
    cfg = common.load_config()
    consumer: _FakeConsumer | None = None
    producer: _FakeProducer | None = None

    def consumer_factory(config: dict[str, object]) -> _FakeConsumer:
        nonlocal consumer
        consumer = _FakeConsumer(config, bronze_record.model_dump_json().encode())
        return consumer

    def producer_factory(config: dict[str, object]) -> _FakeProducer:
        nonlocal producer
        producer = _FakeProducer(config)
        return producer

    monkeypatch.setattr("processor.fetcher.process_bronze_payload", lambda *_: silver_record)
    monkeypatch.setenv("S2P_SMOKE_RAW_TOPIC", "raw.smoke")
    run_native_fetcher(
        cfg,
        state=_state(_FakeS3(b"")),
        consumer_factory=consumer_factory,
        producer_factory=producer_factory,
        max_messages=1,
    )

    assert consumer is not None
    assert producer is not None
    assert consumer.topics == ["raw.fetched", "raw.smoke"]
    assert consumer.config["enable.auto.commit"] is False
    assert consumer.config["enable.auto.offset.store"] is False
    assert producer.records[0]["topic"] == "docs.normalized"
    assert consumer.commits[0][0][0].partition == 2
    assert consumer.commits[0][0][0].offset == 8
    assert consumer.commits[0][1] is False
    assert consumer.closed is True


def test_native_fetcher_does_not_commit_after_delivery_failure(
    bronze_record: BronzeRecord,
    silver_record: Any,
    monkeypatch: Any,
) -> None:
    cfg = common.load_config()
    consumer: _FakeConsumer | None = None

    def consumer_factory(config: dict[str, object]) -> _FakeConsumer:
        nonlocal consumer
        consumer = _FakeConsumer(config, bronze_record.model_dump_json().encode())
        return consumer

    def producer_factory(config: dict[str, object]) -> _FakeProducer:
        return _FakeProducer(config, delivery_error=RuntimeError("broker rejected record"))

    monkeypatch.setattr("processor.fetcher.process_bronze_payload", lambda *_: silver_record)
    with pytest.raises(RuntimeError, match="producer delivery failed"):
        run_native_fetcher(
            cfg,
            state=_state(_FakeS3(b"")),
            consumer_factory=consumer_factory,
            producer_factory=producer_factory,
            max_messages=1,
        )

    assert consumer is not None
    assert consumer.commits == []
    assert consumer.closed is True


def test_native_consumer_defaults_to_earliest(monkeypatch: Any) -> None:
    monkeypatch.setenv("S2P_KAFKA_START_OFFSET", "beginning")
    monkeypatch.delenv("S2P_SMOKE_RAW_TOPIC", raising=False)
    monkeypatch.delenv("S2P_FETCHER_INPUT_TOPICS", raising=False)
    cfg = common.load_config()

    assert native_consumer_config(cfg)["auto.offset.reset"] == "earliest"
    assert fetcher_input_topics(cfg) == ["raw.fetched", "raw.smoke"]


def test_fetcher_input_topics_support_isolated_traffic_class(monkeypatch: Any) -> None:
    cfg = common.load_config()
    monkeypatch.setenv("S2P_FETCHER_INPUT_TOPICS", " raw.smoke, raw.smoke ")

    assert fetcher_input_topics(cfg) == ["raw.smoke"]


def test_native_fetcher_supports_isolated_output_lane(
    bronze_record: BronzeRecord,
    silver_record: Any,
    monkeypatch: Any,
) -> None:
    cfg = common.load_config()
    producer: _FakeProducer | None = None

    def consumer_factory(config: dict[str, object]) -> _FakeConsumer:
        return _FakeConsumer(config, bronze_record.model_dump_json().encode())

    def producer_factory(config: dict[str, object]) -> _FakeProducer:
        nonlocal producer
        producer = _FakeProducer(config)
        return producer

    monkeypatch.setattr("processor.fetcher.process_bronze_payload", lambda *_: silver_record)
    monkeypatch.setenv("S2P_FETCHER_INPUT_TOPICS", "raw.smoke")
    monkeypatch.setenv("S2P_FETCHER_OUTPUT_TOPIC", "docs.normalized.smoke")
    run_native_fetcher(
        cfg,
        state=_state(_FakeS3(b"")),
        consumer_factory=consumer_factory,
        producer_factory=producer_factory,
        max_messages=1,
    )

    assert producer is not None
    assert producer.records[0]["topic"] == "docs.normalized.smoke"
