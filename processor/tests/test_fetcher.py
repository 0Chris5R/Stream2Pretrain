"""Tests for :mod:`processor.fetcher`."""

from __future__ import annotations

import gzip
from typing import Any

import pytest
from botocore.exceptions import ClientError

from processor.fetcher import (
    FetcherState,
    PdfProcessingTemporarilyDisabled,
    RawObjectEmpty,
    RawObjectMissing,
    _is_non_release_github_record,
    _markdown_prose_projection,
    fetch_raw_bytes,
    fetcher_input_topics,
    normalize,
    process_bronze_payload,
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


@pytest.mark.parametrize(
    "ref",
    [b"viable/strict/1787664910", b"viable%2Fstrict%2F1787664910", b"ciflow/test"],
)
def test_historical_ci_refs_are_not_corpus_records(ref: bytes) -> None:
    class Message:
        def __init__(self) -> None:
            self.headers = [("source_feed", b"github-releases"), ("github_ref", ref)]

    assert _is_non_release_github_record(Message()) is True


def test_real_github_release_ref_is_not_filtered() -> None:
    class Message:
        def __init__(self) -> None:
            self.headers = [
                ("source_feed", b"github-releases"),
                ("github_ref", b"v2.8.0"),
            ]

    assert _is_non_release_github_record(Message()) is False


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
    assert uses_scientific_extraction(blog) is False
    assert uses_scientific_extraction(paper) is True


def test_hf_card_projection_excludes_frontmatter_and_fenced_code() -> None:
    payload = b"""---
license: apache-2.0
pipeline_tag: text-generation
---
# Useful Model

This card explains the intended use and evaluation results.

```python
SECRET = "not training prose"
```
"""

    text, title, metadata = _markdown_prose_projection(payload)

    assert title == "Useful Model"
    assert "intended use" in text
    assert "SECRET" not in text
    assert "pipeline_tag" in metadata


def test_fetch_raw_bytes_rejects_oversized_stored_object(
    bronze_record: BronzeRecord, monkeypatch: Any
) -> None:
    monkeypatch.setenv("S2P_MAX_RAW_OBJECT_BYTES", "16")
    s3 = _FakeS3(b"x" * 64, gzip_encoded=False)
    state = _state(s3)

    with pytest.raises(ValueError, match="raw object exceeds the configured bound"):
        fetch_raw_bytes(state, bronze_record)


def test_fetch_raw_bytes_rejects_oversized_gzip_expansion(
    bronze_record: BronzeRecord, monkeypatch: Any
) -> None:
    monkeypatch.setenv("S2P_MAX_RAW_OBJECT_BYTES", "1024")
    monkeypatch.setenv("S2P_MAX_EXPANDED_OBJECT_BYTES", "32")
    s3 = _FakeS3(b"x" * 128)
    state = _state(s3)

    with pytest.raises(ValueError, match="expanded raw object exceeds the configured bound"):
        fetch_raw_bytes(state, bronze_record)


def test_missing_raw_object_is_a_record_local_durable_failure(
    bronze_record: BronzeRecord,
) -> None:
    class _MissingS3:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": f"missing {Bucket}/{Key}"}},
                "GetObject",
            )

    state = _state(_FakeS3(b""))
    state.s3 = _MissingS3()

    with pytest.raises(RawObjectMissing, match=bronze_record.doc_id):
        fetch_raw_bytes(state, bronze_record)


def test_empty_raw_object_is_a_record_local_durable_failure(
    bronze_record: BronzeRecord,
) -> None:
    state = _state(_FakeS3(b"", gzip_encoded=False))

    with pytest.raises(RawObjectEmpty, match=bronze_record.doc_id):
        process_bronze_payload(state, bronze_record.model_dump_json().encode("utf-8"))


def test_transient_raw_object_failure_remains_retryable(
    bronze_record: BronzeRecord,
) -> None:
    class _UnavailableS3:
        def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
            raise ClientError(
                {"Error": {"Code": "SlowDown", "Message": f"retry {Bucket}/{Key}"}},
                "GetObject",
            )

    state = _state(_FakeS3(b""))
    state.s3 = _UnavailableS3()

    with pytest.raises(RuntimeError, match="raw object read failed"):
        fetch_raw_bytes(state, bronze_record)


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
    assert silver.model_text == ""
    assert silver.segments == []
    assert silver.extracted_with == "hf-api-json-v1"


def test_normalize_oai_xml_extracts_text_without_training_on_markup(
    bronze_record: BronzeRecord,
) -> None:
    state = _state(_FakeS3(b""))
    metadata_bronze = bronze_record.model_copy(
        update={
            "source_format": "metadata",
            "source_feed": "arxiv-oai-cs",
            "extraction_pipeline": "oai-pmh-metadata-v1",
            "spdx_license": "CC-BY-4.0",
            "spdx_license_source": "oai_metadata",
        }
    )
    payload = b"""<record xmlns:dc="http://purl.org/dc/elements/1.1/">
      <dc:title>A Structured Scientific Record</dc:title>
      <dc:creator>Ada Researcher</dc:creator>
      <dc:description>We evaluate a reproducible streaming pipeline.</dc:description>
      <dc:identifier>https://arxiv.org/abs/2608.00001</dc:identifier>
    </record>"""

    silver = normalize(state, metadata_bronze, payload)

    assert silver is not None
    assert silver.title == "A Structured Scientific Record"
    assert "reproducible streaming pipeline" in silver.text
    assert "<dc:" not in silver.text
    assert "https://arxiv.org" not in silver.text
    assert silver.model_text == ""
    assert silver.segments == []
    assert silver.extracted_with == "oai-pmh-metadata-v1"


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


def test_temporarily_disabled_pdf_is_deferred_before_minio(
    bronze_record: BronzeRecord, monkeypatch: Any
) -> None:
    monkeypatch.setenv("S2P_PDF_PROCESSING_ENABLED", "0")
    s3 = _FakeS3(b"%PDF-1.7\nnot fetched", gzip_encoded=False)
    state = _state(s3)
    pdf = bronze_record.model_copy(
        update={
            "source_format": "pdf",
            "content_type": "application/pdf",
            "extraction_pipeline": "docling-pdf-cpu-2.114.0",
        }
    )

    with pytest.raises(PdfProcessingTemporarilyDisabled):
        process_bronze_payload(state, pdf.model_dump_json().encode())

    assert s3.calls == []


def test_serialize_for_kafka_roundtrip(silver_record: Any) -> None:
    key, value = serialize_for_kafka(silver_record)
    assert key == silver_record.doc_id.encode("utf-8")
    assert value
    # Ensure the serialised payload is JSON-decodable.
    import orjson

    decoded = orjson.loads(value)
    assert decoded["doc_id"] == silver_record.doc_id


def test_fetcher_input_topics_default_to_production_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("S2P_FETCHER_INPUT_TOPICS", raising=False)
    from processor import common

    assert fetcher_input_topics(common.load_config()) == ["raw.fetched"]


def test_fetcher_input_topics_support_isolated_traffic_class(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from processor import common

    cfg = common.load_config()
    monkeypatch.setenv("S2P_FETCHER_INPUT_TOPICS", " raw.smoke, raw.smoke ")

    assert fetcher_input_topics(cfg) == ["raw.smoke"]
