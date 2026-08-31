from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import httpx
from botocore.exceptions import ClientError

from processor.shadow_classifier import SHADOW_GENERATION, ShadowRuntime
from schemas.gold import GoldRecord


class _S3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def head_object(self, **kwargs: str) -> None:
        bucket, key = kwargs["Bucket"], kwargs["Key"]
        if (bucket, key) not in self.objects:
            raise ClientError(
                {
                    "Error": {"Code": "NoSuchKey"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            )

    def put_object(self, **kwargs: Any) -> None:
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs["Body"]


def _gold() -> GoldRecord:
    return GoldRecord(
        doc_id="sha256:" + "a" * 64,
        text="A complete technical paper body with equations and reasoning.",
        lang="en",
        tokens=12,
        quality_score=4,
        edu_score=4,
        route="pretrain",
        eligible_routes=["pretrain", "posttrain_candidate"],
        license="CC-BY-4.0",
        license_source="manual",
        risk_tier=1,
        valid_from=datetime(2026, 8, 31, tzinfo=UTC),
        scoring_version="pretrain-content-v3",
        classifier_revision="finepdfs@pinned",
        policy_revision="git:test",
        trace_id="a" * 32,
        source_feed="arxiv-html",
        source_format="html",
        extraction_pipeline="arxiv-html-v1",
        training_usage="pretrain_and_posttrain",
        scientific_artifact_s3_uri="s3://silver/scientific.json",
    )


def test_shadow_score_writes_idempotent_non_gating_audit(monkeypatch) -> None:
    s3 = _S3()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/shadow"
        return httpx.Response(
            200,
            json={
                "classifiers": {
                    "meta-rater-reasoning": {"score": 3.5, "coverage_ratio": 1.0},
                    "finemath": {"score": 2.0, "coverage_ratio": 0.8},
                    "cso-topics": {"topics": ["machine learning"]},
                }
            },
        )

    client = httpx.Client(
        base_url="http://shadow",
        transport=httpx.MockTransport(handler),
    )
    runtime = ShadowRuntime(
        cfg=None,  # type: ignore[arg-type]
        model_url="http://shadow",
        s3=s3,
        client=client,
        bucket="gold",
    )
    record = _gold()

    first = runtime.score(record.model_dump_json(by_alias=True).encode())
    second = runtime.score(record.model_dump_json(by_alias=True).encode())

    assert first == second
    assert len(s3.objects) == 1
    body = next(iter(s3.objects.values()))
    assert SHADOW_GENERATION.encode() in body
    assert b'"route":"pretrain"' in body


def test_shadow_skips_unknown_source_without_calling_models() -> None:
    client = httpx.Client(
        base_url="http://shadow",
        transport=httpx.MockTransport(lambda _request: (_ for _ in ()).throw(AssertionError())),
    )
    runtime = ShadowRuntime(
        cfg=None,  # type: ignore[arg-type]
        model_url="http://shadow",
        s3=_S3(),
        client=client,
        bucket="gold",
    )
    record = _gold().model_copy(
        update={"source_feed": "unknown", "scientific_artifact_s3_uri": None}
    )

    assert runtime.score(record.model_dump_json(by_alias=True).encode()) is None
