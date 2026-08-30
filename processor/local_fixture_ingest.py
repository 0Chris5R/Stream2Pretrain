"""Emit deterministic local-only Bronze fixtures into the real pipeline.

This module is used by ``compose.local.yml`` to prove accepted, duplicate,
heuristic-reject, and PII-reject branches without depending on a live paper.
"""

from __future__ import annotations

import base64
import gzip
import io
import os
from dataclasses import dataclass
from datetime import UTC, datetime

import boto3
from confluent_kafka import Producer

from ingest.common.license_admission import decide_license_admission
from schemas.bronze import BronzeRecord


@dataclass(frozen=True, slots=True)
class LocalFixture:
    name: str
    title: str
    body: str
    source_feed: str = "local-controlled-fixtures-v3"


_CLEAN_BODY = (
    "A good experiment begins with a clear question. The researcher describes what will be "
    "measured and why the measurement matters. Before collecting data, the researcher records "
    "the method so that another person can repeat the work. Each observation is stored with a "
    "date, a source, and enough context to explain how it was produced. The analysis compares "
    "the observations and reports both the main result and the uncertainty around it. A table is "
    "useful for exact values, while a figure can make a trend easier to see. Neither should "
    "replace a written explanation of the evidence. A careful report also discusses cases that "
    "do not support the initial idea. This practice helps readers distinguish a strong conclusion "
    "from an interesting guess. When a new study becomes available, its results can be added "
    "without removing the earlier record. Over time, the collection shows how scientific "
    "knowledge changes in response to better methods and new evidence. Reproducible work is "
    "valuable because its claims can be checked rather than merely trusted."
)

_PII_BODY = (
    "Privacy filtering is another independently testable stage in a responsible corpus pipeline. "
    "This controlled fixture includes the synthetic contact address pipeline-test@example.invalid "
    "so the regular expression scanner has an unambiguous signal. The surrounding prose is "
    "deliberately well formed and long enough to pass ordinary document quality checks before "
    "privacy classification. In a production design, detected spans should be quarantined or "
    "redacted according to an explicit policy, and the unredacted source should remain access "
    "controlled. A student demonstration only needs to prove that the signal is detected, counted, "
    "and excluded from the clean training table. The address uses a reserved invalid domain and "
    "does not identify a real person or organization."
)

_HEURISTIC_BODY = (
    "A controlled structural failure should survive extraction and reach the curation rules. "
    "This document otherwise uses ordinary grammatical educational prose with enough words for "
    "the length and language checks. It explains that reproducible pipelines retain each raw "
    "signal before a final route is selected. The placeholder block lorem ipsum dolor sit amet, "
    "consectetur adipiscing elit; lorem ipsum dolor sit amet is intentional and should trigger "
    "the boilerplate rule without confusing valid braces in scientific math. "
    "No private information or accidental duplicate is included. The resulting "
    "decision must remain visible in the audit table with its classifier versions and exact "
    "rejection reason. This provides a stable heuristic-rejection branch for the local "
    "demonstration without relying on changing content from an external source."
)

FIXTURES = (
    LocalFixture("clean", "Clean scientific pipeline fixture.", _CLEAN_BODY),
    LocalFixture("duplicate-clean", "Clean scientific pipeline fixture.", _CLEAN_BODY),
    LocalFixture("heuristic-canary", "Heuristic quarantine fixture.", _HEURISTIC_BODY),
    LocalFixture("pii-canary", "Privacy quarantine fixture.", _PII_BODY),
)


def _controlled_figure_data_uri() -> str:
    """Generate a readable chart that exercises both OCR and figure routing."""
    from PIL import Image, ImageDraw  # type: ignore[import-not-found]

    image = Image.new("RGB", (640, 320), "white")
    draw = ImageDraw.Draw(image)
    draw.line((70, 255, 590, 255), fill="black", width=3)
    draw.line((70, 255, 70, 45), fill="black", width=3)
    draw.line((90, 225, 250, 175, 410, 120, 565, 70), fill="#2563eb", width=8)
    draw.text((210, 15), "CURATION QUALITY", fill="black")
    draw.text((260, 270), "PIPELINE STAGE", fill="black")
    draw.text((10, 135), "SCORE", fill="black")
    draw.text((80, 230), "RAW", fill="black")
    draw.text((510, 80), "GOLD", fill="black")
    output = io.BytesIO()
    image.save(output, format="PNG")
    encoded = base64.b64encode(output.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _html(fixture: LocalFixture) -> bytes:
    scientific_body = f"<h2 id='methods'>Methods.</h2><p>{fixture.body}</p>"
    metadata = ""
    evidence = ""
    if fixture.name in {"clean", "duplicate-clean"}:
        figure_data_uri = _controlled_figure_data_uri()
        metadata = (
            "<div class='ltx_creator ltx_role_author'>Ada Student Email: "
            "ada.student@example.invalid</div>"
        )
        scientific_body = (
            "<h6 class='ltx_title_abstract'>Abstract.</h6>"
            "<p>This controlled paper demonstrates a reproducible, section-aware data curation "
            "pipeline. It preserves scientific evidence while separating provenance metadata "
            "from the text that can enter a language-model training export.</p>"
            "<h2 id='methods'>2 Methods.</h2>"
            f"<p>{fixture.body}</p>"
        )
        evidence = (
            "<section id='results'><h2>Results.</h2>"
            "<p>The controlled result links written evidence to a figure, a structured table, "
            "and an equation. All three survive extraction with stable identifiers and can be "
            "inspected independently in the cockpit.</p>"
            f"<figure><img id='figure-local' src='{figure_data_uri}' "
            "alt='Controlled pipeline result image'/><figcaption>"
            "Figure 1: Controlled visual evidence for the local pipeline."
            "</figcaption></figure>"
            "<table id='table-local'><caption>Controlled measurements</caption>"
            "<tr><th>Stage.</th><th>Status.</th></tr>"
            "<tr><td>Extraction.</td><td>Ready.</td></tr></table>"
            "<math id='equation-local' display='block'><semantics><annotation "
            "encoding='application/x-tex'>q = 1</annotation></semantics></math>"
            "</section><h2>Limitations.</h2>"
            "<p>This fixture verifies mechanics rather than model accuracy on a labeled corpus. "
            "The real arXiv run is evaluated separately.</p>"
            "<h2>Acknowledgements.</h2><p>We thank the local test operator.</p>"
            "<h2>References.</h2><ol><li class='ltx_bibitem' id='bib-local'>"
            "Student, A. A provenance-only bibliography entry.</li></ol>"
        )
    return (
        "<!doctype html><html lang='en'><head>"
        f"<title>{fixture.title}</title></head><body><main><article><h1>"
        f"{fixture.title}</h1>{metadata}{scientific_body}{evidence}</article></main></body></html>"
    ).encode()


def main() -> None:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
    access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("MINIO_BRONZE_BUCKET", "bronze")
    brokers = os.environ.get("REDPANDA_BROKERS", "redpanda:29092")
    topic = os.environ.get("S2P_RAW_TOPIC", "raw.fetched")
    admissions_topic = os.environ.get("S2P_LICENSE_ADMISSIONS_TOPIC", "license.admissions")
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )
    producer = Producer({"bootstrap.servers": brokers})
    fetched_at = datetime.now(UTC)

    for fixture in FIXTURES:
        source_feed = fixture.source_feed
        url = f"https://local.stream2pretrain.invalid/v3/fixtures/{fixture.name}"
        admission = decide_license_admission(
            source_url=url,
            source_feed=source_feed,
            license_value="CC0-1.0",
            license_source="manual_override",
        )
        doc_id = admission.decision.doc_id
        producer.produce(
            admissions_topic,
            key=admission.decision.decision_id.encode("utf-8"),
            value=admission.decision.model_dump_json().encode("utf-8"),
        )
        if producer.flush(15):
            raise RuntimeError("the fixture licence admission was not delivered")
        key = (
            f"year={fetched_at:%Y}/month={fetched_at:%m}/day={fetched_at:%d}/"
            f"source={source_feed}/{doc_id.removeprefix('sha256:')}.html.gz"
        )
        payload = gzip.compress(_html(fixture))
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=payload,
            ContentType="text/html",
            ContentEncoding="gzip",
        )
        record = BronzeRecord(
            doc_id=doc_id,
            url=url,
            fetched_at=fetched_at,
            http_status=200,
            content_type="text/html",
            raw_html_s3_uri=f"s3://{bucket}/{key}",
            source_feed=source_feed,
            trace_id=admission.decision.trace_id,
            bytes_size=len(payload),
            source_format="html",
            extraction_pipeline="local-controlled-fixture-0.2",
            spdx_license="CC0-1.0",
            spdx_license_source="manual_override",
        )
        producer.produce(
            topic,
            key=doc_id.encode("utf-8"),
            value=record.model_dump_json().encode("utf-8"),
        )

    producer.flush(15)
    print(f"emitted {len(FIXTURES)} controlled Bronze fixtures to {topic}")


if __name__ == "__main__":
    main()
