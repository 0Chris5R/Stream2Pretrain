"""Preserve exact structured evidence across transient-object expiry.

Normal records carry compressed extracted JSON through Kafka until admission.
Accepted evidence goes to Gold; original PDFs and figure binaries remain on
the transient 24-hour lifecycle. No evidence is reconstructed from excerpts.
"""

from __future__ import annotations

import gzip
import hashlib
from typing import Any
from urllib.parse import urlsplit

from botocore.exceptions import ClientError

from schemas.scientific import ScientificDocument


def evidence_capsule(document: ScientificDocument) -> bytes:
    return gzip.compress(document.model_dump_json().encode(), mtime=0)


class ScientificEvidenceUnavailableError(ValueError):
    """Permanent missing evidence; never publish an executable candidate."""


class ScientificHandoff:
    def __init__(self, s3: Any, bucket: str) -> None:
        self.s3 = s3
        self.bucket = bucket

    def preserve(self, doc_id: str, capsule: bytes | None, uri: str | None) -> str | None:
        if capsule is not None:
            payload = gzip.decompress(capsule)
        elif uri:
            parsed = urlsplit(uri)
            if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
                raise ScientificEvidenceUnavailableError("invalid structured evidence URI")
            try:
                body = self.s3.get_object(Bucket=parsed.netloc, Key=parsed.path.lstrip("/"))["Body"]
                try:
                    payload = body.read()
                finally:
                    body.close()
            except ClientError as exc:
                if str(exc.response.get("Error", {}).get("Code")) in {"NoSuchKey", "404"}:
                    raise ScientificEvidenceUnavailableError(
                        "structured evidence object is missing"
                    ) from exc
                raise
        else:
            return None
        scientific = ScientificDocument.model_validate_json(payload)
        if scientific.doc_id != doc_id:
            raise ValueError("scientific handoff doc_id mismatch")
        if capsule is None and uri and urlsplit(uri).netloc == self.bucket:
            return uri
        digest = hashlib.sha256(payload).hexdigest()
        key = f"scientific-evidence/{doc_id.removeprefix('sha256:')}/{digest}.json"
        self.s3.put_object(
            Bucket=self.bucket, Key=key, Body=payload, ContentType="application/json"
        )
        return f"s3://{self.bucket}/{key}"
