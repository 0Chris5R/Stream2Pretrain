"""Decontamination attestation: signed certificate of contamination scan.

One :class:`DeconAttestation` is emitted per Iceberg snapshot of the gold
table. The attestation is canonicalised (sorted JSON, no whitespace) before
signing so verifiers can reproduce the exact bytes. Signatures are produced by
``processor/sign.py`` using either an in-cluster Ed25519 key (prototype) or
Sigstore Rekor (future).
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, RootModel

BenchmarkName = Literal["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"]


class BenchmarkHit(BaseModel):
    """A single benchmark's hit count within the snapshot."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    benchmark: BenchmarkName
    hits: int = Field(..., ge=0)


class PerBenchmarkHits(RootModel[dict[BenchmarkName, int]]):
    """Compact map form used in the wire JSON."""

    model_config = ConfigDict(frozen=True)


class DeconAttestation(BaseModel):
    """Snapshot-bound, signed contamination certificate."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    snapshot_id: int = Field(..., ge=0)
    committed_at: datetime
    benchmark_set_version: str = Field(
        ...,
        description="Version pin of the benchmark corpus, e.g. 'v2026-06-01'.",
    )
    benchmarks: list[BenchmarkName]
    tokens_scanned: int = Field(..., ge=0)
    tokens_flagged: int = Field(..., ge=0)
    rejected_doc_hashes: list[str] = Field(
        default_factory=list,
        description="Sha256 doc_ids whose 13-gram or embedding sketch matched.",
    )
    per_benchmark_hits: dict[BenchmarkName, int] = Field(
        ...,
        description="Hit counts per benchmark (zero entries included).",
    )

    # Signature payload. Both fields populated post-sign.
    signature: str = Field(
        ...,
        description="Base64-encoded Ed25519 (or cosign) signature over canonical JSON.",
    )
    signer_cert: str = Field(
        ...,
        description="X.509 PEM certificate bound to the signing key.",
    )
