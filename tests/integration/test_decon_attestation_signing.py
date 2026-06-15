"""End-to-end test of the Decon-Gate attestation signing path.

This test does not require the streaming topology to be running: it exercises
the same canonicalisation + Ed25519 signing primitive that ``processor/sign.py``
uses in production, plus the verifier that the UI's attestation viewer relies
on. The intent is to catch regressions where the canonical-JSON contract
between writer and verifier silently diverges.

If ``processor.sign`` is not importable (for example in a tests-only sandbox
that did not install the processor extras), the suite falls back to verifying
the canonicalisation rule directly via stdlib + ``cryptography``.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone

import pytest

from schemas.decon import DeconAttestation


def _canonical_json(payload: dict[str, object]) -> bytes:
    """Canonicalisation rule: sorted keys, no whitespace, UTF-8."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _strip_signature_fields(payload: dict[str, object]) -> dict[str, object]:
    return {k: v for k, v in payload.items() if k not in {"signature", "signer_cert"}}


def _make_unsigned_attestation_payload() -> dict[str, object]:
    """Build a representative attestation body without signature fields."""
    return {
        "snapshot_id": 84219315,
        "committed_at": "2026-06-15T10:30:00+00:00",
        "benchmark_set_version": "v2026-06-01",
        "benchmarks": ["MMLU", "GSM8K", "HumanEval", "MATH", "GPQA"],
        "tokens_scanned": 4_823_910,
        "tokens_flagged": 217,
        "rejected_doc_hashes": [
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
        ],
        "per_benchmark_hits": {
            "MMLU": 41,
            "GSM8K": 12,
            "HumanEval": 0,
            "MATH": 164,
            "GPQA": 0,
        },
    }


def test_canonical_json_is_stable_across_key_orders() -> None:
    """Canonical JSON output must not depend on input dict key order."""
    a = _make_unsigned_attestation_payload()
    b = {k: a[k] for k in reversed(list(a.keys()))}
    assert _canonical_json(a) == _canonical_json(b)


def test_attestation_signature_roundtrip() -> None:
    """Sign with Ed25519, verify, and confirm tampering breaks verification."""
    cryptography = pytest.importorskip(
        "cryptography", reason="cryptography not installed"
    )
    from cryptography.exceptions import InvalidSignature
    from cryptography.hazmat.primitives.asymmetric import ed25519

    body = _make_unsigned_attestation_payload()
    canonical = _canonical_json(body)

    sk = ed25519.Ed25519PrivateKey.generate()
    pk = sk.public_key()
    signature = sk.sign(canonical)

    # Verifier replays the canonicalisation rule.
    pk.verify(signature, canonical)

    # Tamper: bump tokens_flagged - signature must fail.
    tampered = dict(body)
    tampered["tokens_flagged"] = body["tokens_flagged"] + 1  # type: ignore[operator]
    with pytest.raises(InvalidSignature):
        pk.verify(signature, _canonical_json(tampered))

    # The DeconAttestation model itself accepts the resulting blob.
    attestation = DeconAttestation(
        snapshot_id=body["snapshot_id"],  # type: ignore[arg-type]
        committed_at=datetime.fromisoformat(str(body["committed_at"])),
        benchmark_set_version=str(body["benchmark_set_version"]),
        benchmarks=list(body["benchmarks"]),  # type: ignore[arg-type]
        tokens_scanned=int(body["tokens_scanned"]),  # type: ignore[arg-type]
        tokens_flagged=int(body["tokens_flagged"]),  # type: ignore[arg-type]
        rejected_doc_hashes=list(body["rejected_doc_hashes"]),  # type: ignore[arg-type]
        per_benchmark_hits=dict(body["per_benchmark_hits"]),  # type: ignore[arg-type]
        signature=base64.b64encode(signature).decode("ascii"),
        signer_cert="-----BEGIN CERTIFICATE-----\nMIIBIjANBgkqhkiG9w0BAQEFA\n-----END CERTIFICATE-----\n",
    )
    assert attestation.snapshot_id == body["snapshot_id"]


def test_signature_fields_are_excluded_from_canonicalisation() -> None:
    """The signed bytes must not include the signature fields themselves."""
    body = _make_unsigned_attestation_payload()
    body_with_sig = {**body, "signature": "ZmFrZQ==", "signer_cert": "PEMBLOB"}
    assert _canonical_json(body) == _canonical_json(_strip_signature_fields(body_with_sig))


def test_processor_sign_module_if_available() -> None:
    """If the processor's signer is installed, exercise the public surface.

    We do not assert any specific signature value (the key may be ephemeral);
    we only check that ``sign(attestation)`` returns base64 bytes that the
    same module's ``verify`` accepts.
    """
    sign_mod = pytest.importorskip(
        "processor.sign",
        reason="processor.sign not importable in this environment",
    )
    body = _make_unsigned_attestation_payload()

    sign_fn = getattr(sign_mod, "sign_attestation", None)
    verify_fn = getattr(sign_mod, "verify_attestation", None)
    if sign_fn is None or verify_fn is None:
        pytest.skip("processor.sign is missing the expected public functions")

    signed = sign_fn(body)
    # ``signed`` is expected to be a dict with signature / signer_cert added.
    assert "signature" in signed and "signer_cert" in signed
    assert verify_fn(signed) is True
