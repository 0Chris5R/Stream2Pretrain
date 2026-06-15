"""Tests for :mod:`processor.sign`."""

from __future__ import annotations

from processor.sign import AttestationSigner, verify_signature


def test_sign_and_verify_roundtrip() -> None:
    signer = AttestationSigner(cosign_binary=None)
    payload = b'{"snapshot_id":42}'
    res = signer.sign(payload)
    assert res.backend == "ed25519"
    assert verify_signature(payload, res.signature_b64, res.cert_pem)


def test_verify_rejects_tampered_payload() -> None:
    signer = AttestationSigner(cosign_binary=None)
    res = signer.sign(b"original")
    assert not verify_signature(b"tampered", res.signature_b64, res.cert_pem)


def test_cert_is_self_signed_pem() -> None:
    signer = AttestationSigner(cosign_binary=None)
    pem = signer.cert_pem
    assert pem.startswith("-----BEGIN CERTIFICATE-----")
    assert pem.rstrip().endswith("-----END CERTIFICATE-----")
