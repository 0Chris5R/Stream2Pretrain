"""Ed25519 signer used for immutable generated training artifacts."""

from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from cryptography import x509
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.x509 import CertificateBuilder, Name, NameAttribute, random_serial_number
from cryptography.x509.oid import NameOID


@dataclass(frozen=True, slots=True)
class SignResult:
    signature_b64: str
    cert_pem: str
    backend: str


class AttestationSigner:
    """Sign artifact bytes with a configured or ephemeral Ed25519 key."""

    def __init__(
        self,
        *,
        key_path: str | os.PathLike[str] | None = None,
        cert_path: str | os.PathLike[str] | None = None,
        cosign_binary: str | None = None,
    ) -> None:
        del cosign_binary
        env_key = os.environ.get("S2P_FOUNDRY_SIGNING_KEY")
        env_cert = os.environ.get("S2P_FOUNDRY_SIGNING_CERT")
        self._key_path = Path(key_path) if key_path else (Path(env_key) if env_key else None)
        self._cert_path = Path(cert_path) if cert_path else (Path(env_cert) if env_cert else None)
        self._private = self._load_or_create_key()
        self._cert_pem = self._load_or_build_cert()

    def _load_or_create_key(self) -> Ed25519PrivateKey:
        if self._key_path and self._key_path.is_file():
            data = self._key_path.read_bytes()
            if len(data) == 32:
                return Ed25519PrivateKey.from_private_bytes(data)
            try:
                decoded = base64.b64decode(data.strip(), validate=True)
            except ValueError:
                decoded = b""
            if len(decoded) == 32:
                return Ed25519PrivateKey.from_private_bytes(decoded)
            key = load_pem_private_key(data, password=None)
            if not isinstance(key, Ed25519PrivateKey):
                raise TypeError(f"expected an Ed25519 key at {self._key_path}")
            return key
        return Ed25519PrivateKey.generate()

    def _load_or_build_cert(self) -> str:
        if self._cert_path and self._cert_path.is_file():
            return self._cert_path.read_text(encoding="utf-8")
        name = Name(
            [
                NameAttribute(NameOID.ORGANIZATION_NAME, "Stream2Pretrain"),
                NameAttribute(NameOID.COMMON_NAME, "post-training-foundry"),
            ]
        )
        now = datetime.now(UTC)
        certificate = (
            CertificateBuilder()
            .subject_name(name)
            .issuer_name(name)
            .public_key(self._private.public_key())
            .serial_number(random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=365))
            .sign(private_key=self._private, algorithm=None)
        )
        return certificate.public_bytes(serialization.Encoding.PEM).decode("ascii")

    @property
    def cert_pem(self) -> str:
        return self._cert_pem

    def sign(self, payload: bytes) -> SignResult:
        signature = self._private.sign(payload)
        return SignResult(
            signature_b64=base64.b64encode(signature).decode("ascii"),
            cert_pem=self._cert_pem,
            backend="ed25519",
        )


def verify_signature(payload: bytes, signature_b64: str, cert_pem: str) -> bool:
    """Verify artifact bytes against the public key in an X.509 certificate."""
    try:
        certificate = x509.load_pem_x509_certificate(cert_pem.encode("ascii"))
        public_key = certificate.public_key()
        if not isinstance(public_key, Ed25519PublicKey):
            return False
        public_key.verify(base64.b64decode(signature_b64), payload)
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False
