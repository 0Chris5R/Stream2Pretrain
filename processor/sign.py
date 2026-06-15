"""Decon-attestation signer (cosign keyless preferred, Ed25519 fallback).

Production setup expects a Sigstore Rekor / Fulcio identity bound by IRSA
or OIDC token federation; ``cosign sign-blob --identity-token ...``. When
the cluster does not have Sigstore wired up (typical for the Stream2Pretrain
prototype), the operator falls back to an in-cluster Ed25519 key mounted
from a Kubernetes Secret at ``$S2P_DECON_SIGNING_KEY``.

The verifier path mirrors this: hand it the canonical JSON bytes plus the
signature, and it returns True / False without needing to know which path
produced the signature - the algorithm is encoded in the
``DeconAttestation.signer_cert`` PEM block.
"""

from __future__ import annotations

import base64
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    load_pem_public_key,
)
from cryptography.x509 import (
    CertificateBuilder,
    Name,
    NameAttribute,
    random_serial_number,
)
from cryptography.x509.oid import NameOID

DEFAULT_KEY_PATH_ENV = "S2P_DECON_SIGNING_KEY"
DEFAULT_CERT_PATH_ENV = "S2P_DECON_SIGNING_CERT"
USE_COSIGN_ENV = "S2P_USE_COSIGN"
COSIGN_TIMEOUT_SECONDS = 5
COSIGN_FAIL_TTL_SECONDS = 60


@dataclass(frozen=True, slots=True)
class SignResult:
    """Output of :meth:`AttestationSigner.sign`."""

    signature_b64: str
    cert_pem: str
    backend: str


class AttestationSigner:
    """Sign canonical JSON for ``DeconAttestation`` records.

    Parameters
    ----------
    key_path
        Path to a PEM-encoded Ed25519 private key. If absent, an ephemeral
        key is generated. Ephemeral mode is suitable for tests only - the
        verifier needs the matching public key, which is embedded in the
        attestation's ``signer_cert`` field.
    cert_path
        Optional path to a pre-existing X.509 PEM certificate. When set,
        we use it verbatim (re-emitting it on every signature) so cosign
        verifiers can pin the signing identity.
    cosign_binary
        Path to the ``cosign`` executable. ``None`` disables the cosign
        path even if the binary is installed.
    """

    def __init__(
        self,
        *,
        key_path: str | os.PathLike[str] | None = None,
        cert_path: str | os.PathLike[str] | None = None,
        cosign_binary: str | None = None,
    ) -> None:
        env_key = os.environ.get(DEFAULT_KEY_PATH_ENV)
        env_cert = os.environ.get(DEFAULT_CERT_PATH_ENV)
        self._key_path = Path(key_path) if key_path else (Path(env_key) if env_key else None)
        self._cert_path = Path(cert_path) if cert_path else (Path(env_cert) if env_cert else None)
        # Cosign is opt-in. Default behaviour is the in-process Ed25519 path
        # because every Iceberg micro-batch commit calls sign() once and a
        # 30s subprocess timeout (cosign trying to reach an unreachable
        # Fulcio) would block the writer's lock and cascade into KEDA
        # mis-scaling. Set ``S2P_USE_COSIGN=1`` and pass a binary path to
        # opt back in once Sigstore is wired up.
        use_cosign = os.environ.get(USE_COSIGN_ENV, "").lower() in {"1", "true", "yes"}
        chosen = cosign_binary or ("cosign" if use_cosign else None)
        self._cosign_binary = chosen if chosen and shutil.which(chosen) else None
        self._cosign_disabled_until: float = 0.0
        self._private = self._load_or_create_key()
        self._cert_pem = self._load_or_build_cert()

    def _load_or_create_key(self) -> Ed25519PrivateKey:
        if self._key_path and self._key_path.is_file():
            data = self._key_path.read_bytes()
            key = load_pem_private_key(data, password=None)
            if not isinstance(key, Ed25519PrivateKey):
                raise TypeError(f"Expected Ed25519PrivateKey at {self._key_path}, got {type(key)}")
            return key
        return Ed25519PrivateKey.generate()

    def _load_or_build_cert(self) -> str:
        if self._cert_path and self._cert_path.is_file():
            return self._cert_path.read_text()
        # Build a self-signed X.509 wrapper around the public key so verifiers
        # can pin "this is the Stream2Pretrain Decon-Gate signing identity".
        from datetime import datetime, timedelta, timezone

        public_key = self._private.public_key()
        subject = issuer = Name(
            [
                NameAttribute(NameOID.ORGANIZATION_NAME, "Stream2Pretrain"),
                NameAttribute(NameOID.COMMON_NAME, "decon-gate"),
            ]
        )
        now = datetime.now(timezone.utc)
        cert = (
            CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(public_key)
            .serial_number(random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + timedelta(days=365))
            .sign(private_key=self._private, algorithm=None)
        )
        return cert.public_bytes(serialization.Encoding.PEM).decode("ascii")

    @property
    def cert_pem(self) -> str:
        """X.509 PEM certificate string embedded in every attestation."""
        return self._cert_pem

    def sign(self, payload: bytes) -> SignResult:
        """Sign ``payload`` (canonical JSON bytes) and return the result.

        Cosign failures are sticky for ``COSIGN_FAIL_TTL_SECONDS`` so a
        single bad attempt does not block subsequent flushes.
        """
        import time

        now = time.monotonic()
        if self._cosign_binary is not None and now >= self._cosign_disabled_until:
            try:
                return self._sign_cosign(payload)
            except Exception:
                self._cosign_disabled_until = now + COSIGN_FAIL_TTL_SECONDS
        return self._sign_ed25519(payload)

    def _sign_ed25519(self, payload: bytes) -> SignResult:
        sig = self._private.sign(payload)
        return SignResult(
            signature_b64=base64.b64encode(sig).decode("ascii"),
            cert_pem=self._cert_pem,
            backend="ed25519",
        )

    def _sign_cosign(self, payload: bytes) -> SignResult:
        """Sign using the system ``cosign`` binary in keyless mode if possible."""
        with tempfile.TemporaryDirectory(prefix="s2p-sign-") as tmpdir:
            blob_path = Path(tmpdir) / "blob"
            blob_path.write_bytes(payload)
            sig_path = Path(tmpdir) / "blob.sig"
            cert_path = Path(tmpdir) / "blob.crt"
            args = [
                self._cosign_binary or "cosign",
                "sign-blob",
                "--yes",
                "--output-signature",
                str(sig_path),
                "--output-certificate",
                str(cert_path),
                str(blob_path),
            ]
            subprocess.run(args, check=True, timeout=COSIGN_TIMEOUT_SECONDS, capture_output=True)
            return SignResult(
                signature_b64=sig_path.read_text().strip(),
                cert_pem=cert_path.read_text(),
                backend="cosign",
            )


def verify_signature(payload: bytes, signature_b64: str, cert_pem: str) -> bool:
    """Verify a signature using the certificate's embedded public key."""
    try:
        from cryptography import x509

        cert = x509.load_pem_x509_certificate(cert_pem.encode("ascii"))
        public_key = cert.public_key()
        if not isinstance(public_key, Ed25519PublicKey):
            # Try to load the PEM as a raw public key if the cert is missing.
            try:
                public_key = load_pem_public_key(cert_pem.encode("ascii"))
            except Exception:
                return False
        signature = base64.b64decode(signature_b64)
        public_key.verify(signature, payload)  # type: ignore[union-attr]
        return True
    except (InvalidSignature, ValueError, TypeError):
        return False
    except Exception:
        return False
