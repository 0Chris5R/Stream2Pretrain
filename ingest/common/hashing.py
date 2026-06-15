"""Content hashing + URL canonicalization helpers.

The ``doc_id`` is the sha256 of the canonical URL prefixed with ``sha256:``.
Canonicalization rules (kept intentionally narrow so they are deterministic):

- lower-case scheme and host
- drop default ports (80/443)
- strip ``utm_*`` and a small allow-list of analytics query params
- preserve path case (some sites are case-sensitive)
- sort remaining query params lexicographically
- drop the URL fragment

This matches the dedup expectations in section 7 of RESEARCH.md: ``doc_id``
collisions across pollers must short-circuit before the bronze write.
"""

from __future__ import annotations

import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

# Tracking parameters dropped during canonicalization.
_TRACKING_PARAMS = re.compile(
    r"^(utm_.*|gclid|fbclid|mc_eid|mc_cid|igshid|ref|ref_src|ref_url)$",
    re.IGNORECASE,
)


def canonical_url(url: str) -> str:
    """Return a canonical form of ``url`` suitable for hashing.

    Deterministic. Idempotent. Never raises on a syntactically valid http(s) URL.
    """
    if not url:
        raise ValueError("url must be non-empty")
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    if scheme not in {"http", "https"}:
        raise ValueError(f"unsupported scheme: {scheme!r}")
    host = parts.hostname or ""
    host = host.lower()
    port = parts.port
    if port is not None and not (
        (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    ):
        netloc = f"{host}:{port}"
    else:
        netloc = host
    # Drop tracking params; sort the rest.
    kept = [
        (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=False)
        if not _TRACKING_PARAMS.match(k)
    ]
    kept.sort()
    query = urlencode(kept, doseq=True)
    path = parts.path or "/"
    return urlunsplit((scheme, netloc, path, query, ""))


def doc_id_for_url(url: str) -> str:
    """Compute the canonical ``sha256:<hex>`` doc_id for ``url``."""
    canon = canonical_url(url)
    digest = hashlib.sha256(canon.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


def content_sha256(payload: bytes) -> str:
    """Return ``sha256:<hex>`` of an arbitrary bytestring."""
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"
