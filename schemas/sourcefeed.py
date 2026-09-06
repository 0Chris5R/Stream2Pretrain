"""SourceFeed CRD specification.

These Pydantic models mirror the ``spec`` block of the K8s CRDs declared in
``charts/stream2pretrain/crds/``. Keeping them here (instead of inside the
ingest component) means the FastAPI submit API and the OPA Gatekeeper policy
generator can validate manifests without taking a dependency on Bytewax.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


def _to_lower_camel(name: str) -> str:
    head, *tail = name.split("_")
    return head + "".join(part.capitalize() for part in tail)


FeedProtocol = Literal[
    "rss",
    "atom",
    "oai-pmh",
    "rest-json",
    "manual",
]

LicenseDefault = Literal["per-record"]


class RateLimitSpec(BaseModel):
    """Politeness limits for a single SourceFeed."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True, alias_generator=_to_lower_camel
    )

    requests_per_second: float = Field(..., gt=0.0)
    burst: int = Field(..., gt=0)
    respect_x_poll_interval: bool = Field(
        default=False,
        description="If true, the poller defers to the response's X-Poll-Interval header.",
    )


class AuthSpec(BaseModel):
    """Reference to a Secret holding the auth token."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True, alias_generator=_to_lower_camel
    )

    type: Literal["none", "bearer", "header", "basic"] = "none"
    secret_name: str | None = None
    secret_key: str | None = None
    header_name: str | None = None


class SourceFeedSpec(BaseModel):
    """Spec of a single SourceFeed CRD instance."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True, alias_generator=_to_lower_camel
    )

    name: str = Field(..., min_length=1, max_length=63)
    protocol: FeedProtocol
    endpoint: HttpUrl
    poll_interval_seconds: int = Field(..., gt=0, le=86400)
    rate_limit: RateLimitSpec
    auth: AuthSpec = Field(default_factory=AuthSpec)
    license_default: LicenseDefault = "per-record"
    enabled: bool = True

    # Advisory hostname inventory for CNI/FQDN policy generation and audit.
    # Kubernetes NetworkPolicy cannot itself select destinations by DNS name.
    egress_allow: list[str] = Field(
        default_factory=list,
        description=(
            "Expected destination hostnames for policy generation and audit. "
            "Enforcement requires an FQDN-aware CNI; plain Kubernetes "
            "NetworkPolicy cannot match DNS names."
        ),
    )

    # Optional content-type filter for REST endpoints whose responses we need
    # to gate (e.g. only accept text/html for HTML extractors).
    accept_content_types: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_auth_consistency(self) -> SourceFeedSpec:
        if self.auth.type == "none":
            return self
        if not self.auth.secret_name or not self.auth.secret_key:
            raise ValueError(
                "auth.secret_name and auth.secret_key are required when auth.type != 'none'"
            )
        if self.auth.type == "header" and not self.auth.header_name:
            raise ValueError("auth.header_name is required when auth.type == 'header'")
        return self
