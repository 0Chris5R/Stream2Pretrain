"""SourceFeed and MixtureRecipe CRD specs.

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
    "sitemap",
    "manual",
]

LicenseDefault = Literal[
    "per-record",
    "unknown",
    "CC-BY-4.0",
    "CC-BY-SA-4.0",
    "CC-BY-3.0",
    "CC-BY-SA-3.0",
    "CC0-1.0",
    "Apache-2.0",
    "MIT",
    "BSD-2-Clause",
    "BSD-3-Clause",
    "MPL-2.0",
    "ISC",
    "Unlicense",
]


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
    license_default: LicenseDefault = "unknown"
    enabled: bool = True

    # Optional egress allow-list - hostnames the per-feed NetworkPolicy permits.
    egress_allow: list[str] = Field(
        default_factory=list,
        description="Hostnames the SourceFeed pod may dial (NetworkPolicy egress).",
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


class MixtureSourceWeight(BaseModel):
    """A single source's weight within a mixture."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True, alias_generator=_to_lower_camel
    )

    source_feed: str = Field(..., min_length=1, max_length=63)
    weight: float = Field(..., gt=0.0, le=1.0)


class MixtureRecipeSpec(BaseModel):
    """Spec of a MixtureRecipe CRD instance.

    Two MixtureRecipe instances pointing at the same SourceFeed set form the
    shadow A/B comparison: each materializes a separate Iceberg branch and a
    proxy LM is trained on each branch in rolling windows.
    """

    model_config = ConfigDict(
        extra="forbid", frozen=True, populate_by_name=True, alias_generator=_to_lower_camel
    )

    name: str = Field(..., min_length=1, max_length=63)
    branch: str = Field(
        ...,
        min_length=1,
        max_length=63,
        description="Iceberg branch name this recipe writes to.",
    )
    sources: list[MixtureSourceWeight] = Field(..., min_length=1)
    min_quality_score: float = Field(default=2.0, ge=0.0, le=5.0)
    min_edu_score: float = Field(default=2.0, ge=0.0, le=5.0)
    max_risk_tier: Literal[1, 2, 3] = 2
    languages: list[str] = Field(
        default_factory=lambda: ["en"],
        description="Language allow-list (ISO codes).",
    )
    target_tokens_per_hour: int | None = Field(
        default=None,
        ge=0,
        description="Optional throttle target; None means uncapped.",
    )

    @model_validator(mode="after")
    def _check_weights_sum(self) -> MixtureRecipeSpec:
        total = sum(s.weight for s in self.sources)
        # Allow small floating point slack but reject obvious misconfigurations.
        if not (0.999 <= total <= 1.001):
            raise ValueError(f"source weights must sum to 1.0 (got {total:.6f})")
        return self
