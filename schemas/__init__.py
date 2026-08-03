"""Stream2Pretrain shared schemas.

Every component (ingest, processor, UI BFF, decon-gate sidecar) imports its
record types from this package. Schemas are Pydantic v2 models so they double
as runtime validators and as JSON-Schema sources for cross-language consumers
(the Next.js UI reads `schemas/json_schema/*.json` to type its TanStack Query
hooks).

Public API
----------
- :class:`schemas.bronze.BronzeRecord`
- :class:`schemas.silver.SilverRecord`
- :class:`schemas.gold.GoldRecord`
- :class:`schemas.code.CodeFileRecord`
- :class:`schemas.decon.DeconAttestation`
- :class:`schemas.sourcefeed.SourceFeedSpec`
- :mod:`schemas.topics` - topic name + partition / replication constants.
"""

from __future__ import annotations

from schemas.bronze import (
    BronzeRecord,
    SourceFormat,
    SpdxLicenseSource,
)
from schemas.code import CodeFileRecord
from schemas.decon import BenchmarkHit, DeconAttestation
from schemas.gold import GoldRecord
from schemas.silver import SilverRecord, SilverTags
from schemas.sourcefeed import (
    MixtureRecipeSpec,
    MixtureSourceWeight,
    SourceFeedSpec,
)
from schemas.topics import (
    CODE_SOURCE_FORMAT,
    DECON_ATTEST,
    DOCS_CURATED,
    DOCS_NORMALIZED,
    RAW_FETCHED,
    TopicConfig,
    dev_topic_configs,
    prod_topic_configs,
)

__all__ = [
    "CODE_SOURCE_FORMAT",
    "DECON_ATTEST",
    "DOCS_CURATED",
    "DOCS_NORMALIZED",
    "RAW_FETCHED",
    "BenchmarkHit",
    "BronzeRecord",
    "CodeFileRecord",
    "DeconAttestation",
    "GoldRecord",
    "MixtureRecipeSpec",
    "MixtureSourceWeight",
    "SilverRecord",
    "SilverTags",
    "SourceFeedSpec",
    "SourceFormat",
    "SpdxLicenseSource",
    "TopicConfig",
    "dev_topic_configs",
    "prod_topic_configs",
]
