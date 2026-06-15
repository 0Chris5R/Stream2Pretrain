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
- :class:`schemas.decon.DeconAttestation`
- :class:`schemas.sourcefeed.SourceFeedSpec`
- :class:`schemas.sourcefeed.MixtureRecipeSpec`
- :mod:`schemas.topics` - topic name + partition / replication constants.
"""

from __future__ import annotations

from schemas.bronze import BronzeRecord
from schemas.decon import BenchmarkHit, DeconAttestation
from schemas.gold import GoldRecord
from schemas.silver import SilverRecord, SilverTags
from schemas.sourcefeed import (
    MixtureRecipeSpec,
    MixtureSourceWeight,
    SourceFeedSpec,
)
from schemas.topics import (
    DECON_ATTEST,
    DOCS_CURATED,
    DOCS_NORMALIZED,
    RAW_FETCHED,
    TopicConfig,
    dev_topic_configs,
    prod_topic_configs,
)

__all__ = [
    "BronzeRecord",
    "SilverRecord",
    "SilverTags",
    "GoldRecord",
    "DeconAttestation",
    "BenchmarkHit",
    "SourceFeedSpec",
    "MixtureRecipeSpec",
    "MixtureSourceWeight",
    "RAW_FETCHED",
    "DOCS_NORMALIZED",
    "DOCS_CURATED",
    "DECON_ATTEST",
    "TopicConfig",
    "dev_topic_configs",
    "prod_topic_configs",
]
