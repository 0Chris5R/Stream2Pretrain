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
- :mod:`schemas.topics` - topic name + partition / replication constants.
"""

from __future__ import annotations

from schemas.bronze import (
    BronzeRecord,
    SourceFormat,
    SpdxLicenseSource,
)
from schemas.decon import BenchmarkHit, DeconAttestation
from schemas.foundry import (
    AnswerManifest,
    ArtifactAuditRecord,
    EnvironmentManifest,
    FoundryAnswer,
    FoundryArtifactRecord,
    FoundryEvent,
    OracleRecipe,
    OracleResult,
    PaperBundle,
    PaperEvidenceGraph,
    ProviderTrace,
    QuotaState,
    TaskSpec,
    Trajectory,
    TrajectoryTurn,
    ValidationReport,
    VerifierSpec,
)
from schemas.gold import CorpusRoute, GoldRecord, SegmentScore
from schemas.license_admission import LicenseAdmissionDecision
from schemas.scientific import (
    ScientificCitation,
    ScientificDocument,
    ScientificEquation,
    ScientificFigure,
    ScientificParagraph,
    ScientificSection,
    ScientificTable,
    SectionRole,
)
from schemas.silver import SilverRecord, SilverSegment, SilverTags
from schemas.sourcefeed import (
    MixtureRecipeSpec,
    MixtureSourceWeight,
    SourceFeedSpec,
)
from schemas.topics import (
    CURATION_DECISIONS,
    DECON_ATTEST,
    DOCS_CURATED,
    DOCS_NORMALIZED,
    FOUNDRY_ARTIFACTS,
    FOUNDRY_EVENTS,
    FOUNDRY_JOBS,
    LICENSE_ADMISSIONS,
    RAW_FETCHED,
    RAW_SMOKE,
    TopicConfig,
    dev_topic_configs,
    prod_topic_configs,
)

__all__ = [
    "CURATION_DECISIONS",
    "DECON_ATTEST",
    "DOCS_CURATED",
    "DOCS_NORMALIZED",
    "FOUNDRY_ARTIFACTS",
    "FOUNDRY_EVENTS",
    "FOUNDRY_JOBS",
    "LICENSE_ADMISSIONS",
    "RAW_FETCHED",
    "RAW_SMOKE",
    "AnswerManifest",
    "ArtifactAuditRecord",
    "BenchmarkHit",
    "BronzeRecord",
    "CorpusRoute",
    "DeconAttestation",
    "EnvironmentManifest",
    "FoundryAnswer",
    "FoundryArtifactRecord",
    "FoundryEvent",
    "GoldRecord",
    "LicenseAdmissionDecision",
    "MixtureRecipeSpec",
    "MixtureSourceWeight",
    "OracleRecipe",
    "OracleResult",
    "PaperBundle",
    "PaperEvidenceGraph",
    "ProviderTrace",
    "QuotaState",
    "ScientificCitation",
    "ScientificDocument",
    "ScientificEquation",
    "ScientificFigure",
    "ScientificParagraph",
    "ScientificSection",
    "ScientificTable",
    "SectionRole",
    "SegmentScore",
    "SilverRecord",
    "SilverSegment",
    "SilverTags",
    "SourceFeedSpec",
    "SourceFormat",
    "SpdxLicenseSource",
    "TaskSpec",
    "TopicConfig",
    "Trajectory",
    "TrajectoryTurn",
    "ValidationReport",
    "VerifierSpec",
    "dev_topic_configs",
    "prod_topic_configs",
]
