"""Stream2Pretrain processor - Bytewax dataflows + curation operators.

This package contains the streaming processors that turn raw fetched
documents into durable accepted/rejected decisions on ``curation.decisions``
and a clean, mixture-ready subset on ``docs.curated`` and Iceberg Gold.

Modules
-------
- :mod:`processor.fetcher`         - HTML extraction + lang ID + validity
- :mod:`processor.curate`          - main curation dataflow
- :mod:`processor.iceberg_writer`  - micro-batch Iceberg sink
- :mod:`processor.sign`            - Ed25519 artifact signer
- :mod:`processor.tokenize`        - GPT-2 / sentencepiece token-count helper
- :mod:`processor.operators`       - reusable Bytewax operators
- :mod:`processor.mixture_controller` - kopf operator for MixtureRecipe CRDs

The shared Kafka / OTel / settings glue lives in :mod:`processor.common`.
"""

from __future__ import annotations

__version__ = "0.1.0"
