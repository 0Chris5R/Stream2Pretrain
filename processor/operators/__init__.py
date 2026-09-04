"""Reusable Bytewax operators for FineWeb-style curation.

Each module here exposes a small, deterministic, side-effect-free callable
class so the unit tests can exercise it without spinning up a Bytewax
dataflow. The operators are composed in :mod:`processor.curate` into the
end-to-end curation pipeline.

Modules
-------
- :mod:`processor.operators.extract`      - Resiliparse HTML extraction
- :mod:`processor.operators.langid`       - fastlangid language ID
- :mod:`processor.operators.gopher`       - Gopher heuristic filters
- :mod:`processor.operators.c4`           - C4 nopunc / curly-brace / lorem-ipsum
- :mod:`processor.operators.kenlm_score`  - KenLM perplexity
- :mod:`processor.operators.minhash`      - Rensa MinHash signature
- :mod:`processor.operators.lshbloom`     - band-partitioned Bloom near-dup
- :mod:`processor.operators.quality`      - source-specific ModernBERT CPU classifier
- :mod:`processor.operators.pii`          - regex pack + optional Presidio
- :mod:`processor.operators.validity`     - validity-interval enricher
"""

from __future__ import annotations
