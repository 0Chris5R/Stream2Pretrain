"""Mixture controller package.

A kopf-based Kubernetes operator that reconciles ``MixtureRecipe`` CRDs.
On create or update, the controller spawns two Bytewax pipelines - branch
A and branch B - reading the same ``SourceFeed`` set with different
mixture weights. A small proxy LM continuously trains on each branch on
rolling windows; the per-domain perplexity delta drives an auto-promotion
decision.

Modules
-------
- :mod:`processor.mixture_controller.controller`  - kopf reconciler
- :mod:`processor.mixture_controller.proxy_lm`    - tiny proxy LM
- :mod:`processor.mixture_controller.metrics`     - prometheus exporter
"""

from __future__ import annotations
