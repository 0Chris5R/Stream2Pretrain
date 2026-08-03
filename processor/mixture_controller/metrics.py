"""Prometheus metrics surfaced by the mixture controller.

Per RESEARCH.md section 4 the controller exposes:

- ``s2p_mixture_branch_perplexity{branch=...}``
- ``s2p_mixture_perplexity_delta{recipe=...}``
- ``s2p_mixture_promotion_total{recipe=...,decision=...}``
- ``s2p_mixture_branch_tokens_total{branch=...}``

The metrics are constructed from the ``prometheus_client`` library and
served by an HTTP endpoint started by the controller's ``main`` entrypoint.
The classes here are deliberately thin wrappers so unit tests can build a
fresh registry per test and assert on counter / gauge values without
coupling to a global state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

from prometheus_client import CollectorRegistry, Counter, Gauge

PromotionDecision = Literal["promote", "hold", "rollback"]


@dataclass
class MixtureMetrics:
    """Bundle of Prometheus instruments scoped to one controller process."""

    registry: CollectorRegistry = field(default_factory=CollectorRegistry)
    branch_perplexity: Gauge = field(init=False)
    perplexity_delta: Gauge = field(init=False)
    promotion_total: Counter = field(init=False)
    branch_tokens_total: Counter = field(init=False)

    def __post_init__(self) -> None:
        self.branch_perplexity = Gauge(
            "s2p_mixture_branch_perplexity",
            "Per-branch proxy-LM perplexity (lower is better).",
            ["recipe", "branch"],
            registry=self.registry,
        )
        self.perplexity_delta = Gauge(
            "s2p_mixture_perplexity_delta",
            "branch_a - branch_b proxy-LM perplexity, smoothed over the window.",
            ["recipe"],
            registry=self.registry,
        )
        self.promotion_total = Counter(
            "s2p_mixture_promotion_total",
            "Total auto-promotion decisions emitted by the mixture controller.",
            ["recipe", "decision"],
            registry=self.registry,
        )
        self.branch_tokens_total = Counter(
            "s2p_mixture_branch_tokens_total",
            "Total tokens trained into each branch's proxy LM.",
            ["recipe", "branch"],
            registry=self.registry,
        )

    def observe_branch(self, recipe: str, branch: str, perplexity: float, tokens: int) -> None:
        """Record one training-window observation for a branch."""
        self.branch_perplexity.labels(recipe=recipe, branch=branch).set(perplexity)
        self.branch_tokens_total.labels(recipe=recipe, branch=branch).inc(tokens)

    def observe_delta(self, recipe: str, delta: float) -> None:
        self.perplexity_delta.labels(recipe=recipe).set(delta)

    def record_decision(self, recipe: str, decision: PromotionDecision) -> None:
        self.promotion_total.labels(recipe=recipe, decision=decision).inc()
