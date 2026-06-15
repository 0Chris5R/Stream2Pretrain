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

try:
    from prometheus_client import CollectorRegistry, Counter, Gauge
except Exception:  # pragma: no cover - optional dep at unit-test time
    CollectorRegistry = object  # type: ignore[assignment, misc]

    class _StubMetric:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.value = 0.0

        def labels(self, *args: object, **kwargs: object) -> "_StubMetric":
            return self

        def set(self, value: float) -> None:
            self.value = value

        def inc(self, amount: float = 1.0) -> None:
            self.value += amount

    Counter = _StubMetric  # type: ignore[assignment, misc]
    Gauge = _StubMetric  # type: ignore[assignment, misc]


PromotionDecision = Literal["promote", "hold", "rollback"]


@dataclass
class MixtureMetrics:
    """Bundle of Prometheus instruments scoped to one controller process."""

    registry: object = field(default_factory=lambda: CollectorRegistry())
    branch_perplexity: object = field(init=False)
    perplexity_delta: object = field(init=False)
    promotion_total: object = field(init=False)
    branch_tokens_total: object = field(init=False)

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
        self.branch_perplexity.labels(recipe=recipe, branch=branch).set(perplexity)  # type: ignore[union-attr]
        self.branch_tokens_total.labels(recipe=recipe, branch=branch).inc(tokens)  # type: ignore[union-attr]

    def observe_delta(self, recipe: str, delta: float) -> None:
        self.perplexity_delta.labels(recipe=recipe).set(delta)  # type: ignore[union-attr]

    def record_decision(self, recipe: str, decision: PromotionDecision) -> None:
        self.promotion_total.labels(recipe=recipe, decision=decision).inc()  # type: ignore[union-attr]
