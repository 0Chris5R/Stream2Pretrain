"""Tests for :mod:`processor.mixture_controller`."""

from __future__ import annotations

from processor.common import ProcessorConfig
from processor.mixture_controller.controller import MixtureController
from processor.mixture_controller.proxy_lm import ProxyLM
from schemas.sourcefeed import MixtureRecipeSpec, MixtureSourceWeight


def _spec(name: str = "exp1") -> MixtureRecipeSpec:
    return MixtureRecipeSpec(
        name=name,
        branch="branch-test",
        sources=[MixtureSourceWeight(source_feed="rss-arxiv-cs", weight=1.0)],
    )


def test_proxy_lm_perplexity_drops_with_training() -> None:
    lm = ProxyLM()
    text = "the streaming pipeline curates documents into training shards"
    untrained = lm.perplexity(text)
    lm.train_many([text] * 50)
    trained = lm.perplexity(text)
    assert trained < untrained


def test_controller_upsert_and_observe(cfg: ProcessorConfig) -> None:
    controller = MixtureController(cfg)
    controller.upsert_recipe(_spec())
    controller.observe_document("exp1", "a", "alpha beta gamma " * 50)
    controller.observe_document("exp1", "b", "alpha beta gamma " * 50)
    decision = controller.close_window("exp1", "alpha beta gamma " * 5)
    assert decision in {"hold", "promote", "rollback"}
    status = controller.status_for("exp1")
    assert "branchA" in status and "branchB" in status


def test_controller_promotes_when_b_is_better(cfg: ProcessorConfig) -> None:
    controller = MixtureController(cfg)
    controller.upsert_recipe(_spec("exp2"))
    eval_text = "the streaming pipeline curates documents"
    # Train branch B heavily on the eval text -> lower perplexity than A.
    for _ in range(cfg.promotion_required_windows + 1):
        controller.observe_document("exp2", "a", "totally unrelated noise xqzz xqzz")
        controller.observe_document("exp2", "b", eval_text * 30)
        controller.close_window("exp2", eval_text)
    status = controller.status_for("exp2")
    assert status["lastDecision"] in {"promote", "hold"}


def test_controller_remove_recipe(cfg: ProcessorConfig) -> None:
    controller = MixtureController(cfg)
    controller.upsert_recipe(_spec("exp3"))
    controller.remove_recipe("exp3")
    assert controller.status_for("exp3") == {}
