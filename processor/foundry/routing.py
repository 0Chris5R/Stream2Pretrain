"""Fixed single-provider role routing for the post-training foundry."""

from __future__ import annotations

ROLE_PROVIDER: dict[str, str] = {
    "structure_compiler": "hetzner",
    "claim_compiler": "hetzner",
    "evidence_compiler": "hetzner",
    "dependency_compiler": "hetzner",
    "canonicalization_compiler": "hetzner",
    "conflict_compiler": "hetzner",
    "graph_critic": "hetzner",
    "graph_repair": "hetzner",
    "task_designer": "hetzner",
    "answerability_critic": "hetzner",
    "solver_a": "hetzner",
    "solver_b": "hetzner",
    "grounding_critic": "hetzner",
    "verifier_compiler": "hetzner",
    "verifier_critic": "hetzner",
    "adversary": "hetzner",
    "final_repair": "hetzner",
}


def provider_for_role(role: str) -> str:
    try:
        return ROLE_PROVIDER[role]
    except KeyError as exc:
        raise ValueError(f"no provider route for foundry role {role}") from exc


__all__ = ["ROLE_PROVIDER", "provider_for_role"]
