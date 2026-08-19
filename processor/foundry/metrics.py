"""Foundry outcome, provider, validation, and security metrics."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

JOBS = Counter("s2p_foundry_jobs_total", "Foundry jobs by terminal state", ["state"])
STAGES = Counter("s2p_foundry_stage_events_total", "Append-only foundry stage events", ["state"])
ARTIFACTS = Counter(
    "s2p_foundry_artifacts_total",
    "Foundry artifacts by kind, family, and status",
    ["kind", "family", "status"],
)
HUMAN_AUDITS = Counter(
    "s2p_foundry_human_audits_total",
    "Append-only manual artifact audit decisions",
    ["decision"],
)
PROVIDER_CALLS = Counter(
    "s2p_foundry_provider_calls_total",
    "Provider calls by provider, role, and returned model",
    ["provider", "role", "model", "status"],
)
PROVIDER_TOKENS = Counter(
    "s2p_foundry_provider_tokens_total", "Provider token usage", ["provider", "role", "direction"]
)
PROVIDER_LATENCY = Histogram(
    "s2p_foundry_provider_latency_seconds", "Provider call latency", ["provider", "role"]
)
PROVIDER_TTFT = Histogram(
    "s2p_foundry_provider_time_to_first_token_seconds",
    "Provider time to first streamed token",
    ["provider", "role"],
)
PROVIDER_OUTPUT_RATE = Histogram(
    "s2p_foundry_provider_output_tokens_per_second",
    "Observed provider output throughput",
    ["provider", "role"],
)
QUOTA_REMAINING = Gauge(
    "s2p_foundry_quota_remaining",
    "Locally estimated provider quota remaining",
    ["provider", "window", "kind"],
)
VALIDATION = Gauge(
    "s2p_foundry_validation_pass", "Latest validation gate result", ["task_family", "gate"]
)
MUTATION_KILL_RATE = Histogram(
    "s2p_foundry_mutation_kill_ratio",
    "Mutation kill ratio of candidate environments",
    ["task_family"],
)
SECURITY_BLOCKS = Counter(
    "s2p_foundry_security_blocks_total",
    "Blocked secret, drift, hidden-state, or runtime-network attempts",
    ["reason"],
)
QUEUED_CANDIDATES = Gauge(
    "s2p_foundry_queued_candidates",
    "Post-training candidates durably waiting for a daily ranked run",
)
PROVIDER_AVAILABLE = Gauge(
    "s2p_foundry_provider_available",
    "Whether the configured provider model was discovered successfully",
    ["provider"],
)


__all__ = [
    "ARTIFACTS",
    "HUMAN_AUDITS",
    "JOBS",
    "MUTATION_KILL_RATE",
    "PROVIDER_AVAILABLE",
    "PROVIDER_CALLS",
    "PROVIDER_LATENCY",
    "PROVIDER_OUTPUT_RATE",
    "PROVIDER_TOKENS",
    "PROVIDER_TTFT",
    "QUEUED_CANDIDATES",
    "QUOTA_REMAINING",
    "SECURITY_BLOCKS",
    "STAGES",
    "VALIDATION",
]
