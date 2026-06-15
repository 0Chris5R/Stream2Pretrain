# Cloud Computing & Big Data — Prüfungsleistung 2026

Datengetriebener Prototyp auf Kubernetes-Basis. Project name: **Stream2Pretrain**. Domain focus: streaming curation for fresh AI-research pretraining data (arXiv, GitHub, HuggingFace, AI-lab blogs).

## What this directory contains right now

This is the **research and planning phase** of the project. No application code has been written yet.

| File / dir | Purpose |
|---|---|
| `RESEARCH.md` | Full deep-research report + implementation plan. The single most important document. |
| `SOURCES.md` | Phase-1 + Phase-2 source feeds with endpoints, rate limits, and big-data V's mapping. |
| `lecture_slides/` | Archived copy of all 19 markdown lecture decks from `farberg.de/talks/cloud/` for offline reference. |
| `CLAUDE.md` | Decision log + working notes for the AI pair. |
| `README.md` | This file. |

The eventual project repo (`stream2pretrain-curator/`) will live as a sibling directory once Week 1 of the implementation plan begins. See `RESEARCH.md` section 8 for the planned repo layout.

## Project idea in one paragraph

A Kubernetes-native, streaming-first curation pipeline for LLM pretraining data. It ingests live web documents (RSS, sitemaps, manual submissions), runs FineWeb-class curation operators (HTML extraction, language ID, Gopher/C4 heuristics, MinHash near-deduplication, classifier-based quality scoring, PII filtering, benchmark decontamination) as long-running stateful stream operators on event-time semantics, and lands curated tokens into an Apache Iceberg lakehouse on MinIO. Three differentiators that survived adversarial novelty review: (a) a streaming Decon-Gate sidecar that emits a per-Iceberg-snapshot signed contamination attestation, (b) a per-document validity-interval column with an `as_of(timestamp)` query view for deterministic contamination replay, and (c) a shadow-mode A/B mixture comparison primitive where two `MixtureRecipe` CRDs read the same live SourceFeed.

## Where to read what

- **Use case + Big-Data motivation** — `RESEARCH.md` section 1 (Executive Summary)
- **State of the art / why this is not redundant** — `RESEARCH.md` section 2
- **Genuinely novel angles (with adversarial verification)** — `RESEARCH.md` section 3
- **Architecture diagram + component breakdown** — `RESEARCH.md` section 4
- **Final tech stack picks with reasoning** — `RESEARCH.md` section 5
- **Bronze/Silver/Gold data model + data passport** — `RESEARCH.md` section 6
- **Week-by-week implementation plan (4-6 weeks)** — `RESEARCH.md` section 7
- **Demo / screenshot story for the grader** — `RESEARCH.md` section 9
- **Honest risks and open questions** — `RESEARCH.md` section 10

## Lecture-stack alignment (from `lecture_slides/`)

The course is heavily Kubernetes-centric: Terraform + Ansible -> k3s on DHBWCloud, Helm + Skaffold for app delivery, KEDA for autoscaling, Prometheus + Grafana + Loki + Alloy for observability, Traefik + cert-manager for ingress + TLS, Valkey as a queue example. **No Hadoop, Spark, or Kafka decks** in the course - choosing Redpanda + Bytewax counts as a "begründete Abweichung" eligible for bonus points.

## Source data the research was built from

- Lecture slides (this folder): all 19 markdown files from `farberg.de/talks/cloud/`, fetched 2026-06-15 with Keycloak login
- Project assignment: the prompt text the user pasted on 2026-06-15
- Web research via Perplexity + targeted GitHub/arXiv searches across 6 landscape lenses, 5 novelty lenses, and 14 adversarial verifiers, run as a single Workflow on 2026-06-15 (run id `wf_14fc06f4-2b8`, 30 subagents, 244 tool uses)

## Next steps

Start Week 1 of the implementation plan in `RESEARCH.md` section 7.
