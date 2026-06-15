# Stream2Pretrain - top-level Makefile
#
# All Python targets are run via `uv` (project rule). Helm targets assume
# `helm`, `helmfile`, and `kubectl` are on PATH.

SHELL := /usr/bin/env bash
.DEFAULT_GOAL := help

PY_DIRS := schemas ingest processor tests
HELM_CHART := charts/stream2pretrain
HELMFILE := helmfile.yaml

.PHONY: help
help: ## Print this help.
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z0-9_.-]+:.*?## / { printf "  %-18s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: fmt
fmt: ## Format Python sources with ruff.
	uv run ruff format $(PY_DIRS)
	uv run ruff check --fix $(PY_DIRS)

.PHONY: lint
lint: ## Lint Python sources (no fixes).
	uv run ruff check $(PY_DIRS)
	uv run ruff format --check $(PY_DIRS)

.PHONY: typecheck
typecheck: ## Run mypy across the workspace.
	uv run mypy $(PY_DIRS)

.PHONY: test
test: ## Run the pytest suite.
	uv run pytest

.PHONY: build
build: ## Build all component container images via docker buildx bake.
	@if [ -f docker-bake.hcl ]; then \
		docker buildx bake --file docker-bake.hcl ; \
	else \
		echo "docker-bake.hcl not present yet; build per-component Dockerfiles instead" ; \
		exit 1 ; \
	fi

.PHONY: dev-up
dev-up: ## Start the local dev stack (Redpanda + MinIO) via docker compose.
	docker compose -f docker-compose.dev.yml up -d
	@echo "Redpanda console: http://localhost:8080"
	@echo "MinIO console:    http://localhost:9001 (minioadmin / minioadmin)"

.PHONY: dev-down
dev-down: ## Stop and remove the local dev stack (volumes preserved).
	docker compose -f docker-compose.dev.yml down

.PHONY: dev-reset
dev-reset: ## Stop the dev stack and remove volumes (destructive).
	docker compose -f docker-compose.dev.yml down -v

.PHONY: seed-topics
seed-topics: ## Create the four core Redpanda topics on the local dev cluster.
	bash scripts/seed_topics.sh

.PHONY: helm-lint
helm-lint: ## helm lint the Stream2Pretrain chart.
	helm lint $(HELM_CHART)

.PHONY: helm-template
helm-template: ## Render Helm templates locally for inspection.
	helm template stream2pretrain $(HELM_CHART) --debug

.PHONY: k3s-up
k3s-up: ## Provision the k3s cluster (calls infra/k3s-install.sh).
	bash infra/k3s-install.sh

.PHONY: deploy
deploy: ## Apply the helmfile against the active kube context.
	helmfile -f $(HELMFILE) apply

.PHONY: port-forward
port-forward: ## Forward common service ports to localhost (Redpanda console, MinIO console, UI).
	@echo "Forwarding redpanda-console:8080, minio-console:9001, ui:3000 (Ctrl-C to stop)"
	kubectl port-forward svc/redpanda-console 8080:8080 & \
	kubectl port-forward svc/minio-console 9001:9001 & \
	kubectl port-forward svc/stream2pretrain-ui 3000:3000 & \
	wait
