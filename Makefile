# NexusBench Makefile
# ─────────────────────────────────────────────────────────────────────────────
# Pre-built language images:        make images
# Start control plane (local):      make run
# Start worker (local):             make run-worker
# Start Redpanda only:              make up-infra
# Run all tests (unit):             make test
# Run telemetry unit tests:         make test-telemetry
# Run queue unit tests:             make test-queue
# Run worker unit tests:            make test-worker
# Run telemetry integration tests:  make test-integration
# Full local stack:                 make up
#
# ── Phase 4 additions ─────────────────────────────────────────────────────────
# Validate Terraform HCL:           make tf-validate
# Validate K8s manifests (dry-run): make k8s-validate
# Lint Go code:                     make lint
# Full local CI gate:               make ci
# Build + push all images to reg:   make build-push REGISTRY=<url>
#
# ── Phase 5 additions ─────────────────────────────────────────────────────────
# Phase 5 dry-run smoke test:       make smoke-phase5
# Phase 5 live smoke test:          make smoke-phase5-live
# Phase 5 per-package unit tests:   make test-phase5

REGISTRY   ?= nexusbench
TAG        ?= latest
SANDBOX_DIR = docker/sandbox

LANGUAGES  = go rust cpp python binary

# GO_PKGS excludes docker/sandbox/ from `go test` because Dockerfile.golang
# has a .go extension but is not a Go source file.
GO_PKGS := $(shell go list ./... 2>/dev/null | grep -v 'docker/sandbox')

.PHONY: images $(addprefix image-,$(LANGUAGES)) \
        run run-worker up up-infra down \
        test test-telemetry test-queue test-worker test-integration \
        test-phase5 smoke smoke-phase5 smoke-phase5-live \
        deps clean-images sizes \
        lint tf-validate k8s-validate ci build-push

# ── Image targets ─────────────────────────────────────────────────────────────

images: $(addprefix image-,$(LANGUAGES))
	@echo ""
	@echo "✓ All sandbox images built:"
	@docker images --filter "reference=$(REGISTRY)-sandbox-*" \
	    --format "  {{.Repository}}:{{.Tag}}  ({{.Size}})"

image-go:
	docker build -t $(REGISTRY)-sandbox-go:$(TAG) -f $(SANDBOX_DIR)/Dockerfile.golang $(SANDBOX_DIR)

image-rust:
	docker build -t $(REGISTRY)-sandbox-rust:$(TAG) -f $(SANDBOX_DIR)/Dockerfile.rust $(SANDBOX_DIR)

image-cpp:
	docker build -t $(REGISTRY)-sandbox-cpp:$(TAG) -f $(SANDBOX_DIR)/Dockerfile.cpp $(SANDBOX_DIR)

image-python:
	docker build -t $(REGISTRY)-sandbox-python:$(TAG) -f $(SANDBOX_DIR)/Dockerfile.python $(SANDBOX_DIR)

image-binary:
	docker build -t $(REGISTRY)-sandbox-binary:$(TAG) -f $(SANDBOX_DIR)/Dockerfile.binary $(SANDBOX_DIR)

# ── Development ───────────────────────────────────────────────────────────────

## Run the control plane locally (no Docker, hot path for development)
run:
	go run ./cmd/server

## Run a single worker process locally.
## Requires Redpanda running (make up-infra) and SUBMISSION_DIR set.
run-worker:
	go run ./cmd/worker

## Start the full docker-compose stack (control plane + worker + Redpanda + Console)
up:
	docker compose up --build

## Start only the infrastructure services (Redpanda + Console).
up-infra:
	docker compose up redpanda console -d
	@echo ""
	@echo "✓ Redpanda listening on localhost:19092"
	@echo "✓ Console UI at http://localhost:8088"
	@echo ""
	@echo "Wait ~10s for Redpanda to be healthy, then run:"
	@echo "  rpk cluster info --brokers 127.0.0.1:19092"

## Stop all compose services and remove volumes
down:
	docker compose down -v

# ── Testing ───────────────────────────────────────────────────────────────────

## Run all unit tests (race detector on, coverage report written to coverage.out).
## Does NOT require Redpanda or Docker.
test:
	go test $(GO_PKGS) -v -race -timeout 60s -count=1 -coverprofile=coverage.out -covermode=atomic
	@go tool cover -func=coverage.out | tail -1

## Run only the telemetry package unit tests.
test-telemetry:
	go test ./internal/telemetry/... -v -race -timeout 30s -count=1

## Run only the queue package unit tests (no Redpanda required).
test-queue:
	go test ./internal/queue/... -v -race -timeout 30s -count=1

## Run only the orchestrator package unit tests.
test-orchestrator:
	go test ./internal/orchestrator/... -v -race -timeout 30s -count=1

## Run only the worker package unit tests (no Docker, no Redpanda required).
test-worker:
	go test ./internal/worker/... -v -race -timeout 30s -count=1

## Run telemetry integration tests against a live Redpanda broker.
test-integration:
	@echo "Running integration tests (requires Redpanda at 127.0.0.1:19092)..."
	go test ./internal/telemetry/... -tags=integration -v -race -timeout 120s -count=1

## Run all Phase 5 unit tests with race detector.
## Covers all new Phase 5 packages: models, queue, contest, submission, worker,
## botfleet, correctness, validator, api (bus + SSE), telemetry, orchestrator.
## No infrastructure required — all tests use in-memory or httptest fakes.
test-phase5:
	@echo "Running Phase 5 unit test suite (race detector)..."
	go test \
	    ./internal/models/... \
	    ./internal/queue/... \
	    ./internal/contest/... \
	    ./internal/submission/... \
	    ./internal/worker/... \
	    ./internal/botfleet/... \
	    ./internal/correctness/... \
	    ./internal/validator/... \
	    ./internal/api/... \
	    ./internal/telemetry/... \
	    ./internal/orchestrator/... \
	    -v -race -timeout 90s -count=1
	@echo ""
	@echo "✓ Phase 5 unit tests complete"

## Run end-to-end smoke tests against localhost:8080 (Phase 1–4)
smoke:
	@bash scripts/smoke_test.sh

## Phase 5 smoke test — dry-run mode (default, CI-safe).
## Validates: go build, all unit tests, binary builds, router endpoints,
## and all new Kubernetes YAML manifests. No running infrastructure needed.
smoke-phase5:
	@bash scripts/smoke_test_phase5.sh --dry-run

## Phase 5 smoke test — live mode.
## Exercises the full Phase 5 flow against a running docker compose stack.
## Requires: docker compose up --build -d, curl, jq.
## Override the URL with: NEXUSBENCH_URL=http://host:port make smoke-phase5-live
smoke-phase5-live:
	@bash scripts/smoke_test_phase5.sh --live

# ── Housekeeping ──────────────────────────────────────────────────────────────

## Download and tidy Go dependencies
deps:
	go mod tidy

## Remove all NexusBench sandbox images
clean-images:
	docker rmi $(shell docker images --filter "reference=$(REGISTRY)-sandbox-*" -q) 2>/dev/null || true

## Show sandbox image sizes
sizes:
	@docker images --filter "reference=$(REGISTRY)-sandbox-*" \
	    --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

# ── Phase 4: Terraform & Infra Automation ─────────────────────────────────────

TF_DIR     = terraform
TF_VARFILE = envs/dev.tfvars

## Validate Terraform HCL: fmt check + validate (no cloud credentials needed).
tf-validate:
	@echo "─── terraform fmt ───"
	terraform -chdir=$(TF_DIR) fmt -check -recursive
	@echo "─── terraform validate ───"
	terraform -chdir=$(TF_DIR) init -backend=false -input=false -reconfigure > /dev/null
	terraform -chdir=$(TF_DIR) validate
	@echo "✓ Terraform HCL is valid"

## Dry-run all Kubernetes manifests (includes Phase 5 postgres manifests).
k8s-validate:
	@echo "─── k8s manifest validation (offline) ───"
	@bash scripts/smoke_test_phase4_stage2.sh --dry-run

## Run golangci-lint on all Go packages.
lint:
	@if command -v golangci-lint > /dev/null 2>&1; then \
		golangci-lint run ./... ; \
		echo "✓ lint passed"; \
	else \
		echo "golangci-lint not found — run: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi

## Full local CI gate: lint + unit tests + terraform + k8s + Phase 5 smoke.
## All checks must pass before merging to main.
ci: lint test smoke-phase5 tf-validate k8s-validate
	@echo ""
	@echo "✓ All CI checks passed — safe to push"

## Build all images and push to the registry.
## Usage:  make build-push REGISTRY=us-docker.pkg.dev/my-project/nexusbench TAG=abc1234
build-push:
	@echo "Building and pushing to $(REGISTRY) with tag $(TAG)"
	docker build -t $(REGISTRY)/control-plane:$(TAG) -f Dockerfile.server .
	docker push $(REGISTRY)/control-plane:$(TAG)
	docker build -t $(REGISTRY)/sandbox-go:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.golang $(SANDBOX_DIR)
	docker push $(REGISTRY)/sandbox-go:$(TAG)
	docker build -t $(REGISTRY)/sandbox-rust:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.rust $(SANDBOX_DIR)
	docker push $(REGISTRY)/sandbox-rust:$(TAG)
	docker build -t $(REGISTRY)/sandbox-cpp:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.cpp $(SANDBOX_DIR)
	docker push $(REGISTRY)/sandbox-cpp:$(TAG)
	docker build -t $(REGISTRY)/sandbox-python:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.python $(SANDBOX_DIR)
	docker push $(REGISTRY)/sandbox-python:$(TAG)
	docker build -t $(REGISTRY)/sandbox-binary:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.binary $(SANDBOX_DIR)
	docker push $(REGISTRY)/sandbox-binary:$(TAG)
	@echo "✓ All images pushed to $(REGISTRY)"
