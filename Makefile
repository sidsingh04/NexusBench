# NexusBench Makefile
# ─────────────────────────────────────────────────────────────────────────────
# Pre-built language images:        make images
# Start control plane (local):      make run
# Start Redpanda only:              make up-infra
# Run all tests (unit):             make test
# Run telemetry unit tests:         make test-telemetry
# Run telemetry integration tests:  make test-integration
# Full local stack:                 make up

REGISTRY   ?= nexusbench
TAG        ?= latest
SANDBOX_DIR = docker/sandbox

LANGUAGES  = go rust cpp python binary

# GO_PKGS excludes docker/sandbox/ from `go test` because Dockerfile.golang
# has a .go extension but is not a Go source file.
GO_PKGS := $(shell go list ./... 2>/dev/null | grep -v 'docker/sandbox')

.PHONY: images $(addprefix image-,$(LANGUAGES)) \
        run up up-infra down \
        test test-telemetry test-integration \
        smoke deps clean-images sizes

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

## Start the full docker-compose stack (control plane + Redpanda + Console)
up:
	docker compose up --build

## Start only the infrastructure services (Redpanda + Console).
## Use this when running the control plane with `make run` and you only
## want the backing services in Docker.
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

## Run all unit tests (race detector on). Does NOT require Redpanda.
test:
	go test $(GO_PKGS) -v -race -timeout 60s

## Run only the telemetry package unit tests.
## Fast feedback loop — no broker needed, runs in ~1s.
test-telemetry:
	go test ./internal/telemetry/... -v -race -timeout 30s -count=1

## Run telemetry integration tests against a live Redpanda broker.
## Requires Redpanda running at 127.0.0.1:19092.
## Start it first with:  make up-infra
test-integration:
	@echo "Running integration tests (requires Redpanda at 127.0.0.1:19092)..."
	go test ./internal/telemetry/... -tags=integration -v -race -timeout 120s -count=1

## Run end-to-end smoke tests against localhost:8080
smoke:
	@bash scripts/smoke_test.sh

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
