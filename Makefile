# NexusBench Makefile
# ─────────────────────────────────────────────────────────────────────────────
# Pre-built language images:  make images
# Start control plane (local): make run
# Run tests:                   make test
# Smoke test:                  make smoke
# Full local stack:            make up

REGISTRY   ?= nexusbench
TAG        ?= latest
SANDBOX_DIR = docker/sandbox

# All language images and their Dockerfiles
LANGUAGES  = go rust cpp python binary

.PHONY: images $(addprefix image-,$(LANGUAGES)) run up down test smoke clean

# ── Image targets ─────────────────────────────────────────────────────────────

## Build ALL pre-built sandbox images (run once before `make run`)
images: $(addprefix image-,$(LANGUAGES))
	@echo ""
	@echo "✓ All sandbox images built:"
	@docker images --filter "reference=$(REGISTRY)-sandbox-*" \
	    --format "  {{.Repository}}:{{.Tag}}  ({{.Size}})"

image-go:
	@echo "▶ Building $(REGISTRY)-sandbox-go:$(TAG) ..."
	docker build \
	    -t $(REGISTRY)-sandbox-go:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.go \
	    $(SANDBOX_DIR)

image-rust:
	@echo "▶ Building $(REGISTRY)-sandbox-rust:$(TAG) ..."
	docker build \
	    -t $(REGISTRY)-sandbox-rust:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.rust \
	    $(SANDBOX_DIR)

image-cpp:
	@echo "▶ Building $(REGISTRY)-sandbox-cpp:$(TAG) ..."
	docker build \
	    -t $(REGISTRY)-sandbox-cpp:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.cpp \
	    $(SANDBOX_DIR)

image-python:
	@echo "▶ Building $(REGISTRY)-sandbox-python:$(TAG) ..."
	docker build \
	    -t $(REGISTRY)-sandbox-python:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.python \
	    $(SANDBOX_DIR)

image-binary:
	@echo "▶ Building $(REGISTRY)-sandbox-binary:$(TAG) ..."
	docker build \
	    -t $(REGISTRY)-sandbox-binary:$(TAG) \
	    -f $(SANDBOX_DIR)/Dockerfile.binary \
	    $(SANDBOX_DIR)

# ── Development ───────────────────────────────────────────────────────────────

## Run the control plane locally (hot path for development)
run:
	go run ./cmd/server

## Start the full docker-compose stack
up:
	docker compose up --build

## Stop the stack
down:
	docker compose down -v

# ── Testing ───────────────────────────────────────────────────────────────────

## Run Go unit tests
test:
	go test ./... -v -race -timeout 60s

## Run end-to-end smoke tests against localhost:8080
smoke:
	@bash scripts/smoke_test.sh

# ── Housekeeping ──────────────────────────────────────────────────────────────

## Remove all NexusBench sandbox images
clean-images:
	@echo "Removing sandbox images..."
	docker rmi $(shell docker images --filter "reference=$(REGISTRY)-sandbox-*" -q) 2>/dev/null || true

## Show image sizes
sizes:
	@docker images --filter "reference=$(REGISTRY)-sandbox-*" \
	    --format "table {{.Repository}}\t{{.Tag}}\t{{.Size}}"

## Download Go dependencies
deps:
	go mod tidy
