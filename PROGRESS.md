# NexusBench — Development Progress

> **Hackathon:** IICPC Summer Hackathon 2026 (May 9 – June 10)
> **Platform:** Distributed Benchmarking and Hosting Platform for trading algorithms
> **Module:** `github.com/nexusbench/nexusbench`

---

## What NexusBench Does

Contestants upload a trading engine (matching engine / orderbook) written in C++, Rust, Go, or Python. NexusBench:

1. Sandboxes and deploys the engine in an isolated container with strict CPU and memory limits
2. Bombards it with a distributed fleet of trading bots simulating extreme market conditions
3. Captures p50/p90/p99 latency, max TPS, and correctness (price-time priority) in real time
4. Streams results to a live leaderboard ranked by composite score

---

## Overall Roadmap

| Phase | Name | Status |
|-------|------|--------|
| Phase 1 | Core MVP | ✅ Complete |
| Phase 2 | Telemetry | ✅ Complete |
| Phase 3 | Distributed Workers | ✅ Complete |
| Phase 4 | Terraform & Infra Automation | 🔄 In Progress |
| Phase 5 | Advanced Benchmarking | ⏳ Pending |

---

## Phases Completed

### Phase 1 — Core MVP ✅

**Goal:** upload algo → run in container → replay data → show metrics

**What was built:**

- `internal/models` — core domain types: `Submission`, `BenchmarkResults`, `LeaderboardEntry`, all lifecycle statuses (`pending` → `deploying` → `running` → `benchmarking` → `completed` / `failed`)
- `internal/config` — single `Config` struct loaded from environment variables; `ImageForLanguage`, `AllImages` helpers
- `internal/sandbox` — `DockerManager`: deploys contestant code into isolated containers with cgroup CPU pinning, memory limits, capability dropping, `CopyToContainer` archive injection, and port allocation from a configurable pool
- `internal/submission` — `Service` + `DiskStore`: validates uploads, stores archives on disk, orchestrates container lifecycle; `Store` interface for testability
- `internal/api` — HTTP router (gorilla/mux): `POST /api/v1/submissions`, `GET /api/v1/submissions/{id}`, `GET /api/v1/leaderboard`, `GET /health`, `GET /metrics`
- `cmd/server` — control plane binary
- `docker/sandbox/` — five Dockerfile variants: `go`, `rust`, `cpp`, `python`, `binary`; each extracts the archive and runs the engine on port 7878

### Phase 2 — Telemetry ✅

**Goal:** live metrics → dashboard → logs

**What was built:**

- `internal/telemetry` — `Event` type, `Emitter` interface with `Emit` + `BatchEmit`, `StdoutEmitter`, `RedpandaEmitter` (franz-go, AllISRAcks), `RecordingEmitter` (tests), `NoopEmitter`; topic layout: `metrics.latency`, `metrics.heartbeat`, `metrics.dlq`
- `internal/consumer` — `Consumer` polls `metrics.latency` from Redpanda, writes rows to TimescaleDB via `pgxpool`; `PercentileStore` computes p50/p90/p99 from the time-series table
- `internal/metrics` — Prometheus `Registry`: HTTP request counter + duration histogram
- `docker-compose.yml` — full observability stack: Redpanda + Console, TimescaleDB, Prometheus, Grafana, Loki, Promtail, cAdvisor, Node Exporter
- Grafana dashboards provisioned automatically on startup

### Phase 3 — Distributed Workers ✅

**Goal:** multiple benchmark nodes + scheduler

**Stages completed:**

| Stage | Description | Status |
|-------|-------------|--------|
| 3.1 | Worker abstraction + Redpanda job queue | ✅ |
| 3.2 | Orchestrator + worker heartbeat registry | ✅ |
| 3.3 | Real distributed bot fleet + correctness engine | ✅ |

**Key packages added in Phase 3:**

| Package | Purpose |
|---|---|
| `internal/queue` | `Job` type, `Queue` interface, `MemoryQueue` (tests), `RedpandaQueue` (production) |
| `internal/worker` | `Worker` poll loop, `SandboxExecutor`, `Heartbeater` |
| `internal/orchestrator` | `WorkerRegistry`, HTTP handler for fleet visibility routes |
| `internal/botfleet` | `Fleet`, `Bot`, `OrderGenerator`, `RESTTransport`, `ComputeStats` |
| `internal/correctness` | `GoldenOrderbook`, `Checker`, deterministic price-time priority matching |

**Critical bug fixed in Stage 3.3:** Docker-in-Docker networking — workers running inside containers were connecting to `localhost:{sandboxPort}` but sandbox ports are published on the host machine's network interface, not inside the worker container's namespace. Fixed via `SANDBOX_HOST=host.docker.internal` (Docker Desktop) with `WithSandboxHost` executor option.

---

## Phase 4 — Terraform & Infra Automation 🔄

**Goal:** Provision cloud infrastructure with Terraform, deploy all services to Kubernetes, implement autoscaling on worker fleet based on queue depth, and establish a CI/CD pipeline.

> **⚠️ ARCHITECTURAL DECISION (Security vs. Stability):** 
> We are deliberately **skipping gVisor** (which was mentioned in `docker.go` comments) for this hackathon. GVisor introduces syscall overhead that skews p99 latency and TPS metrics. Since correctness and stability matter most, we will stick to native Docker capability-dropping. 
> To mitigate the container-escape risk without a "screening service", Phase 4 MUST implement **Disposable Workers**: 
> 1. Worker pods/nodes must be highly ephemeral (e.g., rapid recycling or using Spot instances).
> 2. Implement strict Kubernetes NetworkPolicies (no outbound internet access for workers, isolated from the control plane).
> 3. Mount contestant code as Read-Only volumes.

> **See TASK.md for the full incremental execution plan with per-stage gates.**

### Stage breakdown

| Stage | Description | Status |
|-------|-------------|--------|
| 4.1 | Terraform Cloud Provisioning — VPC, managed K8s cluster, two node pools (on-demand control-plane + spot worker), container registry | ✅ Complete |
| 4.2 | Kubernetes Manifests — all seven services deployed, zero-trust NetworkPolicies, RBAC, PodDisruptionBudgets, read-only worker mounts | ⏳ Pending |
| 4.3 | Autoscaling — KEDA ScaledObject on Redpanda consumer-group lag; `QueueDepth` method on `Queue` interface; Prometheus gauge | ⏳ Pending |
| 4.4 | CI/CD Pipeline — GitHub Actions: lint + test + tf-validate + k8s-validate on PRs; build + push + rolling deploy on `main` | ⏳ Pending |

### New files planned

```
terraform/
├── main.tf / variables.tf / outputs.tf / versions.tf
└── modules/
    ├── cluster/      # managed K8s cluster
    ├── node-pools/   # control-plane (on-demand) + worker (spot, tainted)
    └── registry/     # container registry + Workload Identity binding
k8s/
├── namespace.yaml
├── configmaps / secrets/
├── control-plane / worker / consumer
├── redpanda / timescaledb StatefulSets
├── network-policies/   # default-deny-all + allow-* per service
└── rbac/               # minimal worker ServiceAccount + Role
.github/
├── workflows/ci.yml      # PR gate
└── workflows/deploy.yml  # main → build + push + rolling deploy
```

### Code changes planned

| Package | Change |
|---|---|
| `internal/queue` | Add `QueueDepth(ctx) (int64, error)` to `Queue` interface; implement on `RedpandaQueue` and `MemoryQueue` |
| `internal/metrics` | Add `nexusbench_queue_depth` Prometheus gauge; wired via 15s background scrape in control plane |
| `Makefile` | Add `lint`, `ci`, `tf-validate`, `k8s-validate`, `build-push` targets |

### Stage 4.1 — What was built

**16 files created across `terraform/` and `Makefile`.**

| File | Purpose |
|---|---|
| `terraform/versions.tf` | Pins `hashicorp/google ~> 5.30`, `google-beta ~> 5.30`, `kubernetes ~> 2.30`; GCS remote backend block |
| `terraform/variables.tf` | All 15 input variables with descriptions, type constraints, and validation rules; no defaults for sensitive vars |
| `terraform/outputs.tf` | 6 outputs: `cluster_name`, `cluster_endpoint` (sensitive), `cluster_ca_certificate` (sensitive), `kubeconfig_command`, `registry_url`, `worker_pool_id`, `workload_identity_pool` |
| `terraform/main.tf` | Thin root: 2 data sources + 3 module calls, zero resource blocks |
| `terraform/modules/cluster/main.tf` | VPC + subnet (with alias IP ranges for pods/services), Cloud Router + NAT, GKE node service account + 3 IAM role bindings, Workload Identity Pool + OIDC provider, private VPC-native GKE cluster with Calico NetworkPolicy, STABLE release channel, weekly maintenance window |
| `terraform/modules/cluster/variables.tf` | 10 variables scoped to cluster concerns |
| `terraform/modules/cluster/outputs.tf` | 7 outputs consumed by node-pools and registry modules |
| `terraform/modules/node-pools/main.tf` | Control-plane pool (on-demand, fixed count, surge upgrades) + worker pool (spot, cluster autoscaler, `role=benchmark-worker:NoSchedule` taint, 100 GB SSD) |
| `terraform/modules/node-pools/variables.tf` | 8 variables |
| `terraform/modules/node-pools/outputs.tf` | 6 outputs including taint key/value for manifest cross-reference |
| `terraform/modules/registry/main.tf` | Artifact Registry Docker repo with 10-tag keep + 7-day untagged cleanup policies; node SA reader binding; CI push SA + WIF binding scoped to `github_repository` |
| `terraform/modules/registry/variables.tf` | 8 variables |
| `terraform/modules/registry/outputs.tf` | `repository_url`, `repository_id`, `ci_push_service_account_email` |
| `terraform/envs/dev.tfvars` | `e2-standard-2`, worker min=0 max=3, single zone, open master CIDR |
| `terraform/envs/prod.tfvars` | `c2-standard-8`, worker min=1 max=10, 3 zones, IAP + CI CIDR |
| `Makefile` (updated) | `tf-validate` (fmt-check + init -backend=false + validate), `k8s-validate` (kubectl dry-run, graceful skip if kubectl absent), `lint`, `ci`, `build-push` targets; all documented in header |

*(Stages updated as work proceeds — see TASK.md for current status)*

---

## Architecture Diagram

```
  Contestant Browser / curl
         │
         ▼
  ┌─────────────────────────────┐
  │  Control Plane (:8080)      │
  │  cmd/server                 │
  │  ├─ POST /api/v1/submissions│──► Enqueue ──► jobs.benchmark (Redpanda)
  │  ├─ GET  /api/v1/leaderboard│◄── store reads
  │  └─ GET  /internal/workers  │◄── WorkerRegistry (in-memory)
  └─────────────────────────────┘
         ▲ heartbeat (5s)
         │ register
  ┌──────┴──────────────────────┐
  │  Worker (cmd/worker)        │
  │  ├─ Heartbeater goroutine   │
  │  └─ Worker poll loop        │◄── Dequeue ◄── jobs.benchmark
  │       └─ SandboxExecutor    │
  │            ├─ Deploy        │──► Docker sandbox container
  │            ├─ WaitHealthy   │
  │            ├─ [StatusBenchmarking set]
  │            ├─ Bot Fleet     │──► N goroutine bots ──► sandbox /orders
  │            │    └─ FleetResult{Stats, Results}
  │            ├─ GoldenOrderbook → CorrectnessResult
  │            ├─ BuildResults  │──► CompositeScore (p99+TPS+correctness)
  │            └─ BatchEmit     │──► metrics.latency (Redpanda)
  └─────────────────────────────┘
         │ writes results
         ▼
  ┌─────────────────────────────┐         ┌───────────────────────┐
  │  DiskStore (shared volume)  │         │  Consumer             │
  │  /data/submissions/{id}/    │         │  metrics.latency      │
  │  meta.json                  │         │  → TimescaleDB        │
  └─────────────────────────────┘         │  → Grafana dashboard  │
                                          └───────────────────────┘

  Phase 4 target — Kubernetes:
  ┌──────────────────────────────────────────────────────────┐
  │  GKE / EKS Cluster                                       │
  │  ├─ Deployment: control-plane  (1 replica)               │
  │  ├─ Deployment: metrics-consumer (1 replica)             │
  │  ├─ Deployment: worker  ──► HPA (scale on queue depth)   │
  │  │       min=1  max=10  target=5 pending jobs/replica     │
  │  ├─ StatefulSet: Redpanda (3 replicas, PVC storage)      │
  │  ├─ StatefulSet: TimescaleDB (1 replica, PVC storage)    │
  │  ├─ PersistentVolumeClaim: submissions-data (RWX)        │
  │  └─ Ingress: NGINX → control-plane :8080                 │
  └──────────────────────────────────────────────────────────┘
```

---

## Running the Stack

```bash
# Build sandbox images (one-time)
make images

# Start full stack with distributed mode
docker compose up --build -d

# Run smoke tests
STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage3.sh

# Run all unit tests (offline, no infrastructure)
make test

# Scale to 3 workers
docker compose up --scale worker=3 -d
```

---

## Test Coverage

| Package | Tests | Infrastructure required |
|---|---|---|
| `internal/queue` | 9 | None |
| `internal/worker` | 13 (8 worker + 5 executor) | None |
| `internal/orchestrator` | 10 | None |
| `internal/botfleet` | 12 | None (httptest.Server) |
| `internal/correctness` | 13 | None |
| `internal/submission` | existing | None |
| `internal/telemetry` | existing + 4 BatchEmit | None (unit) / Redpanda (integration) |

All unit tests run in < 5 seconds total. The race detector is enabled on every `make test` run.

---

## What's Next — Phase 5

### Advanced Benchmarking

- Stress benchmark: volatile-only market data replay via Redpanda historical feed
- Latency injection: artificial jitter between bot orders to model real network conditions
- Chaos engineering: random container kills, network partition simulation
- Pause / Resume / Kill controls on live benchmark runs via the API
- FIX protocol transport for `BotTransport` interface
- WebSocket transport for streaming order feeds

---

## Running the Stack (updated for Phase 4)

```bash
# Local dev (unchanged from Phase 3)
make images && docker compose up --build -d

# Validate Terraform (no cloud credentials needed)
make tf-validate

# Validate K8s manifests (no cluster needed)
make k8s-validate

# Full local CI gate (mirrors GitHub Actions)
make ci

# Deploy to dev cluster (requires kubeconfig + registry access)
make build-push deploy
```
