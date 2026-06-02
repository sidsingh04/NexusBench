# NexusBench — Development Progress

> **Hackathon:** IICPC Summer Hackathon 2026 (May 9 – June 10)
> **Platform:** Distributed Benchmarking and Hosting Platform for trading algorithms
> **Module:** `github.com/nexusbench/nexusbench`

---

## What NexusBench Does

Contestants upload a **Central Limit Order Book (CLOB) matching engine** written
in C++, Rust, Go, or Python. NexusBench:

1. Sandboxes and deploys the engine in an isolated container with strict CPU and
   memory limits
2. Runs three sequential benchmark jobs — one per volatility profile
   (Low / Medium / High) — configured by the active contest
3. Each job bombards the engine with a distributed fleet of trading bots
   simulating realistic order flow at increasing concurrency and price variance
4. Captures p50/p90/p99 latency, sustained TPS, and correctness (price-time
   priority) per run
5. Computes a weighted composite score across all three runs and streams results
   to a live SSE leaderboard

### What the Platform Tests

NexusBench evaluates **CLOB matching engines only** — not trading strategies,
not portfolio managers, not execution algorithms. The engine must:

- Accept `POST /orders` with `{order_id, kind, side, price, quantity}` (REST/JSON)
- Return `{order_id, accepted, executed_price, executed_qty}` per order
- Handle three order kinds: `limit`, `market`, `cancel`
- Maintain strict **price-time priority**: among resting orders at the same
  price, the earliest-arriving order fills first
- Correctly reject invalid cancels (unknown or already-filled order IDs)
- Survive sustained concurrent load without correctness degradation

---

## Overall Roadmap

| Phase | Name | Status |
|-------|------|--------|
| Phase 1 | Core MVP | ✅ Complete |
| Phase 2 | Telemetry | ✅ Complete |
| Phase 3 | Distributed Workers | ✅ Complete |
| Phase 4 | Terraform & Infra Automation | ✅ Complete |
| Phase 5 | Advanced Benchmarking | 🔄 In Progress (Stage 5.1 ✅, Stage 5.2 ✅, Stage 5.3 ✅, Stage 5.4 ✅, Stage 5.5 ✅, Stage 5.6 ✅, Stage 5.7 ✅, Stage 5.8 ✅) |
| Phase 6 | Frontend | 🔲 Planned |
| Cloud Deployment | GCP Production Deploy | 🔲 Planned (after Phase 6) |

---

## Architectural Principles (enforced across all phases)

These are non-negotiable constraints. Every new module and every code change
must satisfy all of them.

1. **Deep modules over shallow ones.** Each package owns its domain completely.
   Callers depend on narrow interfaces, never on implementation details.
   No package exists solely to re-export another package's types.

2. **No broken existing tests.** Every stage gate requires `make test -race`
   to pass before proceeding. A stage that introduces a regression is not done.

3. **Incremental, testable stages.** Each stage produces one working, tested
   deliverable. No stage lasts longer than a day of focused work.

4. **No external dependencies added without justification.** The core packages
   (`queue`, `botfleet`, `correctness`, `models`) use stdlib only. Dependencies
   belong at the edges (API layer, infrastructure adapters).

5. **Interfaces at boundaries, structs inside.** A package exposes an interface
   only when there is more than one implementation or a test double is needed.
   Internal helpers are unexported functions, not interfaces.

6. **Separation of concerns strictly by layer:**
   - `models` — pure data types, zero logic, zero imports from other internal packages
   - `correctness`, `botfleet` — pure domain logic, no I/O, no storage
   - `queue`, `sandbox`, `telemetry` — I/O adapters behind interfaces
   - `submission`, `worker` — orchestration; wires domain + adapters
   - `api` — HTTP translation layer only; no business logic

---

## Phases Completed

### Phase 1 — Core MVP ✅

**Goal:** upload algo → run in container → replay data → show metrics

**What was built:**

- `internal/models` — core domain types: `Submission`, `BenchmarkResults`,
  `LeaderboardEntry`, all lifecycle statuses (`pending → deploying → running →
  benchmarking → completed / failed`)
- `internal/config` — single `Config` struct loaded from environment variables;
  `ImageForLanguage`, `AllImages` helpers
- `internal/sandbox` — `DockerManager`: deploys contestant code into isolated
  containers with cgroup CPU pinning, memory limits, capability dropping,
  `CopyToContainer` archive injection, port allocation from a configurable pool
- `internal/submission` — `Service` + `DiskStore`: validates uploads, stores
  archives on disk, orchestrates container lifecycle; `Store` interface for
  testability
- `internal/api` — HTTP router (gorilla/mux): `POST /api/v1/submissions`,
  `GET /api/v1/submissions/{id}`, `GET /api/v1/leaderboard`, `GET /health`,
  `GET /metrics`
- `cmd/server` — control plane binary
- `docker/sandbox/` — five Dockerfile variants: `go`, `rust`, `cpp`, `python`,
  `binary`; each extracts the archive and runs the engine on port 7878

---

### Phase 2 — Telemetry ✅

**Goal:** live metrics → dashboard → logs

**What was built:**

- `internal/telemetry` — `Event` type, `Emitter` interface with `Emit` +
  `BatchEmit`, `StdoutEmitter`, `RedpandaEmitter` (franz-go, AllISRAcks),
  `RecordingEmitter` (tests), `NoopEmitter`; topics: `metrics.latency`,
  `metrics.heartbeat`, `metrics.dlq`
- `internal/consumer` — `Consumer` polls `metrics.latency` from Redpanda,
  writes rows to TimescaleDB via `pgxpool`; `PercentileStore` computes
  p50/p90/p99 from the time-series table
- `internal/metrics` — Prometheus `Registry`: HTTP request counter + duration
  histogram
- `docker-compose.yml` — full observability stack: Redpanda + Console,
  TimescaleDB, Prometheus, Grafana, Loki, Promtail, cAdvisor, Node Exporter
- Grafana dashboards provisioned automatically on startup

---

### Phase 3 — Distributed Workers ✅

**Goal:** multiple benchmark nodes + scheduler

**What was built:**

| Package | Purpose |
|---|---|
| `internal/queue` | `Job` type, `Queue` interface, `MemoryQueue` (tests), `RedpandaQueue` (production) |
| `internal/worker` | `Worker` poll loop, `SandboxExecutor`, `Heartbeater` |
| `internal/orchestrator` | `WorkerRegistry`, HTTP handler for fleet visibility routes |
| `internal/botfleet` | `Fleet`, `Bot`, `OrderGenerator`, `RESTTransport`, `ComputeStats` |
| `internal/correctness` | `GoldenOrderbook`, `Checker`, deterministic price-time priority matching |

**Critical design: deep modules**

- `queue.Queue` interface is narrow (Enqueue / Dequeue / CommitJob / QueueDepth /
  Close). Workers never see broker internals.
- `botfleet` has zero imports from `worker`, `submission`, or `sandbox`.
  The correctness checker has zero imports from `botfleet` — types are
  mirrored (`GoldenOrder` mirrors `botfleet.Order`) to avoid import cycles.
- `SandboxExecutor` depends on a `sandboxDeployer` interface (unexported),
  not on `*sandbox.DockerManager` directly — tests inject a fake deployer.

**Critical bug fixed:** Docker-in-Docker networking — workers connecting to
`localhost:{sandboxPort}` were reaching themselves, not the sandbox. Fixed via
`SANDBOX_HOST=host.docker.internal` + `WithSandboxHost` executor option.

---

### Phase 4 — Terraform & Infra Automation ✅

**Goal:** provision cloud infrastructure, deploy to Kubernetes, autoscale
workers on queue depth, establish CI/CD.

**Architectural decision:** gVisor skipped. Capability-dropping Docker is the
isolation mechanism. Worker nodes are disposable spot instances.
NetworkPolicies enforce zero-trust: workers have no path to TimescaleDB or the
internet.

| Stage | Description | Status |
|-------|-------------|--------|
| 4.1 | Terraform: VPC, GKE cluster, two node pools, Artifact Registry | ✅ |
| 4.2 | Kubernetes manifests: all services, NetworkPolicies, RBAC, PVCs | ✅ |
| 4.3 | KEDA autoscaling on Redpanda consumer-group lag | ✅ |
| 4.4 | GitHub Actions CI/CD: lint + test + validate + build + deploy | ✅ |

**16 live-cluster bugs found and fixed** during Stage 4.2/4.3 smoke testing
(Redpanda StatefulSet DNS mismatch, readiness probe circular dependency, PVC
StorageClass mismatch, NetworkPolicy missing ingress sides, worker root-FS
write, Docker socket permission). All documented in TASK.md.

---

## Phase 5 — Advanced Benchmarking

> **Status: 🔄 In Progress**
> Stage 5.1 through Stage 5.8 are ✅ complete and tested.
> **Current Focus: Implementing Stage 5.9 (PostgreSQL ContestStore).**

### Goal

Add the contest lifecycle, volatility-aware scoring, per-submission submission
guard, dry-run validator, SSE live leaderboard, and WebSocket bot transport —
without breaking any existing functionality.

### What NexusBench Does After Phase 5

The platform evolves from "run one benchmark and show a score" to "run a timed
competitive contest with three volatility environments, a live leaderboard, and
a safe pre-submission validation path."

### ⚠️ Important Note for Future Phases: Strict Contest Mode

Starting from Stage 5.5, `ADMIN_API_KEY: "testkey"` has been permanently injected into the local `docker-compose.yml` for the `control-plane` service.
- **What this means:** The local development stack now strictly enforces **Phase 5 Contest Mode**. Submissions to `/api/v1/submissions` will be rejected (`ErrContestNotActive`) unless an active contest is first created via the admin endpoints.
- **Why this matters:** Future phases (e.g., Phase 6 Frontend) must be aware that legacy scripts bypassing contests will no longer work locally.

---

### Stage 5.1 — Data Model Additions ✅

**Gate passed:** `go build ./...` and `go test ./internal/models/... ./internal/queue/... -v -race` both green.

**What was built:**

| File | Change |
|---|---|
| `internal/models/models.go` | `ContestStatus`, `VolatilityProfile`, `Contest`, sentinel errors; extended `Submission`, `BenchmarkResults`, `LeaderboardEntry`; added `IsTerminal()`, `ResultByLabel()`, `ProfileByLabel()` |
| `internal/queue/job.go` | Added `ContestID`, `VolatilityLabel`, `RemainingProfiles` to `Job`; new `NewProfileJob()` constructor and `IsLastProfile()` predicate |
| `internal/config/config.go` | Added `AdminAPIKey` and `PostgresDSN` fields (read from env) |
| `internal/models/models_phase5_test.go` | 9 new unit tests |
| `internal/queue/job_phase5_test.go` | 6 new unit tests |

---

### Stage 5.2 — ContestService and Admin Endpoints ✅

**Gate passed:** `go test ./internal/contest/... ./internal/api/... -race` green.

**What was built:** `internal/contest/` package with `ContestStore` interface, `MemoryContestStore`, `ContestService`, defaults; admin HTTP endpoints; leaderboard deduplication (AD-1); team history endpoint (AD-2); hybrid drain-and-wait auto-close (AD-3).

---

### Stage 5.3 — One-Active-Submission Guard ✅

**Gate passed:** `go test ./internal/submission/... -race` green.

**What was built:** `ContestGetter` interface; `WithContestGetter` builder; guard in `Ingest`; `checkNoActiveSubmission` helper.

---

### Stage 5.4 — Volatility-Aware Scoring ✅

**Gate passed:** `go test ./internal/worker/... -race` green.

**What was built:** profile-aware `buildResults`; `buildFleetConfigFromProfile`; 5 scoring unit tests.

---

### Stage 5.5 — Sequential Three-Job Dispatch ✅

**Gate passed:** `go test ./... -race` green.

**What was built:** `enqueueNextProfile`; `computeAndWriteFinalScore`; `appendProfileResult`; re-enqueue chain in `Execute`.

---

### Stage 5.6 — Dry-Run Validator ✅

**Gate passed:** `go test ./internal/validator/... ./internal/api/... -race` green.

**What was built:** `internal/validator/` package; `scenarios.go` with 20-order fixed sequence; `POST /api/v1/submissions/{id}/validate` with rate limiter; `ValidatorFactory` interface in api layer; 5 unit tests.

---

### Stage 5.7 — SSE Live Leaderboard ✅

**Gate passed:** `go test ./internal/api/... -race` green.

**What was built:** `internal/api/bus.go` (`LeaderboardBus`); `GET /api/v1/leaderboard/stream` SSE endpoint; `runLeaderboardWatcher` goroutine in `cmd/server/main.go` (5s poll, hash-based change detection, zero idle cost when no subscribers); 4 bus unit tests.

**Architectural note:** The worker process isolation dilemma (worker cannot call the in-memory bus directly) was resolved by a store-polling watcher on the control plane side, extending the existing `runQueueDepthScraper` pattern. Zero new infrastructure. Zero changes to the worker.

---

### Stage 5.8 — WebSocket Bot Transport ✅

**Gate passed:** `go test ./internal/botfleet/... ./internal/worker/... -race` green.

**What was built:**

| File | Change |
|---|---|
| `internal/botfleet/bot.go` | Added `Close() error` to `BotTransport` interface; `RESTTransport.Close()` no-op; `Bot.Close()` teardown method; `closeOnce` helper for idempotent close |
| `internal/botfleet/websocket.go` | `WebSocketTransport`: stdlib-only RFC 6455 implementation (no new module deps); client masking; Ping/Pong control frame handling; `closeOnce`-backed `Close()` |
| `internal/botfleet/fleet.go` | `FleetConfig.Protocol` field; `FleetConfig.Validate()` rejects unknown protocols; `Fleet.newBot` selects transport by protocol; `defer bot.Close()` after each bot goroutine |
| `internal/botfleet/websocket_test.go` | 4 unit tests using raw TCP test server (no library) |
| `internal/worker/executor.go` | `targetURLForProtocol` helper; `runFleet` accepts and sets `cfg.Protocol`; `buildFleetConfigFromProfile` and `buildFleetConfig` remain protocol-agnostic (protocol set by runFleet after construction) |

**Design decisions:**

- **Zero new module dependencies.** `WebSocketTransport` is implemented using only `net`, `net/http`, `bufio`, `crypto/sha1`, `encoding/base64`, `encoding/binary`, `encoding/json` — all stdlib. `golang.org/x/net/websocket` (deprecated) and `gorilla/websocket` were both rejected: the former is unmaintained and the latter would add a module dependency to a core package.

- **`Close()` added to `BotTransport` interface.** This is a backward-compatible interface extension: `RESTTransport.Close()` returns nil immediately, so all existing code that constructs a `RESTTransport` and passes it as a `BotTransport` continues to compile and run identically. The Fleet calls `bot.Close()` via `defer` in the goroutine, which in turn calls `transport.Close()`. For REST this is a no-op; for WebSocket it closes the TCP connection.

- **Protocol → target URL mapping in executor.** `targetURLForProtocol` translates `models.ProtocolWebSocket` → `ws://host:port/orders` and everything else → `http://host:port`. This is the single place in the system where the URL scheme is determined. The `/orders` path suffix is included in the WebSocket URL because `WebSocketTransport` uses it during the HTTP upgrade handshake.

- **Protocol set by `runFleet`, not by the config builders.** `buildFleetConfig` and `buildFleetConfigFromProfile` do not set `cfg.Protocol` — they would need to accept a `models.Protocol` parameter and re-export it, creating unnecessary coupling. Instead, `runFleet` sets `cfg.Protocol = string(protocol)` after calling either builder. This is a clean separation: config shape is determined by the profile, protocol is determined by the submission.

- **Backward compatibility.** All 13 existing `internal/botfleet` tests pass without modification. All 13 existing `internal/worker` tests pass without modification. The `FleetConfig.Protocol` field defaults to `""` which `Validate` and `newBot` both treat as `"rest"`.

---

### Phase 5 — Default Volatility Profiles

| Parameter | Low | Medium | High |
|---|---|---|---|
| `BotCount` | 10 | 100 | 1,000 |
| `TestDuration` | 60s | 120s | 180s |
| Order mix (Limit/Market/Cancel) | 80/10/10% | 60/30/10% | 40/40/20% |
| `PriceSpread` (cents) | 100 | 500 | 2,000 |
| `MaxQuantity` | 10 | 100 | 1,000 |
| `TargetP99Ns` | 10ms | 5ms | 1ms |
| `TargetSustainTPS` | 5,000 | 20,000 | 50,000 |
| `LatencyWeight` | 0.20 | 0.35 | 0.50 |
| `ThroughputWeight` | 0.30 | 0.35 | 0.30 |
| `CorrectnessWeight` | 0.50 | 0.30 | 0.20 |

---

## Phase 6 — Frontend

> **Status: 🔲 Planned (after Phase 5 is complete and stable)**

The frontend is a thin React + TypeScript shell. It does **not** rebuild anything Grafana already does.

**Frontend owns:** Login, contest status, submission upload, dry-run trigger + `ValidationResult` display, live leaderboard via SSE, past contest archive.

**Grafana owns (embedded as iframes):** latency/TPS/correctness charts, container health, system health.

---

## Cloud Deployment — GCP (After Phase 6)

> **Status: 🔲 Planned**

The Terraform and CI/CD pipeline are already GCP-specific and production-ready from Phase 4. Deployment after Phase 6 is a configuration exercise, not a development task.

---

## Architecture Diagram

```
  Contestant / Admin
       │
       ▼
  ┌─────────────────────────────────────────────────┐
  │  Control Plane (:8080)                          │
  │  cmd/server                                     │
  │  ├─ POST /api/v1/submissions      (contestant)  │──► Enqueue ──► jobs.benchmark (Redpanda)
  │  ├─ POST /api/v1/submissions/{id}/validate      │──► Validator (in-process)
  │  ├─ GET  /api/v1/leaderboard      (poll)        │◄── ContestService
  │  ├─ GET  /api/v1/leaderboard/stream (SSE)       │◄── LeaderboardBus
  │  ├─ POST /api/v1/admin/contests   (admin)       │──► ContestService
  │  ├─ POST /api/v1/admin/contests/{id}/close      │──► ContestService
  │  └─ GET  /internal/workers                      │◄── WorkerRegistry
  └─────────────────────────────────────────────────┘
         ▲ heartbeat (5s)
  ┌──────┴──────────────────────────────────────────┐
  │  Worker (cmd/worker)                            │
  │  ├─ Heartbeater goroutine                       │
  │  └─ Worker poll loop                            │◄── Dequeue ◄── jobs.benchmark
  │       └─ SandboxExecutor                        │
  │            ├─ Deploy sandbox container          │
  │            ├─ WaitHealthy                       │
  │            ├─ Run BotFleet (REST or WebSocket)  │──► sandbox /orders
  │            │    └─ FleetResult{Stats, Results}  │
  │            ├─ GoldenOrderbook → CorrectnessResult│
  │            ├─ BuildResults (profile-aware score) │
  │            ├─ BatchEmit telemetry               │──► metrics.latency (Redpanda)
  │            └─ Re-enqueue next profile job       │──► jobs.benchmark
  └─────────────────────────────────────────────────┘
```

---

## Running the Stack

```bash
make images          # Build sandbox images (one-time)
docker compose up --build -d   # Start full stack
make test            # Unit tests (no infrastructure required)
make ci              # Full CI gate (mirrors GitHub Actions)
docker compose up --scale worker=3 -d  # Scale workers for load testing
make tf-validate     # Validate Terraform (no cloud credentials needed)
make k8s-validate    # Validate K8s manifests (no cluster needed)
```

---

## Test Coverage

| Package | Tests | Infrastructure |
|---|---|---|
| `internal/queue` | 15 | None |
| `internal/worker` | 18 | None |
| `internal/orchestrator` | 10 | None |
| `internal/botfleet` | 17 (+4 WebSocket) | None (httptest + raw TCP) |
| `internal/correctness` | 13 | None |
| `internal/submission` | existing + 2 | None |
| `internal/contest` | 8 | None (MemoryContestStore) |
| `internal/validator` | 5 | None (httptest.Server) |
| `internal/api` | existing + 4 (bus) | None |
| `internal/telemetry` | existing + 4 | None (unit) / Redpanda (integration) |
| `internal/models` (phase 5) | +15 | None |
| `internal/auth` | planned Phase 6 | None |

All unit tests run in < 5 seconds total. Race detector enabled on every `make test` run.
