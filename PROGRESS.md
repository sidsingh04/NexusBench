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
| Phase 5 | Advanced Benchmarking | ✅ Complete (all 10 stages) |
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
  `LeaderboardEntry`, all lifecycle statuses
- `internal/config` — single `Config` struct loaded from environment variables
- `internal/sandbox` — `DockerManager`: deploys contestant code into isolated containers
- `internal/submission` — `Service` + `DiskStore`: validates uploads, orchestrates lifecycle
- `internal/api` — HTTP router: `POST /api/v1/submissions`, `GET /api/v1/leaderboard`, etc.
- `cmd/server` — control plane binary
- `docker/sandbox/` — five Dockerfile variants: `go`, `rust`, `cpp`, `python`, `binary`

---

### Phase 2 — Telemetry ✅

**Goal:** live metrics → dashboard → logs

**What was built:** `internal/telemetry`, `internal/consumer`, `internal/metrics`,
full observability stack in `docker-compose.yml` (Redpanda, TimescaleDB, Prometheus,
Grafana, Loki, Promtail, cAdvisor, Node Exporter).

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

**Critical bug fixed:** Docker-in-Docker networking via `SANDBOX_HOST=host.docker.internal`.

---

### Phase 4 — Terraform & Infra Automation ✅

**Goal:** provision cloud infrastructure, deploy to Kubernetes, autoscale workers, establish CI/CD.

| Stage | Description | Status |
|-------|-------------|--------|
| 4.1 | Terraform: VPC, GKE cluster, two node pools, Artifact Registry | ✅ |
| 4.2 | Kubernetes manifests: all services, NetworkPolicies, RBAC, PVCs | ✅ |
| 4.3 | KEDA autoscaling on Redpanda consumer-group lag | ✅ |
| 4.4 | GitHub Actions CI/CD: lint + test + validate + build + deploy | ✅ |

---

## Phase 5 — Advanced Benchmarking ✅ COMPLETE

> **Status: ✅ All 10 stages complete.**
> Gate: `make test -race` green, `make smoke-phase5` green.

### Stage overview

| Stage | Description | Status |
|-------|-------------|--------|
| 5.1 | Data model additions (`Contest`, `VolatilityProfile`, sentinel errors) | ✅ |
| 5.2 | ContestService + admin endpoints + leaderboard dedup + SSE bus | ✅ |
| 5.3 | One-active-submission guard | ✅ |
| 5.4 | Volatility-aware scoring | ✅ |
| 5.5 | Sequential three-job dispatch + FinalScore | ✅ |
| 5.6 | Dry-run Validator (20-order fixed sequence, rate limiter) | ✅ |
| 5.7 | SSE live leaderboard (`LeaderboardBus`, store-polling watcher) | ✅ |
| 5.8 | WebSocket `BotTransport` (stdlib RFC 6455, zero new deps) | ✅ |
| 5.9 | PostgreSQL `ContestStore` (`pgxpool`, JSONB profiles, UPSERT snapshot) | ✅ |
| 5.10 | Integration smoke test (`scripts/smoke_test_phase5.sh`) | ✅ |

### What Phase 5 adds

The platform evolves from "run one benchmark and show a score" to a full
competitive contest platform with:

- **Contest lifecycle:** create (draft) → activate → benchmark → auto-close
- **Three sequential volatility runs** per submission (Low / Medium / High),
  each with independently tuned bot counts, order mixes, and scoring targets
- **Volatility-aware composite scoring** where correctness multiplies both
  latency and throughput scores — an engine with 0% correctness scores 0
  regardless of latency
- **One-active-submission guard** per team per contest (HTTP 409 on double-submit)
- **Dry-run validator** — 20-order deterministic smoke test covering 7 correctness
  axes, with a 2-minute rate limiter per submission
- **SSE live leaderboard** — push-based stream delivering `update` events on
  score changes and a `frozen` event when the contest closes
- **WebSocket bot transport** — persistent connection per bot, stdlib RFC 6455
  implementation, zero new module dependencies
- **PostgreSQL ContestStore** — durable contest persistence that survives server
  restarts; `MemoryContestStore` remains for all unit tests

### ⚠️ Important: Strict Contest Mode

`ADMIN_API_KEY: "testkey"` is permanently set in `docker-compose.yml`.
All submissions require an active contest. Legacy scripts that bypass contest
creation will receive `ErrContestNotActive`.

---

### Stage 5.9 — PostgreSQL ContestStore ✅

**Gate passed:** `make test -race` green (unit tests use MemoryContestStore).
Integration: `docker compose up -d postgres && POSTGRES_DSN=... go run ./cmd/server`.

**What was built:**

| File | Change |
|---|---|
| `internal/contest/postgres.go` | `PostgresContestStore` implementing `ContestStore` via `pgxpool`; `migrate()` creates two tables idempotently on startup; JSONB for `VolatilityProfile` columns; UPSERT for `SnapshotLeaderboard` |
| `cmd/server/main.go` | `buildContestStore()` — selects `PostgresContestStore` when `DISTRIBUTED_MODE=true` and `POSTGRES_DSN` is set; graceful fallback to `MemoryContestStore` with a warning; `safeDSNPrefix()` logs host without exposing credentials |
| `docker-compose.yml` | `postgres` service (postgres:16-alpine, port 5433 on host to avoid collision with TimescaleDB); `POSTGRES_DSN` wired into control-plane |
| `k8s/postgres/statefulset.yaml` | PostgreSQL StatefulSet on control-plane node pool; security context identical to TimescaleDB (uid 999, all caps dropped, emptyDir tmpfs mounts) |
| `k8s/postgres/service.yaml` | ClusterIP service on port 5432 (internal only) |
| `k8s/postgres/pvc.yaml` | 10Gi PVC on `standard-rwo` StorageClass |
| `k8s/network-policies/allow-postgres-ingress.yaml` | Allows ingress on 5432 from control-plane only; workers explicitly excluded |

**Design decisions:**

- `pgx/v5` was already in `go.mod` as an indirect dependency of TimescaleDB consumer — zero new modules added.
- `VolatilityProfile` stored as JSONB: avoids a third normalised table, keeps the schema flat and psql-inspectable. `time.Duration` round-trips losslessly through JSON as int64 nanoseconds.
- Failure policy: if Postgres is unreachable at startup, the server logs an error and falls back to `MemoryContestStore` rather than `os.Exit`. Operators who require durable contest state must ensure the database is healthy before starting the server.
- `migrate()` uses `CREATE TABLE IF NOT EXISTS` — idempotent, safe on every startup, no migration tool needed for the hackathon lifecycle.

---

### Stage 5.10 — Integration Smoke Test ✅

**Gate passed:** `bash scripts/smoke_test_phase5.sh --dry-run` green.
Full live gate: `bash scripts/smoke_test_phase5.sh --live` against docker compose.

**What was built:**

| File | Change |
|---|---|
| `scripts/smoke_test_phase5.sh` | Two-mode smoke test (dry-run + live); 5 dry-run sections, 10 live steps |
| `k8s/postgres/statefulset.yaml` | Written in Stage 5.9, verified by smoke test Section 5 |
| `k8s/network-policies/allow-postgres-ingress.yaml` | Written in Stage 5.9, verified by smoke test Section 5 |
| `Makefile` | Added `smoke-phase5`, `smoke-phase5-live`, `test-phase5` targets; `ci` now includes `smoke-phase5` |

**Dry-run sections:**

| Section | What it validates |
|---|---|
| 1 | `go build ./...` + `go vet ./...` |
| 2 | All unit tests per-package with `-race` + full sweep with coverage |
| 3 | `cmd/server`, `cmd/worker`, `cmd/consumer` build cleanly |
| 4 | All Phase 5 API endpoints are registered in `internal/api/router.go` |
| 5 | All new Kubernetes YAML files parse as valid YAML and contain required fields; workers excluded from postgres NetworkPolicy |

**Live steps:**

| Step | What it exercises |
|---|---|
| 1 | Create contest (status=draft) + activate (status=active) |
| 2 | Subscribe to SSE stream; assert initial snapshot event received |
| 3 | Build minimal echo engine binary; submit as `language=binary` |
| 4 | Call dry-run validator; assert 429 on second call (rate limiter) |
| 5 | Poll submission until status=completed (all 3 profiles, up to 5 min) |
| 6 | Assert FinalScore > 0 on the leaderboard |
| 7 | Assert SSE received ≥1 "update" event |
| 8 | Close contest via admin endpoint |
| 9 | Assert leaderboard snapshot has ≥1 entry |
| 10 | Assert SSE received "frozen" event |
| Bonus | Admin 401 without key, 401 with wrong key; Phase 1–4 endpoints still return 200 |

**Smoke engine:** If `ENGINE_BINARY` is not set, the script builds a minimal
Go echo server in a temp directory using `GOOS=linux GOARCH=amd64 go build`.
The engine accepts all limit/market orders with a partial fill and rejects
cancel orders for unknown IDs. It is sufficient to exercise the full Phase 5
pipeline but will not pass all 20 validator scenarios (it does not implement
full CLOB price-time priority). A correctly-implemented contestant engine
should pass all scenarios.

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

> **Status: 🔲 Planned (Phase 5 is now complete)**

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
  ┌─────────────────────────────────────────────────────────┐
  │  Control Plane (:8080)   cmd/server                     │
  │  ├─ POST /api/v1/submissions          (contestant)      │──► Enqueue ──► jobs.benchmark
  │  ├─ POST /api/v1/submissions/{id}/validate              │──► Validator (in-process)
  │  ├─ GET  /api/v1/leaderboard          (poll)            │◄── ContestService
  │  ├─ GET  /api/v1/leaderboard/stream   (SSE)             │◄── LeaderboardBus
  │  ├─ GET  /api/v1/teams/{name}/submissions               │◄── submission.Store
  │  ├─ POST /api/v1/admin/contests       (admin)           │──► ContestService ──► PostgreSQL
  │  ├─ POST /api/v1/admin/contests/{id}/activate           │──► ContestService
  │  ├─ POST /api/v1/admin/contests/{id}/close              │──► ContestService + LeaderboardBus
  │  └─ GET  /internal/workers                              │◄── WorkerRegistry
  └─────────────────────────────────────────────────────────┘
         ▲ heartbeat (5s)
  ┌──────┴──────────────────────────────────────────────────┐
  │  Worker (cmd/worker)                                    │
  │  ├─ Heartbeater goroutine                               │
  │  └─ Worker poll loop                           ◄── Dequeue ◄── jobs.benchmark (Redpanda)
  │       └─ SandboxExecutor                               │
  │            ├─ Deploy sandbox container                  │
  │            ├─ WaitHealthy                               │
  │            ├─ Run BotFleet (REST or WebSocket) ──► sandbox /orders
  │            ├─ GoldenOrderbook → CorrectnessResult       │
  │            ├─ BuildResults (profile-aware scoring)      │
  │            ├─ BatchEmit telemetry          ──► metrics.latency (Redpanda)
  │            ├─ Append to AllResults, persist FinalScore  │
  │            └─ Enqueue next profile job     ──► jobs.benchmark
  └─────────────────────────────────────────────────────────┘
  LeaderboardWatcher (goroutine in cmd/server):
    polls submission.Store every 5s → detects score change → bus.Broadcast("update")
```

---

## Running the Stack

```bash
make images                          # Build sandbox images (one-time)
docker compose up --build -d         # Start full stack (includes postgres)
make test                            # Unit tests (no infrastructure required)
make test-phase5                     # Phase 5 specific unit tests
make smoke-phase5                    # Phase 5 dry-run smoke test (CI-safe)
make smoke-phase5-live               # Phase 5 full live test (requires docker compose)
make ci                              # Full CI gate (lint + test + smoke-phase5 + tf + k8s)
docker compose up --scale worker=3 -d  # Scale workers for load testing
make tf-validate                     # Validate Terraform (no cloud credentials)
make k8s-validate                    # Validate K8s manifests (no cluster)
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

`PostgresContestStore` is integration-tested via `docker compose up -d postgres` + `POSTGRES_DSN=... go run ./cmd/server`. No unit tests for the Postgres implementation — `MemoryContestStore` is the unit-test target; the interface contract is verified by `contest/service_test.go` which both implementations must satisfy.
