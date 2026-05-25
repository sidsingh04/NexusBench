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

## Phases Completed

### Phase 1 — Core MVP ✅

**Goal:** upload algo → run in container → replay data → show metrics

**What was built:**

- `internal/models` — core domain types: `Submission`, `BenchmarkResults`, `LeaderboardEntry`, all lifecycle statuses (`pending` → `deploying` → `running` → `benchmarking` → `completed` / `failed`)
- `internal/config` — single `Config` struct loaded from environment variables; `ImageForLanguage`, `AllImages` helpers
- `internal/sandbox` — `DockerManager`: deploys contestant code into isolated containers with cgroup CPU pinning, memory limits, capability dropping, bind-mount of the submission archive, and port allocation from a configurable pool
- `internal/submission` — `Service` + `DiskStore`: validates uploads, stores archives on disk, orchestrates container lifecycle; `Store` interface for testability
- `internal/api` — HTTP router (gorilla/mux): `POST /api/v1/submissions`, `GET /api/v1/submissions/{id}`, `GET /api/v1/leaderboard`, `GET /health`, `GET /metrics`
- `cmd/server` — control plane binary
- `docker/sandbox/` — five Dockerfile variants: `go`, `rust`, `cpp`, `python`, `binary`; each extracts the archive and runs the engine on port 7878

### Phase 2 — Telemetry ✅

**Goal:** live metrics → dashboard → logs

**What was built:**

- `internal/telemetry` — `Event` type (kind, submission ID, timestamp, latency ns, meta), `Emitter` interface, `StdoutEmitter`, `RedpandaEmitter` (franz-go, AllISRAcks, idempotent production), `RecordingEmitter` (tests), `NoopEmitter`; topic layout: `metrics.latency`, `metrics.heartbeat`, `metrics.dlq`
- `internal/consumer` — `Consumer` polls `metrics.latency` from Redpanda, writes rows to TimescaleDB via `pgxpool`; `PercentileStore` computes p50/p90/p99 from the time-series table
- `internal/metrics` — Prometheus `Registry`: HTTP request counter + duration histogram; `RecordHTTPRequest` on every route
- `docker-compose.yml` — full observability stack: Redpanda + Console, TimescaleDB, Prometheus, Grafana, Loki, Promtail, cAdvisor, Node Exporter
- `cmd/consumer` — metrics consumer binary
- Grafana dashboards provisioned automatically on startup

### Phase 3 — Distributed Workers (in progress)

**Goal:** multiple benchmark nodes + scheduler

---

## Phase 3 Detail

### Stage 3.1 — Worker Abstraction + Job Queue ✅

**Core insight:** extract "run a benchmark" from a monolithic in-process call into a self-contained `Job` that travels over a durable queue to whichever worker is free.

**Files created:**

| File | Purpose |
|---|---|
| `internal/queue/job.go` | `Job` type (self-contained snapshot of Submission) + `Queue` interface (Enqueue / Dequeue / CommitJob / Close) |
| `internal/queue/memory.go` | `MemoryQueue` — in-process fake; used by all unit tests |
| `internal/queue/redpanda.go` | `RedpandaQueue` — durable production transport on `jobs.benchmark` topic; separate producer/consumer clients; manual offset commit for at-least-once delivery |
| `internal/worker/worker.go` | `Worker` poll loop; `Store` + `Executor` interfaces; idempotent guard (checks submission status before executing); at-least-once commit discipline |
| `internal/worker/executor.go` | `SandboxExecutor` — deploys sandbox via `sandboxDeployer` interface (satisfied by `*sandbox.DockerManager`), waits for health; Stage 3.1 returns stub results |
| `internal/worker/worker_test.go` | 8 unit tests: happy path, idempotent skip, executor failure, offset commit on success/failure, context cancel, store error does not commit, nil dep validation |
| `internal/worker/executor_test.go` | 5 unit tests: stub results, deploy error, store load error, container always stopped on error, ctx cancel during health poll |
| `cmd/worker/main.go` | Worker binary entrypoint |
| `cmd/smokecheck/main.go` | CLI tool using `kadm.ListEndOffsets` to verify topic watermark; used by smoke tests |

**Files modified (backward-compatible):**

| File | Change |
|---|---|
| `internal/config/config.go` | Added `DistributedMode`, `RedpandaBrokers`, `WorkerID`, `JobTimeout`, `OrchestratorURL`; added `getEnvBool`, `getEnvStringSlice`, `hostname()` |
| `internal/submission/service.go` | Added `jobQueue queue.Queue` field + `WithQueue()` method; `Ingest` dispatches to queue when non-nil, preserving Phase 1/2 path when nil |
| `Dockerfile.server` | Builds three binaries: `server`, `consumer`, `worker` |
| `docker-compose.yml` | Added `DISTRIBUTED_MODE=true` to control-plane; added `worker` service |
| `Makefile` | Added `run-worker`, `test-queue`, `test-worker` targets |

**Key design decisions:**

- **Redpanda as job queue** (not Redis): no new dependency, same broker already used for telemetry, gives at-least-once delivery via consumer groups, partition-keyed by submission ID
- **`Queue` interface is deep**: 4 methods hide all transport complexity; `MemoryQueue` lets all worker tests run in milliseconds with zero infrastructure
- **No circular imports**: `worker.Store` is defined in the `worker` package (mirrors `submission.Store`) so `worker` never imports `submission`
- **`sandboxDeployer` interface**: `SandboxExecutor` accepts an interface, not `*sandbox.DockerManager`; satisfied at the `cmd/worker` call site — tests inject a fake with no Docker

**Smoke test:** `scripts/smoke_test_phase3_stage1.sh`
- Steps 1–4 offline: compile, unit tests, binary builds, full test suite
- Steps 5–6 online: Redpanda reachable, topic exists, watermark increases after submission

---

### Stage 3.2 — Orchestrator + Worker Heartbeat ✅

**Core insight:** the queue handles job delivery; the orchestrator handles *fleet visibility*. These are separate concerns. Workers register with the orchestrator on startup and send heartbeats every 5 seconds. The orchestrator marks workers dead after 15 seconds with no heartbeat, surfacing fleet health via the API without requiring any changes to the Redpanda at-least-once delivery mechanism.

**Files created:**

| File | Purpose |
|---|---|
| `internal/orchestrator/registry.go` | `WorkerRegistry` — goroutine-safe in-memory map of `workerID → WorkerRecord`; `Register`, `Heartbeat`, `List`, `Get`, `Stats`; TTL-based dead detection in `List` |
| `internal/orchestrator/handler.go` | HTTP handlers for worker fleet routes; exported `HTTPRegister`, `HTTPHeartbeat`, `HTTPList`, `HTTPStats` methods for gorilla/mux integration |
| `internal/orchestrator/registry_test.go` | 10 unit tests: register, re-register resets state, empty ID error, heartbeat updates, heartbeat unknown worker, list marks dead, list alive, stats counts, get returns copy, concurrent heartbeats (race detector) |
| `internal/worker/heartbeat.go` | `Heartbeater` — background goroutine; registers on startup, pings every 5s, auto-re-registers on 404; exported `HeartbeatStatus` type decouples worker from orchestrator package |
| `scripts/smoke_test_phase3_stage2.sh` | 6-step smoke test: offline compile/test + online route check + worker registers + stays alive after 2 intervals |

**Files modified:**

| File | Change |
|---|---|
| `internal/api/router.go` | `NewRouter` accepts `*orchestrator.Handler` (nil-safe); mounts `/internal/workers/*` routes only in distributed mode |
| `internal/worker/executor.go` | Replaced `WithHealthPollInterval` method with functional options pattern (`ExecutorOption`); added `WithJobCallbacks(onStart, onFinish)` so heartbeater tracks busy/idle state |
| `cmd/server/main.go` | Constructs `WorkerRegistry` + `orchestrator.Handler`; passes handler to `NewRouter` |
| `cmd/worker/main.go` | Wires `Heartbeater` + status callbacks via atomics; starts heartbeater goroutine alongside worker poll loop |
| `internal/config/config.go` | Added `OrchestratorURL` field + env var parsing |
| `docker-compose.yml` | Added `ORCHESTRATOR_URL` to worker service; added `control-plane` health dependency for worker |
| `Makefile` | Added `test-orchestrator` target |

**Key design decisions:**

- **Orchestrator does not dispatch jobs** — that is the queue's responsibility. It only tracks liveness, decoupling fleet visibility from delivery semantics
- **No cycle between `worker` and `orchestrator`**: `heartbeat.go` defines its own `heartbeatPayload` struct (matching `orchestrator.HeartbeatUpdate` JSON tags) rather than importing the orchestrator package
- **Nil-safe router**: `orchHandler == nil` in local mode → zero overhead, zero routes mounted, Phase 1/2 completely unchanged
- **Status via atomics**: `workerBusy` is `atomic.Int32`, `currentJobID` guarded by `sync.RWMutex` — no lock contention between the poll loop and the heartbeat ticker

---

### Stage 3.3 — Real Distributed Bot Fleet + Correctness Engine ✅

**Core insight:** the sandbox executor stub is replaced by a real distributed load generator. N goroutines act as independent trading bots, each running a tight send-loop against the sandbox endpoint. A deterministic golden orderbook validates fills for correctness. Telemetry is batched and streamed to Redpanda after each run.

**Files created:**

| File | Purpose |
|---|---|
| `internal/botfleet/order.go` | Protocol-agnostic `Order`, `Fill`, `OrderResult` types — the atomic data structures flowing through the entire fleet |
| `internal/botfleet/generator.go` | `OrderGenerator` interface + `RandomGenerator`: configurable Limit/Market/Cancel ratios, per-bot seeded RNG, price/quantity ranges |
| `internal/botfleet/bot.go` | `Bot` struct + `RESTTransport`: single bot run-loop, sends orders to sandbox endpoint, records per-order `OrderResult` |
| `internal/botfleet/fleet.go` | `Fleet`: spawns N bots with configurable ramp-up, collects all results via a buffered channel, returns `FleetResult` |
| `internal/botfleet/stats.go` | `ComputeStats`: sort-based p50/p90/p99, 100ms sliding-window MaxTPS, SustainedTPS — stdlib only |
| `internal/botfleet/fleet_test.go` | 12 unit tests: exact percentile assertions, ratio distribution ±10%, goroutine leak check, context cancellation, fleet echo server integration |
| `internal/correctness/orderbook.go` | `GoldenOrderbook`: deterministic price-time priority matching engine — pure in-memory, no randomness, identical fills for identical input sequences |
| `internal/correctness/checker.go` | `Checker`: compares contestant fills vs golden fills by OrderID, computes `CorrectnessResult{Score, TotalFills, CorrectFills, IncorrectFills}` |
| `internal/correctness/checker_test.go` | 13 unit tests: perfect match, zero match, partial match, empty slices, missing/extra fills, all orderbook scenarios |
| `scripts/smoke_test_phase3_stage3.sh` | 7-step smoke test: offline compile/vet/test + online submission lifecycle + concurrent worker assertion |

**Files modified:**

| File | Change |
|---|---|
| `internal/worker/executor.go` | Replaced stub with real fleet: `runFleet` → `Fleet.Run`, `checkCorrectness`, `emitFleetTelemetry` (batched); set `StatusBenchmarking` before fleet; `WithEmitter` functional option |
| `internal/worker/executor_test.go` | Updated tests: `TestSandboxExecutor_ReturnsRealResults` now uses an `httptest.Server` echo endpoint; `miniFleetConfig()` helper; `WithFleetConfig` option for fast 50ms test runs |
| `internal/telemetry/emitter.go` | Added `BatchEmit(ctx, []Event) error` to `Emitter` interface; implemented on `StdoutEmitter`, `NoopEmitter`, `RecordingEmitter` with concurrent-safe batch buffering |
| `internal/telemetry/redpanda.go` | `RedpandaEmitter.BatchEmit`: validates all events, builds `[]*kgo.Record` slice, calls `ProduceSync` once per batch for efficiency |
| `internal/telemetry/event_test.go` | Added 4 `BatchEmit` tests: all-valid batch, skip-invalid with error, concurrent `RecordingEmitter`, empty batch no-op |
| `internal/config/config.go` | Added `BotCount`, `BotTestDuration`, `BotRampUpDuration`, `BotOrderRatio{Limit,Market,Cancel}`, `BotPerRequestTimeout`; `getEnvFloat64` helper |
| `docker-compose.yml` | Added `BOT_COUNT`, `BOT_TEST_DURATION`, `BOT_RAMP_UP_DURATION`, `BOT_ORDER_RATIO_*`, `BOT_PER_REQUEST_TIMEOUT` env vars to `worker` service |

**Key design decisions:**

- **No external deps in botfleet/correctness**: sort-based percentiles (stdlib), no prometheus/opentelemetry in the hot path — these packages can be imported by any future component without dependency bloat
- **Telemetry never blocks results**: `emitFleetTelemetry` runs *after* `buildResults` is computed; errors are logged but not propagated — a Redpanda hiccup cannot fail a benchmark
- **Batching strategy**: 100-event batches amortise lock + network overhead; the final partial batch is always flushed so no events are silently dropped
- **`StatusBenchmarking` lifecycle**: executor explicitly sets this before the fleet starts so the API reflects real in-progress state (not the generic `StatusDeploying`)
- **`WithEmitter` functional option**: keeps `SandboxExecutor` testable without Redpanda; `cmd/worker` wires the real `RedpandaEmitter`; tests use `NoopEmitter` (the default)
- **`RESTTransport` as the default bot transport**: FIX and WebSocket transports implement `BotTransport` in Stage 5 without changing `Bot` or `Fleet`

**Smoke test:** `scripts/smoke_test_phase3_stage3.sh`
- Steps 1–3 offline: botfleet + correctness compile/test, full test suite, go vet, all binaries build
- Steps 4–7 online: control plane health, worker registered, echo server submission reaches `completed` with real metrics, 3 concurrent jobs verified

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
```

---

## Running the Stack

```bash
# Build sandbox images (one-time)
make images

# Start full stack with distributed mode
docker compose up --build -d

# Run Stage 3.1 smoke test
STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage1.sh

# Run Stage 3.2 smoke test
STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage2.sh

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

## What's Next

### Stage 3.4 — Terraform + Kubernetes

- Provision cloud node pool with Terraform (GCP or AWS)
- Kubernetes manifests for control-plane, worker (HPA on Redpanda queue depth), Redpanda operator
- Autoscaling policies: HPA scales worker replicas when `jobs.benchmark` consumer lag grows
- CI/CD pipeline: GitHub Actions workflow that builds sandbox images, runs `make test`, deploys to K8s

### Stage 3.5 — Advanced Benchmarking

- Stress benchmark (volatile-only replay from historical market data via Redpanda)
- Latency injection: artificial delays injected between bot orders to model network jitter
- Chaos engineering: random container kills, network partition simulation
- Pause / Resume / Kill controls on running benchmarks via the API
