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
| Phase 5 | Advanced Benchmarking | 🔄 In Progress (Stage 5.1 ✅) |
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
> Stage 5.1 ✅ complete. Next: Stage 5.2 (ContestService + admin endpoints).
> See TASK.md for the full stage plan.

### Goal

Add the contest lifecycle, volatility-aware scoring, per-submission submission
guard, dry-run validator, SSE live leaderboard, and WebSocket bot transport —
without breaking any existing functionality.

### What NexusBench Does After Phase 5

The platform evolves from "run one benchmark and show a score" to "run a timed
competitive contest with three volatility environments, a live leaderboard, and
a safe pre-submission validation path."

---

### Stage 5.1 — Data Model Additions ✅

**Gate passed:** `go build ./...` and `go test ./internal/models/... ./internal/queue/... -v -race` both green.

**What was built:**

| File | Change |
|---|---|
| `internal/models/models.go` | `ContestStatus`, `VolatilityProfile`, `Contest`, sentinel errors; extended `Submission`, `BenchmarkResults`, `LeaderboardEntry`; added `IsTerminal()`, `ResultByLabel()`, `ProfileByLabel()` |
| `internal/queue/job.go` | Added `ContestID`, `VolatilityLabel`, `RemainingProfiles` to `Job`; new `NewProfileJob()` constructor and `IsLastProfile()` predicate |
| `internal/config/config.go` | Added `AdminAPIKey` and `PostgresDSN` fields (read from env) |
| `internal/models/models_phase5_test.go` | 9 new unit tests: `IsTerminal`, `ResultByLabel`, `ProfileByLabel`, sentinel error identity |
| `internal/queue/job_phase5_test.go` | 6 new unit tests: `NewProfileJob` correctness, slice immutability, ID format, JSON round-trip |

**Backward compatibility:** all Phase 1–4 fields (`Results`, `CompositeScore`, `P99LatencyMs`, `MaxTPS`, `CorrectnessScore`) retained. Zero existing tests broken.

**Design decisions enforced:**
- `models` still imports nothing from other internal packages.
- `NewProfileJob` defensive-copies `remaining` so caller mutations cannot corrupt in-flight job state.
- `SubmissionStatus.IsTerminal()` is the single authoritative definition of which statuses unblock resubmission — used by the one-active-submission guard in Stage 5.3.
- The three sentinel errors live in `models` so any package can check them with `errors.Is` without creating an import cycle.

---

### 5.1 — Contest Model and Lifecycle (Specification Reference)

#### What it is

A `Contest` is the top-level entity that governs everything during an active
hackathon run. There is exactly **one active contest at a time**. All
submissions, benchmark runs, and leaderboard entries are scoped to it.

#### Data model additions to `internal/models`

```go
// ContestStatus is the lifecycle state of a contest.
type ContestStatus string

const (
    ContestStatusDraft    ContestStatus = "draft"
    ContestStatusActive   ContestStatus = "active"
    ContestStatusClosed   ContestStatus = "closed"
)

// VolatilityProfile configures one benchmark run within a contest.
// The admin sets these when creating the contest.
type VolatilityProfile struct {
    Label          string        // "low" | "medium" | "high"
    BotCount       int           // number of concurrent bots
    TestDuration   time.Duration // how long the fleet runs
    MarketDataPath string        // path to historical data segment on shared PVC
    OrderRatios    OrderRatios   // Limit/Market/Cancel fractions (must sum to 1.0)
    PriceSpread    int64         // max deviation from mid-price in cents
    MaxQuantity    int64         // max order size in units
    TargetP99Ns    int64         // scoring ceiling: p99 at or below this → normP99=1.0
    TargetMaxTPS   float64       // scoring ceiling: TPS at or above this → normTPS=1.0
    // Scoring weights for this profile (must sum to 1.0)
    LatencyWeight     float64
    ThroughputWeight  float64
    CorrectnessWeight float64
}

// Contest is the top-level contest entity.
type Contest struct {
    ID                 string            `json:"id"`
    Name               string            `json:"name"`
    Status             ContestStatus     `json:"status"`
    Profiles           [3]VolatilityProfile `json:"profiles"` // [0]=low [1]=medium [2]=high
    // Aggregate weights for final leaderboard score
    LowWeight          float64           `json:"low_weight"`    // default 0.20
    MediumWeight       float64           `json:"medium_weight"` // default 0.35
    HighWeight         float64           `json:"high_weight"`   // default 0.45
    SubmissionsClosedAt *time.Time       `json:"submissions_closed_at,omitempty"`
    ContestClosedAt    *time.Time        `json:"contest_closed_at,omitempty"`
    EndsAt             *time.Time        `json:"ends_at,omitempty"` // nil = manual close only
    CreatedAt          time.Time         `json:"created_at"`
    UpdatedAt          time.Time         `json:"updated_at"`
}
```

`Submission` gains one field: `ContestID string`. `Job` gains `ContestID string`
and `VolatilityLabel string` (which profile this job runs).

`BenchmarkResults` gains `VolatilityLabel string` and
`RunScore float64` (per-profile score before weighting).

`LeaderboardEntry` gains `LowScore`, `MediumScore`, `HighScore`, `FinalScore`
and replaces the single `CompositeScore` with these four columns.

#### Contest store

A new `ContestStore` interface in `internal/contest`:

```go
type ContestStore interface {
    Save(c *models.Contest) error
    Get(id string) (*models.Contest, error)
    GetActive() (*models.Contest, error) // returns ErrNoActiveContest if none
    List() ([]*models.Contest, error)
    Update(c *models.Contest) error
    // SnapshotLeaderboard persists the final ranked leaderboard JSON for
    // archival. Called exactly once when a contest closes.
    SnapshotLeaderboard(contestID string, entries []*models.LeaderboardEntry) error
    GetLeaderboardSnapshot(contestID string) ([]*models.LeaderboardEntry, error)
}
```

Implementation: `PostgresContestStore` backed by two tables — `contests` and
`contest_leaderboard_snapshots`. A `MemoryContestStore` for tests.

The `internal/contest` package owns the store, service, and all contest
lifecycle logic. It has no imports from `submission` or `worker`.

#### Admin endpoints added to `internal/api`

```
POST   /api/v1/admin/contests                     create contest (draft)
POST   /api/v1/admin/contests/{id}/activate       start contest
POST   /api/v1/admin/contests/{id}/close          force-close contest
GET    /api/v1/admin/contests                     list all past contests
GET    /api/v1/admin/contests/{id}/leaderboard    get archived leaderboard
```

All admin routes are gated by `adminAuthMiddleware` which checks
`Authorization: Bearer <ADMIN_API_KEY>` against the env var from `config.Config`.

#### Auto-close behaviour

A background goroutine in `cmd/server/main.go` ticks every 30 seconds. If the
active contest has `EndsAt` set and `time.Now()` is past it, the goroutine
calls `contestService.Close()`.

On close:
1. Contest status → `closed`, `ContestClosedAt` set.
2. All jobs in the queue whose `ContestID` matches are not re-enqueued after
   their worker finishes. Workers check `ContestClosed` before writing results:
   if the contest is already closed, they stop the container, discard the
   `BenchmarkResults`, and mark the submission `StatusFailed` with message
   `"contest closed before benchmark completed"`.
3. Final leaderboard is computed from all `StatusCompleted` submissions in this
   contest and written to `contest_leaderboard_snapshots`.
4. An SSE broadcast fires with `{type: "leaderboard_frozen", contest_id: "..."}`.

`SubmissionsClosedAt` is a separate, earlier timestamp after which `Ingest`
rejects new uploads with `409` and `"CONTEST_SUBMISSIONS_CLOSED"`.

---

### 5.2 — One-Active-Submission Rule

A contestant cannot have two submissions in-flight simultaneously within the
same contest. This prevents queue flooding.

**Implementation:** in `Service.Ingest` in `internal/submission/service.go`,
before enqueuing, call `store.List()` and iterate. If any submission from the
same `TeamName` with the same `ContestID` has a non-terminal status (`pending`,
`building`, `deploying`, `running`, `benchmarking`), return:

```go
return nil, ErrSubmissionInProgress // maps to HTTP 409
```

This is a single loop over the store, around 10 new lines of code.

---

### 5.3 — Volatility-Aware Scoring

This replaces the hardcoded `buildResults` constants in `internal/worker/executor.go`.

#### Per-run score (one `VolatilityProfile`)

```
normP99 = clamp(1 - (P99ns - profile.TargetP99Ns) / (10×TargetP99Ns - TargetP99Ns), 0, 1)
normTPS = clamp(SustainedTPS / profile.TargetMaxTPS, 0, 1)

RunScore = correctness × (profile.LatencyWeight × normP99 + profile.ThroughputWeight × normTPS)
         + profile.CorrectnessWeight × correctness
```

Correctness acts as a multiplier: an engine with 0% correct fills scores zero
on latency and throughput too, regardless of raw numbers. This is the right
behaviour because a fast-but-wrong matching engine is not a matching engine.

SustainedTPS (full-run average) is used for normTPS, not MaxTPS (100ms burst).
MaxTPS is recorded for display but not used in scoring.

#### Default profile parameters

| Parameter | Low | Medium | High |
|---|---|---|---|
| `BotCount` | 10 | 100 | 1,000 |
| `TestDuration` | 60s | 120s | 180s |
| Order mix (Limit/Market/Cancel) | 80/10/10% | 60/30/10% | 40/40/20% |
| `PriceSpread` (cents) | 100 | 500 | 2,000 |
| `MaxQuantity` | 10 | 100 | 1,000 |
| `TargetP99Ns` | 10ms | 5ms | 1ms |
| `TargetMaxTPS` | 5,000 | 20,000 | 50,000 |
| `LatencyWeight` | 0.20 | 0.35 | 0.50 |
| `ThroughputWeight` | 0.30 | 0.35 | 0.30 |
| `CorrectnessWeight` | 0.50 | 0.30 | 0.20 |

#### Final leaderboard score

```
FinalScore = (contest.LowWeight × LowRunScore)
           + (contest.MediumWeight × MediumRunScore)
           + (contest.HighWeight × HighRunScore)
FinalScore × 100  // scaled 0–100
```

Default weights: Low=0.20, Medium=0.35, High=0.45.

A failed run (engine crash, contest closed mid-run) contributes 0.0 to the
weighted sum. It does not void the other two runs.

#### Sequential job dispatch

`Service.Ingest` dispatches exactly **three jobs** per submission — one per
profile — but enqueues them sequentially: job[1] is enqueued only after
job[0] is committed by a worker. This prevents concurrent jobs from the same
submission hitting the same sandbox container.

Implementation: the worker, after committing job[i], checks whether a
`next_profile` field is set on the job. If so, it enqueues the next job via
the control plane's internal queue. Alternatively, a `JobChain` field on `Job`
carries the remaining profiles — the worker pops the first and re-enqueues the
rest after committing.

The `FinalScore` aggregation runs in `ContestService` when all three
`BenchmarkResults` for a submission are written, computing the weighted sum and
updating the `LeaderboardEntry`.

---

### 5.4 — Dry-Run Validator

A pre-submission smoke test that catches wiring errors before the contestant
burns a benchmark slot.

#### What it tests

A fixed, deterministic sequence of exactly 20 orders with a fixed RNG seed:

| # | Scenario | Expected outcome |
|---|---|---|
| 1 | Limit buy at $100.00, qty 10 | Rests in book (no sell side yet) |
| 2 | Limit sell at $101.00, qty 10 | Rests in book (above best bid) |
| 3 | Limit sell at $100.00, qty 5 | Crosses buy → partial fill both sides |
| 4 | Market buy, qty 20 | Sweeps remaining sell side |
| 5 | Cancel of order from scenario 2 | Accepted (order is resting) |
| 6–15 | Mix of limit/market/cancel orders | Various correctness checks |
| 16–20 | Cancel of unknown IDs | All must be rejected (`accepted: false`) |

The result is a `ValidationResult` with per-scenario `{passed bool, reason string}`.
No `BenchmarkResults` written. No leaderboard entry. Submission status unchanged.

#### Implementation

New endpoint: `POST /api/v1/submissions/{id}/validate`

The handler:
1. Checks the submission exists and is not currently `StatusBenchmarking`
   (returns `409` if so).
2. Checks the rate limit: one validation per submission per 2 minutes
   (tracked in-memory with a `sync.Map[submissionID → lastValidatedAt]`).
3. Creates a `SandboxExecutor` with `WithFleetConfig` override:
   `BotCount=1, TestDuration=10s, Seed=42 (fixed)`.
4. Runs the fixed order sequence through `RESTTransport`, collecting fills.
5. Compares against the `GoldenOrderbook` output for the same sequence.
6. Returns `ValidationResult` — per-scenario pass/fail, no aggregated score.

The sandbox container is already deployed (the submission is in `StatusRunning`
or later). The validate endpoint talks to the live container — it does not
re-deploy.

`internal/validator` is a new, narrow package:

```go
// Package validator runs a fixed deterministic smoke test against a deployed
// contestant engine and returns per-scenario pass/fail results.
// It has no side effects: it does not modify submission status, does not write
// BenchmarkResults, and does not touch the leaderboard.
package validator

type ScenarioResult struct {
    Name   string `json:"name"`
    Passed bool   `json:"passed"`
    Reason string `json:"reason,omitempty"`
}

type ValidationResult struct {
    SubmissionID string           `json:"submission_id"`
    Scenarios    []ScenarioResult `json:"scenarios"`
    AllPassed    bool             `json:"all_passed"`
    TestedAt     time.Time        `json:"tested_at"`
}

type Validator struct{ /* unexported fields */ }

func New(transport botfleet.BotTransport) *Validator
func (v *Validator) Run(ctx context.Context, submissionID string) (*ValidationResult, error)
```

The `Validator` depends only on `botfleet.BotTransport` and
`correctness.GoldenOrderbook`. It imports nothing from `submission`, `worker`,
or `contest`.

---

### 5.5 — SSE Live Leaderboard

The polling `GET /api/v1/leaderboard` endpoint is joined by a push endpoint.

#### New endpoint

```
GET /api/v1/leaderboard/stream   (text/event-stream)
```

The handler upgrades to SSE and subscribes to the leaderboard broadcast bus.

#### Broadcast bus

A new unexported `leaderboardBus` in `internal/api` — a simple fan-out
broadcaster:

```go
type leaderboardBus struct {
    mu   sync.RWMutex
    subs map[string]chan LeaderboardEvent // key = subscriber UUID
}

type LeaderboardEvent struct {
    Type    string                  `json:"type"` // "update" | "frozen"
    Entries []*models.LeaderboardEntry `json:"entries"`
    ContestID string               `json:"contest_id"`
}
```

The bus is created once in `cmd/server/main.go` and passed to both the API
router and the contest service. When `ContestService` writes a final score for
any submission, it calls `bus.Broadcast(event)`. When a contest closes,
`bus.Broadcast` fires with `{type: "frozen"}`.

The SSE handler subscribes on connect, writes events as `data: <json>\n\n`,
and unsubscribes on client disconnect (via `ctx.Done()`).

No external library needed — SSE is a long-lived HTTP response with
`Content-Type: text/event-stream` and `\n\n`-separated JSON events.

#### Leaderboard columns (updated `LeaderboardEntry`)

| Column | Description |
|---|---|
| `rank` | Rank by `FinalScore` descending |
| `team_name` | Team name |
| `language` | Submission language |
| `final_score` | Weighted aggregate 0–100 |
| `low_score` | Per-profile RunScore |
| `medium_score` | Per-profile RunScore |
| `high_score` | Per-profile RunScore |
| `best_p99_ms` | Lowest p99 across all completed runs |
| `peak_sustained_tps` | Highest sustained TPS across all runs |
| `avg_correctness` | Mean correctness across completed runs |
| `completed_at` | Time of last completed run |

---

### 5.6 — WebSocket Bot Transport

A `WebSocketTransport` implementing the existing `BotTransport` interface in
`internal/botfleet`.

```go
// WebSocketTransport implements BotTransport over a persistent WebSocket
// connection. One WebSocketTransport instance is used per Bot — each bot
// owns its own connection. The connection is established once in NewBot and
// reused for the lifetime of the benchmark run.
//
// Wire protocol (JSON):
//   send:    {"order_id":"...","kind":"...","side":"...","price":0,"quantity":0}
//   receive: {"order_id":"...","accepted":true,"executed_price":0,"executed_qty":0}
type WebSocketTransport struct { /* unexported */ }

func NewWebSocketTransport(url string) (*WebSocketTransport, error)
func (t *WebSocketTransport) Send(ctx context.Context, o Order) (Fill, error)
func (t *WebSocketTransport) Close() error
```

`BotTransport` gains a `Close() error` method so both `RESTTransport` and
`WebSocketTransport` can be cleanly shut down after a fleet run. The `Fleet`
calls `Close()` on each transport after `Run` returns.

`Fleet.Run` selects the transport based on `FleetConfig.Protocol`:
- `models.ProtocolREST` → `RESTTransport` (existing)
- `models.ProtocolWebSocket` → `WebSocketTransport` (new)

No changes to the `Bot`, `OrderGenerator`, `ComputeStats`, or
`GoldenOrderbook` — the transport is the only variation point.

---

### Phase 5 — Default Volatility Profiles

The admin can override any of these when creating a contest. These are the
defaults if no override is provided:

```go
func DefaultLowProfile() models.VolatilityProfile {
    return models.VolatilityProfile{
        Label: "low", BotCount: 10, TestDuration: 60 * time.Second,
        OrderRatios: botfleet.OrderRatios{Limit: 0.80, Market: 0.10, Cancel: 0.10},
        PriceSpread: 100, MaxQuantity: 10,
        TargetP99Ns: 10_000_000, TargetMaxTPS: 5_000,
        LatencyWeight: 0.20, ThroughputWeight: 0.30, CorrectnessWeight: 0.50,
    }
}

func DefaultMediumProfile() models.VolatilityProfile { /* medium params */ }
func DefaultHighProfile() models.VolatilityProfile   { /* high params */ }
```

These live in `internal/contest/defaults.go` and are used by
`ContestService.CreateWithDefaults`.

---

## Phase 6 — Frontend

> **Status: 🔲 Planned (after Phase 5 is complete and stable)**

### Boundary: what the frontend owns vs what Grafana owns

The frontend is a thin React + TypeScript shell. It does **not** rebuild
anything Grafana already does.

**Frontend owns:**
- Login (admin token, contestant team token)
- Contest status display (active/closed, time remaining, volatility profiles)
- Submission upload form (language, protocol, file)
- Dry-run trigger and `ValidationResult` display (per-scenario pass/fail)
- Live leaderboard via SSE (`/api/v1/leaderboard/stream`)
- Past contest leaderboard archive (read from PostgreSQL snapshot)

**Grafana owns (embedded as iframes):**
- p50/p90/p99 latency charts over time
- TPS over time
- Container health (CPU, memory, restart count)
- System health (node CPU, node memory)
- Correctness breakdown (correct vs incorrect fills over time)

Grafana panel iframes are scoped per-submission using `var-submission_id` URL
parameter so contestants see only their own metrics.

### Auth model

**Admin:** single `ADMIN_API_KEY` env var. One `adminAuthMiddleware` in
`internal/api/router.go` checks `Authorization: Bearer <key>`. Approximately
15 lines.

**Contestants:** team UUID token stored in PostgreSQL `teams` table
(`id, name, token, created_at`). One `contestantAuthMiddleware`. No passwords,
no sessions, no OAuth. The contestant registers once; the platform returns
their team token.

### New packages

`internal/auth` — middleware constructors and token validation. No business
logic. Depends only on `models` and `config`.

### Deployment

- `frontend/` — React + TypeScript source, built to `frontend/dist/`
- `docker/frontend/Dockerfile` — nginx serving `frontend/dist/`
- Added as a service to `docker-compose.yml`
- Added to `k8s/frontend/` (deployment + service + ingress route)
- GitHub Actions matrix gains a `frontend` image build job

---

## Cloud Deployment — GCP (After Phase 6)

> **Status: 🔲 Planned**

The Terraform and CI/CD pipeline are already GCP-specific and production-ready
from Phase 4. Deployment after Phase 6 is a configuration exercise, not a
development task.

### Decision: stay on GCP

AWS migration would require rewriting all three Terraform modules and the
GitHub Actions deploy workflow (GCP-specific providers, Workload Identity
Federation, Artifact Registry, GKE credentials). This is 1.5–2 days of work
on already-working infrastructure, immediately before a deadline. Deferred
to post-hackathon.

### What is needed (one-time, ~2-3 hours total)

1. Create GCP project, enable billing, enable `container.googleapis.com`
   and `artifactregistry.googleapis.com` APIs.
2. `terraform apply -var-file=envs/prod.tfvars -var="project_id=<your-project>"`
   — provisions VPC, GKE cluster, Artifact Registry (~10 minutes).
3. `terraform output kubeconfig_command | bash` — configures kubectl.
4. `kubectl create secret generic nexusbench-secrets --from-literal=POSTGRES_PASSWORD=<pw>`
5. Set GitHub Actions secrets from `terraform output` values:
   `GCP_WORKLOAD_IDENTITY_PROVIDER`, `GCP_SERVICE_ACCOUNT`, `GKE_CLUSTER_NAME`,
   `GKE_CLUSTER_REGION`, `REGISTRY`.
6. Flip `deletion_protection = true` in `modules/cluster/main.tf`.
7. Replace `master_authorized_cidr_blocks = "0.0.0.0/0"` with real CIDRs in
   `prod.tfvars`.
8. Push to `main` — CI/CD builds images, pushes to Artifact Registry, deploys
   to GKE, runs smoke test.

### Estimated cost

~$10–15/day during active development, ~$150–200 for the full contest window
(2–4 weeks). Spot workers scale to minimum between contest runs. GCP for
Startups or Google Cloud research credits can cover this entirely.

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
  │  ├─ GET  /api/v1/leaderboard/stream (SSE)       │◄── leaderboardBus
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
  │            ├─ Run BotFleet (N bots, 1 profile)  │──► sandbox /orders
  │            │    └─ FleetResult{Stats, Results}  │
  │            ├─ GoldenOrderbook → CorrectnessResult│
  │            ├─ BuildResults (profile-aware score) │
  │            ├─ BatchEmit telemetry               │──► metrics.latency (Redpanda)
  │            └─ Re-enqueue next profile job       │──► jobs.benchmark
  └─────────────────────────────────────────────────┘
         │ writes BenchmarkResults
         ▼
  ┌──────────────────────┐    ┌────────────────────────────────┐
  │  DiskStore           │    │  PostgreSQL                    │
  │  /data/submissions/  │    │  ├─ contests                   │
  │  {id}/meta.json      │    │  ├─ teams                      │
  └──────────────────────┘    │  └─ contest_leaderboard_snapshots│
                              └────────────────────────────────┘
                                       │ consumer reads
  ┌────────────────────────────────────▼───────────────────────┐
  │  Consumer                                                  │
  │  metrics.latency → TimescaleDB → Grafana dashboard         │
  └────────────────────────────────────────────────────────────┘

  Kubernetes (GKE):
  ┌──────────────────────────────────────────────────────────┐
  │  GKE Cluster                                             │
  │  ├─ Deployment: control-plane  (1 replica)              │
  │  ├─ Deployment: metrics-consumer (1 replica)            │
  │  ├─ Deployment: worker  ──► KEDA HPA (queue depth)      │
  │  │       min=1  max=10  lagThreshold=5                   │
  │  ├─ StatefulSet: Redpanda (1 dev / 3 prod replicas)     │
  │  ├─ StatefulSet: TimescaleDB (1 replica, PVC)           │
  │  ├─ StatefulSet: PostgreSQL (1 replica, PVC)            │
  │  ├─ PVC: submissions-data (50Gi)                        │
  │  └─ Ingress: NGINX → control-plane :8080                │
  └──────────────────────────────────────────────────────────┘
```

---

## Running the Stack

```bash
# Build sandbox images (one-time)
make images

# Start full stack
docker compose up --build -d

# Run unit tests (no infrastructure required)
make test

# Run full CI gate (mirrors GitHub Actions)
make ci

# Scale workers for local load testing
docker compose up --scale worker=3 -d

# Validate Terraform (no cloud credentials needed)
make tf-validate

# Validate K8s manifests (no cluster needed)
make k8s-validate
```

---

## Test Coverage

| Package | Tests | Infrastructure |
|---|---|---|
| `internal/queue` | 9 | None |
| `internal/worker` | 13 | None |
| `internal/orchestrator` | 10 | None |
| `internal/botfleet` | 12 | None (httptest.Server) |
| `internal/correctness` | 13 | None |
| `internal/submission` | existing | None |
| `internal/telemetry` | existing + 4 | None (unit) / Redpanda (integration) |
| `internal/models` (phase 5) | +15 (9 models + 6 queue) | None |
| `internal/contest` | planned Stage 5.2 | None (MemoryContestStore) |
| `internal/validator` | planned Stage 5.6 | None (httptest.Server) |
| `internal/auth` | planned Phase 6 | None |

All unit tests run in < 5 seconds total. Race detector enabled on every
`make test` run. Every new package in Phase 5 must maintain this standard.
