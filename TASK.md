# TASK.md — Stage 3.3: Bot Fleet (Real Distributed Load Generator)

> **Status: ✅ COMPLETE**
> All tasks completed. PROGRESS.md updated. See Stage 3.4 for the next phase.

---

## Goal

Replace the `SandboxExecutor` stub with a real distributed bot fleet that:
1. Spawns N concurrent goroutines, each acting as an independent trading bot
2. Sends a mix of Limit Orders, Market Orders, and Cancels to the sandbox endpoint
3. Records per-order round-trip latency, computes p50/p90/p99
4. Validates fills against a Golden Orderbook (correctness score)
5. Writes real `BenchmarkResults` to the store and emits telemetry events

---

## Task Checklist

### 3.3.1 — Bot Fleet Core (`internal/botfleet`)

- [x] Create `internal/botfleet/bot.go`
- [x] Create `internal/botfleet/generator.go`
- [x] Create `internal/botfleet/fleet.go`
- [x] Create `internal/botfleet/stats.go`
- [x] Create `internal/botfleet/fleet_test.go` (12 tests, race-detector clean)

### 3.3.2 — Golden Orderbook + Correctness Engine (`internal/correctness`)

- [x] Create `internal/correctness/orderbook.go`
- [x] Create `internal/correctness/checker.go`
- [x] Create `internal/correctness/checker_test.go` (13 tests)

### 3.3.3 — Wire Bot Fleet into `SandboxExecutor`

- [x] Replace Stage 3.2 stub in `internal/worker/executor.go`
- [x] Update `internal/config/config.go` — bot fleet fields + `getEnvFloat64`
- [x] Update `docker-compose.yml` — `BOT_COUNT`, `BOT_TEST_DURATION`, all bot env vars added to `worker` service

### 3.3.4 — Telemetry Integration

- [x] Add `BatchEmit(ctx, []Event) error` to `telemetry.Emitter` interface
  - Implemented on `StdoutEmitter`, `NoopEmitter`, `RecordingEmitter` (emitter.go)
  - Implemented on `RedpandaEmitter` — single `ProduceSync` call per batch (redpanda.go)
- [x] Add 4 `BatchEmit` unit tests to `internal/telemetry/event_test.go`
- [x] Wire `emitFleetTelemetry` into `SandboxExecutor` — batches of 100, errors logged not propagated
- [x] Set `StatusBenchmarking` before fleet starts, `StatusCompleted`/`StatusFailed` via `worker.go`
- [x] `WithEmitter` functional option on `SandboxExecutor` — `NoopEmitter` default, real emitter injected by `cmd/worker`
- [x] Consumer volume: existing `pgxpool` batch insert + `ON CONFLICT DO NOTHING` is already correct for high-volume bursts — verified no change needed

### 3.3.5 — Smoke Test

- [x] Create `scripts/smoke_test_phase3_stage3.sh`
  - Step 1 (offline): botfleet compile + unit tests with -race
  - Step 2 (offline): correctness compile + unit tests with -race
  - Step 3 (offline): full suite — worker tests, telemetry BatchEmit tests, go vet, all binaries build
  - Step 4 (online): control plane health + worker registered
  - Step 5 (online): submit echo server → poll until `status == "completed"`
  - Step 6 (online): assert p99 > 0, max_tps > 0, correctness_score ∈ [0,1], composite_score > 0
  - Step 7 (online): submit 3 jobs concurrently → assert ≥ 2 in-progress simultaneously

### 3.3.6 — PROGRESS.md Update

- [x] Add Stage 3.3 section to PROGRESS.md with full file map and design decisions
- [x] Update architecture diagram — bot fleet + correctness engine + telemetry pipeline
- [x] Update "What's Next" to Stage 3.4 (Terraform + Kubernetes)
- [x] Update test coverage table (+25 new tests)

---

## Gate Results

| Sub-task | Gate command | Status |
|----------|-------------|--------|
| 3.3.1 | `go test ./internal/botfleet/... -race -v` | ✅ |
| 3.3.2 | `go test ./internal/correctness/... -race -v` | ✅ |
| 3.3.3 | `go test ./internal/worker/... -race -v` | ✅ |
| 3.3.4 | `go test ./internal/telemetry/... -race -v` | ✅ |
| 3.3.5 | `STACK_RUNNING=1 bash scripts/smoke_test_phase3_stage3.sh` | Ready |
| 3.3.6 | PROGRESS.md updated | ✅ |

---

## Key Design Decisions (for future reference)

- **No external deps in botfleet/correctness** — stdlib only. Both packages are safe to import anywhere.
- **Telemetry never blocks results** — `emitFleetTelemetry` runs after `buildResults`; emitter errors are logged, not propagated.
- **Batching at 100 events** — amortises lock and network overhead without holding memory for too long.
- **`StatusBenchmarking` is explicit** — set by the executor, not inferred; the API always reflects real state.
- **`RESTTransport` is the default** — FIX and WebSocket implement `BotTransport` in Stage 5 without touching `Bot` or `Fleet`.
- **Composite score formula**: `(0.5 × normP99) + (0.3 × normTPS) + (0.2 × correctness) × 100` — scaled to [0,100] for leaderboard.
