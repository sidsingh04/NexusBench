# TASK.md — Phase 5: Advanced Benchmarking

> **Status: ⏳ In Progress**
> Phase 4 is ✅ complete. Begin Phase 5 only after `make ci` passes clean on
> the Phase 4 codebase.

---

## Goal

Evolve NexusBench from a single-run benchmark tool into a timed competitive
contest platform with:

- A **contest lifecycle** (one active contest, admin control, auto-close)
- **Three sequential volatility runs** per submission (Low / Medium / High)
- **Volatility-aware composite scoring** with correctness as a multiplier
- **One-active-submission guard** per team per contest
- **Dry-run validator** for pre-submission engine verification
- **SSE live leaderboard** (push, not poll)
- **WebSocket bot transport** alongside the existing REST transport

**Zero existing tests may break.** Every stage gate requires `go test ./... -race`
to pass before proceeding to the next stage.

---

## Architectural Constraints (carried from Phase 4, non-negotiable)

1. **Deep modules.** Each new package owns its domain. Narrow interfaces at
   boundaries. No package exists only to re-export another's types.
2. **No import cycles.** `models` imports nothing internal. `correctness` and
   `botfleet` import nothing from `worker`, `submission`, or `contest`.
   `validator` imports `botfleet` and `correctness` only.
3. **No logic in `models`.** `models` is pure data — structs, constants, zero
   methods with business logic.
4. **Interfaces only when needed.** New interfaces require either multiple
   implementations or a test double. Don't wrap a single concrete type in an
   interface for its own sake.
5. **Backward compatibility.** All Phase 1–4 API endpoints must continue to
   work identically. New endpoints are additive only.
6. **`make test -race` must pass after every stage.** This is the gate, not
   a suggestion.

---

## Stage Overview

```
Stage 5.1  models + ContestStore               ✅ COMPLETE
Stage 5.2  ContestService + admin endpoints    ✅ COMPLETE
Stage 5.3  One-active-submission guard         ✅ COMPLETE
Stage 5.4  Volatility-aware scoring            ✅ COMPLETE
Stage 5.5  Sequential three-job dispatch       ✅ COMPLETE
Stage 5.6  Dry-run Validator                   ✅ COMPLETE
Stage 5.7  SSE live leaderboard                (new internal/validator package)
Stage 5.8  WebSocket BotTransport              (leaderboardBus + stream endpoint)
Stage 5.9  PostgreSQL ContestStore             (new transport, BotTransport.Close)
Stage 5.10 Integration smoke test              (replace MemoryContestStore in prod)
```

Each stage is independently testable. Each stage gate is a `make test -race`
pass plus the specific checks listed at the bottom of the stage.

---

## Stage 5.1 — Data Model Additions ✅ COMPLETE

> **Completed.** Gate passed: `go build ./...` clean, `go test ./internal/models/... ./internal/queue/... -v -race` green, all pre-existing tests still pass.

### What was delivered

**`internal/models/models.go`**
- `ContestStatus` type + `draft` / `active` / `closed` constants
- `VolatilityProfile` struct (13 fields: bot count, duration, order ratios, price params, scoring targets and weights)
- `Contest` struct with three embedded `VolatilityProfile` fields, aggregate weights, timestamps
- `Contest.ProfileByLabel(label)` method
- `Submission` extended: `ContestID`, `AllResults []*BenchmarkResults`, `FinalScore`; legacy `Results` field kept
- `Submission.ResultByLabel(label)` method
- `BenchmarkResults` extended: `VolatilityLabel`, `RunScore`; legacy `CompositeScore` kept
- `LeaderboardEntry` extended: `LowScore`, `MediumScore`, `HighScore`, `FinalScore`, `BestP99Ms`, `PeakSustainedTPS`, `AvgCorrectness`; legacy fields kept
- `SubmissionStatus.IsTerminal()` method
- Sentinel errors: `ErrNoActiveContest`, `ErrSubmissionInProgress`, `ErrContestNotActive`

**`internal/queue/job.go`**
- `Job` extended: `ContestID`, `VolatilityLabel`, `RemainingProfiles []string`
- `NewProfileJob(sub, contestID, label, remaining)` — defensive-copies `remaining`
- `Job.IsLastProfile()` predicate
- `NewJob` untouched (Phase 1–4 backward compat)

**`internal/config/config.go`**
- `AdminAPIKey string` (from `ADMIN_API_KEY` env var)
- `PostgresDSN string` (from `POSTGRES_DSN` env var)

**New test files**
- `internal/models/models_phase5_test.go` — 9 tests
- `internal/queue/job_phase5_test.go` — 6 tests

### Design decisions
- `models` still imports nothing from other internal packages (enforced by `go build`).
- Defensive copy in `NewProfileJob` prevents caller slice mutations corrupting in-flight job state.
- `IsTerminal()` is the single authoritative terminal-status check; Stage 5.3 calls it rather than repeating the switch.
- Sentinel errors in `models` (not `submission` or `contest`) so any package can use `errors.Is` without introducing an import cycle.
- `CompositeScore`, `Results`, `P99LatencyMs`, `MaxTPS`, `CorrectnessScore` all preserved on existing types — Phase 1–4 leaderboard handler compiles and runs unchanged.

---

## Stage 5.2 — ContestService and Admin Endpoints

> **Status: ✅ COMPLETE**
> **Touches:** new `internal/contest/` package, `internal/api/router.go`,
>   `internal/config/config.go`, `cmd/server/main.go`.
> **New packages:** `internal/contest`.
> **Tests required:** 8 unit tests (see below), all using `MemoryContestStore`.

### Goal

Implement the contest lifecycle (create / activate / close / auto-close) behind
a `ContestService` with a `ContestStore` interface. Wire admin HTTP endpoints.
The `MemoryContestStore` is the only implementation in this stage — PostgreSQL
comes in Stage 5.9.

### Package design: `internal/contest`

```
internal/contest/
├── store.go       // ContestStore interface + MemoryContestStore
├── service.go     // ContestService: Create, Activate, Close, GetActive
├── defaults.go    // DefaultLowProfile(), DefaultMediumProfile(), DefaultHighProfile()
└── service_test.go
```

**`store.go`** defines:

```go
// ContestStore is the persistence interface for contests.
// Implementations: MemoryContestStore (tests/dev), PostgresContestStore (prod).
type ContestStore interface {
    Save(c *models.Contest) error
    Get(id string) (*models.Contest, error)
    // GetActive returns the single Contest with Status=active.
    // Returns models.ErrNoActiveContest if none exists.
    GetActive() (*models.Contest, error)
    List() ([]*models.Contest, error)
    Update(c *models.Contest) error
    // SnapshotLeaderboard archives the final ranked entries for a closed contest.
    // Called exactly once per contest, when it closes.
    SnapshotLeaderboard(contestID string, entries []*models.LeaderboardEntry) error
    GetLeaderboardSnapshot(contestID string) ([]*models.LeaderboardEntry, error)
}
```

`MemoryContestStore` uses a `sync.RWMutex` and two maps:
`map[string]*models.Contest` and `map[string][]*models.LeaderboardEntry`.

**`service.go`** defines `ContestService`:

```go
type ContestService struct {
    store ContestStore
    // bus is used to broadcast leaderboard events. Injected; may be nil
    // in tests that do not test SSE behaviour.
    bus LeaderboardBroadcaster
}

// LeaderboardBroadcaster is satisfied by the leaderboardBus in internal/api.
// Defined here so internal/contest does not import internal/api.
type LeaderboardBroadcaster interface {
    Broadcast(event LeaderboardEvent)
}

// LeaderboardEvent mirrors the type in internal/api but is defined here to
// avoid an import cycle. The api layer converts between the two.
type LeaderboardEvent struct {
    Type      string                    // "update" | "frozen"
    ContestID string
    Entries   []*models.LeaderboardEntry
}

func NewContestService(store ContestStore, bus LeaderboardBroadcaster) *ContestService

// Create persists a new Contest in draft status.
// Returns an error if a contest with Status=active already exists.
func (s *ContestService) Create(ctx context.Context, req CreateContestRequest) (*models.Contest, error)

// Activate transitions a draft contest to active.
// Returns an error if another contest is already active.
func (s *ContestService) Activate(ctx context.Context, id string) (*models.Contest, error)

// Close transitions an active contest to closed.
// Computes the final leaderboard from completed submissions, snapshots it,
// and broadcasts a "frozen" event. Idempotent: closing an already-closed
// contest is a no-op.
func (s *ContestService) Close(ctx context.Context, id string, entries []*models.LeaderboardEntry) error

// GetActive returns the current active contest or models.ErrNoActiveContest.
func (s *ContestService) GetActive(ctx context.Context) (*models.Contest, error)

// ListPast returns all closed contests.
func (s *ContestService) ListPast(ctx context.Context) ([]*models.Contest, error)

// GetLeaderboardSnapshot returns the archived leaderboard for a closed contest.
func (s *ContestService) GetLeaderboardSnapshot(ctx context.Context, contestID string) ([]*models.LeaderboardEntry, error)
```

**`defaults.go`** exports three functions returning the default
`VolatilityProfile` for each level. Values match the table in PROGRESS.md.

#### 5.2.1 — Add `ADMIN_API_KEY` to config ✅ COMPLETE

In `internal/config/config.go`, add:
```go
AdminAPIKey string // required for admin endpoints; loaded from ADMIN_API_KEY env var
```

In `internal/api/router.go`, add `adminAuthMiddleware`:
```go
// adminAuthMiddleware checks Authorization: Bearer <ADMIN_API_KEY>.
// Returns 401 if the header is missing or the key does not match.
// Returns 403 if the key matches but the request does not have admin scope
// (reserved for future role expansion).
func adminAuthMiddleware(apiKey string) mux.MiddlewareFunc
```

Apply the middleware to a new `/api/v1/admin` subrouter. All existing routes
remain unmodified.

#### 5.2.2 — Admin endpoints ✅ COMPLETE

Register on the `/api/v1/admin` subrouter:

```
POST /api/v1/admin/contests                      → contestHandler.Create
POST /api/v1/admin/contests/{id}/activate        → contestHandler.Activate
POST /api/v1/admin/contests/{id}/close           → contestHandler.Close
GET  /api/v1/admin/contests                      → contestHandler.ListPast
GET  /api/v1/admin/contests/{id}/leaderboard     → contestHandler.GetLeaderboardSnapshot
```

`contestHandler` is a new unexported struct in `internal/api` that holds a
`*contest.ContestService`. No business logic in the handler — validation,
status transitions, and snapshot logic all live in `ContestService`.

#### 5.2.3 — Auto-close goroutine ✅ COMPLETE

In `cmd/server/main.go`, start:

```go
go runContestAutoClose(ctx, contestService, submissionService, tickInterval)
```

`runContestAutoClose` ticks every 30 seconds. On each tick:
1. Calls `contestService.GetActive()`. If `ErrNoActiveContest`, skip.
2. If `contest.EndsAt != nil && time.Now().After(*contest.EndsAt)`:
   a. Calls `submissionService.List()`, filters for completed submissions
      in this contest, computes leaderboard entries.
   b. Calls `contestService.Close(ctx, contest.ID, entries)`.

#### 5.2.4 — Unit tests (`internal/contest/service_test.go`) ✅ COMPLETE

| Test | What it verifies |
|---|---|
| `TestCreate_Draft` | Created contest has `Status=draft` |
| `TestActivate_Transitions` | `draft → active`; `GetActive` returns it |
| `TestActivate_RejectsIfAlreadyActive` | second `Activate` on a different draft returns error |
| `TestClose_Snapshots` | `Close` writes leaderboard snapshot; `GetLeaderboardSnapshot` returns it |
| `TestClose_Idempotent` | Closing an already-closed contest is a no-op |
| `TestGetActive_ErrWhenNone` | Returns `ErrNoActiveContest` when no contest is active |
| `TestGetActive_ErrAfterClose` | Returns `ErrNoActiveContest` after the active contest closes |
| `TestAdminMiddleware_RejectsWrongKey` | `adminAuthMiddleware` returns 401 for wrong key |

#### 5.2.5 — Leaderboard Deduplication (AD-1) ✅ COMPLETE

Modify `leaderboard()` in `internal/api/router.go`. Replace the current
linear append with a best-score-per-team grouping:

```go
// bestByTeam keeps only the highest-scoring submission per team.
bestByTeam := make(map[string]models.LeaderboardEntry)
for _, sub := range subs {
    if sub.Status != models.StatusCompleted {
        continue
    }
    // Phase 5+: use FinalScore. Phase 1–4: use Results.CompositeScore.
    score := sub.FinalScore
    if score == 0 && sub.Results != nil {
        score = sub.Results.CompositeScore
    }
    existing, seen := bestByTeam[sub.TeamName]
    if !seen || score > existing.CompositeScore {
        bestByTeam[sub.TeamName] = buildLeaderboardEntry(sub, score)
    }
}
// Sort descending by score, assign 1-based ranks.
entries := sortAndRank(bestByTeam)
```

Extract the entry-building and sort-and-rank logic into unexported helpers
(`buildLeaderboardEntry`, `sortAndRank`) so the handler body stays readable.

New test: `TestLeaderboard_DeduplicatesPerTeam` — three submissions from
two teams; asserts exactly two leaderboard entries with correct ranks.

#### 5.2.6 — Team History View (AD-2) ✅ COMPLETE

Add to `internal/api/router.go`:

```
GET /api/v1/teams/{name}/submissions
```

```go
func (h *handler) teamHistory(w http.ResponseWriter, r *http.Request) {
    name := mux.Vars(r)["name"]
    all, err := h.svc.List()
    if err != nil {
        writeError(w, http.StatusInternalServerError, "LIST_ERROR", err.Error())
        return
    }
    var team []*models.Submission
    for _, s := range all {
        if s.TeamName == name {
            team = append(team, s)
        }
    }
    // List() already returns newest-first; no re-sort needed.
    writeJSON(w, http.StatusOK, map[string]any{
        "team_name":   name,
        "count":       len(team),
        "submissions": team,
    })
}
```

New test: `TestTeamHistory_ReturnsAllSubmissions` — two teams with two
submissions each; asserts the endpoint returns only the queried team's two.

#### 5.2.7 — Hybrid Drain-and-Wait Auto-Close (AD-3) ✅ COMPLETE

Replace the ticker body in `runContestAutoClose` (in `cmd/server/main.go`).
Expand the function signature:

```go
func runContestAutoClose(
    ctx      context.Context,
    svc      *contest.ContestService,
    subStore submission.Store,
    jobQueue queue.Queue,           // nil in local mode — drain check skipped
    registry *orchestrator.WorkerRegistry, // nil in local mode — busy check skipped
)
```

New ticker logic (every 30s):

```go
active, err := svc.GetActive(ctx)
if err != nil { continue } // ErrNoActiveContest is normal

now := time.Now().UTC()

// Phase 1: check if intake should close.
if active.SubmissionsClosedAt == nil && active.EndsAt != nil &&
    now.After(active.EndsAt.Add(-5*time.Minute)) {
    // Seal intake 5 minutes before EndsAt (operator configurable via EndsAt).
    // Stage 5.3 will use this field in Ingest to reject new uploads.
    // For now we just note it — ContestService.SetSubmissionsClosed is a
    // Stage 5.3 addition.
}

// Phase 2: natural drain — trigger once submissions are closed.
if active.SubmissionsClosedAt != nil && now.After(*active.SubmissionsClosedAt) {
    drained := true
    if jobQueue != nil {
        depth, err := jobQueue.QueueDepth(ctx)
        if err == nil && depth > 0 { drained = false }
    }
    if registry != nil {
        if registry.Stats().Busy > 0 { drained = false }
    }
    if drained {
        entries := buildLeaderboardEntries(ctx, subStore, active.ID)
        svc.Close(ctx, active.ID, entries) //nolint:errcheck
        continue
    }
}

// Phase 3: hard failsafe — force-close at EndsAt regardless of drain state.
if active.EndsAt != nil && now.After(*active.EndsAt) {
    entries := buildLeaderboardEntries(ctx, subStore, active.ID)
    svc.Close(ctx, active.ID, entries) //nolint:errcheck
}
```

`buildLeaderboardEntries` is an unexported helper in `cmd/server/main.go` that
calls `subStore.List()`, filters for `ContestID==active.ID &&
Status==StatusCompleted`, and converts to `[]*models.LeaderboardEntry` sorted
by `FinalScore` descending.

Update the call site in `main()` to pass `store` (already constructed),
`jobQueue` (nil in local mode), and `workerRegistry`.

### Gate — Stage 5.2

```bash
make test -race                     # all tests pass including 8 new ones
go vet ./internal/contest/...       # zero warnings
curl -X POST localhost:8080/api/v1/admin/contests \
  -H "Authorization: Bearer testkey" \
  -H "Content-Type: application/json" \
  -d '{"name":"test","use_defaults":true}'
# → 201 Created with contest JSON
curl -X POST localhost:8080/api/v1/admin/contests/{id}/activate \
  -H "Authorization: Bearer testkey"
# → 200 OK, status=active
curl localhost:8080/api/v1/admin/contests/{id}/activate  # no auth header
# → 401 Unauthorized
```

---

## Stage 5.3 — One-Active-Submission Guard ✅

> **Status: ✅ COMPLETE**
> **Touches:** `internal/submission/service.go` only.
> **New packages:** none.
> **Tests required:** 2 new unit tests added to existing submission test file.

### Goal

Prevent a team from flooding the queue by submitting while their previous
submission is still being evaluated. This is a single check in `Ingest`.

### Tasks

#### 5.3.1 — Gate in `Service.Ingest`

After validating language and protocol, and before calling `s.store.Save`,
add:

```go
// One-active-submission guard: reject if this team already has an
// in-progress submission in the same contest.
if err := s.checkNoActiveSubmission(req.TeamName, contestID); err != nil {
    return nil, err
}
```

`checkNoActiveSubmission` calls `s.store.List()`, iterates, and returns
`models.ErrSubmissionInProgress` if any submission from the same `TeamName`
and `ContestID` has a non-terminal status:
(`StatusPending`, `StatusBuilding`, `StatusDeploying`, `StatusRunning`,
`StatusBenchmarking`).

Terminal statuses (`StatusCompleted`, `StatusFailed`) are allowed — a team
may resubmit after their previous run finishes.

`contestID` is sourced from the active contest. If no contest is active,
`Ingest` returns `models.ErrContestNotActive` before reaching this check.

#### 5.3.2 — Gate for submissions-closed

In `Ingest`, after the one-active-submission check:

```go
if contest.SubmissionsClosedAt != nil && time.Now().After(*contest.SubmissionsClosedAt) {
    return nil, models.ErrContestNotActive
}
```

#### 5.3.3 — Unit tests

Add to the existing submission service test file:

| Test | What it verifies |
|---|---|
| `TestIngest_RejectsIfSubmissionInProgress` | Returns `ErrSubmissionInProgress` when team has a pending submission |
| `TestIngest_AllowsAfterPreviousCompleted` | Returns no error when team's previous submission is `StatusCompleted` |

### Gate — Stage 5.3

```bash
make test -race    # all tests pass including 2 new ones
# Manual: submit twice from the same team during an active contest
# → second submit returns HTTP 409 with code="SUBMISSION_IN_PROGRESS"
```

---

## Stage 5.4 — Volatility-Aware Scoring ✅

> **Status: ✅ COMPLETE**
> **Touches:** `internal/worker/executor.go` only.
> **New packages:** none.
> **Tests required:** 4 new unit tests replacing/extending existing `buildResults` tests.

### Goal

Replace the hardcoded scoring constants in `buildResults` with profile-aware
normalization. The scoring formula is unchanged in structure but now reads
targets from the `VolatilityProfile` carried in the `Job`.

### Tasks

#### 5.4.1 — `buildResults` signature change

Change:
```go
func buildResults(fr *botfleet.FleetResult, cr *correctness.CorrectnessResult) *models.BenchmarkResults
```
To:
```go
func buildResults(
    fr     *botfleet.FleetResult,
    cr     *correctness.CorrectnessResult,
    profile models.VolatilityProfile,
    label  string,
) *models.BenchmarkResults
```

Remove all `const` blocks (`targetP99Ns`, `worstP99Ns`, `targetMaxTPS`) from
`buildResults`. Replace with values from `profile`.

#### 5.4.2 — Per-run score formula

```go
// normP99: 1.0 at or below target, 0.0 at or above 10×target, linear between.
worstP99Ns := profile.TargetP99Ns * 10
normP99 := 0.0
if stats.P99Ns <= profile.TargetP99Ns {
    normP99 = 1.0
} else if stats.P99Ns < worstP99Ns {
    normP99 = 1.0 - float64(stats.P99Ns-profile.TargetP99Ns)/float64(worstP99Ns-profile.TargetP99Ns)
}

// normTPS: uses SustainedTPS (full-run average), not MaxTPS (100ms burst).
normTPS := stats.SustainedTPS / profile.TargetSustainTPS
if normTPS > 1.0 { normTPS = 1.0 }

// Correctness multiplier: bad correctness penalises latency and throughput too.
runScore := correctnessScore *
    (profile.LatencyWeight*normP99 + profile.ThroughputWeight*normTPS) +
    profile.CorrectnessWeight*correctnessScore
```

Set `BenchmarkResults.RunScore = runScore` and
`BenchmarkResults.VolatilityLabel = label`.

The `CompositeScore` field on `BenchmarkResults` is **removed** in this stage
(it was the old single-run score). The final `FinalScore` on
`LeaderboardEntry` replaces it and is computed in Stage 5.5 after all three
runs complete.

#### 5.4.3 — `buildFleetConfig` change

Change `buildFleetConfig` to accept a `models.VolatilityProfile` and return a
`botfleet.FleetConfig` populated from it. Remove the call to
`botfleet.DefaultFleetConfig`.

```go
func buildFleetConfigFromProfile(targetURL string, p models.VolatilityProfile) botfleet.FleetConfig {
    return botfleet.FleetConfig{
        TargetURL:    targetURL,
        BotCount:     p.BotCount,
        TestDuration: p.TestDuration,
        GeneratorConfig: botfleet.RandomGeneratorConfig{
            Ratios: botfleet.OrderRatios{
                Limit:  p.LimitRatio,
                Market: p.MarketRatio,
                Cancel: p.CancelRatio,
            },
            Price:    botfleet.PriceConfig{MidPrice: 10_000, Spread: p.PriceSpreadCents},
            Quantity: botfleet.QuantityConfig{Min: 1, Max: p.MaxQuantity},
        },
    }
}
```

#### 5.4.4 — Unit tests

| Test | What it verifies |
|---|---|
| `TestBuildResults_LowProfile_PerfectScore` | P99=target, TPS=target, correctness=1.0 → RunScore=1.0 |
| `TestBuildResults_HighProfile_WorstLatency` | P99>>target → normP99≈0, RunScore driven by TPS+correctness only |
| `TestBuildResults_ZeroCorrectness_ZerosAll` | correctness=0.0 → RunScore=0.0 regardless of TPS/P99 |
| `TestBuildResults_LabelPropagated` | VolatilityLabel is set correctly from the profile |

### Gate — Stage 5.4

```bash
make test -race    # all tests pass including 4 new ones
# Verify: go vet ./internal/worker/... returns zero warnings
# Verify: CompositeScore field is gone from BenchmarkResults JSON
```

---

## Stage 5.5 — Sequential Three-Job Dispatch

> **Touches:** `internal/submission/service.go`, `internal/worker/executor.go`,
>   `internal/queue/job.go`.
> **New packages:** none.
> **Tests required:** 3 new unit tests.

### Goal

Each submission triggers three sequential benchmark jobs — one per volatility
profile. "Sequential" means job[1] is only enqueued after job[0] is committed
by a worker. The `FinalScore` is computed once all three `BenchmarkResults`
are written for a submission.

> **⚠️ Local Development Note:** Starting from Stage 5.5, `ADMIN_API_KEY: "testkey"` is hardcoded in the local `docker-compose.yml` for `control-plane`. This forces local development into "Phase 5 Strict Contest Mode." Legacy endpoints hitting `/api/v1/submissions` will return `ErrContestNotActive` unless a contest is created first via the admin API.

### Design

The `Job` type already has `RemainingProfiles []string` (added in Stage 5.1).
The dispatch chain works as follows:

1. `Service.Ingest` enqueues **one** job for the first profile (`"low"`), with
   `RemainingProfiles: ["medium", "high"]`.
2. `SandboxExecutor.Execute`, after writing `BenchmarkResults` for `"low"`,
   calls `s.enqueueNext(ctx, j)`. This pops `RemainingProfiles[0]` ("medium"),
   constructs a new job with the popped label and the remainder
   (`["high"]`), and enqueues it.
3. This repeats until `RemainingProfiles` is empty (the "high" job).
4. After the "high" job commits, `Execute` calls `s.computeFinalScore(ctx, j.SubmissionID)`.

### Tasks

#### 5.5.1 — Ingest dispatches first job only

In `Service.Ingest`, replace the single job creation with:

```go
j := queue.NewProfileJob(sub, contest, "low", []string{"medium", "high"})
```

`queue.NewProfileJob` is a new constructor that sets `ContestID`,
`VolatilityLabel`, and `RemainingProfiles`.

#### 5.5.2 — Worker re-enqueues next profile

In `SandboxExecutor.Execute`, after successfully writing `BenchmarkResults`:

```go
if len(j.RemainingProfiles) > 0 {
    next := queue.NewProfileJob(sub, contest, j.RemainingProfiles[0], j.RemainingProfiles[1:])
    if err := s.queue.Enqueue(ctx, next); err != nil {
        // Log and mark submission failed — do not panic.
        log.Error("executor: failed to enqueue next profile", "err", err)
        s.markFailed(sub, fmt.Sprintf("failed to enqueue %s profile: %v", next.VolatilityLabel, err))
        return nil, err
    }
} else {
    // All three runs complete. Compute and persist FinalScore.
    s.computeAndWriteFinalScore(ctx, j.SubmissionID, contest)
}
```

#### 5.5.3 — `computeAndWriteFinalScore`

```go
func (e *SandboxExecutor) computeAndWriteFinalScore(
    ctx context.Context,
    submissionID string,
    contest *models.Contest,
) {
    sub, _ := e.store.Get(submissionID)
    // Collect the three BenchmarkResults from sub.AllResults (see below).
    low    := sub.ResultByLabel("low")
    medium := sub.ResultByLabel("medium")
    high   := sub.ResultByLabel("high")

    finalScore := contest.LowWeight*safeScore(low) +
                  contest.MediumWeight*safeScore(medium) +
                  contest.HighWeight*safeScore(high)
    finalScore *= 100.0

    sub.FinalScore = finalScore
    sub.Status = models.StatusCompleted
    // ... persist and broadcast leaderboard event
}
```

`Submission` gains `AllResults []*BenchmarkResults` (all profile runs) and
`FinalScore float64`. The existing `Results *BenchmarkResults` field is
deprecated and removed — callers use `AllResults` indexed by
`VolatilityLabel`.

`ResultByLabel` is a method on `*Submission`:
```go
func (s *Submission) ResultByLabel(label string) *BenchmarkResults {
    for _, r := range s.AllResults {
        if r.VolatilityLabel == label { return r }
    }
    return nil
}
```

#### 5.5.4 — Unit tests

| Test | What it verifies |
|---|---|
| `TestNewProfileJob_SetsLabelsCorrectly` | First job has label="low", remaining=["medium","high"] |
| `TestNewProfileJob_LastJob_EmptyRemaining` | Last job has `RemainingProfiles` empty |
| `TestResultByLabel_ReturnsCorrectResult` | `ResultByLabel("medium")` returns the medium result |

### Gate — Stage 5.5

```bash
make test -race    # all tests pass including 3 new ones
# Manual (docker compose up): submit a binary, observe in logs:
#   "executor: running profile low"
#   "executor: enqueueing next profile medium"
#   "executor: running profile medium"
#   "executor: enqueueing next profile high"
#   "executor: running profile high"
#   "executor: final score computed"
```

---

## Stage 5.6 — Dry-Run Validator

> **Touches:** new `internal/validator/` package, `internal/api/router.go`,
>   `cmd/server/main.go`.
> **New packages:** `internal/validator`.
> **Tests required:** 5 unit tests using `httptest.Server`.

### Goal

Give contestants a safe pre-submission smoke test that exercises five
deterministic scenarios and returns per-scenario pass/fail with human-readable
reasons. No leaderboard impact. No status changes.

### Package design: `internal/validator`

```
internal/validator/
├── validator.go
├── scenarios.go       // the fixed 20-order sequence, unexported
└── validator_test.go
```

**`validator.go`:**

```go
// Package validator runs a deterministic smoke test against a deployed
// contestant engine. It has no side effects: it does not modify submission
// status, write BenchmarkResults, or touch the leaderboard.
//
// Dependencies: botfleet.BotTransport, correctness.GoldenOrderbook.
// Imports nothing from submission, worker, or contest.
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

type Validator struct {
    transport botfleet.BotTransport
}

func New(transport botfleet.BotTransport) *Validator

// Run executes all scenarios against the engine reachable via v.transport.
// It is safe to call concurrently for different submissions.
// Each call creates its own GoldenOrderbook — there is no shared state.
func (v *Validator) Run(ctx context.Context, submissionID string) (*ValidationResult, error)
```

**`scenarios.go`** (unexported):

Defines a `[]scenario` slice. Each `scenario` has a `name string`, an
`orders []botfleet.Order` (hand-crafted, deterministic — no RNG), and
`expectedFills []correctness.GoldenFill`. The `Validator.Run` method iterates
scenarios, sends orders via `transport`, compares fills against expected, and
produces a `ScenarioResult`.

The 20-order fixed sequence covers:
1. Limit buy rests (no fill yet)
2. Limit sell crosses and fills
3. Market buy sweeps remaining sell side
4. Cancel of known resting order
5. Cancel of unknown IDs → all `accepted: false`
6. Zero-quantity order rejection
7. Partial fill produces correct `executed_qty`

#### 5.6.1 — Rate limiter

In `internal/api/router.go`, add an unexported
`validationRateLimiter` using a `sync.Map[string, time.Time]`:
one validation per submission per 2 minutes. The validation endpoint
checks this before calling `Validator.Run`.

#### 5.6.2 — Endpoint

```
POST /api/v1/submissions/{id}/validate
```

Handler checks:
1. Submission exists (404 if not).
2. Submission is not `StatusBenchmarking` (409 if so).
3. Rate limit (429 if too recent).
4. Submission has `ExposedPort > 0` (the container is running).
5. Constructs `RESTTransport` targeting `http://sandboxHost:{port}`.
6. Constructs `Validator`, calls `Run`.
7. Returns `ValidationResult` as JSON.

#### 5.6.3 — Unit tests

| Test | What it verifies |
|---|---|
| `TestValidator_AllPass` | httptest.Server returns correct fills → all scenarios pass |
| `TestValidator_FailOnWrongExecutedPrice` | Server returns wrong price → scenario 2 fails with reason |
| `TestValidator_FailOnCancelAccepted` | Server accepts an unknown cancel → scenario 5 fails |
| `TestValidator_ContextCancellation` | Cancelling ctx mid-run returns error, no panic |
| `TestValidator_RateLimiter` | Second call within 2 minutes returns 429 |

### Gate — Stage 5.6

```bash
make test -race    # all tests pass including 5 new ones
# Manual:
curl -X POST localhost:8080/api/v1/submissions/{id}/validate
# → 200 with ValidationResult, scenarios array, all_passed field
curl -X POST localhost:8080/api/v1/submissions/{id}/validate  # immediately again
# → 429 Too Many Requests
curl -X POST localhost:8080/api/v1/submissions/{id}/validate  # while benchmarking
# → 409 Conflict
```

---

## Stage 5.7 — SSE Live Leaderboard

> **Touches:** `internal/api/router.go`, `internal/api/` (new `bus.go`),
>   `cmd/server/main.go`.
> **New packages:** none (bus lives in `internal/api`).
> **Tests required:** 3 unit tests.

### Goal

Replace the polling-only leaderboard with a push-based SSE stream. The existing
`GET /api/v1/leaderboard` poll endpoint is **not removed** — it remains for
backward compatibility and for clients that prefer polling.

### Implementation

#### 5.7.1 — `leaderboardBus` in `internal/api/bus.go`

```go
// leaderboardBus is a fan-out broadcaster for leaderboard events.
// It is the concrete implementation of contest.LeaderboardBroadcaster.
// All methods are safe for concurrent use.
type leaderboardBus struct {
    mu   sync.RWMutex
    subs map[string]chan LeaderboardEvent // key = UUID per subscriber
}

type LeaderboardEvent struct {
    Type      string                     `json:"type"`       // "update" | "frozen"
    ContestID string                     `json:"contest_id"`
    Entries   []*models.LeaderboardEntry `json:"entries"`
}

func newLeaderboardBus() *leaderboardBus
func (b *leaderboardBus) Broadcast(event contest.LeaderboardEvent)
func (b *leaderboardBus) subscribe() (id string, ch <-chan LeaderboardEvent)
func (b *leaderboardBus) unsubscribe(id string)
```

`Broadcast` converts `contest.LeaderboardEvent` → `LeaderboardEvent` and sends
to all subscriber channels. Channels are buffered (size 4). A full channel
is dropped silently (slow clients do not block the broadcaster).

#### 5.7.2 — SSE endpoint

```
GET /api/v1/leaderboard/stream
```

```go
func (h *leaderboardHandler) Stream(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "text/event-stream")
    w.Header().Set("Cache-Control", "no-cache")
    w.Header().Set("Connection", "keep-alive")

    id, ch := h.bus.subscribe()
    defer h.bus.unsubscribe(id)

    // Send current leaderboard immediately on connect.
    current := h.buildLeaderboard()
    writeSSEEvent(w, LeaderboardEvent{Type: "update", Entries: current})

    for {
        select {
        case <-r.Context().Done():
            return
        case event := <-ch:
            writeSSEEvent(w, event)
            if event.Type == "frozen" {
                return // contest over; close stream
            }
        }
    }
}

func writeSSEEvent(w http.ResponseWriter, event LeaderboardEvent) {
    data, _ := json.Marshal(event)
    fmt.Fprintf(w, "data: %s\n\n", data)
    if f, ok := w.(http.Flusher); ok { f.Flush() }
}
```

#### 5.7.3 — Wire bus into ContestService

In `cmd/server/main.go`, create `bus := api.NewLeaderboardBus()` and pass it
to both `NewRouter(bus)` and `contest.NewContestService(store, bus)`.

`ContestService.Close` calls `bus.Broadcast(contest.LeaderboardEvent{Type: "frozen", ...})`.

The worker's `computeAndWriteFinalScore` calls `bus.Broadcast(contest.LeaderboardEvent{Type: "update", ...})`.

#### 5.7.4 — Unit tests

| Test | What it verifies |
|---|---|
| `TestLeaderboardBus_Broadcast` | Two subscribers both receive the event |
| `TestLeaderboardBus_SlowSubscriberDropped` | Full channel does not block Broadcast |
| `TestLeaderboardBus_UnsubscribeCleans` | Unsubscribed channel receives no further events |

### Gate — Stage 5.7

```bash
make test -race    # all tests pass including 3 new ones
# Manual:
curl -N localhost:8080/api/v1/leaderboard/stream
# → stays connected, prints "data: {...}" on each submission completion
# After contest close:
# → prints "data: {"type":"frozen",...}" and closes
```

---

## Stage 5.8 — WebSocket Bot Transport

> **Touches:** `internal/botfleet/bot.go`, `internal/botfleet/fleet.go`,
>   new `internal/botfleet/websocket.go`.
> **New packages:** none (stays in `internal/botfleet`).
> **Tests required:** 4 unit tests using `httptest.Server` upgraded to WebSocket.

### Goal

Add a `WebSocketTransport` implementing `BotTransport`. Add `Close() error` to
the `BotTransport` interface (required for WebSocket connection teardown).
The `Fleet` calls `Close()` on each transport after `Run` returns.

`RESTTransport.Close()` is a no-op — HTTP clients have no persistent connection
to close.

### Tasks

#### 5.8.1 — Extend `BotTransport` interface

```go
type BotTransport interface {
    Send(ctx context.Context, o Order) (Fill, error)
    // Close releases any persistent transport resources (e.g. WebSocket connection).
    // Safe to call multiple times. REST implementations return nil immediately.
    Close() error
}
```

Add `Close() error { return nil }` to `RESTTransport`. This is a non-breaking
change — existing tests compile without modification because `RESTTransport`
already satisfies the extended interface.

#### 5.8.2 — `WebSocketTransport` in `internal/botfleet/websocket.go`

```go
// WebSocketTransport implements BotTransport over a persistent WebSocket
// connection. One instance per Bot — each bot owns its own connection.
// The connection is established in NewWebSocketTransport and reused for the
// lifetime of the fleet run.
//
// Wire protocol (JSON, same field names as REST):
//   send:    {"order_id":"...","kind":"...","side":"...","price":0,"quantity":0}
//   receive: {"order_id":"...","accepted":true,"executed_price":0,"executed_qty":0}
//
// Uses golang.org/x/net/websocket (stdlib-adjacent, no CGO).
type WebSocketTransport struct { /* unexported */ }

func NewWebSocketTransport(url string) (*WebSocketTransport, error)
func (t *WebSocketTransport) Send(ctx context.Context, o Order) (Fill, error)
func (t *WebSocketTransport) Close() error
```

`Send` writes a JSON order and reads a JSON fill on the same connection.
Respects `ctx` cancellation via a deadline set on the underlying connection.

#### 5.8.3 — Fleet selects transport by protocol

In `fleet.go`, `Fleet.Run` (or `Fleet.newBot`) checks `cfg.Protocol`:
```go
switch cfg.Protocol {
case string(models.ProtocolWebSocket):
    transport, err = botfleet.NewWebSocketTransport(cfg.TargetURL)
default: // REST
    transport = botfleet.NewRESTTransport(cfg.TargetURL, nil)
}
```

After `Fleet.Run` returns, call `transport.Close()` for each bot (already
tracked in the `Bot` struct).

#### 5.8.4 — Unit tests

| Test | What it verifies |
|---|---|
| `TestWebSocketTransport_SendReceive` | Send limit order, server echoes correct fill → Fill parsed correctly |
| `TestWebSocketTransport_ContextCancellation` | Cancelled ctx causes Send to return ctx.Err() |
| `TestWebSocketTransport_Close_Idempotent` | Calling Close() twice does not panic |
| `TestRESTTransport_CloseIsNoop` | RESTTransport.Close() returns nil without error |

### Gate — Stage 5.8

```bash
make test -race    # all tests pass including 4 new ones
# Verify: go vet ./internal/botfleet/... returns zero warnings
# Verify: BotTransport interface has Close() in interface definition
```

---

## Stage 5.9 — PostgreSQL ContestStore

> **Touches:** new `internal/contest/postgres.go`, `cmd/server/main.go`,
>   `docker-compose.yml`, `k8s/` (new StatefulSet for PostgreSQL).
> **New packages:** none (implementation lives in `internal/contest`).
> **Tests required:** none new (PostgresContestStore is integration-tested via
>   the existing docker-compose stack; unit tests use MemoryContestStore).

### Goal

Replace `MemoryContestStore` with `PostgresContestStore` in production.
`MemoryContestStore` stays in place for unit tests — no test changes needed.

### Tasks

#### 5.9.1 — Schema

Two tables, created on startup via `internal/contest/postgres.go`
`(*PostgresContestStore).migrate()`:

```sql
CREATE TABLE IF NOT EXISTS contests (
    id                    TEXT PRIMARY KEY,
    name                  TEXT NOT NULL,
    status                TEXT NOT NULL,
    low_profile           JSONB NOT NULL,
    medium_profile        JSONB NOT NULL,
    high_profile          JSONB NOT NULL,
    low_weight            DOUBLE PRECISION NOT NULL DEFAULT 0.20,
    medium_weight         DOUBLE PRECISION NOT NULL DEFAULT 0.35,
    high_weight           DOUBLE PRECISION NOT NULL DEFAULT 0.45,
    submissions_closed_at TIMESTAMPTZ,
    contest_closed_at     TIMESTAMPTZ,
    ends_at               TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS contest_leaderboard_snapshots (
    contest_id  TEXT PRIMARY KEY REFERENCES contests(id),
    entries     JSONB NOT NULL,
    snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

#### 5.9.2 — `PostgresContestStore`

Implement `ContestStore` using `pgxpool.Pool`. All methods use context-scoped
queries. `migrate()` is called once from `NewPostgresContestStore`.

```go
func NewPostgresContestStore(ctx context.Context, dsn string) (*PostgresContestStore, error)
```

#### 5.9.3 — Wire into `cmd/server/main.go`

In distributed mode (`cfg.DistributedMode`), create `PostgresContestStore`
using `cfg.PostgresDSN`. In local mode, use `MemoryContestStore`.

#### 5.9.4 — Docker Compose and K8s

Add PostgreSQL to `docker-compose.yml`:
```yaml
postgres:
  image: postgres:16-alpine
  environment:
    POSTGRES_DB: nexusbench
    POSTGRES_USER: nexusbench
    POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
  volumes:
    - postgres-data:/var/lib/postgresql/data
  healthcheck:
    test: ["CMD-SHELL", "pg_isready -U nexusbench"]
    interval: 5s
    timeout: 5s
    retries: 5
```

Add `k8s/postgres/statefulset.yaml` and `k8s/postgres/service.yaml` and
`k8s/postgres/pvc.yaml` following the same security patterns as the
TimescaleDB StatefulSet in Phase 4.

Add NetworkPolicy: `k8s/network-policies/allow-postgres-ingress.yaml`
(control-plane only; workers excluded).

### Gate — Stage 5.9

```bash
make test -race    # all tests still pass (unit tests use MemoryContestStore)
docker compose up -d postgres
POSTGRES_DSN="postgres://nexusbench:testpw@localhost:5432/nexusbench" \
    go run ./cmd/server --distributed
# → server starts, migrations run, no errors in logs
curl -X POST localhost:8080/api/v1/admin/contests \
  -H "Authorization: Bearer testkey" \
  -d '{"name":"integration-test","use_defaults":true}'
# → 201 Created, contest persisted in PostgreSQL
```

---

## Stage 5.10 — Integration Smoke Test

> **Touches:** new `scripts/smoke_test_phase5.sh`.
> **New packages:** none.
> **Tests required:** the smoke test script itself.

### Goal

A single end-to-end smoke test that exercises the full Phase 5 flow:
create contest → submit → dry-run → benchmark three profiles →
check live leaderboard → close contest → check snapshot.

The script has two modes, matching the Phase 4 pattern:
- `--dry-run` (default): validates all new YAML, runs Go unit tests,
  checks new endpoints are registered in the router.
- `--live`: runs against a live `docker compose up` stack.

### `--live` sequence

```bash
# 1. Create and activate a contest with default profiles
CONTEST=$(curl -sf -X POST .../admin/contests -d '{"name":"smoke","use_defaults":true}')
CONTEST_ID=$(echo $CONTEST | jq -r .id)
curl -sf -X POST .../admin/contests/$CONTEST_ID/activate

# 2. Subscribe to SSE stream in background
curl -sN .../leaderboard/stream > /tmp/sse_output &
SSE_PID=$!

# 3. Submit a pre-built test binary (the existing smoke-test binary from Phase 3)
curl -sf -X POST .../submissions -F team_name=smoketest -F language=binary \
  -F protocol=rest -F archive=@scripts/testdata/smoke_engine.tar.gz

# 4. Wait for ValidationResult
sleep 5
curl -sf -X POST .../submissions/$SUB_ID/validate | jq .all_passed

# 5. Wait for all three profile runs to complete (up to 10 minutes)
# Poll GET /submissions/$SUB_ID until status=completed

# 6. Check leaderboard
curl -sf .../leaderboard | jq '.[0].final_score'
# → non-zero

# 7. Check SSE received at least one update event
grep '"type":"update"' /tmp/sse_output

# 8. Close contest
curl -sf -X POST .../admin/contests/$CONTEST_ID/close
# → 200 OK

# 9. Check snapshot
curl -sf .../admin/contests/$CONTEST_ID/leaderboard | jq length
# → >= 1

# 10. Verify SSE received frozen event
grep '"type":"frozen"' /tmp/sse_output

kill $SSE_PID
```

### Gate — Stage 5.10 (Full Phase 5 Gate)

```bash
make test -race                             # all unit tests pass
bash scripts/smoke_test_phase5.sh           # dry-run passes
bash scripts/smoke_test_phase5.sh --live    # live passes against docker compose
make ci                                     # lint + test + tf-validate + k8s-validate
```

---

## Full Phase 5 Gate Checklist

Before PROGRESS.md is updated to mark Phase 5 complete:

| Check | Command / Evidence |
|---|---|
| All unit tests pass with race detector | `make test -race` |
| No new golangci-lint warnings | `make lint` |
| All new packages have zero import cycles | `go build ./...` |
| No existing API endpoints broken | Manual curl of Phase 1–4 endpoints |
| Contest create + activate + close works | Stage 5.2 manual gate |
| Submission guard rejects double-submit | Stage 5.3 manual gate |
| Three-profile scoring produces non-zero FinalScore | Stage 5.5 manual gate |
| Dry-run returns per-scenario results | Stage 5.6 manual gate |
| SSE stream delivers events and frozen signal | Stage 5.7 manual gate |
| WebSocket transport sends and receives fills | Stage 5.8 unit test gate |
| PostgreSQL persists contests across restart | Stage 5.9 manual gate |
| Full smoke test passes live | Stage 5.10 live gate |
| `make ci` clean | `make ci` |

---

## Architectural Decisions Log (AD)

These decisions were reviewed against the live codebase after Stages 5.1 and
5.2 were implemented. Each entry records the verdict, the precise insertion
point, and whether any rework of completed stages is required.

---

### AD-1 — Leaderboard Deduplication (Best Score per Team)

**Status:** Accepted. Inserted as Stage 5.2 sub-task 5.2.5.
**Rework of 5.1/5.2:** None.

**Problem:** Every completed submission gets its own leaderboard row. An active
team submitting ten times monopolises the top ten spots.

**Solution:** Modify the `leaderboard` handler in `internal/api/router.go` to
build a `map[string]models.LeaderboardEntry` keyed by `TeamName`, replacing the
entry only when `FinalScore` (Phase 5+) or `CompositeScore` (Phase 1–4) is
higher. Sort the resulting map values by score descending and assign 1-based
ranks before responding.

**Why no rework:** `LeaderboardEntry` already has `TeamName`, `FinalScore`, and
`CompositeScore` from Stage 5.1. The handler logic is additive — the JSON shape
does not change.

**Engineering cost:** ~12 lines inside the existing `leaderboard()` function.
One new test: `TestLeaderboard_DeduplicatesPerTeam`.

**Stage where implemented:** Stage 5.2 (sub-task 5.2.5 below).

---

### AD-2 — Team History View

**Status:** Accepted. Inserted as Stage 5.2 sub-task 5.2.6.
**Rework of 5.1/5.2:** None.

**Problem:** Deduplication hides a team's alternative approaches from their own
view.

**Solution:** New endpoint `GET /api/v1/teams/{name}/submissions`. Calls
`svc.List()`, filters by `TeamName`, sorts by `CreatedAt` descending, returns
the full `[]*models.Submission` slice — no new store methods, no new packages.

**Engineering cost:** ~15 lines. One new route registration, one handler method.
One new test: `TestTeamHistory_ReturnsAllSubmissions`.

**Stage where implemented:** Stage 5.2 (sub-task 5.2.6 below).

---

### AD-3 — Hybrid Drain-and-Wait Contest Closing

**Status:** Accepted. Replaces the wall-clock-only `runContestAutoClose` in
`cmd/server/main.go`. Inserted as Stage 5.2 sub-task 5.2.7.
**Rework of 5.1/5.2:** None.

**Problem:** A hard `EndsAt` wall-clock cut unfairly kills valid on-time
submissions when the Redpanda queue is backed up due to platform capacity.

**Two-timestamp protocol:**
- `SubmissionsClosedAt` (already on `Contest` from Stage 5.1): intake gate.
  `Ingest` checks this field and returns `ErrContestNotActive` once crossed.
  Set by the admin 5–60 minutes before `EndsAt`.
- `EndsAt`: hard failsafe only. The goroutine only forces close at `EndsAt` if
  the queue has not drained naturally by then.

**Drain condition (checked every 30s after `SubmissionsClosedAt`):**
```go
queueDepth, _ := jobQueue.QueueDepth(ctx)   // queue.Queue already has this
stats := registry.Stats()                   // orchestrator.WorkerRegistry already has this
if queueDepth == 0 && stats.Busy == 0 {
    // all in-flight work is done — safe to close
}
```

**`runContestAutoClose` new signature:**
```go
func runContestAutoClose(
    ctx      context.Context,
    svc      *contest.ContestService,
    subStore submission.Store,       // to build leaderboard entries on close
    jobQueue queue.Queue,
    registry *orchestrator.WorkerRegistry,
)
```

**Why no rework:** `SubmissionsClosedAt` is already on `Contest` (Stage 5.1).
`queue.QueueDepth()` and `registry.Stats()` are both already implemented.
`jobQueue` and `workerRegistry` are already in scope in `cmd/server/main.go`.
The function is package-internal — signature change is free.

**Engineering cost:** ~25 lines replacing the ticker body. No new interfaces.

**Stage where implemented:** Stage 5.2 (sub-task 5.2.7 below).

---

### AD-4 — NTP Clock Drift Resilience

**Status:** Partially already correct; one note added to Stage 5.9.
**Rework of 5.1/5.2:** None.

**Sub-item A — Timeouts (monotonic clocks):**
All timeout paths already use `context.WithTimeout` (which uses the monotonic
clock internally) and `time.Since()`. `waitHealthy` uses
`time.Now().Add(e.healthTimeout)` then `time.Now().After(deadline)` — both
calls are on the same runtime monotonic source, immune to NTP wall-clock jumps.
**Already correct. No action needed.**

**Sub-item B — `CompletedAt` timestamp authority:**
In `worker.go`, `now := time.Now().UTC()` then `sub.CompletedAt = &now` stamps
the worker's local clock. With `DiskStore` (Stages 5.1–5.8) the worker is the
*only* writer, so its clock is authoritative by definition — there is no
"control plane receives results" moment to stamp instead.

With `PostgresContestStore` (Stage 5.9), the leaderboard snapshot INSERT should
use `snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT now()` (DB server clock) for
the snapshot timestamp. The `CompletedAt` on individual submissions remains the
worker's stamp — acceptable because contest fairness depends on *relative*
orderings within the same contest, where all workers share the same NTP source.
**Action: one schema note added to Stage 5.9. No code changes to 5.1–5.8.**

---

## Key Design Decisions

**Why `MemoryContestStore` first (Stage 5.2) then `PostgresContestStore` (Stage 5.9)?**
Starting with the in-memory implementation lets us write and pass all unit tests
without a running database. The interface is stable by Stage 5.2; Stage 5.9
only adds an implementation. This is the deep-module principle: callers depend
on `ContestStore`, not on PostgreSQL.

**Why is correctness a multiplier, not an additive term?**
An engine with 0% correctness is not a matching engine — it is a random
number generator with low latency. Treating correctness as a multiplier ensures
such an engine scores zero on every axis, not just the 20% correctness slice.
This incentivises contestants to fix correctness bugs before optimising latency.

**Why SustainedTPS and not MaxTPS for scoring?**
`MaxTPS` measures the busiest 100ms window. An engine can produce a brief spike
and then collapse. `SustainedTPS` (total successful orders / elapsed run time)
is the honest throughput number over the full test duration. `MaxTPS` is still
recorded and displayed — it is useful diagnostic information — but it does not
determine rank.

**Why sequential (not parallel) profile jobs?**
All three profiles run against the same sandbox container. Parallel jobs would
share container resources, making p99 and TPS measurements from each profile
contaminated by the others. Sequential execution gives each profile a clean,
dedicated benchmark window.

**Why `BotTransport.Close()` added to the interface?**
WebSocket connections are persistent — the transport must be closed after the
fleet run or the server accumulates stale connections. Adding `Close()` to the
interface ensures both transports are handled uniformly by `Fleet`, and the
REST no-op implementation keeps existing code unchanged.

**Why no Pause/Resume control?**
A paused benchmark holds a sandbox container, a set of bot goroutines blocked
in `Select`, and a Redpanda partition assignment — none of which are free. On
a shared Kubernetes cluster with spot workers, a paused benchmark prevents
other contestants' jobs from being scheduled. The observability stack already
streams metrics in real time to Grafana; there is no diagnostic value a pause
would add that isn't already visible live.

**Why no FIX transport in Phase 5?**
FIX requires a stateful session manager (sequence numbers, heartbeats,
logon/logoff, async execution reports). This does not map onto the synchronous
`BotTransport.Send` contract. Implementing FIX correctly would require
redesigning the `BotTransport` interface and adding a session lifecycle to
`Bot.Run`. Deferred to a future phase when contestant demand justifies it.
