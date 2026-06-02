package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// sandboxDeployer is the subset of *sandbox.DockerManager that SandboxExecutor
// needs. Unexported interface keeps executor.go independent of the sandbox
// package; tests inject a fakeSandboxDeployer instead.
// cmd/worker passes *sandbox.DockerManager directly — the compiler verifies
// it satisfies this interface at that call site.
type sandboxDeployer interface {
	Deploy(ctx context.Context, sub *models.Submission) (containerID string, hostPort int, err error)
	Stop(ctx context.Context, containerID string) error
	ContainerHealthy(ctx context.Context, containerID string) (bool, error)
}

// ContestQuerier is satisfied by *contest.ContestService.
//
// Defined here (not in internal/contest) to keep the dependency arrow
// pointing inward: worker defines what it needs; contest satisfies it
// structurally. This avoids a worker→contest import and keeps the interface
// narrow — the executor only needs one method.
//
// Injected via WithContestStore. When nil (Phase 1–4 compatibility mode or
// tests that don't exercise contest lookups), profile-aware fleet config and
// FinalScore computation are skipped.
type ContestQuerier interface {
	// GetActive returns the currently active contest.
	// Returns models.ErrNoActiveContest when none is active.
	GetActive(ctx context.Context) (*models.Contest, error)
}

// SandboxExecutor implements Executor using a sandboxDeployer.
type SandboxExecutor struct {
	docker             sandboxDeployer
	store              Store
	jobQueue           queue.Queue    // nil = local mode; non-nil = distributed (Phase 3+)
	contestQuerier     ContestQuerier // nil = Phase 1–4 compat; non-nil = Phase 5
	emitter            telemetry.Emitter
	healthPollInterval time.Duration
	healthTimeout      time.Duration
	fleetCfgOverride   *botfleet.FleetConfig
	sandboxHost        string // hostname the worker uses to reach published sandbox ports

	onStart  func(submissionID string)
	onFinish func()

	telemetryBatchSize int
}

// NewSandboxExecutor constructs a SandboxExecutor with production defaults.
// Apply functional options (WithJobCallbacks, WithHealthPollInterval, etc.) to
// customize behavior for tests or cmd/worker.
func NewSandboxExecutor(docker sandboxDeployer, store Store, opts ...ExecutorOption) *SandboxExecutor {
	e := &SandboxExecutor{
		docker:             docker,
		store:              store,
		healthPollInterval: 2 * time.Second,
		healthTimeout:      2 * time.Minute,
		telemetryBatchSize: 100,
		emitter:            telemetry.NoopEmitter{},
		sandboxHost:        "localhost",
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecutorOption is a functional option for SandboxExecutor.
type ExecutorOption func(*SandboxExecutor)

// WithHealthPollInterval overrides the health-check poll interval.
func WithHealthPollInterval(d time.Duration) ExecutorOption {
	return func(e *SandboxExecutor) { e.healthPollInterval = d }
}

// WithSandboxHost sets the hostname the executor uses to build the bot fleet
// target URL.
//
// When the worker runs inside a Docker container, sandbox port bindings are
// published on the HOST's network interface — not on localhost inside the
// worker container. Correct values:
//
//	"host-gateway"  — Docker Desktop (Windows/Mac)
//	"172.17.0.1"    — Linux Docker Engine default bridge
//	"localhost"     — Non-containerised local dev (default)
func WithSandboxHost(host string) ExecutorOption {
	return func(e *SandboxExecutor) {
		if host != "" {
			e.sandboxHost = host
		}
	}
}

// WithEmitter wires a telemetry.Emitter into the executor.
func WithEmitter(e telemetry.Emitter) ExecutorOption {
	return func(ex *SandboxExecutor) { ex.emitter = e }
}

// WithFleetConfig overrides the bot fleet configuration.
// Use in tests or custom benchmark scenarios.
func WithFleetConfig(cfg botfleet.FleetConfig) ExecutorOption {
	return func(e *SandboxExecutor) { e.fleetCfgOverride = &cfg }
}

// WithJobCallbacks wires onStart / onFinish callbacks for heartbeater sync.
//
//   - onStart(submissionID) is called just before Execute begins working.
//   - onFinish() is called when Execute returns (success or failure).
//
// Both must be goroutine-safe and return quickly.
func WithJobCallbacks(onStart func(string), onFinish func()) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.onStart = onStart
		e.onFinish = onFinish
	}
}

// WithJobQueue wires a queue.Queue into the executor so it can re-enqueue
// the next profile job after each Phase 5 profile run completes.
//
// Must be set in distributed mode. In local mode (nil) re-enqueue is skipped
// and all three profile jobs are treated as sequential in-process calls by
// the worker (which is acceptable for local-only testing).
func WithJobQueue(q queue.Queue) ExecutorOption {
	return func(e *SandboxExecutor) { e.jobQueue = q }
}

// WithContestStore wires a ContestQuerier into the executor so it can look
// up the active contest's VolatilityProfile for Phase 5 jobs.
//
// When nil (Phase 1–4 compat, or tests that don't exercise contest code),
// Execute falls back to DefaultFleetConfig and the legacy scoring path.
func WithContestStore(cq ContestQuerier) ExecutorOption {
	return func(e *SandboxExecutor) { e.contestQuerier = cq }
}

// Execute runs one benchmark profile for j:
//
//  1. Load submission from store.
//  2. Deploy sandbox container.
//  3. Persist container metadata.
//  4. Wait for container to become healthy.
//  5. Run the bot fleet (profile-aware if Phase 5, protocol-aware always).
//  6. Compute correctness score.
//  7. Build BenchmarkResults (profile-aware scoring if Phase 5).
//  8. Persist results to sub.AllResults (Phase 5) or sub.Results (Phase 1–4).
//  9. If Phase 5 and more profiles remain, enqueue the next profile job.
//     If Phase 5 and this is the last profile, compute FinalScore.
//
// 10. Stop container (always, via defer).
//
// The returned *BenchmarkResults is always the result for THIS profile run.
// worker.processJob uses j.IsLastProfile() to decide whether to mark the
// submission StatusCompleted.
func (e *SandboxExecutor) Execute(ctx context.Context, j queue.Job) (*models.BenchmarkResults, error) {
	log := slog.With(
		"executor", "sandbox",
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"language", j.Language,
		"volatility_label", j.VolatilityLabel,
	)

	if e.onStart != nil {
		e.onStart(j.SubmissionID)
	}
	defer func() {
		if e.onFinish != nil {
			e.onFinish()
		}
	}()

	// ── 1. Load submission ────────────────────────────────────────────────────
	sub, err := e.store.Get(j.SubmissionID)
	if err != nil {
		return nil, fmt.Errorf("executor: load submission %s: %w", j.SubmissionID, err)
	}

	// ── 2. Deploy sandbox ─────────────────────────────────────────────────────
	if j.VolatilityLabel != "" {
		log.Info("executor: deploying sandbox for profile run", "profile", j.VolatilityLabel)
	} else {
		log.Info("executor: deploying sandbox")
	}
	containerID, hostPort, err := e.docker.Deploy(ctx, sub)
	if err != nil {
		return nil, fmt.Errorf("executor: deploy sandbox for %s: %w", j.SubmissionID, err)
	}

	// ── 10. Cleanup — always stop the container when Execute returns ──────────
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if stopErr := e.docker.Stop(stopCtx, containerID); stopErr != nil {
			log.Warn("executor: failed to stop container on cleanup",
				"container_id", containerID[:12],
				"err", stopErr,
			)
		}
	}()

	// ── 3. Persist container metadata ─────────────────────────────────────────
	sub.ContainerID = containerID
	sub.ExposedPort = hostPort
	sub.ContainerName = fmt.Sprintf("nexusbench-%s", sub.ID[:8])
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to persist container metadata", "err", updateErr)
	}

	// ── 4. Wait for health ────────────────────────────────────────────────────
	log.Info("executor: waiting for container to become healthy",
		"container_id", containerID[:12],
		"host_port", hostPort,
	)
	if err = e.waitHealthy(ctx, containerID, log); err != nil {
		return nil, fmt.Errorf("executor: sandbox not healthy: %w", err)
	}
	log.Info("executor: container is healthy",
		"container_id", containerID[:12],
		"host_port", hostPort,
	)

	// ── 5. Run bot fleet ──────────────────────────────────────────────────────
	sub.Status = models.StatusBenchmarking
	sub.StatusMsg = "bot fleet running"
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to set StatusBenchmarking", "err", updateErr)
	}

	// Resolve the VolatilityProfile for this job (Phase 5 only).
	// profile is the zero value for Phase 1–4 jobs (label == "").
	var profile models.VolatilityProfile
	if j.VolatilityLabel != "" && e.contestQuerier != nil {
		if contest, cerr := e.contestQuerier.GetActive(ctx); cerr == nil {
			if p, ok := contest.ProfileByLabel(j.VolatilityLabel); ok {
				profile = p
			} else {
				log.Warn("executor: contest has no profile for label — using zero profile",
					"label", j.VolatilityLabel)
			}
		} else {
			log.Warn("executor: could not load active contest for profile lookup — using zero profile",
				"label", j.VolatilityLabel, "err", cerr)
		}
	}

	// Build the target URL for the bot fleet. The scheme depends on the
	// submission's Protocol field:
	//   REST      → http://host:port   (RESTTransport)
	//   WebSocket → ws://host:port     (WebSocketTransport)
	// The fleet config reads cfg.Protocol to pick the correct BotTransport
	// implementation; the URL scheme must be consistent with that choice.
	targetURL := targetURLForProtocol(sub.Protocol, e.sandboxHost, hostPort)
	log.Info("executor: target URL resolved",
		"protocol", sub.Protocol,
		"target_url", targetURL,
	)

	fleetResult, correctnessResult, err := e.runFleet(ctx, j, targetURL, sub.Protocol, profile, log)
	if err != nil {
		return nil, fmt.Errorf("executor: bot fleet: %w", err)
	}

	// ── 7. Build BenchmarkResults ─────────────────────────────────────────────
	results := buildResults(fleetResult, correctnessResult, profile, j.VolatilityLabel)

	if j.VolatilityLabel != "" {
		log.Info("executor: profile run complete",
			"volatility_label", results.VolatilityLabel,
			"p50_ms", results.P50LatencyMs,
			"p99_ms", results.P99LatencyMs,
			"sustained_tps", results.SustainedTPS,
			"correctness", results.CorrectnessScore,
			"run_score", results.RunScore,
		)
	} else {
		log.Info("executor: benchmark complete",
			"p50_ms", results.P50LatencyMs,
			"p99_ms", results.P99LatencyMs,
			"max_tps", results.MaxTPS,
			"correctness", results.CorrectnessScore,
			"composite_score", results.CompositeScore,
		)
	}

	// ── 8. Persist results ────────────────────────────────────────────────────
	// Phase 5 jobs append to AllResults; Phase 1–4 jobs write to Results
	// (the legacy field) for backward compatibility.
	if j.VolatilityLabel != "" {
		if err := e.appendProfileResult(sub, results, log); err != nil {
			return nil, err
		}
	}
	// Phase 1–4: worker.processJob writes sub.Results = results after Execute
	// returns, so we do nothing here for that path.

	// ── 9. Dispatch next profile job or compute FinalScore ────────────────────
	if j.VolatilityLabel != "" {
		if len(j.RemainingProfiles) > 0 {
			if err := e.enqueueNextProfile(ctx, sub, j, log); err != nil {
				return nil, err
			}
		} else {
			// This was the last profile. Compute and persist the FinalScore.
			e.computeAndWriteFinalScore(ctx, sub, j, log)
		}
	}

	return results, nil
}

// targetURLForProtocol builds the target URL for the bot fleet based on the
// submission's declared protocol.
//
//   - REST (default, backward compat): "http://host:port"
//   - WebSocket:                       "ws://host:port"
//
// The /orders path suffix is intentionally omitted here: REST sends to
// /orders as a hardcoded path in RESTTransport.Send; WebSocketTransport uses
// the path from the URL during the upgrade handshake (and /orders is the
// conventional path). Both handle this internally, so the executor need not
// repeat the path.
func targetURLForProtocol(protocol models.Protocol, host string, port int) string {
	switch protocol {
	case models.ProtocolWebSocket:
		return fmt.Sprintf("ws://%s:%d/orders", host, port)
	default:
		// REST, FIX (not yet implemented), or empty — all fall back to HTTP.
		return fmt.Sprintf("http://%s:%d", host, port)
	}
}

// appendProfileResult reloads the submission from the store (to pick up any
// concurrent writes from previous profile jobs), appends the new result to
// AllResults, and persists the updated submission.
//
// We reload before appending because in distributed mode another worker may
// have written the previous profile's result between the time Execute loaded
// sub at step 1 and now. A stale read would clobber that result.
func (e *SandboxExecutor) appendProfileResult(
	sub *models.Submission,
	results *models.BenchmarkResults,
	log *slog.Logger,
) error {
	// Re-read from store to get the latest AllResults slice.
	fresh, err := e.store.Get(sub.ID)
	if err != nil {
		log.Warn("executor: could not re-read submission for AllResults append — using cached copy",
			"submission_id", sub.ID, "err", err)
		fresh = sub // fall back to the in-memory copy
	}

	fresh.AllResults = append(fresh.AllResults, results)

	if updateErr := e.store.Update(fresh); updateErr != nil {
		return fmt.Errorf("executor: persist AllResults for %s: %w", sub.ID, updateErr)
	}
	// Update the caller's pointer so step 9 can use the freshened copy.
	*sub = *fresh
	return nil
}

// enqueueNextProfile constructs and enqueues the job for the next volatility
// profile in the chain. Called after each non-final profile run commits.
func (e *SandboxExecutor) enqueueNextProfile(
	ctx context.Context,
	sub *models.Submission,
	j queue.Job,
	log *slog.Logger,
) error {
	nextLabel := j.RemainingProfiles[0]
	remaining := j.RemainingProfiles[1:]

	log.Info("executor: enqueueing next profile job",
		"current_label", j.VolatilityLabel,
		"next_label", nextLabel,
		"remaining_after_next", remaining,
	)

	nextJob := queue.NewProfileJob(sub, j.ContestID, nextLabel, remaining)

	if e.jobQueue == nil {
		log.Error("executor: cannot enqueue next profile — no job queue configured (local mode?)",
			"next_label", nextLabel)
		e.markFailed(sub, fmt.Sprintf(
			"cannot enqueue next profile %q: worker has no job queue (local mode)", nextLabel))
		return fmt.Errorf("executor: no job queue to enqueue next profile %q", nextLabel)
	}

	if err := e.jobQueue.Enqueue(ctx, nextJob); err != nil {
		log.Error("executor: failed to enqueue next profile job",
			"next_label", nextLabel, "err", err)
		e.markFailed(sub, fmt.Sprintf(
			"failed to enqueue next profile %q: %v", nextLabel, err))
		return fmt.Errorf("executor: enqueue next profile %q: %w", nextLabel, err)
	}

	log.Info("executor: next profile job enqueued",
		"next_job_id", nextJob.ID,
		"next_label", nextLabel,
	)
	return nil
}

// computeAndWriteFinalScore aggregates the three RunScores from AllResults
// using the contest's aggregate weights and writes the result to the
// submission store.
func (e *SandboxExecutor) computeAndWriteFinalScore(
	ctx context.Context,
	sub *models.Submission,
	j queue.Job,
	log *slog.Logger,
) {
	fresh, err := e.store.Get(sub.ID)
	if err != nil {
		log.Error("executor: computeAndWriteFinalScore: cannot reload submission",
			"submission_id", sub.ID, "err", err)
		return
	}

	lowW, medW, highW := 0.20, 0.35, 0.45
	if e.contestQuerier != nil {
		if contest, cerr := e.contestQuerier.GetActive(ctx); cerr == nil {
			lowW = contest.LowWeight
			medW = contest.MediumWeight
			highW = contest.HighWeight
		} else {
			log.Warn("executor: computeAndWriteFinalScore: cannot load contest weights — using defaults",
				"err", cerr)
		}
	}

	safeScore := func(label string) float64 {
		if r := fresh.ResultByLabel(label); r != nil {
			return r.RunScore
		}
		return 0.0
	}

	finalScore := (lowW*safeScore("low") +
		medW*safeScore("medium") +
		highW*safeScore("high")) * 100.0

	log.Info("executor: FinalScore computed",
		"submission_id", fresh.ID,
		"low_score", safeScore("low"),
		"medium_score", safeScore("medium"),
		"high_score", safeScore("high"),
		"low_weight", lowW,
		"medium_weight", medW,
		"high_weight", highW,
		"final_score", finalScore,
	)

	fresh.FinalScore = finalScore
	if updateErr := e.store.Update(fresh); updateErr != nil {
		log.Error("executor: failed to persist FinalScore",
			"submission_id", fresh.ID, "err", updateErr)
	}
}

// markFailed writes StatusFailed to the submission store.
func (e *SandboxExecutor) markFailed(sub *models.Submission, msg string) {
	sub.Status = models.StatusFailed
	sub.StatusMsg = msg
	if err := e.store.Update(sub); err != nil {
		slog.Error("executor: markFailed: store.Update failed",
			"submission_id", sub.ID, "err", err)
	}
}

// runFleet constructs and runs the bot fleet with the correct configuration
// for this job. The protocol parameter is read from the submission and
// propagated into the FleetConfig so newBot selects the correct transport.
//
//   - Phase 5, profile resolved: buildFleetConfigFromProfile (profile-aware).
//   - Phase 5, profile unavailable: buildFleetConfig (falls back to defaults).
//   - Phase 1–4 (label == ""): buildFleetConfig (fleetCfgOverride or defaults).
//
// In all cases, protocol is threaded through so WebSocket submissions use
// WebSocketTransport regardless of which config path is taken.
func (e *SandboxExecutor) runFleet(
	ctx context.Context,
	j queue.Job,
	targetURL string,
	protocol models.Protocol,
	profile models.VolatilityProfile,
	log *slog.Logger,
) (*botfleet.FleetResult, *correctness.CorrectnessResult, error) {

	var cfg botfleet.FleetConfig
	if j.VolatilityLabel != "" && profile.BotCount > 0 {
		cfg = buildFleetConfigFromProfile(targetURL, profile)
	} else {
		cfg = e.buildFleetConfig(targetURL)
	}

	// Stage 5.8: set the protocol on the fleet config so Fleet.newBot picks
	// the correct BotTransport. This is the single place where the submission's
	// Protocol field reaches the transport layer.
	//
	// Backward compat: if protocol is empty or "rest" (Phase 1–4 submissions
	// that pre-date the Protocol field), cfg.Protocol stays "" which defaults
	// to "rest" inside Fleet.newBot. No change in behavior.
	cfg.Protocol = string(protocol)

	log.Info("executor: launching bot fleet",
		"target_url", targetURL,
		"protocol", cfg.Protocol,
		"bot_count", cfg.BotCount,
		"test_duration", cfg.TestDuration,
	)

	fleet, err := botfleet.NewFleet(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("construct fleet: %w", err)
	}

	fleetResult, err := fleet.Run(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("fleet run: %w", err)
	}

	log.Info("executor: bot fleet complete",
		"total_orders", fleetResult.Stats.TotalOrders,
		"successful_orders", fleetResult.Stats.SuccessfulOrders,
		"error_orders", fleetResult.Stats.ErrorOrders,
		"sustained_tps", fleetResult.Stats.SustainedTPS,
	)

	correctnessResult := e.checkCorrectness(fleetResult.Results, log)
	e.emitFleetTelemetry(ctx, j.SubmissionID, fleetResult.Results, log)

	return fleetResult, &correctnessResult, nil
}

// buildFleetConfig returns the fleet configuration for Phase 1–4 jobs (or
// Phase 5 jobs where the profile could not be resolved).
//
// Priority: fleetCfgOverride (tests/dry-run) → DefaultFleetConfig.
// Protocol is NOT set here; it is applied by runFleet after construction.
func (e *SandboxExecutor) buildFleetConfig(targetURL string) botfleet.FleetConfig {
	if e.fleetCfgOverride != nil {
		cfg := *e.fleetCfgOverride
		cfg.TargetURL = targetURL
		return cfg
	}
	return botfleet.DefaultFleetConfig(targetURL)
}

// buildFleetConfigFromProfile translates a VolatilityProfile into the
// botfleet.FleetConfig used for a Phase 5 benchmark run.
// Protocol is NOT set here; it is applied by runFleet after construction.
func buildFleetConfigFromProfile(targetURL string, p models.VolatilityProfile) botfleet.FleetConfig {
	return botfleet.FleetConfig{
		BotCount:          p.BotCount,
		RampUpDuration:    5 * time.Second,
		TestDuration:      p.TestDuration,
		TargetURL:         targetURL,
		PerBotHTTPTimeout: 2 * time.Second,
		GeneratorConfig: botfleet.RandomGeneratorConfig{
			Ratios: botfleet.OrderRatios{
				Limit:  p.LimitRatio,
				Market: p.MarketRatio,
				Cancel: p.CancelRatio,
			},
			Price: botfleet.PriceConfig{
				MidPrice: 10_000,
				Spread:   p.PriceSpreadCents,
			},
			Quantity: botfleet.QuantityConfig{
				Min: 1,
				Max: p.MaxQuantity,
			},
		},
	}
}

// buildResults converts fleet + correctness data into models.BenchmarkResults.
//
// Dispatch logic (keyed on label):
//   - label == "" (Phase 1–4): legacy hardcoded constants; CompositeScore set.
//   - label != "" (Phase 5): profile-aware targets; RunScore set (0.0–1.0).
//     CompositeScore = RunScore×100 for legacy leaderboard handler compat.
func buildResults(
	fr *botfleet.FleetResult,
	cr *correctness.CorrectnessResult,
	profile models.VolatilityProfile,
	label string,
) *models.BenchmarkResults {

	stats := fr.Stats

	correctnessScore := 0.0
	var totalOrders, correctFills, incorrectFills int64
	if cr != nil {
		correctnessScore = cr.Score
		totalOrders = stats.TotalOrders
		correctFills = cr.CorrectFills
		incorrectFills = cr.IncorrectFills
	}

	duration := fr.FinishedAt.Sub(fr.StartedAt).String()

	result := &models.BenchmarkResults{
		VolatilityLabel:   label,
		P50LatencyMs:      stats.P50Ms(),
		P90LatencyMs:      stats.P90Ms(),
		P99LatencyMs:      stats.P99Ms(),
		MaxTPS:            stats.MaxTPS,
		SustainedTPS:      stats.SustainedTPS,
		CorrectnessScore:  correctnessScore,
		TotalOrders:       totalOrders,
		CorrectFills:      correctFills,
		IncorrectFills:    incorrectFills,
		BenchmarkDuration: duration,
		CompletedAt:       fr.FinishedAt,
	}

	if label == "" {
		// ── Phase 1–4 backward-compatible scoring ─────────────────────────────
		const (
			legacyTargetP99Ns  = float64(1_000_000)   // 1 ms
			legacyWorstP99Ns   = float64(100_000_000) // 100 ms
			legacyTargetMaxTPS = float64(50_000)
		)
		normP99 := 0.0
		p99 := stats.P99Ns
		if p99 <= legacyTargetP99Ns {
			normP99 = 1.0
		} else if p99 < legacyWorstP99Ns {
			normP99 = 1.0 - (p99-legacyTargetP99Ns)/(legacyWorstP99Ns-legacyTargetP99Ns)
		}
		normTPS := stats.MaxTPS / legacyTargetMaxTPS
		if normTPS > 1.0 {
			normTPS = 1.0
		}
		result.CompositeScore = (0.5*normP99 + 0.3*normTPS + 0.2*correctnessScore) * 100.0
		return result
	}

	// ── Phase 5 profile-aware scoring ─────────────────────────────────────────
	worstP99Ns := float64(profile.TargetP99Ns) * 10.0
	normP99 := 0.0
	p99 := stats.P99Ns
	switch {
	case p99 <= float64(profile.TargetP99Ns):
		normP99 = 1.0
	case p99 < worstP99Ns:
		normP99 = 1.0 - (p99-float64(profile.TargetP99Ns))/(worstP99Ns-float64(profile.TargetP99Ns))
	}

	normTPS := 0.0
	if profile.TargetSustainTPS > 0 {
		normTPS = stats.SustainedTPS / profile.TargetSustainTPS
		if normTPS > 1.0 {
			normTPS = 1.0
		}
	}

	runScore := correctnessScore*
		(profile.LatencyWeight*normP99+profile.ThroughputWeight*normTPS) +
		profile.CorrectnessWeight*correctnessScore

	result.RunScore = runScore
	result.CompositeScore = runScore * 100.0
	return result
}

// checkCorrectness replays the order sequence through the GoldenOrderbook and
// compares canonical fills against what the contestant's engine returned.
func (e *SandboxExecutor) checkCorrectness(
	results []botfleet.OrderResult,
	log *slog.Logger,
) correctness.CorrectnessResult {

	book := correctness.NewGoldenOrderbook()
	checker := correctness.NewChecker()

	var goldenFills []correctness.GoldenFill
	var contestantFills []correctness.ContestantFill

	for i := range results {
		r := &results[i]
		if r.Err != nil {
			continue
		}

		goldenOrder := correctness.GoldenOrder{
			ID:       r.Order.ID,
			Kind:     correctness.OrderKind(r.Order.Kind),
			Side:     correctness.Side(r.Order.Side),
			Price:    r.Order.Price,
			Quantity: r.Order.Quantity,
		}

		gf, err := book.Apply(goldenOrder)
		if err != nil {
			log.Warn("correctness: golden orderbook rejected order",
				"order_id", r.Order.ID, "err", err)
			continue
		}
		goldenFills = append(goldenFills, gf)

		contestantFills = append(contestantFills, correctness.ContestantFill{
			OrderID:       r.Fill.OrderID,
			ExecutedPrice: r.Fill.ExecutedPrice,
			ExecutedQty:   r.Fill.ExecutedQty,
			Accepted:      r.Fill.Accepted,
		})
	}

	result := checker.Check(contestantFills, goldenFills)
	log.Info("executor: correctness check complete",
		"score", result.Score,
		"total_fills", result.TotalFills,
		"correct", result.CorrectFills,
		"incorrect", result.IncorrectFills,
	)
	return result
}

// emitFleetTelemetry converts OrderResults to telemetry.Events and sends
// them to the configured Emitter in batches of e.telemetryBatchSize.
//
// Errors are logged but NOT propagated — telemetry failure must never prevent
// the benchmark result from being stored.
func (e *SandboxExecutor) emitFleetTelemetry(
	ctx context.Context,
	submissionID string,
	results []botfleet.OrderResult,
	log *slog.Logger,
) {
	if len(results) == 0 {
		return
	}

	batch := make([]telemetry.Event, 0, e.telemetryBatchSize)
	emitted := 0
	errors := 0

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := e.emitter.BatchEmit(ctx, batch); err != nil {
			log.Warn("executor: telemetry batch emit error",
				"batch_size", len(batch), "err", err)
			errors++
		} else {
			emitted += len(batch)
		}
		batch = batch[:0]
	}

	for i := range results {
		r := &results[i]
		if r.Err != nil {
			continue
		}

		kind := telemetry.KindOrderAck
		if !r.Fill.Accepted {
			kind = telemetry.KindReject
		} else if r.Fill.ExecutedQty > 0 {
			kind = telemetry.KindFill
		} else if r.Order.Kind == botfleet.KindCancel {
			kind = telemetry.KindCancelAck
		}

		batch = append(batch, telemetry.Event{
			Kind:         kind,
			SubmissionID: submissionID,
			Timestamp:    r.SentAt.UTC(),
			OrderID:      r.Order.ID,
			LatencyNs:    r.LatencyNs,
			Meta: map[string]string{
				"order_kind": string(r.Order.Kind),
				"side":       string(r.Order.Side),
				"accepted":   strconv.FormatBool(r.Fill.Accepted),
			},
		})

		if len(batch) >= e.telemetryBatchSize {
			flush()
		}
	}
	flush()

	log.Info("executor: telemetry emission complete",
		"emitted", emitted,
		"batch_errors", errors,
		"total_results", len(results),
	)
}

func (e *SandboxExecutor) waitHealthy(ctx context.Context, containerID string, log *slog.Logger) error {
	deadline := time.Now().Add(e.healthTimeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("container did not become healthy within %s", e.healthTimeout)
		}
		healthy, err := e.docker.ContainerHealthy(ctx, containerID)
		if err != nil {
			return fmt.Errorf("health check error: %w", err)
		}
		if healthy {
			return nil
		}
		log.Debug("executor: not yet healthy, retrying",
			"container_id", containerID[:12],
			"poll_interval", e.healthPollInterval,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.healthPollInterval):
		}
	}
}
