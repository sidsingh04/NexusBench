package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/correctness"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/telemetry"
	"github.com/nexusbench/nexusbench/internal/validator"
)

// sandboxDeployer is the subset of *sandbox.DockerManager that SandboxExecutor
// needs. Unexported interface keeps executor.go independent of the sandbox
// package; tests inject a fakeSandboxDeployer instead.
type sandboxDeployer interface {
	Deploy(ctx context.Context, sub *models.Submission) (containerID string, hostPort int, err error)
	Stop(ctx context.Context, containerID string) error
	ContainerHealthy(ctx context.Context, containerID string) (bool, error)
}

// ContestQuerier is satisfied by *contest.ContestService.
// Defined here to keep the dependency arrow pointing inward.
type ContestQuerier interface {
	GetActive(ctx context.Context) (*models.Contest, error)
}

// SandboxExecutor implements Executor using a sandboxDeployer.
type SandboxExecutor struct {
	docker             sandboxDeployer
	store              Store
	jobQueue           queue.Queue
	contestQuerier     ContestQuerier
	emitter            telemetry.Emitter
	healthPollInterval time.Duration
	healthTimeout      time.Duration
	fleetCfgOverride   *botfleet.FleetConfig
	sandboxHost        string

	onStart  func(submissionID string)
	onFinish func()

	telemetryBatchSize int

	warmupDelay time.Duration

	// validatorFactory constructs a *validator.Validator targeting a given URL.
	// When nil, the pre-flight gate is skipped entirely (Phase 1–4 compat and
	// tests that do not exercise the validator). Injected via
	// WithPreflightValidator.
	validatorFactory func(targetURL string) *validator.Validator
}

// NewSandboxExecutor constructs a SandboxExecutor with production defaults.
func NewSandboxExecutor(docker sandboxDeployer, store Store, opts ...ExecutorOption) *SandboxExecutor {
	e := &SandboxExecutor{
		docker:             docker,
		store:              store,
		healthPollInterval: 2 * time.Second,
		healthTimeout:      2 * time.Minute,
		telemetryBatchSize: 100,
		emitter:            telemetry.NoopEmitter{},
		sandboxHost:        "localhost",
		warmupDelay:        2 * time.Second,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecutorOption is a functional option for SandboxExecutor.
type ExecutorOption func(*SandboxExecutor)

func WithHealthPollInterval(d time.Duration) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.healthPollInterval = d
		// In tests where we override health poll interval to be extremely fast,
		// we also skip the proxy warmup delay so tests don't pause for 2 seconds.
		if d < time.Second {
			e.warmupDelay = 0
		}
	}
}

func WithSandboxHost(host string) ExecutorOption {
	return func(e *SandboxExecutor) {
		if host != "" {
			e.sandboxHost = host
		}
	}
}

func WithEmitter(em telemetry.Emitter) ExecutorOption {
	return func(e *SandboxExecutor) { e.emitter = em }
}

func WithFleetConfig(cfg botfleet.FleetConfig) ExecutorOption {
	return func(e *SandboxExecutor) { e.fleetCfgOverride = &cfg }
}

func WithJobCallbacks(onStart func(string), onFinish func()) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.onStart = onStart
		e.onFinish = onFinish
	}
}

func WithJobQueue(q queue.Queue) ExecutorOption {
	return func(e *SandboxExecutor) { e.jobQueue = q }
}

func WithContestStore(cq ContestQuerier) ExecutorOption {
	return func(e *SandboxExecutor) { e.contestQuerier = cq }
}

// WithPreflightValidator wires a validator factory into the executor.
// The factory is called once per low-profile job, after WaitHealthy and
// before the bot fleet starts.
//
// When nil (the default), the pre-flight gate is completely skipped:
//   - Phase 1–4 jobs are unaffected
//   - Phase 5 medium/high profile jobs are unaffected (gate runs only on low)
//   - Existing tests without a factory configured continue to pass unchanged
func WithPreflightValidator(factory func(targetURL string) *validator.Validator) ExecutorOption {
	return func(e *SandboxExecutor) { e.validatorFactory = factory }
}

// Execute runs one benchmark profile for j.
//
//  1. Load submission from store.
//  2. Deploy sandbox container.
//  3. Persist container metadata.
//  4. Wait for container to become healthy.
//     4a. Run pre-flight validator (Phase 5 low-profile jobs only).
//     Failure: mark StatusFailed, return error — bot fleet never starts.
//  5. Run the bot fleet.
//  6. Compute correctness score.
//  7. Build BenchmarkResults.
//  8. Persist results.
//  9. Enqueue next profile job or compute FinalScore.
//  10. Stop container (always, via defer).
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
				"container_id", containerID[:12], "err", stopErr)
		}
	}()

	// ── 3. Persist container metadata ─────────────────────────────────────────
	sub.ContainerID = containerID
	sub.ExposedPort = hostPort
	sub.ContainerName = fmt.Sprintf("nexusbench-%s", sub.ID[:8])
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to persist container metadata", "err", updateErr)
	}

	// Build the target URL now — it is needed by both the pre-flight validator
	// (step 4a) and the bot fleet (step 5), so we resolve it once here.
	targetURL := targetURLForProtocol(sub.Protocol, e.sandboxHost, hostPort)
	log.Info("executor: target URL resolved",
		"protocol", sub.Protocol, "target_url", targetURL)

	// ── 4. Wait for health ────────────────────────────────────────────────────
	log.Info("executor: waiting for container to become healthy",
		"container_id", containerID[:12], "host_port", hostPort)
	if err = e.waitHealthy(ctx, containerID, targetURL, log); err != nil {
		return nil, fmt.Errorf("executor: sandbox not healthy: %w", err)
	}
	log.Info("executor: container is healthy",
		"container_id", containerID[:12], "host_port", hostPort)

	// ── 4a. Pre-flight validator ──────────────────────────────────────────────
	// Runs only on the first (low) profile job. On failure: writes DryRunResult
	// to the submission, marks StatusFailed, and returns — bot fleet never runs.
	// Skipped silently for Phase 1–4 jobs (VolatilityLabel == "") and for
	// medium/high profile jobs.
	if preflightErr := e.runPreflightValidator(ctx, sub, targetURL, j, log); preflightErr != nil {
		return nil, preflightErr
	}

	// ── 5. Run bot fleet ──────────────────────────────────────────────────────
	sub.Status = models.StatusBenchmarking
	sub.StatusMsg = "bot fleet running"
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to set StatusBenchmarking", "err", updateErr)
	}

	var profile models.VolatilityProfile
	switch j.VolatilityLabel {
	case "low":
		profile = contest.DefaultLowProfile()
	case "medium":
		profile = contest.DefaultMediumProfile()
	case "high":
		profile = contest.DefaultHighProfile()
	}

	if j.VolatilityLabel != "" && e.contestQuerier != nil {
		if active, cerr := e.contestQuerier.GetActive(ctx); cerr == nil {
			if p, ok := active.ProfileByLabel(j.VolatilityLabel); ok {
				profile = p
			} else {
				log.Warn("executor: contest has no profile for label — using default",
					"label", j.VolatilityLabel)
			}
		} else {
			log.Warn("executor: could not load active contest — using default profile",
				"label", j.VolatilityLabel, "err", cerr)
		}
	}

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
	if j.VolatilityLabel != "" {
		if err := e.appendProfileResult(sub, results, log); err != nil {
			return nil, err
		}
	}

	// ── 9. Dispatch next profile job or compute FinalScore ────────────────────
	if j.VolatilityLabel != "" {
		if len(j.RemainingProfiles) > 0 {
			if err := e.enqueueNextProfile(ctx, sub, j, log); err != nil {
				return nil, err
			}
		} else {
			e.computeAndWriteFinalScore(ctx, sub, j, log)
		}
	}

	return results, nil
}

// ── Pre-flight validator ──────────────────────────────────────────────────────

// runPreflightValidator runs the dry-run validator against the live sandbox
// before the bot fleet starts. It is a no-op (returns nil) in three cases:
//
//  1. e.validatorFactory is nil — no factory configured (local mode, tests).
//  2. j.VolatilityLabel is "" — Phase 1–4 job.
//  3. j.VolatilityLabel is not "low" — medium/high profile jobs skip the gate
//     because the previous profile's bot fleet has already placed resting orders;
//     running the validator against a non-empty book would produce false failures.
//
// On validation failure: writes a DryRunResult with per-scenario detail to the
// submission, marks StatusFailed with a human-readable summary, and returns a
// non-nil error so Execute stops before the bot fleet runs.
//
// On validation success: writes a DryRunResult with AllPassed=true so the
// frontend can confirm "Validation Passed" to the contestant.
func (e *SandboxExecutor) runPreflightValidator(
	ctx context.Context,
	sub *models.Submission,
	targetURL string,
	j queue.Job,
	log *slog.Logger,
) error {
	if e.validatorFactory == nil {
		return nil // no factory: skip silently
	}
	if j.VolatilityLabel == "" {
		return nil // Phase 1–4 job: skip silently
	}
	if j.VolatilityLabel != "low" {
		return nil // medium/high: skip (book not empty from previous profile)
	}

	v := e.validatorFactory(targetURL)

	log.Info("executor: running pre-flight validator",
		"submission_id", sub.ID,
		"target_url", targetURL,
	)

	// 2-minute hard timeout. Generous for 21 scenarios + 3s concurrent burst.
	valCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	result, err := v.Run(valCtx, sub.ID)
	if err != nil {
		// Context timeout or transport-level failure.
		dryRun := &models.DryRunResult{
			AllPassed:   false,
			RanAt:       time.Now().UTC(),
			FailSummary: fmt.Sprintf("validator did not complete: %v", err),
		}
		e.writeDryRunResult(sub, dryRun, log)
		e.markFailed(sub, "pre-flight validator did not complete: "+err.Error())
		return fmt.Errorf("executor: pre-flight validator: %w", err)
	}

	// Convert validator.ValidationResult → models.DryRunResult.
	dryRun := &models.DryRunResult{
		AllPassed: result.AllPassed,
		RanAt:     result.TestedAt,
		Scenarios: make([]models.DryRunScenarioResult, len(result.Scenarios)),
	}
	for i, sc := range result.Scenarios {
		dryRun.Scenarios[i] = models.DryRunScenarioResult{
			Name:   sc.Name,
			Passed: sc.Passed,
			Reason: sc.Reason,
		}
	}

	if !result.AllPassed {
		var failedNames []string
		for _, sc := range result.Scenarios {
			if !sc.Passed {
				failedNames = append(failedNames, sc.Name)
			}
		}
		dryRun.FailSummary = fmt.Sprintf(
			"%d/%d scenarios failed: [%s]",
			len(failedNames), len(result.Scenarios),
			strings.Join(failedNames, ", "),
		)
		e.writeDryRunResult(sub, dryRun, log)
		e.markFailed(sub, "pre-flight validation failed — "+dryRun.FailSummary)

		log.Warn("executor: pre-flight validation FAILED — bot fleet will NOT run",
			"submission_id", sub.ID,
			"failed_count", len(failedNames),
			"failed_scenarios", failedNames,
		)
		return fmt.Errorf("executor: pre-flight validation failed: %s", dryRun.FailSummary)
	}

	// All scenarios passed — persist so the frontend can show "Validation Passed".
	e.writeDryRunResult(sub, dryRun, log)
	log.Info("executor: pre-flight validation PASSED — proceeding to bot fleet",
		"submission_id", sub.ID,
		"scenarios_passed", len(result.Scenarios),
	)
	return nil
}

// writeDryRunResult reloads the submission from the store (to pick up any
// concurrent writes), sets DryRunResult, and persists. Reloading prevents
// clobbering concurrent status updates from other goroutines.
func (e *SandboxExecutor) writeDryRunResult(
	sub *models.Submission,
	dryRun *models.DryRunResult,
	log *slog.Logger,
) {
	fresh, err := e.store.Get(sub.ID)
	if err != nil {
		log.Warn("executor: writeDryRunResult: could not reload submission — using cached copy",
			"submission_id", sub.ID, "err", err)
		fresh = sub
	}
	fresh.DryRunResult = dryRun
	if updateErr := e.store.Update(fresh); updateErr != nil {
		log.Error("executor: writeDryRunResult: store.Update failed",
			"submission_id", sub.ID, "err", updateErr)
	}
	// Keep the caller's copy in sync.
	*sub = *fresh
}

// ── Remaining executor methods (unchanged) ────────────────────────────────────

func targetURLForProtocol(protocol models.Protocol, host string, port int) string {
	switch protocol {
	case models.ProtocolWebSocket:
		return fmt.Sprintf("ws://%s:%d/orders", host, port)
	default:
		return fmt.Sprintf("http://%s:%d", host, port)
	}
}

func (e *SandboxExecutor) appendProfileResult(
	sub *models.Submission,
	results *models.BenchmarkResults,
	log *slog.Logger,
) error {
	fresh, err := e.store.Get(sub.ID)
	if err != nil {
		log.Warn("executor: could not re-read submission for AllResults append — using cached copy",
			"submission_id", sub.ID, "err", err)
		fresh = sub
	}
	fresh.AllResults = append(fresh.AllResults, results)
	if updateErr := e.store.Update(fresh); updateErr != nil {
		return fmt.Errorf("executor: persist AllResults for %s: %w", sub.ID, updateErr)
	}
	*sub = *fresh
	return nil
}

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
		log.Error("executor: cannot enqueue next profile — no job queue configured",
			"next_label", nextLabel)
		e.markFailed(sub, fmt.Sprintf(
			"cannot enqueue next profile %q: worker has no job queue", nextLabel))
		return fmt.Errorf("executor: no job queue to enqueue next profile %q", nextLabel)
	}

	if err := e.jobQueue.Enqueue(ctx, nextJob); err != nil {
		log.Error("executor: failed to enqueue next profile job",
			"next_label", nextLabel, "err", err)
		e.markFailed(sub, fmt.Sprintf("failed to enqueue next profile %q: %v", nextLabel, err))
		return fmt.Errorf("executor: enqueue next profile %q: %w", nextLabel, err)
	}

	log.Info("executor: next profile job enqueued",
		"next_job_id", nextJob.ID, "next_label", nextLabel)
	return nil
}

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
		if c, cerr := e.contestQuerier.GetActive(ctx); cerr == nil {
			lowW = c.LowWeight
			medW = c.MediumWeight
			highW = c.HighWeight
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
		"final_score", finalScore,
	)

	fresh.FinalScore = finalScore
	if updateErr := e.store.Update(fresh); updateErr != nil {
		log.Error("executor: failed to persist FinalScore",
			"submission_id", fresh.ID, "err", updateErr)
	}
}

func (e *SandboxExecutor) markFailed(sub *models.Submission, msg string) {
	sub.Status = models.StatusFailed
	sub.StatusMsg = msg
	if err := e.store.Update(sub); err != nil {
		slog.Error("executor: markFailed: store.Update failed",
			"submission_id", sub.ID, "err", err)
	}
}

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

func (e *SandboxExecutor) buildFleetConfig(targetURL string) botfleet.FleetConfig {
	if e.fleetCfgOverride != nil {
		cfg := *e.fleetCfgOverride
		cfg.TargetURL = targetURL
		return cfg
	}
	return botfleet.DefaultFleetConfig(targetURL)
}

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
		BenchmarkDuration: fr.FinishedAt.Sub(fr.StartedAt).String(),
		CompletedAt:       fr.FinishedAt,
	}

	if label == "" {
		const (
			legacyTargetP99Ns  = float64(1_000_000)
			legacyWorstP99Ns   = float64(100_000_000)
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
		if r.Err != nil {
			continue
		}
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
		"emitted", emitted, "batch_errors", errors, "total_results", len(results))
}

func (e *SandboxExecutor) waitHealthy(ctx context.Context, containerID string, targetURL string, log *slog.Logger) error {
	deadline := time.Now().Add(e.healthTimeout)
	client := &http.Client{Timeout: 1 * time.Second}
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("container did not become healthy within %s", e.healthTimeout)
		}
		healthy, err := e.docker.ContainerHealthy(ctx, containerID)
		if err != nil {
			return fmt.Errorf("health check error: %w", err)
		}
		if healthy {
			// Check HTTP health explicitly to avoid racing the Go binary startup
			httpURL := strings.Replace(targetURL, "ws://", "http://", 1)
			resp, err := client.Get(httpURL + "/health")
			if err == nil && resp.StatusCode == 200 {
				resp.Body.Close()
				// Add a brief warmup delay: Docker Desktop's port forwarding proxy (vpnkit)
				// can sometimes drop new connections (EOF) immediately after the first successful
				// connection while its internal state stabilizes.
				if e.warmupDelay > 0 {
					time.Sleep(e.warmupDelay)
				}
				return nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
		log.Debug("executor: not yet healthy, retrying",
			"container_id", containerID[:12], "poll_interval", e.healthPollInterval)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.healthPollInterval):
		}
	}
}

// PreflightValidatorFactory is the production factory used in cmd/worker/main.go.
// Exported so cmd/worker can reference it without duplicating the construction logic.
func PreflightValidatorFactory(targetURL string) *validator.Validator {
	transport := botfleet.NewRESTTransport(targetURL, &http.Client{Timeout: 5 * time.Second})
	return validator.New(transport)
}
