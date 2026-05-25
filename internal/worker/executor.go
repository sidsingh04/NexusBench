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

// SandboxExecutor implements Executor using a sandboxDeployer.
type SandboxExecutor struct {
	docker             sandboxDeployer
	store              Store
	emitter            telemetry.Emitter
	healthPollInterval time.Duration
	healthTimeout      time.Duration
	fleetCfgOverride   *botfleet.FleetConfig
	sandboxHost        string // hostname the worker uses to reach published sandbox ports

	onStart func(submissionID string)
	onFinish func()

	telemetryBatchSize int
}

// NewSandboxExecutor constructs a SandboxExecutor with production defaults.
// Apply functional options (WithJobCallbacks, WithHealthPollInterval, etc.) to
// customise behaviour for tests or cmd/worker.
func NewSandboxExecutor(docker sandboxDeployer, store Store, opts ...ExecutorOption) *SandboxExecutor {
	e := &SandboxExecutor{
		docker:             docker,
		store:              store,
		healthPollInterval: 2 * time.Second,
		healthTimeout:      2 * time.Minute,
		telemetryBatchSize: 100,
		emitter:            telemetry.NoopEmitter{},
		sandboxHost:        "localhost", // correct for non-containerised dev; override with WithSandboxHost
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecutorOption is a functional option for SandboxExecutor.
type ExecutorOption func(*SandboxExecutor)

// WithHealthPollInterval overrides the health-check poll interval.
// Use in tests to avoid 2-second waits between polls.
func WithHealthPollInterval(d time.Duration) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.healthPollInterval = d
	}
}

// WithSandboxHost sets the hostname the executor uses to build the bot fleet
// target URL (http://<sandboxHost>:<hostPort>).
//
// When the worker runs inside a Docker container, sandbox port bindings are
// published on the HOST's network interface — not on localhost inside the
// worker container. The correct value depends on the runtime environment:
//
//	"host-gateway"   — Docker Desktop (Windows/Mac). Docker resolves this
//	                   special name to the host's internal bridge IP.
//	"172.17.0.1"     — Linux Docker Engine default docker0 bridge IP.
//	"localhost"       — Non-containerised local dev (worker and daemon share
//	                   the same network namespace). This is the default.
//
// Set from config.Config.SandboxHost in cmd/worker/main.go.
func WithSandboxHost(host string) ExecutorOption {
	return func(e *SandboxExecutor) {
		if host != "" {
			e.sandboxHost = host
		}
	}
}

// WithEmitter wires a telemetry.Emitter into the executor so fleet results
// are streamed to Redpanda after each benchmark run.
// Pass telemetry.NoopEmitter{} in tests that don't care about telemetry.
func WithEmitter(e telemetry.Emitter) ExecutorOption {
	return func(ex *SandboxExecutor) {
		ex.emitter = e
	}
}

// WithFleetConfig overrides the bot fleet configuration used for this executor.
// Use in tests or for custom benchmark scenarios. When not set, the executor
// derives defaults from the config package at job execution time.
func WithFleetConfig(cfg botfleet.FleetConfig) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.fleetCfgOverride = &cfg
	}
}

// WithJobCallbacks wires onStart and onFinish callbacks so cmd/worker can
// keep its heartbeater status in sync with the executor's job lifecycle.
//
//   - onStart(submissionID) is called just before Execute begins working.
//   - onFinish() is called when Execute returns (success or failure).
//
// Both callbacks must be goroutine-safe and return quickly.
func WithJobCallbacks(onStart func(string), onFinish func()) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.onStart = onStart
		e.onFinish = onFinish
	}
}

// Execute runs the benchmark lifecycle for j:
//  1. Load submission from store.
//  2. Deploy sandbox container.
//  3. Persist container metadata.
//  4. Wait for container to become healthy.
//  5. Run the distributed bot fleet against the sandbox endpoint.
//  6. Compute correctness score against the golden orderbook.
//  7. Build and return BenchmarkResults.
//  8. Stop container (always, via defer).
func (e *SandboxExecutor) Execute(ctx context.Context, j queue.Job) (*models.BenchmarkResults, error) {
	log := slog.With(
		"executor", "sandbox",
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"language", j.Language,
	)

	// Notify heartbeater: we are now busy with this job.
	if e.onStart != nil {
		e.onStart(j.SubmissionID)
	}
	// Notify heartbeater: we are idle again when Execute returns.
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
	log.Info("executor: deploying sandbox")
	containerID, hostPort, err := e.docker.Deploy(ctx, sub)
	if err != nil {
		return nil, fmt.Errorf("executor: deploy sandbox for %s: %w", j.SubmissionID, err)
	}

	// ── 8. Cleanup — always stop the container when Execute returns ───────────
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
	if err := e.waitHealthy(ctx, containerID, log); err != nil {
		return nil, fmt.Errorf("executor: sandbox not healthy: %w", err)
	}
	log.Info("executor: container is healthy",
		"container_id", containerID[:12],
		"host_port", hostPort,
	)

	// ── 5. Run bot fleet ──────────────────────────────────────────────────────
	// Set StatusBenchmarking so the API surfaces the in-progress state
	// rather than the generic StatusDeploying while the fleet is running.
	sub.Status = models.StatusBenchmarking
	sub.StatusMsg = "bot fleet running"
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to set StatusBenchmarking", "err", updateErr)
	}

	targetURL := fmt.Sprintf("http://%s:%d", e.sandboxHost, hostPort)
	fleetResult, correctnessResult, err := e.runFleet(ctx, j, targetURL, log)
	if err != nil {
		return nil, fmt.Errorf("executor: bot fleet: %w", err)
	}

	// ── 7. Build BenchmarkResults ─────────────────────────────────────────────
	results := buildResults(fleetResult, correctnessResult)
	log.Info("executor: benchmark complete",
		"p50_ms", results.P50LatencyMs,
		"p99_ms", results.P99LatencyMs,
		"max_tps", results.MaxTPS,
		"correctness", results.CorrectnessScore,
		"composite_score", results.CompositeScore,
	)

	return results, nil
}

// runFleet constructs the fleet, runs it, and in parallel runs the golden
// orderbook over the same order sequence to produce a correctness result.
func (e *SandboxExecutor) runFleet(
	ctx context.Context,
	j queue.Job,
	targetURL string,
	log *slog.Logger,
) (*botfleet.FleetResult, *correctness.CorrectnessResult, error) {

	cfg := e.buildFleetConfig(targetURL)

	log.Info("executor: launching bot fleet",
		"target_url", targetURL,
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

	// ── 6. Correctness check ──────────────────────────────────────────────────
	correctnessResult := e.checkCorrectness(fleetResult.Results, log)

	// ── Emit per-order telemetry in batches ───────────────────────────────────
	// We do this after correctness so a slow emitter never delays the score.
	e.emitFleetTelemetry(ctx, j.SubmissionID, fleetResult.Results, log)

	return fleetResult, &correctnessResult, nil
}

// checkCorrectness replays the order sequence through the GoldenOrderbook and
// compares the canonical fills against what the contestant's engine returned.
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
			continue // transport error — exclude from correctness check
		}

		// Translate botfleet types to correctness types (no import cycle).
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
// Design decisions:
//   - Batching avoids per-event lock/network overhead on the emitter.
//   - Errors are logged but NOT propagated — a telemetry failure must never
//     prevent the benchmark result from being written to the store.
//   - Only successful OrderResults (Err == nil) produce events. Transport
//     errors are already reflected in Stats.ErrorOrders; emitting them as
//     KindReject events would confuse latency percentiles with zero values.
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
			continue // skip transport-level errors
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
	flush() // final partial batch

	log.Info("executor: telemetry emission complete",
		"emitted", emitted,
		"batch_errors", errors,
		"total_results", len(results),
	)
}

// buildFleetConfig returns the fleet configuration for this run.
// Uses e.fleetCfgOverride if set (tests / custom scenarios), otherwise defaults.
func (e *SandboxExecutor) buildFleetConfig(targetURL string) botfleet.FleetConfig {
	if e.fleetCfgOverride != nil {
		cfg := *e.fleetCfgOverride
		cfg.TargetURL = targetURL
		return cfg
	}
	return botfleet.DefaultFleetConfig(targetURL)
}

// buildResults converts fleet + correctness data into the models.BenchmarkResults
// that the store and leaderboard consume.
//
// Composite score formula (from TASK.md):
//
//	0.5 * (1 - NormalisedP99) + 0.3 * NormalisedTPS + 0.2 * CorrectnessScore
//
// NormalisedP99: we target a p99 of 1ms (1_000_000 ns). Anything at or below
// scores 1.0; anything above decays linearly toward 0 at 100ms.
// NormalisedTPS: we target a peak TPS of 50_000. Anything at or above scores
// 1.0; lower values scale proportionally.
func buildResults(
	fr *botfleet.FleetResult,
	cr *correctness.CorrectnessResult,
) *models.BenchmarkResults {

	stats := fr.Stats

	const (
		targetP99Ns  = 1_000_000     // 1ms
		worstP99Ns   = 100_000_000   // 100ms — floor for score
		targetMaxTPS = float64(50_000)
	)

	// Normalised P99 (0.0 = worst, 1.0 = best)
	normP99 := 0.0
	if stats.P99Ns <= targetP99Ns {
		normP99 = 1.0
	} else if stats.P99Ns < worstP99Ns {
		normP99 = 1.0 - (stats.P99Ns-targetP99Ns)/(worstP99Ns-targetP99Ns)
	}

	// Normalised TPS (0.0 – 1.0, capped at 1.0)
	normTPS := stats.MaxTPS / targetMaxTPS
	if normTPS > 1.0 {
		normTPS = 1.0
	}

	correctnessScore := 0.0
	var totalOrders, correctFills, incorrectFills int64
	if cr != nil {
		correctnessScore = cr.Score
		totalOrders = stats.TotalOrders
		correctFills = cr.CorrectFills
		incorrectFills = cr.IncorrectFills
	}

	compositeScore := (0.5 * (1.0 - normP99)) + (0.3 * normTPS) + (0.2 * correctnessScore)
	// Invert: lower P99 → higher composite. Correct formula per spec:
	// 0.5*(normP99) + 0.3*(normTPS) + 0.2*(correctness)
	compositeScore = (0.5 * normP99) + (0.3 * normTPS) + (0.2 * correctnessScore)
	// Scale to 0–100 for leaderboard readability.
	compositeScore *= 100.0

	duration := fr.FinishedAt.Sub(fr.StartedAt).String()

	return &models.BenchmarkResults{
		P50LatencyMs:      stats.P50Ms(),
		P90LatencyMs:      stats.P90Ms(),
		P99LatencyMs:      stats.P99Ms(),
		MaxTPS:            stats.MaxTPS,
		SustainedTPS:      stats.SustainedTPS,
		CorrectnessScore:  correctnessScore,
		TotalOrders:       totalOrders,
		CorrectFills:      correctFills,
		IncorrectFills:    incorrectFills,
		CompositeScore:    compositeScore,
		BenchmarkDuration: duration,
		CompletedAt:       fr.FinishedAt,
	}
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
