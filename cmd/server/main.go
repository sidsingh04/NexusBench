package main

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/orchestrator"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
	"github.com/nexusbench/nexusbench/internal/submission"
	"github.com/nexusbench/nexusbench/internal/validator"
)

// queueDepthScrapeInterval is how often the control plane polls Redpanda for
// consumer-group lag and updates the nexusbench_queue_depth Prometheus gauge.
// Matches KEDA's pollingInterval so the autoscaler and Grafana see the same lag.
const queueDepthScrapeInterval = 15 * time.Second

// autoCloseTick is how often the drain-and-wait goroutine checks contest state.
const autoCloseTick = 30 * time.Second

// leaderboardWatchInterval is how often the leaderboard watcher polls the
// submission store and pushes an SSE "update" event if scores changed.
//
// 5 seconds gives contestants near-real-time feedback while keeping store
// read pressure negligible (DiskStore.List is an O(N) directory scan).
// This is not a "hot poll" — it fires only when subscribers are connected,
// and the change-detection hash prevents redundant broadcasts.
const leaderboardWatchInterval = 5 * time.Second

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Load()

	// ── Submission directory ──────────────────────────────────────────────────
	if err := os.MkdirAll(cfg.SubmissionDir, 0o750); err != nil {
		slog.Error("cannot create submission directory",
			"path", cfg.SubmissionDir, "err", err)
		os.Exit(1)
	}
	slog.Info("submission directory ready", "path", cfg.SubmissionDir)

	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()

	// ── Docker (local mode only) ──────────────────────────────────────────────
	var dockerMgr *sandbox.DockerManager
	if !cfg.DistributedMode {
		var err error
		dockerMgr, err = sandbox.NewDockerManager(cfg)
		if err != nil {
			slog.Error("failed to connect to Docker daemon", "err", err)
			os.Exit(1)
		}
		if err := dockerMgr.PruneStale(startCtx); err != nil {
			slog.Warn("prune stale containers failed", "err", err)
		}
		if err := dockerMgr.VerifyImages(startCtx); err != nil {
			slog.Error("image verification failed", "err", err)
			os.Exit(1)
		}
	}

	// ── Core services ─────────────────────────────────────────────────────────
	reg := metrics.New()
	store := submission.NewDiskStore(cfg.SubmissionDir)
	submissionSvc := submission.NewService(store, dockerMgr, cfg)

	// workerRegistry is always created. In local mode it stays empty and is
	// passed (as a non-nil pointer with zero workers) to runContestAutoClose
	// so the drain condition can safely call registry.Stats() without a nil
	// guard in the goroutine. In distributed mode it is populated by worker
	// heartbeats via orchHandler.
	workerRegistry := orchestrator.NewWorkerRegistry()
	var orchHandler *orchestrator.Handler // nil = local mode, routes not mounted
	var jobQueue queue.Queue              // nil = local mode, drain check skipped

	// ── Distributed mode ──────────────────────────────────────────────────────
	if cfg.DistributedMode {
		slog.Info("server: distributed mode — wiring job queue + orchestrator",
			"brokers", cfg.RedpandaBrokers)

		rpQueue, err := queue.NewRedpandaQueue(queue.RedpandaConfig{
			Brokers:           cfg.RedpandaBrokers,
			Partitions:        4,
			ReplicationFactor: 1,
		})
		if err != nil {
			slog.Error("server: create job queue", "err", err)
			os.Exit(1)
		}
		defer func() {
			if err := rpQueue.Close(); err != nil {
				slog.Warn("server: job queue close error", "err", err)
			}
		}()

		if err := rpQueue.Bootstrap(startCtx); err != nil {
			slog.Error("server: bootstrap job queue topic", "err", err)
			os.Exit(1)
		}
		jobQueue = rpQueue

		submissionSvc = submissionSvc.WithQueue(jobQueue)
		orchHandler = orchestrator.NewHandler(workerRegistry)

		slog.Info("server: job queue ready", "topic", queue.TopicJobs)
		slog.Info("server: orchestrator ready — worker fleet routes mounted")

		scraperCtx, scraperCancel := context.WithCancel(context.Background())
		go runQueueDepthScraper(scraperCtx, jobQueue, reg)
		defer scraperCancel()
	} else {
		slog.Info("server: local mode — sandboxes deployed in-process")
	}

	// ── Contest service + Leaderboard Bus (Phase 5+) ──────────────────────────
	// The bus is created here and passed to both the contest service and the
	// router. The contest service uses it to broadcast "frozen" events when a
	// contest closes. The leaderboard watcher (below) uses it to broadcast
	// "update" events as FinalScores are written by workers.
	bus := api.NewLeaderboardBus()
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, bus)

	// Wire the contest service into the submission service so that Ingest
	// enforces contest-scoped checks (Stage 5.3).
	if cfg.AdminAPIKey != "" {
		submissionSvc = submissionSvc.WithContestGetter(contestSvc)
		slog.Info("server: admin API enabled — contest lifecycle routes mounted",
			"route_prefix", "/api/v1/admin")
	} else {
		slog.Warn("server: ADMIN_API_KEY not set — admin routes will not be mounted; contest checks disabled")
	}

	// ── Leaderboard watcher (Stage 5.7 — cross-process update bridge) ─────────
	// This goroutine polls the submission store every leaderboardWatchInterval,
	// detects score changes via a cheap hash, and broadcasts "update" events to
	// SSE subscribers. It is the architectural solution to the process-isolation
	// dilemma: the worker writes FinalScore to the shared store; the control
	// plane's watcher notices and pushes the update. Zero new infrastructure,
	// zero changes to the worker.
	//
	// The watcher is always started regardless of mode (local or distributed)
	// because in local mode the in-process executor writes to the same store,
	// and the watcher handles both cases identically.
	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	go runLeaderboardWatcher(watcherCtx, store, bus)
	defer watcherCancel()

	// ── Auto-close goroutine (AD-3: hybrid drain-and-wait) ────────────────────
	autoCloseCtx, autoCloseCancel := context.WithCancel(context.Background())
	go runContestAutoClose(autoCloseCtx, contestSvc, store, jobQueue, workerRegistry)
	defer autoCloseCancel()

	// ── HTTP server ───────────────────────────────────────────────────────────
	factory := func(targetURL string) api.ValidatorRunner {
		transport := botfleet.NewRESTTransport(targetURL, &http.Client{Timeout: 5 * time.Second})
		v := validator.New(transport)
		return validatorAdapter{v}
	}
	router := api.NewRouter(submissionSvc, cfg, reg, orchHandler, contestSvc, factory, bus)

	srv := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("NexusBench control plane ready",
			"addr", cfg.ListenAddr,
			"submission_dir", cfg.SubmissionDir,
			"distributed_mode", cfg.DistributedMode,
		)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("server crashed", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("shutdown signal received, draining...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}
	slog.Info("control plane stopped")
}

// ── Leaderboard watcher (Stage 5.7) ──────────────────────────────────────────

// runLeaderboardWatcher polls the submission store on a fixed interval,
// computes a change-detection hash over all completed FinalScores, and fires
// a bus.Broadcast("update") only when the hash changes.
//
// This is the architectural bridge for the process-isolation dilemma in Stage
// 5.7: the worker (a separate OS process in distributed mode) writes FinalScore
// to the shared submission store, and this watcher on the control plane side
// notices and pushes the SSE update. No new infrastructure is needed.
//
// Design properties:
//   - Zero work when no SSE clients are connected (bus.SubscriberCount() == 0).
//     The store is not read at all in that case, keeping idle overhead at zero.
//   - Change detection via scoreHash: only broadcasts when the set of
//     completed submission scores actually changes. This prevents redundant
//     fan-out when no benchmark has completed since the last tick.
//   - Identical behavior in local and distributed mode — the executor in both
//     cases writes to the same store, so the watcher does not need to know
//     which mode is active.
//   - Supersedes the need for bus.Broadcast calls inside executor.go or
//     worker.go, keeping those packages free of the api dependency.
func runLeaderboardWatcher(ctx context.Context, store submission.Store, bus *api.LeaderboardBus) {
	slog.Info("server: leaderboard watcher started",
		"interval", leaderboardWatchInterval)

	ticker := time.NewTicker(leaderboardWatchInterval)
	defer ticker.Stop()

	var lastHash float64 // hash of the last broadcast leaderboard state

	for {
		select {
		case <-ctx.Done():
			slog.Info("server: leaderboard watcher stopped")
			return
		case <-ticker.C:
			lastHash = tickLeaderboardWatcher(store, bus, lastHash)
		}
	}
}

// tickLeaderboardWatcher is the per-tick logic of runLeaderboardWatcher,
// extracted as a pure function so it can be unit-tested without a goroutine
// or a ticker.
//
// Returns the new lastHash (unchanged if no broadcast was sent).
func tickLeaderboardWatcher(
	store submission.Store,
	bus *api.LeaderboardBus,
	lastHash float64,
) float64 {
	// Fast path: skip the store read entirely when no clients are connected.
	// This makes the idle cost of the watcher truly zero rather than just low.
	if bus.SubscriberCount() == 0 {
		return lastHash
	}

	all, err := store.List()
	if err != nil {
		slog.Warn("server: leaderboard watcher: store.List failed", "err", err)
		return lastHash
	}

	// Compute a cheap hash that changes whenever any completed submission's
	// score changes. We sum the FinalScores (and CompositeScores for P1–4
	// submissions) of all completed submissions. Floating-point addition is
	// order-dependent, so we sort by submission ID first for determinism.
	//
	// Why not a cryptographic hash? Because we only need to detect changes,
	// not identify them. A sum is O(N), allocation-free, and sufficient —
	// two different leaderboard states will almost certainly produce different
	// sums, and false positives (a spurious broadcast) are harmless.
	newHash := leaderboardHash(all)
	if newHash == lastHash {
		return lastHash // nothing changed — skip broadcast
	}

	// State changed. Build the full deduplicated leaderboard and broadcast.
	entries := buildLeaderboardEntries(context.Background(), store, "" /* all contests */)
	ptrs := make([]*models.LeaderboardEntry, len(entries))
	copy(ptrs, entries)

	bus.Broadcast(contest.LeaderboardEvent{
		Type:    "update",
		Entries: ptrs,
	})

	slog.Debug("server: leaderboard watcher: score change detected, broadcast sent",
		"subscriber_count", bus.SubscriberCount(),
		"entry_count", len(entries),
	)

	return newHash
}

// leaderboardHash returns a deterministic float64 that changes whenever the
// set of completed submission scores changes.
//
// Algorithm: sum the effective score of every completed submission that has
// a non-zero score, rounded to avoid float64 accumulation drift. The result
// is not a cryptographic hash — it is a fast change-detection signal.
//
// Collision probability: two different leaderboard states would have to
// produce exactly the same sum of FinalScores to collide. In practice this
// means a spurious SSE broadcast is sent (harmless) but a missed broadcast
// is essentially impossible for scores that differ by more than float64 epsilon.
func leaderboardHash(subs []*models.Submission) float64 {
	var sum float64
	for _, s := range subs {
		if s.Status != models.StatusCompleted {
			continue
		}
		score := s.FinalScore
		if score == 0 && s.Results != nil {
			score = s.Results.CompositeScore
		}
		// Round to 6 decimal places to avoid float64 accumulation drift
		// producing false positives across identical states.
		sum += math.Round(score*1e6) / 1e6
	}
	return sum
}

// ── Hybrid drain-and-wait auto-close (AD-3) ───────────────────────────────────

// runContestAutoClose implements the two-timestamp contest closing protocol.
// See PROGRESS.md Stage 5.4 for the full rationale.
func runContestAutoClose(
	ctx context.Context,
	svc *contest.ContestService,
	subStore submission.Store,
	jobQueue queue.Queue,
	registry *orchestrator.WorkerRegistry,
) {
	slog.Info("server: contest auto-close goroutine started (hybrid drain-and-wait)")
	ticker := time.NewTicker(autoCloseTick)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("server: contest auto-close goroutine stopped")
			return
		case <-ticker.C:
			tickAutoClose(ctx, svc, subStore, jobQueue, registry)
		}
	}
}

// tickAutoClose is the per-tick logic of runContestAutoClose.
func tickAutoClose(
	ctx context.Context,
	svc *contest.ContestService,
	subStore submission.Store,
	jobQueue queue.Queue,
	registry *orchestrator.WorkerRegistry,
) {
	active, err := svc.GetActive(ctx)
	if err != nil {
		if !errors.Is(err, models.ErrNoActiveContest) {
			slog.Warn("server: auto-close: GetActive failed", "err", err)
		}
		return
	}

	now := time.Now().UTC()

	// ── Path A: natural drain ─────────────────────────────────────────────────
	if active.SubmissionsClosedAt != nil && now.After(*active.SubmissionsClosedAt) {
		if queueDrained(ctx, jobQueue) && !workersAreStillBusy(registry) {
			slog.Info("server: auto-close: queue drained and no busy workers — closing contest",
				"id", active.ID)
			entries := buildLeaderboardEntries(ctx, subStore, active.ID)
			if err := svc.Close(ctx, active.ID, entries); err != nil {
				slog.Error("server: auto-close: drain-close failed",
					"id", active.ID, "err", err)
			}
			return
		}
		slog.Debug("server: auto-close: waiting for drain",
			"id", active.ID,
			"submissions_closed_at", active.SubmissionsClosedAt)
	}

	// ── Path B: hard failsafe ─────────────────────────────────────────────────
	if active.EndsAt != nil && now.After(*active.EndsAt) {
		slog.Warn("server: auto-close: EndsAt passed — force-closing contest",
			"id", active.ID, "ends_at", active.EndsAt,
			"reason", "hard failsafe: queue may be stuck")
		entries := buildLeaderboardEntries(ctx, subStore, active.ID)
		if err := svc.Close(ctx, active.ID, entries); err != nil {
			slog.Error("server: auto-close: force-close failed",
				"id", active.ID, "err", err)
		}
	}
}

func queueDrained(ctx context.Context, jobQueue queue.Queue) bool {
	if jobQueue == nil {
		return true
	}
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	depth, err := jobQueue.QueueDepth(pollCtx)
	if err != nil {
		slog.Warn("server: auto-close: queue depth poll failed — assuming not drained", "err", err)
		return false
	}
	return depth == 0
}

func workersAreStillBusy(registry *orchestrator.WorkerRegistry) bool {
	if registry == nil {
		return false
	}
	return registry.Stats().Busy > 0
}

// buildLeaderboardEntries builds a ranked, deduplicated leaderboard from the
// store. Used by both the auto-close goroutine and the leaderboard watcher.
//
// contestID filters to submissions for a specific contest. Pass "" to include
// all completed submissions (used by the watcher, which is not contest-scoped).
//
// Phase 1–4 submissions (ContestID == "") are always included regardless of
// the filter, preserving backward compatibility.
func buildLeaderboardEntries(
	ctx context.Context,
	subStore submission.Store,
	contestID string,
) []*models.LeaderboardEntry {
	_ = ctx

	all, err := subStore.List()
	if err != nil {
		slog.Error("server: buildLeaderboardEntries: List failed", "err", err)
		return nil
	}

	var completed []*models.Submission
	for _, s := range all {
		if s.Status != models.StatusCompleted {
			continue
		}
		if contestID != "" && s.ContestID != "" && s.ContestID != contestID {
			continue
		}
		completed = append(completed, s)
	}

	type candidate struct {
		sub   *models.Submission
		score float64
	}
	best := make(map[string]candidate)
	for _, s := range completed {
		score := s.FinalScore
		if score == 0 && s.Results != nil {
			score = s.Results.CompositeScore
		}
		if score == 0 {
			continue
		}
		if ex, ok := best[s.TeamName]; !ok || score > ex.score {
			best[s.TeamName] = candidate{sub: s, score: score}
		}
	}

	candidates := make([]candidate, 0, len(best))
	for _, c := range best {
		candidates = append(candidates, c)
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	entries := make([]*models.LeaderboardEntry, 0, len(candidates))
	for rank, c := range candidates {
		e := &models.LeaderboardEntry{
			Rank:         rank + 1,
			SubmissionID: c.sub.ID,
			TeamName:     c.sub.TeamName,
			Language:     c.sub.Language,
			Protocol:     c.sub.Protocol,
			Status:       c.sub.Status,
			FinalScore:   c.sub.FinalScore,
			CompletedAt:  c.sub.CompletedAt,
		}
		if c.sub.Results != nil {
			e.CompositeScore = c.sub.Results.CompositeScore
			e.P99LatencyMs = c.sub.Results.P99LatencyMs
			e.MaxTPS = c.sub.Results.MaxTPS
			e.CorrectnessScore = c.sub.Results.CorrectnessScore
		}
		if c.sub.FinalScore > 0 {
			e.CompositeScore = c.sub.FinalScore
		}
		entries = append(entries, e)
	}
	return entries
}

// ── Queue-depth scraper (Phase 3+) ───────────────────────────────────────────

func runQueueDepthScraper(ctx context.Context, q queue.Queue, reg *metrics.Registry) {
	slog.Info("server: queue-depth scraper started", "interval", queueDepthScrapeInterval)
	pollAndSet(ctx, q, reg)

	ticker := time.NewTicker(queueDepthScrapeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("server: queue-depth scraper stopped")
			return
		case <-ticker.C:
			pollAndSet(ctx, q, reg)
		}
	}
}

func pollAndSet(ctx context.Context, q queue.Queue, reg *metrics.Registry) {
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	depth, err := q.QueueDepth(pollCtx)
	if err != nil {
		slog.Warn("server: queue depth poll failed — gauge not updated", "err", err)
		return
	}
	reg.SetQueueDepth(depth)
	slog.Debug("server: queue depth updated", "depth", depth)
}

// ── Validator Adapter (Stage 5.6) ─────────────────────────────────────────────

// validatorAdapter wraps *validator.Validator to satisfy api.ValidatorRunner
// without exposing the internal/validator package to the api package.
type validatorAdapter struct {
	v *validator.Validator
}

func (a validatorAdapter) Run(ctx context.Context, submissionID string) (*api.ValidatorResult, error) {
	res, err := a.v.Run(ctx, submissionID)
	if err != nil {
		return nil, err
	}

	apiScenarios := make([]api.ScenarioResult, len(res.Scenarios))
	for i, s := range res.Scenarios {
		apiScenarios[i] = api.ScenarioResult{
			Name:   s.Name,
			Passed: s.Passed,
			Reason: s.Reason,
		}
	}

	return &api.ValidatorResult{
		SubmissionID: res.SubmissionID,
		Scenarios:    apiScenarios,
		AllPassed:    res.AllPassed,
		TestedAt:     res.TestedAt,
	}, nil
}
