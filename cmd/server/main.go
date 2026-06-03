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
const queueDepthScrapeInterval = 15 * time.Second

// autoCloseTick is how often the drain-and-wait goroutine checks contest state.
const autoCloseTick = 30 * time.Second

// leaderboardWatchInterval is how often the leaderboard watcher polls the
// submission store and pushes an SSE "update" event if scores changed.
//
// 5 seconds gives contestants near-real-time feedback while keeping store
// read pressure negligible (DiskStore.List is an O(N) directory scan).
// Zero work when no SSE clients are connected.
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

	// ── Contest store (Stage 5.9) ─────────────────────────────────────────────
	// In distributed mode with a PostgresDSN configured, use the durable
	// PostgresContestStore. Otherwise fall back to MemoryContestStore.
	//
	// MemoryContestStore remains the correct choice for:
	//   - Local development (DISTRIBUTED_MODE=false)
	//   - CI / unit test runs (no real database)
	//   - Distributed mode without a Postgres DSN (operator misconfiguration
	//     is logged as a warning rather than a hard crash so the server still
	//     starts and serves Phase 1–4 traffic)
	//
	// Backward compatibility: all Phase 1–4 paths are unaffected — they never
	// call any ContestStore method.
	contestStore := buildContestStore(startCtx, cfg)

	// ── Contest service + Leaderboard Bus (Phase 5+) ──────────────────────────
	// The bus is created here and passed to both the contest service and the
	// router. The contest service uses it to broadcast "frozen" events when a
	// contest closes. The leaderboard watcher (below) uses it to broadcast
	// "update" events as FinalScores are written by workers.
	bus := api.NewLeaderboardBus()
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
	watcherCtx, watcherCancel := context.WithCancel(context.Background())
	go runLeaderboardWatcher(watcherCtx, store, bus, contestSvc)
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
		WriteTimeout: 10 * time.Minute,
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

	// Close the Postgres pool cleanly after the HTTP server has stopped
	// accepting new requests. All in-flight contest operations will have
	// returned by the time Shutdown() returns.
	if pgStore, ok := contestStore.(*contest.PostgresContestStore); ok {
		pgStore.Close()
		slog.Info("server: postgres contest store closed")
	}

	slog.Info("control plane stopped")
}

// ── Contest store factory (Stage 5.9) ─────────────────────────────────────────

// buildContestStore returns the correct ContestStore implementation based on
// the runtime configuration:
//
//   - Distributed mode + PostgresDSN set → PostgresContestStore
//     (durable, survives server restarts, correct choice for production)
//   - All other cases → MemoryContestStore
//     (in-process, zero infrastructure, correct for local dev and CI)
//
// Failure policy: if PostgresContestStore cannot be created (DSN wrong,
// database unreachable), the error is logged and the server falls back to
// MemoryContestStore rather than calling os.Exit. This matches the behavior
// of Phase 1–4 — the server is still useful for benchmarking even if the
// contest store is in-memory.
//
// Operators who require durable contest state must ensure the database is
// reachable before starting the control plane. The CI gate (Stage 5.9)
// verifies this.
func buildContestStore(ctx context.Context, cfg *config.Config) contest.ContestStore {
	if cfg.DistributedMode && cfg.PostgresDSN != "" {
		slog.Info("server: connecting to PostgreSQL contest store",
			"dsn_prefix", safeDSNPrefix(cfg.PostgresDSN))

		pgStore, err := contest.NewPostgresContestStore(ctx, cfg.PostgresDSN)
		if err != nil {
			// Non-fatal: log and fall back. The operator will see repeated
			// log lines and can restart with a corrected DSN.
			slog.Error("server: failed to connect to PostgreSQL — falling back to in-memory contest store",
				"err", err)
			return contest.NewMemoryContestStore()
		}

		slog.Info("server: PostgreSQL contest store ready (schema migrated)")
		return pgStore
	}

	if cfg.DistributedMode && cfg.PostgresDSN == "" {
		slog.Warn("server: DISTRIBUTED_MODE=true but POSTGRES_DSN is empty — contest store will NOT survive restarts; set POSTGRES_DSN for production")
	}

	return contest.NewMemoryContestStore()
}

// safeDSNPrefix returns the scheme+host portion of a DSN for logging without
// exposing the password.
//
// "postgres://user:secret@host:5432/db" → "postgres://***@host:5432/db"
func safeDSNPrefix(dsn string) string {
	// Find "://" and then the "@" separator.
	schemeEnd := -1
	for i := 0; i+3 <= len(dsn); i++ {
		if dsn[i:i+3] == "://" {
			schemeEnd = i + 3
			break
		}
	}
	if schemeEnd < 0 {
		return "<unparseable>"
	}
	atIdx := -1
	for i := schemeEnd; i < len(dsn); i++ {
		if dsn[i] == '@' {
			atIdx = i
			break
		}
	}
	if atIdx < 0 {
		// No credentials in DSN — safe to log as-is.
		return dsn
	}
	return dsn[:schemeEnd] + "***" + dsn[atIdx:]
}

// ── Leaderboard watcher (Stage 5.7) ──────────────────────────────────────────

// runLeaderboardWatcher polls the submission store on a fixed interval,
// computes a change-detection hash over all completed FinalScores, and fires
// a bus.Broadcast("update") only when the hash changes.
//
// See Stage 5.7 in PROGRESS.md for the full architectural rationale.
func runLeaderboardWatcher(ctx context.Context, store submission.Store, bus *api.LeaderboardBus, contestSvc *contest.ContestService) {
	slog.Info("server: leaderboard watcher started", "interval", leaderboardWatchInterval)
	ticker := time.NewTicker(leaderboardWatchInterval)
	defer ticker.Stop()

	var lastHash float64

	for {
		select {
		case <-ctx.Done():
			slog.Info("server: leaderboard watcher stopped")
			return
		case <-ticker.C:
			lastHash = tickLeaderboardWatcher(ctx, store, bus, contestSvc, lastHash)
		}
	}
}

func tickLeaderboardWatcher(
	ctx context.Context,
	store submission.Store,
	bus *api.LeaderboardBus,
	contestSvc *contest.ContestService,
	lastHash float64,
) float64 {
	if bus.SubscriberCount() == 0 {
		return lastHash
	}
	all, err := store.List()
	if err != nil {
		slog.Warn("server: leaderboard watcher: store.List failed", "err", err)
		return lastHash
	}
	newHash := leaderboardHash(all)
	if newHash == lastHash {
		return lastHash
	}

	var activeID string
	if contestSvc != nil {
		if active, err := contestSvc.GetActive(ctx); err == nil {
			activeID = active.ID
		}
	}

	entries := buildLeaderboardEntries(context.Background(), store, activeID)
	ptrs := make([]*models.LeaderboardEntry, len(entries))
	copy(ptrs, entries)

	bus.Broadcast(contest.LeaderboardEvent{Type: "update", ContestID: activeID, Entries: ptrs})

	slog.Debug("server: leaderboard watcher: broadcast sent",
		"subscriber_count", bus.SubscriberCount(),
		"entry_count", len(entries),
	)
	return newHash
}

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
		sum += 1.0 + math.Round(score*1e6)/1e6
	}
	return sum
}

// ── Hybrid drain-and-wait auto-close (AD-3) ───────────────────────────────────

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

	if active.SubmissionsClosedAt != nil && now.After(*active.SubmissionsClosedAt) {
		if queueDrained(ctx, jobQueue) && !workersAreStillBusy(registry) {
			slog.Info("server: auto-close: queue drained — closing contest", "id", active.ID)
			entries := buildLeaderboardEntries(ctx, subStore, active.ID)
			if err := svc.Close(ctx, active.ID, entries); err != nil {
				slog.Error("server: auto-close: drain-close failed", "id", active.ID, "err", err)
			}
			return
		}
		slog.Debug("server: auto-close: waiting for drain",
			"id", active.ID, "submissions_closed_at", active.SubmissionsClosedAt)
	}

	if active.EndsAt != nil && now.After(*active.EndsAt) {
		slog.Warn("server: auto-close: EndsAt passed — force-closing", "id", active.ID)
		entries := buildLeaderboardEntries(ctx, subStore, active.ID)
		if err := svc.Close(ctx, active.ID, entries); err != nil {
			slog.Error("server: auto-close: force-close failed", "id", active.ID, "err", err)
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

// buildLeaderboardEntries builds a ranked, deduplicated leaderboard.
// contestID="" includes all completed submissions (cross-contest).
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
// without the api package importing internal/validator.
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
