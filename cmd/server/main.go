package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/orchestrator"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// queueDepthScrapeInterval is how often the control plane polls Redpanda for
// consumer-group lag and updates the nexusbench_queue_depth Prometheus gauge.
// Matches KEDA's pollingInterval so the autoscaler and Grafana see the same lag.
const queueDepthScrapeInterval = 15 * time.Second

// autoCloseTick is how often the drain-and-wait goroutine checks contest state.
const autoCloseTick = 30 * time.Second

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

	// ── Contest service (Phase 5+) ────────────────────────────────────────────
	// MemoryContestStore for all modes through Stage 5.8.
	// Stage 5.9 switches to PostgresContestStore when DistributedMode=true.
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil) // nil bus → Stage 5.7

	// Wire the contest service into the submission service so that Ingest
	// enforces contest-scoped checks (Stage 5.3):
	//  - requires an active contest
	//  - rejects uploads after SubmissionsClosedAt
	//  - blocks duplicate in-flight submissions per team
	if cfg.AdminAPIKey != "" {
		submissionSvc = submissionSvc.WithContestGetter(contestSvc)
		slog.Info("server: admin API enabled — contest lifecycle routes mounted",
			"route_prefix", "/api/v1/admin")
	} else {
		slog.Warn("server: ADMIN_API_KEY not set — admin routes will not be mounted; contest checks disabled")
	}

	// ── Auto-close goroutine (AD-3: hybrid drain-and-wait) ────────────────────
	// Receives the submission store and worker registry so it can:
	//  a) build real leaderboard entries from completed submissions on close, and
	//  b) check queue depth + busy-worker count for the drain condition.
	//
	// jobQueue is nil in local mode → drain check is skipped (no queue to drain).
	// workerRegistry is always non-nil → Stats() is always safe to call.
	autoCloseCtx, autoCloseCancel := context.WithCancel(context.Background())
	go runContestAutoClose(autoCloseCtx, contestSvc, store, jobQueue, workerRegistry)
	defer autoCloseCancel()

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(submissionSvc, cfg, reg, orchHandler, contestSvc)

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

// ── Hybrid drain-and-wait auto-close (AD-3) ───────────────────────────────────

// runContestAutoClose implements the two-timestamp contest closing protocol:
//
//  1. Intake gate  — SubmissionsClosedAt (checked in Ingest, Stage 5.3):
//     New uploads are rejected after this timestamp. Set by the admin 5–60
//     minutes before EndsAt to give in-flight submissions time to complete.
//
//  2. Drain close — once SubmissionsClosedAt has passed AND the queue is
//     empty AND no workers are busy, the contest closes naturally. This is the
//     fair path: all on-time submissions have finished.
//
//  3. Hard failsafe — if EndsAt has passed and the queue has NOT drained
//     (stuck due to infrastructure failure), the contest is force-closed.
//     Teams whose jobs were not completed are not scored for those runs.
//
// jobQueue may be nil in local (Phase 1–4) mode — the drain check is skipped
// and the goroutine only uses the EndsAt failsafe.
//
// workerRegistry is always non-nil — Stats() is always safe to call.
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

// tickAutoClose is the per-tick logic of runContestAutoClose, extracted so it
// can be unit-tested independently of the goroutine and ticker machinery.
func tickAutoClose(
	ctx context.Context,
	svc *contest.ContestService,
	subStore submission.Store,
	jobQueue queue.Queue,
	registry *orchestrator.WorkerRegistry,
) {
	active, err := svc.GetActive(ctx)
	if err != nil {
		// ErrNoActiveContest is the normal state between contests.
		if !errors.Is(err, models.ErrNoActiveContest) {
			slog.Warn("server: auto-close: GetActive failed", "err", err)
		}
		return
	}

	now := time.Now().UTC()

	// ── Path A: natural drain ─────────────────────────────────────────────────
	// Trigger once submissions are closed (SubmissionsClosedAt is set and past).
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
	// Force-close if EndsAt has passed, regardless of drain state.
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

// queueDrained returns true when there are no unconsumed jobs in the queue.
// Returns true when jobQueue is nil (local mode — no queue to drain).
func queueDrained(ctx context.Context, jobQueue queue.Queue) bool {
	if jobQueue == nil {
		return true // local mode: no queue → always drained
	}
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	depth, err := jobQueue.QueueDepth(pollCtx)
	if err != nil {
		slog.Warn("server: auto-close: queue depth poll failed — assuming not drained",
			"err", err)
		return false // fail-safe: assume not drained on error
	}
	return depth == 0
}

// workersAreStillBusy returns true if any registered worker reports busy status.
// A nil registry is treated as "no workers registered" → not busy.
func workersAreStillBusy(registry *orchestrator.WorkerRegistry) bool {
	if registry == nil {
		return false
	}
	return registry.Stats().Busy > 0
}

// buildLeaderboardEntries reads all completed submissions for the given
// contestID from the store and converts them to ranked LeaderboardEntry values.
//
// This is called by the auto-close goroutine immediately before closing so the
// snapshot reflects the final state. It uses the same deduplication and
// ranking logic as the public leaderboard endpoint (best-score-per-team).
//
// For Phase 1–4 submissions (ContestID is empty), all completed submissions
// are included regardless of contestID to keep the snapshot non-empty.
func buildLeaderboardEntries(
	ctx context.Context,
	subStore submission.Store,
	contestID string,
) []*models.LeaderboardEntry {
	_ = ctx // reserved for future store implementations that accept a context

	all, err := subStore.List()
	if err != nil {
		slog.Error("server: buildLeaderboardEntries: List failed", "err", err)
		return nil
	}

	// Filter to this contest's completed submissions.
	// Phase 5: match ContestID. Phase 1–4: include all completed (ContestID=="").
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

	// Deduplicate: best score per team (mirrors leaderboard handler logic).
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

	// Sort and rank.
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
		// Phase 1–4 compat: populate legacy columns.
		if c.sub.Results != nil {
			e.CompositeScore = c.sub.Results.CompositeScore
			e.P99LatencyMs = c.sub.Results.P99LatencyMs
			e.MaxTPS = c.sub.Results.MaxTPS
			e.CorrectnessScore = c.sub.Results.CorrectnessScore
		}
		// Phase 5: use FinalScore for CompositeScore too (backward compat field).
		if c.sub.FinalScore > 0 {
			e.CompositeScore = c.sub.FinalScore
		}
		entries = append(entries, e)
	}
	return entries
}

// ── Queue-depth scraper (Phase 3+) ───────────────────────────────────────────

// runQueueDepthScraper polls the queue's consumer-group lag on a fixed
// interval and updates the nexusbench_queue_depth Prometheus gauge.
func runQueueDepthScraper(ctx context.Context, q queue.Queue, reg *metrics.Registry) {
	slog.Info("server: queue-depth scraper started", "interval", queueDepthScrapeInterval)
	pollAndSet(ctx, q, reg) // fire immediately on startup

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
