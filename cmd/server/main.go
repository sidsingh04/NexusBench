package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/orchestrator"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// queueDepthScrapeInterval is how often the control plane polls Redpanda for
// consumer-group lag and updates the nexusbench_queue_depth Prometheus gauge.
//
// 15 seconds matches KEDA's pollingInterval in k8s/worker/scaledobject.yaml,
// so the Grafana dashboard and the autoscaler see roughly the same lag value.
// Polling more frequently adds Redpanda admin RPC load for little benefit;
// polling less frequently makes the Grafana graph look choppy.
const queueDepthScrapeInterval = 15 * time.Second

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

	// ── Orchestrator (always created; only wired into the router when needed) ─
	// The WorkerRegistry is cheap — creating it in local mode costs nothing.
	// The HTTP routes are only mounted when orchHandler is passed to NewRouter.
	workerRegistry := orchestrator.NewWorkerRegistry()
	var orchHandler *orchestrator.Handler // nil = local mode, routes not mounted

	// ── Distributed mode ──────────────────────────────────────────────────────
	if cfg.DistributedMode {
		slog.Info("server: distributed mode — wiring job queue + orchestrator",
			"brokers", cfg.RedpandaBrokers,
		)

		jobQueue, err := queue.NewRedpandaQueue(queue.RedpandaConfig{
			Brokers:           cfg.RedpandaBrokers,
			Partitions:        4,
			ReplicationFactor: 1,
		})
		if err != nil {
			slog.Error("server: create job queue", "err", err)
			os.Exit(1)
		}
		defer func() {
			if err := jobQueue.Close(); err != nil {
				slog.Warn("server: job queue close error", "err", err)
			}
		}()

		if err := jobQueue.Bootstrap(startCtx); err != nil {
			slog.Error("server: bootstrap job queue topic", "err", err)
			os.Exit(1)
		}

		submissionSvc = submissionSvc.WithQueue(jobQueue)
		orchHandler = orchestrator.NewHandler(workerRegistry)

		slog.Info("server: job queue ready", "topic", queue.TopicJobs)
		slog.Info("server: orchestrator ready — worker fleet routes mounted",
			"register_path", "/internal/workers/register",
			"heartbeat_path", "/internal/workers/{id}/heartbeat",
			"list_path", "/internal/workers",
		)

		// ── Queue-depth scraper (Stage 4.3) ───────────────────────────────────
		// Background goroutine that polls Redpanda consumer-group lag every 15s
		// and writes the result to the nexusbench_queue_depth Prometheus gauge.
		//
		// Design notes:
		//   - The scraper runs only in distributed mode because MemoryQueue
		//     (local mode) doesn't have a Redpanda broker to query.
		//   - It uses a separate context derived from a channel-based shutdown
		//     signal so it stops cleanly on SIGTERM before the HTTP server drains.
		//   - A single failed poll logs a warning and retries on the next tick;
		//     transient broker blips do not crash the control plane.
		//   - The goroutine is intentionally started after Bootstrap() succeeds,
		//     guaranteeing the topic exists before the first lag query.
		scraperCtx, scraperCancel := context.WithCancel(context.Background())
		go runQueueDepthScraper(scraperCtx, jobQueue, reg)

		// scraperCancel is called during graceful shutdown (see below).
		defer scraperCancel()
	} else {
		slog.Info("server: local mode (Phase 1/2) — sandboxes deployed in-process")
	}

	// ── HTTP server ───────────────────────────────────────────────────────────
	// orchHandler is nil in local mode → orchestrator routes not mounted.
	router := api.NewRouter(submissionSvc, cfg, reg, orchHandler)

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
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server crashed", "err", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("shutdown signal received, draining...")

	// Give the HTTP server 30s to drain in-flight requests.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}
	slog.Info("control plane stopped")
}

// runQueueDepthScraper polls the queue's consumer-group lag on a fixed
// interval and updates the nexusbench_queue_depth Prometheus gauge.
//
// It runs as a long-lived background goroutine and exits when ctx is canceled.
// Errors are logged as warnings; the goroutine never crashes on a single failure.
//
// The scraper fires immediately on startup (so the gauge is non-zero on the
// first /metrics scrape) and then on every tick of queueDepthScrapeInterval.
func runQueueDepthScraper(ctx context.Context, q queue.Queue, reg *metrics.Registry) {
	slog.Info("server: queue-depth scraper started",
		"interval", queueDepthScrapeInterval,
	)

	// Fire immediately so the gauge is populated before the first Prometheus
	// scrape (which typically happens within 15s of startup).
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

// pollAndSet performs a single QueueDepth RPC and updates the gauge.
// Exported for readability of runQueueDepthScraper; not part of any interface.
func pollAndSet(ctx context.Context, q queue.Queue, reg *metrics.Registry) {
	// Use a short-lived context for each poll so a slow broker doesn't hold
	// the scraper goroutine hostage across multiple ticks.
	pollCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	depth, err := q.QueueDepth(pollCtx)
	if err != nil {
		slog.Warn("server: queue depth poll failed — gauge not updated",
			"err", err,
		)
		return
	}

	reg.SetQueueDepth(depth)
	slog.Debug("server: queue depth updated", "depth", depth)
}
