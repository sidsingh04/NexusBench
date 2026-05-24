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
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
	"github.com/nexusbench/nexusbench/internal/submission"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	cfg := config.Load()

	// ── Submission directory ─────────────────────────────────────────────────
	if err := os.MkdirAll(cfg.SubmissionDir, 0o755); err != nil {
		slog.Error("cannot create submission directory",
			"path", cfg.SubmissionDir,
			"err", err,
			"hint", "On Windows, ensure SUBMISSION_DIR is under C:\\ or another Docker-shared drive",
		)
		os.Exit(1)
	}
	slog.Info("submission directory ready", "path", cfg.SubmissionDir)

	// ── Docker manager ────────────────────────────────────────────────────────
	dockerMgr, err := sandbox.NewDockerManager(cfg)
	if err != nil {
		slog.Error("failed to connect to Docker daemon", "err", err)
		os.Exit(1)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer startCancel()

	if err := dockerMgr.PruneStale(startCtx); err != nil {
		slog.Warn("prune stale containers failed", "err", err)
	}

	if err := dockerMgr.VerifyImages(startCtx); err != nil {
		if cfg.DistributedMode {
			// In distributed mode the control plane never runs sandboxes itself —
			// workers do. Missing images are a warning, not a fatal error here.
			slog.Warn("image verification failed (non-fatal in distributed mode)", "err", err)
		} else {
			slog.Error("image verification failed", "err", err)
			os.Exit(1)
		}
	}

	// ── Services ──────────────────────────────────────────────────────────────
	reg := metrics.New()
	store := submission.NewDiskStore(cfg.SubmissionDir)
	submissionSvc := submission.NewService(store, dockerMgr, cfg)

	// ── Job queue (Phase 3 distributed mode) ──────────────────────────────────
	// When DISTRIBUTED_MODE=true the control plane enqueues incoming submissions
	// to jobs.benchmark instead of deploying sandboxes inline. A separate worker
	// process (cmd/worker) consumes the queue and runs the full benchmark.
	//
	// When DISTRIBUTED_MODE=false (default) the original Phase 1/2 behaviour is
	// preserved: deployAsync runs in-process. No queue dependency at all.
	if cfg.DistributedMode {
		slog.Info("server: distributed mode enabled — wiring job queue",
			"brokers", cfg.RedpandaBrokers,
		)

		queueCfg := queue.RedpandaConfig{
			Brokers:           cfg.RedpandaBrokers,
			Partitions:        4,
			ReplicationFactor: 1,
		}
		jobQueue, err := queue.NewRedpandaQueue(queueCfg)
		if err != nil {
			slog.Error("server: failed to create job queue", "err", err)
			os.Exit(1)
		}

		// Bootstrap creates the jobs.benchmark topic if it doesn't exist.
		// Idempotent — safe to call on every startup.
		if err := jobQueue.Bootstrap(startCtx); err != nil {
			slog.Error("server: failed to bootstrap job queue topic", "err", err)
			os.Exit(1)
		}

		// Wire the queue into the submission service.
		// WithQueue returns a new *Service; the original is not mutated.
		submissionSvc = submissionSvc.WithQueue(jobQueue)

		slog.Info("server: job queue ready",
			"topic", queue.TopicJobs,
			"brokers", cfg.RedpandaBrokers,
		)

		// Close the queue producer cleanly on shutdown.
		// We register a deferred close via the quit channel below.
		defer func() {
			if err := jobQueue.Close(); err != nil {
				slog.Warn("server: job queue close error", "err", err)
			}
		}()
	} else {
		slog.Info("server: local mode (Phase 1/2) — sandboxes deployed in-process")
	}

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(submissionSvc, cfg, reg)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}
	slog.Info("control plane stopped")
}
