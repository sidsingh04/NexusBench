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

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	cfg := config.Load()

	// ── Submission directory ──────────────────────────────────────────────────
	if err := os.MkdirAll(cfg.SubmissionDir, 0o755); err != nil {
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
	registry := orchestrator.NewWorkerRegistry()
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
		orchHandler = orchestrator.NewHandler(registry)

		slog.Info("server: job queue ready", "topic", queue.TopicJobs)
		slog.Info("server: orchestrator ready — worker fleet routes mounted",
			"register_path", "/internal/workers/register",
			"heartbeat_path", "/internal/workers/{id}/heartbeat",
			"list_path", "/internal/workers",
		)
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}
	slog.Info("control plane stopped")
}
