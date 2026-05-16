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
	// Create the directory and log its absolute path.
	// On Windows this must be under a drive Docker Desktop shares (C:\ by default).
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
		slog.Error("image verification failed", "err", err)
		os.Exit(1)
	}

	// ── Services ──────────────────────────────────────────────────────────────
	store := submission.NewDiskStore(cfg.SubmissionDir)
	submissionSvc := submission.NewService(store, dockerMgr, cfg)

	// ── HTTP server ───────────────────────────────────────────────────────────
	router := api.NewRouter(submissionSvc, cfg)
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
