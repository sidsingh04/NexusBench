// cmd/worker is the entrypoint for a NexusBench distributed benchmark worker.
//
// A worker process:
//  1. Connects to Redpanda and bootstraps the jobs.benchmark topic.
//  2. Connects to the Docker daemon (for sandbox management).
//  3. Polls the job queue and executes benchmark jobs sequentially.
//  4. Writes results back to the shared submission store (disk).
//
// One worker handles one job at a time. Run multiple worker replicas
// (containers / pods) to process jobs in parallel.
//
// Configuration — all via environment variables, all parsed by config.Load():
//
//	WORKER_ID          unique name for this instance    (default: hostname)
//	REDPANDA_BROKERS   comma-separated broker list      (default: 127.0.0.1:19092)
//	SUBMISSION_DIR     path to shared submissions dir   (default: platform default)
//	JOB_TIMEOUT        max duration per job             (default: 35m)
//	SANDBOX_*          sandbox image names, CPU/mem limits, port range, timeout
//
// Example (local dev against docker-compose stack):
//
//	REDPANDA_BROKERS=localhost:19092 \
//	SUBMISSION_DIR=/data/submissions \
//	  go run ./cmd/worker
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
	"github.com/nexusbench/nexusbench/internal/submission"
	"github.com/nexusbench/nexusbench/internal/worker"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// config.Load() reads all env vars — REDPANDA_BROKERS, SUBMISSION_DIR,
	// SANDBOX_*, JOB_TIMEOUT, WORKER_ID — in one place, with no duplication.
	cfg := config.Load()

	slog.Info("worker: starting",
		"worker_id", cfg.WorkerID,
		"brokers", cfg.RedpandaBrokers,
		"job_timeout", cfg.JobTimeout,
		"submission_dir", cfg.SubmissionDir,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Submission store ──────────────────────────────────────────────────────
	if err := os.MkdirAll(cfg.SubmissionDir, 0o755); err != nil {
		slog.Error("worker: cannot create submission dir",
			"path", cfg.SubmissionDir, "err", err)
		os.Exit(1)
	}
	store := submission.NewDiskStore(cfg.SubmissionDir)

	// ── Docker manager ────────────────────────────────────────────────────────
	dockerMgr, err := sandbox.NewDockerManager(cfg)
	if err != nil {
		slog.Error("worker: connect to Docker daemon", "err", err)
		os.Exit(1)
	}

	startCtx, startCancel := context.WithTimeout(ctx, 30*time.Second)
	defer startCancel()

	if err := dockerMgr.VerifyImages(startCtx); err != nil {
		// Non-fatal: jobs for missing images will fail at execute time with
		// a clear error. Do not block startup.
		slog.Warn("worker: some sandbox images are missing", "err", err)
	}

	// ── Job queue ─────────────────────────────────────────────────────────────
	jobQueue, err := queue.NewRedpandaQueue(queue.RedpandaConfig{
		Brokers:           cfg.RedpandaBrokers,
		Partitions:        4,
		ReplicationFactor: 1,
	})
	if err != nil {
		slog.Error("worker: create job queue", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := jobQueue.Close(); err != nil {
			slog.Warn("worker: job queue close error", "err", err)
		}
	}()

	if err := jobQueue.Bootstrap(ctx); err != nil {
		slog.Error("worker: bootstrap job queue topic", "err", err)
		os.Exit(1)
	}

	// ── Executor + Worker ─────────────────────────────────────────────────────
	executor := worker.NewSandboxExecutor(dockerMgr, store)

	w, err := worker.NewWorker(jobQueue, store, executor, worker.Config{
		WorkerID:   cfg.WorkerID,
		JobTimeout: cfg.JobTimeout,
	})
	if err != nil {
		slog.Error("worker: create worker", "err", err)
		os.Exit(1)
	}

	slog.Info("worker: ready, polling for jobs",
		"worker_id", cfg.WorkerID,
		"topic", queue.TopicJobs,
	)

	if err := w.Run(ctx); err != nil {
		slog.Error("worker: run error", "err", err)
		os.Exit(1)
	}

	slog.Info("worker: shutdown complete", "worker_id", cfg.WorkerID)
}
