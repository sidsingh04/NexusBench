// cmd/worker is the entrypoint for a NexusBench distributed benchmark worker.
//
// Startup sequence:
//  1. Load config from environment variables.
//  2. Connect to Docker daemon and verify sandbox images.
//  3. Connect to Redpanda and bootstrap jobs.benchmark topic.
//  4. Start Heartbeater goroutine → registers with orchestrator, then pings every 5s.
//  5. Start Worker poll loop → blocks until SIGTERM/SIGINT.
//
// Configuration (all env vars, all parsed by config.Load()):
//
//	WORKER_ID          unique name            (default: hostname)
//	ORCHESTRATOR_URL   control plane base URL (default: http://localhost:8080)
//	REDPANDA_BROKERS   broker list            (default: 127.0.0.1:19092)
//	SUBMISSION_DIR     shared submissions dir (default: platform default)
//	JOB_TIMEOUT        max per-job duration   (default: 35m)
//	SANDBOX_*          image names, limits, port range
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
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

	cfg := config.Load()

	slog.Info("worker: starting",
		"worker_id", cfg.WorkerID,
		"orchestrator", cfg.OrchestratorURL,
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

	// ── Status tracking for heartbeater ──────────────────────────────────────
	// workerStatus and currentJobID are written by the Worker poll loop and
	// read by the Heartbeater. Both use atomics/sync so no mutex is needed
	// across the two goroutines.
	//
	// workerBusy: 0 = idle, 1 = busy
	// currentJobID: the submission ID being processed, or "" when idle
	var workerBusy atomic.Int32
	var currentJobMu sync.RWMutex
	var currentJobID string
	var jobsCompleted atomic.Int32

	// statusFn is called by the Heartbeater on every tick.
	statusFn := func() worker.HeartbeatStatus {
		currentJobMu.RLock()
		jobID := currentJobID
		currentJobMu.RUnlock()

		if workerBusy.Load() == 1 {
			return worker.HeartbeatStatus{
				Busy:          true,
				CurrentJobID:  jobID,
				JobsCompleted: int(jobsCompleted.Load()),
			}
		}
		return worker.HeartbeatStatus{
			Busy:          false,
			JobsCompleted: int(jobsCompleted.Load()),
		}
	}

	// ── Heartbeater ───────────────────────────────────────────────────────────
	hb := worker.NewHeartbeater(cfg.WorkerID, cfg.OrchestratorURL, statusFn)
	go hb.Run(ctx)

	// ── Executor ─────────────────────────────────────────────────────────────
	// jobStarted / jobFinished are callbacks that keep workerBusy and
	// currentJobID in sync so the heartbeater always reports the correct state.
	jobStarted := func(submissionID string) {
		workerBusy.Store(1)
		currentJobMu.Lock()
		currentJobID = submissionID
		currentJobMu.Unlock()
	}
	jobFinished := func() {
		workerBusy.Store(0)
		currentJobMu.Lock()
		currentJobID = ""
		currentJobMu.Unlock()
		jobsCompleted.Add(1)
	}

	executor := worker.NewSandboxExecutor(dockerMgr, store,
		worker.WithJobCallbacks(jobStarted, jobFinished),
		worker.WithSandboxHost(cfg.SandboxHost))

	// ── Worker ────────────────────────────────────────────────────────────────
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
