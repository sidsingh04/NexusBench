// Package worker implements the distributed benchmark worker.
//
// A Worker is a long-running process that:
//  1. Dequeues a Job from the queue (blocking, at-least-once).
//  2. Guards against double-execution by checking the submission's current
//     status in the store — if it's no longer Pending, the job is skipped
//     and committed (idempotent guard).
//  3. Runs the full benchmark lifecycle via the Executor interface:
//     deploy sandbox → wait healthy → run bot fleet → collect results.
//  4. Writes the final submission status and BenchmarkResults back to the
//     shared store so the control plane can serve them via GET /submissions/{id}.
//  5. Commits the queue offset only after the store write succeeds,
//     upholding the at-least-once delivery guarantee.
//
// Design decisions:
//
//   - Worker depends on three interfaces (Queue, Store, Executor) — never on
//     concrete types. This keeps the unit tests fully in-memory, no Docker,
//     no Redpanda required.
//
//   - One goroutine per job (via Run's sequential poll loop). Concurrency
//     across jobs comes from running multiple Worker processes, not from
//     goroutines within one process. This keeps the per-worker resource
//     footprint predictable and avoids goroutine-level scheduling complexity.
//
//   - Errors during job execution mark the submission as StatusFailed in the
//     store and then commit the queue offset. We do NOT re-queue on error —
//     a failed benchmark is a terminal outcome, not a transient failure.
//     Transient infra errors (Docker unavailable) do NOT commit so the job
//     is re-delivered after worker restart.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
)

// Store is the worker's view of the submission persistence layer.
// It mirrors submission.Store but is defined here so the worker package
// does not import the submission package (avoiding a circular dependency).
type Store interface {
	Get(id string) (*models.Submission, error)
	Update(sub *models.Submission) error
}

// Executor runs the benchmark for a single job and returns the results.
// This is the boundary between the worker orchestration logic and the
// concrete sandbox + bot-fleet implementation.
//
// Implementations:
//   - SandboxExecutor (internal/worker/executor.go): real Docker sandbox.
//   - FakeExecutor (worker_test.go): deterministic fake for unit tests.
//
// Contract:
//   - Execute may take a long time (minutes). Callers pass a context with
//     the job's deadline; implementations must respect ctx cancellation.
//   - On error, Execute must not have modified any external state that
//     cannot be cleaned up — the worker will mark the job as failed.
type Executor interface {
	Execute(ctx context.Context, j queue.Job) (*models.BenchmarkResults, error)
}

// Config holds all tunable parameters for a Worker.
type Config struct {
	// WorkerID uniquely identifies this worker instance in logs and metrics.
	// Defaults to the hostname if empty.
	WorkerID string

	// JobTimeout is the maximum wall-clock time allowed for a single job
	// (sandbox deploy + bot fleet + result collection).
	// After this duration the job's context is canceled and the job fails.
	JobTimeout time.Duration
}

// DefaultConfig returns a Config with production-safe defaults.
func DefaultConfig() Config {
	return Config{
		JobTimeout: 35 * time.Minute, // sandbox timeout (30m) + headroom
	}
}

// Worker polls a Queue for jobs and executes them sequentially.
// Use NewWorker to construct; the zero value is invalid.
type Worker struct {
	id       string
	q        queue.Queue
	store    Store
	executor Executor
	cfg      Config
}

// NewWorker constructs a Worker. All three of q, store, and executor are
// required and must be non-nil.
func NewWorker(q queue.Queue, store Store, executor Executor, cfg Config) (*Worker, error) {
	if q == nil {
		return nil, fmt.Errorf("worker: Queue must not be nil")
	}
	if store == nil {
		return nil, fmt.Errorf("worker: Store must not be nil")
	}
	if executor == nil {
		return nil, fmt.Errorf("worker: Executor must not be nil")
	}

	id := cfg.WorkerID
	if id == "" {
		id = "worker-unknown"
	}
	if cfg.JobTimeout == 0 {
		cfg.JobTimeout = DefaultConfig().JobTimeout
	}

	return &Worker{
		id:       id,
		q:        q,
		store:    store,
		executor: executor,
		cfg:      cfg,
	}, nil
}

// Run starts the poll loop. It blocks until ctx is canceled.
// Jobs are processed one at a time; the loop only blocks between jobs
// when the queue is empty (Dequeue blocks inside the queue implementation).
//
// Typical usage:
//
//	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer cancel()
//	if err := worker.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
func (w *Worker) Run(ctx context.Context) error {
	slog.Info("worker: poll loop started",
		"worker_id", w.id,
		"job_timeout", w.cfg.JobTimeout,
	)

	for {
		// Dequeue blocks until a job arrives or ctx is canceled.
		j, err := w.q.Dequeue(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				slog.Info("worker: context canceled, stopping poll loop", "worker_id", w.id)
				return nil
			}
			// Unexpected queue error — log and retry after a brief backoff
			// to avoid spinning on a persistently broken queue.
			slog.Error("worker: dequeue error", "worker_id", w.id, "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(2 * time.Second):
			}
			continue
		}

		w.processJob(ctx, j)
	}
}

// processJob executes one job and handles all error paths.
// It never returns an error — all outcomes (success, failure, skip) are
// written back to the store and the queue offset is committed.
func (w *Worker) processJob(ctx context.Context, j queue.Job) {
	log := slog.With(
		"worker_id", w.id,
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"team", j.TeamName,
		"language", j.Language,
	)

	log.Info("worker: picked up job",
		"queue_latency_ms", time.Since(j.EnqueuedAt).Milliseconds(),
	)

	// ── Idempotent guard ──────────────────────────────────────────────────────
	// Read the current submission status before executing.
	//
	// Phase 1–4: only StatusPending passes through — any other status means
	// a previous worker already handled this job; skip and commit.
	//
	// Phase 5 multi-profile: after the first profile run, processJob sets the
	// submission to StatusRunning (below). Subsequent profile jobs therefore
	// arrive with StatusRunning — they must be allowed through, not skipped.
	// Terminal statuses (StatusCompleted, StatusFailed) are always skipped.
	sub, err := w.store.Get(j.SubmissionID)
	if err != nil {
		log.Error("worker: cannot read submission; will not commit (job will be re-delivered)", "err", err)
		// Do NOT commit — let the job be re-delivered after a worker restart
		// when the store may be healthy again.
		return
	}

	if sub.Status.IsTerminal() {
		log.Info("worker: submission is already terminal; skipping",
			"current_status", sub.Status,
		)
		w.commitOrLog(ctx, log)
		return
	}
	// For Phase 5 non-first profile jobs, StatusRunning is expected and valid.
	// For Phase 1–4 jobs and the first profile job, StatusPending is expected.
	// Any other non-terminal status (StatusBuilding, StatusDeploying,
	// StatusBenchmarking) means another worker is already executing —
	// skip and commit to avoid double-execution.
	if j.VolatilityLabel == "" && sub.Status != models.StatusPending {
		log.Info("worker: Phase 1-4 job submission is not pending; skipping",
			"current_status", sub.Status,
		)
		w.commitOrLog(ctx, log)
		return
	}
	if j.VolatilityLabel != "" &&
		sub.Status != models.StatusPending &&
		sub.Status != models.StatusRunning {
		log.Info("worker: Phase 5 job submission is in unexpected status; skipping",
			"current_status", sub.Status,
			"volatility_label", j.VolatilityLabel,
		)
		w.commitOrLog(ctx, log)
		return
	}

	// ── Execute ───────────────────────────────────────────────────────────────
	jobCtx, cancel := context.WithTimeout(ctx, w.cfg.JobTimeout)
	defer cancel()

	w.setStatus(log, sub, models.StatusDeploying, "worker picked up job")

	results, execErr := w.executor.Execute(jobCtx, j)

	// ── Write outcome ─────────────────────────────────────────────────────────
	if execErr != nil {
		log.Error("worker: job execution failed", "err", execErr)
		w.setStatus(log, sub, models.StatusFailed, fmt.Sprintf("execution error: %v", execErr))
		// Commit: a failed benchmark is terminal, not retriable.
		w.commitOrLog(ctx, log)
		return
	}

	// ── Success path ────────────────────────────────────────────────────────
	// Re-read the submission so we see AllResults and FinalScore written by
	// Execute (appendProfileResult / computeAndWriteFinalScore both call
	// store.Update before returning).
	if fresh, rErr := w.store.Get(j.SubmissionID); rErr == nil {
		sub = fresh
	}

	now := time.Now().UTC()

	if j.IsLastProfile() {
		// Final profile (or Phase 1–4 single-run): mark submission completed.
		//
		// Phase 1–4 backward compat: also write results to the legacy Results
		// field so existing leaderboard/API consumers still work without change.
		if j.VolatilityLabel == "" {
			sub.Results = results
		}
		sub.CompletedAt = &now
		w.setStatus(log, sub, models.StatusCompleted, "benchmark complete")

		log.Info("worker: submission completed",
			"submission_id", j.SubmissionID,
			"final_score", sub.FinalScore,
			"p99_latency_ms", results.P99LatencyMs,
			"correctness", results.CorrectnessScore,
		)
	} else {
		// Non-final profile: more profiles remain. Execute has already
		// appended this result to AllResults and enqueued the next profile job.
		// Mark StatusRunning so the idempotent guard above lets the next
		// profile job through, and the API shows meaningful in-progress state.
		w.setStatus(log, sub, models.StatusRunning,
			fmt.Sprintf("profile %q done, %v remaining",
				j.VolatilityLabel, j.RemainingProfiles))

		log.Info("worker: profile run committed, next profile enqueued",
			"completed_label", j.VolatilityLabel,
			"remaining", j.RemainingProfiles,
			"run_score", results.RunScore,
		)
	}

	// Commit only after results are durably written.
	w.commitOrLog(ctx, log)
}

// setStatus persists a status update for sub. The log argument is used for
// structured error output so log lines carry the job's context fields.
func (w *Worker) setStatus(log *slog.Logger, sub *models.Submission, status models.SubmissionStatus, msg string) {
	sub.Status = status
	sub.StatusMsg = msg
	if err := w.store.Update(sub); err != nil {
		log.Error("worker: failed to persist status update",
			"new_status", status,
			"err", err,
		)
	}
}

// commitOrLog commits the current job's queue offset. If the commit fails
// it logs the error rather than crashing — the job will be re-delivered on
// restart, which the idempotent guard in processJob handles correctly.
func (w *Worker) commitOrLog(ctx context.Context, log *slog.Logger) {
	if err := w.q.CommitJob(ctx); err != nil {
		log.Error("worker: failed to commit job offset; job may be re-delivered", "err", err)
	}
}
