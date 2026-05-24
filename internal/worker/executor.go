package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
)

// sandboxDeployer is the subset of *sandbox.DockerManager that SandboxExecutor
// needs. Unexported interface keeps executor.go independent of the sandbox
// package; tests inject a fakeSandboxDeployer instead.
// cmd/worker passes *sandbox.DockerManager directly — the compiler verifies
// it satisfies this interface at that call site.
type sandboxDeployer interface {
	Deploy(ctx context.Context, sub *models.Submission) (containerID string, hostPort int, err error)
	Stop(ctx context.Context, containerID string) error
	ContainerHealthy(ctx context.Context, containerID string) (bool, error)
}

// SandboxExecutor implements Executor using a sandboxDeployer.
type SandboxExecutor struct {
	docker             sandboxDeployer
	store              Store
	healthPollInterval time.Duration
	healthTimeout      time.Duration
	// onStart is called (if non-nil) when a job begins executing.
	// Used by cmd/worker to update the heartbeater's status.
	onStart func(submissionID string)
	// onFinish is called (if non-nil) when a job finishes (success or failure).
	onFinish func()
}

// NewSandboxExecutor constructs a SandboxExecutor with production defaults.
// Apply functional options (WithJobCallbacks, WithHealthPollInterval) to
// customise behaviour for tests or cmd/worker.
func NewSandboxExecutor(docker sandboxDeployer, store Store, opts ...ExecutorOption) *SandboxExecutor {
	e := &SandboxExecutor{
		docker:             docker,
		store:              store,
		healthPollInterval: 2 * time.Second,
		healthTimeout:      2 * time.Minute,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// ExecutorOption is a functional option for SandboxExecutor.
type ExecutorOption func(*SandboxExecutor)

// WithHealthPollInterval overrides the health-check poll interval.
// Use in tests to avoid 2-second waits between polls.
func WithHealthPollInterval(d time.Duration) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.healthPollInterval = d
	}
}

// WithJobCallbacks wires onStart and onFinish callbacks so cmd/worker can
// keep its heartbeater status in sync with the executor's job lifecycle.
//
//   - onStart(submissionID) is called just before Execute begins working.
//   - onFinish() is called when Execute returns (success or failure).
//
// Both callbacks must be goroutine-safe and return quickly.
func WithJobCallbacks(onStart func(string), onFinish func()) ExecutorOption {
	return func(e *SandboxExecutor) {
		e.onStart = onStart
		e.onFinish = onFinish
	}
}

// Execute runs the benchmark lifecycle for j:
//  1. Load submission from store.
//  2. Deploy sandbox container.
//  3. Persist container metadata.
//  4. Wait for healthy.
//  5. [Stage 3.2 stub] Return placeholder results.
//  6. Stop container (always, via defer).
func (e *SandboxExecutor) Execute(ctx context.Context, j queue.Job) (*models.BenchmarkResults, error) {
	log := slog.With(
		"executor", "sandbox",
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"language", j.Language,
	)

	// Notify heartbeater: we are now busy with this job.
	if e.onStart != nil {
		e.onStart(j.SubmissionID)
	}
	// Notify heartbeater: we are idle again when Execute returns.
	defer func() {
		if e.onFinish != nil {
			e.onFinish()
		}
	}()

	// ── 1. Load submission ────────────────────────────────────────────────────
	sub, err := e.store.Get(j.SubmissionID)
	if err != nil {
		return nil, fmt.Errorf("executor: load submission %s: %w", j.SubmissionID, err)
	}

	// ── 2. Deploy sandbox ─────────────────────────────────────────────────────
	log.Info("executor: deploying sandbox")
	containerID, hostPort, err := e.docker.Deploy(ctx, sub)
	if err != nil {
		return nil, fmt.Errorf("executor: deploy sandbox for %s: %w", j.SubmissionID, err)
	}

	// ── 6. Cleanup — always stop the container when Execute returns ───────────
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if stopErr := e.docker.Stop(stopCtx, containerID); stopErr != nil {
			log.Warn("executor: failed to stop container on cleanup",
				"container_id", containerID[:12],
				"err", stopErr,
			)
		}
	}()

	// ── 3. Persist container metadata ─────────────────────────────────────────
	sub.ContainerID = containerID
	sub.ExposedPort = hostPort
	sub.ContainerName = fmt.Sprintf("nexusbench-%s", sub.ID[:8])
	if updateErr := e.store.Update(sub); updateErr != nil {
		log.Warn("executor: failed to persist container metadata", "err", updateErr)
	}

	// ── 4. Wait for health ────────────────────────────────────────────────────
	log.Info("executor: waiting for container to become healthy",
		"container_id", containerID[:12],
		"host_port", hostPort,
	)
	if err := e.waitHealthy(ctx, containerID, log); err != nil {
		return nil, fmt.Errorf("executor: sandbox not healthy: %w", err)
	}
	log.Info("executor: container is healthy",
		"container_id", containerID[:12],
		"host_port", hostPort,
	)

	// ── 5. Bot fleet (Stage 3.3) ──────────────────────────────────────────────
	// TODO(Stage 3.3): replace stub with real distributed bot fleet.
	log.Info("executor: stage 3.2 stub — bot fleet not yet wired")

	return &models.BenchmarkResults{
		BenchmarkDuration: "stage-3.2-stub",
		CompletedAt:       time.Now().UTC(),
	}, nil
}

func (e *SandboxExecutor) waitHealthy(ctx context.Context, containerID string, log *slog.Logger) error {
	deadline := time.Now().Add(e.healthTimeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("container did not become healthy within %s", e.healthTimeout)
		}
		healthy, err := e.docker.ContainerHealthy(ctx, containerID)
		if err != nil {
			return fmt.Errorf("health check error: %w", err)
		}
		if healthy {
			return nil
		}
		log.Debug("executor: not yet healthy, retrying",
			"container_id", containerID[:12],
			"poll_interval", e.healthPollInterval,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(e.healthPollInterval):
		}
	}
}
