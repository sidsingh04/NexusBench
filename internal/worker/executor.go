package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
)

// sandboxDeployer is the subset of the Docker manager that SandboxExecutor
// needs. Keeping this as a package-local interface (rather than accepting
// *sandbox.DockerManager directly) means:
//
//  1. Tests can inject a fakeSandboxDeployer without importing the sandbox
//     package or requiring a live Docker daemon.
//  2. SandboxExecutor is decoupled from the concrete DockerManager type —
//     a gVisor or Firecracker implementation can be swapped in transparently.
//
// The concrete *sandbox.DockerManager satisfies this interface; verified at
// compile time by the var _ check in executor_test.go.
type sandboxDeployer interface {
	Deploy(ctx context.Context, sub *models.Submission) (containerID string, hostPort int, err error)
	Stop(ctx context.Context, containerID string) error
	ContainerHealthy(ctx context.Context, containerID string) (bool, error)
}

// SandboxExecutor implements Executor using a sandboxDeployer (typically
// *sandbox.DockerManager in production, a fake in tests).
//
// For Stage 3.1 it deploys the sandbox and returns a placeholder result.
// The full bot-fleet integration is added in Stage 3.2.
type SandboxExecutor struct {
	docker             sandboxDeployer
	store              Store
	healthPollInterval time.Duration
	healthTimeout      time.Duration
}

// NewSandboxExecutor constructs a SandboxExecutor.
//
// docker is any sandboxDeployer (pass *sandbox.DockerManager from cmd/worker).
// store is used to load the full Submission and to persist container metadata.
//
// Accepting the interface rather than the concrete type allows tests to pass
// a fakeSandboxDeployer without needing a live Docker daemon.
func NewSandboxExecutor(docker sandboxDeployer, store Store) *SandboxExecutor {
	return &SandboxExecutor{
		docker:             docker,
		store:              store,
		healthPollInterval: 2 * time.Second,
		healthTimeout:      2 * time.Minute,
	}
}

// WithHealthPollInterval returns a copy of e with the health poll interval
// overridden. Used in tests to avoid 2-second waits between polls.
// Production code should use the default set by NewSandboxExecutor.
func (e *SandboxExecutor) WithHealthPollInterval(d time.Duration) *SandboxExecutor {
	copy := *e
	copy.healthPollInterval = d
	return &copy
}

// Execute runs the benchmark lifecycle for j:
//  1. Loads the full Submission from the store.
//  2. Deploys the sandbox container.
//  3. Persists container metadata (ID, port) to the store.
//  4. Waits for the container to report healthy.
//  5. [Stage 3.2] Runs the bot fleet.
//  6. Stops the container and returns results.
//
// Stage 3.1 stub: step 5 returns placeholder zeros so the full pipeline
// (queue → worker → store → API) can be validated before the bot fleet exists.
// The defer on step 6 runs regardless of outcome, preventing orphaned containers.
func (e *SandboxExecutor) Execute(ctx context.Context, j queue.Job) (*models.BenchmarkResults, error) {
	log := slog.With(
		"executor", "sandbox",
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"language", j.Language,
	)

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
	// Registered immediately after a successful Deploy so no error path can
	// leak an orphaned container.
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
		// Non-fatal: the benchmark proceeds; metadata is best-effort.
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

	// ── 5. Bot fleet ──────────────────────────────────────────────────────────
	// TODO(Stage 3.2): replace stub with real distributed bot fleet.
	log.Info("executor: returning Stage 3.1 stub results (bot fleet not yet wired)")

	return &models.BenchmarkResults{
		BenchmarkDuration: "stage-3.1-stub",
		CompletedAt:       time.Now().UTC(),
	}, nil
}

// waitHealthy polls ContainerHealthy until the container is ready,
// ctx is cancelled, or e.healthTimeout is exceeded.
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

		log.Debug("executor: container not yet healthy, retrying",
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
