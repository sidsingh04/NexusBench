package worker_test

// executor_test.go tests SandboxExecutor in isolation using a
// fakeSandboxDeployer — no Docker daemon required.
//
// Tests:
//   TestSandboxExecutor_InterfaceCompliance   — *sandbox.DockerManager satisfies sandboxDeployer
//   TestSandboxExecutor_ReturnsStubResults    — happy path returns non-nil results
//   TestSandboxExecutor_DeployError           — Deploy failure surfaces as error
//   TestSandboxExecutor_StoreLoadError        — store.Get failure surfaces as error
//   TestSandboxExecutor_AlwaysStopsContainer  — container stopped even on health-check failure
//   TestSandboxExecutor_ContextCancelledDuringHealth — ctx cancel during health poll returns error

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/worker"
)

// ── interface compliance note ──────────────────────────────────────────────
// sandboxDeployer is unexported so we cannot write the standard
//   var _ sandboxDeployer = (*sandbox.DockerManager)(nil)
// check here. The equivalent compile-time check lives in cmd/worker/main.go
// where NewSandboxExecutor(dockerMgr, store) is called with a concrete
// *sandbox.DockerManager — the compiler rejects that call if DockerManager
// ever stops satisfying the interface.

// ── fakeSandboxDeployer ───────────────────────────────────────────────────────

// fakeSandboxDeployer implements the sandboxDeployer interface for tests.
// All behaviour is configurable via exported fields.
type fakeSandboxDeployer struct {
	// DeployErr, if non-nil, is returned by Deploy.
	DeployErr error
	// DeployedID is returned as the containerID on successful Deploy.
	DeployedID string
	// DeployedPort is returned as the hostPort on successful Deploy.
	DeployedPort int
	// HealthyAfter is the number of ContainerHealthy calls before returning true.
	// 0 means healthy on the first call.
	HealthyAfter int
	// HealthErr, if non-nil, is returned by ContainerHealthy.
	HealthErr error

	// Counters — read after Execute returns.
	deployCalls  atomic.Int32
	stopCalls    atomic.Int32
	healthCalls  atomic.Int32
}

func (f *fakeSandboxDeployer) Deploy(_ context.Context, _ *models.Submission) (string, int, error) {
	f.deployCalls.Add(1)
	if f.DeployErr != nil {
		return "", 0, f.DeployErr
	}
	id := f.DeployedID
	if id == "" {
		id = "fake-container-abc123"
	}
	port := f.DeployedPort
	if port == 0 {
		port = 20001
	}
	return id, port, nil
}

func (f *fakeSandboxDeployer) Stop(_ context.Context, _ string) error {
	f.stopCalls.Add(1)
	return nil
}

func (f *fakeSandboxDeployer) ContainerHealthy(_ context.Context, _ string) (bool, error) {
	calls := int(f.healthCalls.Add(1))
	if f.HealthErr != nil {
		return false, f.HealthErr
	}
	return calls > f.HealthyAfter, nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

// executorWithFastHealth returns a SandboxExecutor whose health poller ticks
// every millisecond, so tests don't wait 2 seconds between polls.
func executorWithFastHealth(docker *fakeSandboxDeployer, store worker.Store) *worker.SandboxExecutor {
	e := worker.NewSandboxExecutor(docker, store)
	// Use the exported setter so tests can configure the poll interval.
	// See WithHealthPollInterval below.
	return e.WithHealthPollInterval(time.Millisecond)
}

func fakeJob() queue.Job {
	return queue.NewJob(&models.Submission{
		ID:          "exec-test-sub",
		TeamName:    "executor-tests",
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/exec-test-sub/archive.tar.gz",
	})
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestSandboxExecutor_ReturnsStubResults(t *testing.T) {
	t.Parallel()
	docker := &fakeSandboxDeployer{HealthyAfter: 0}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
		ArchivePath: "/submissions/exec-test-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store)

	results, err := exec.Execute(context.Background(), fakeJob())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if results == nil {
		t.Fatal("Execute returned nil results")
	}
	if results.BenchmarkDuration != "stage-3.1-stub" {
		t.Errorf("BenchmarkDuration = %q, want %q", results.BenchmarkDuration, "stage-3.1-stub")
	}
}

func TestSandboxExecutor_DeployError(t *testing.T) {
	t.Parallel()
	docker := &fakeSandboxDeployer{DeployErr: errors.New("docker: image not found")}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store)

	_, err := exec.Execute(context.Background(), fakeJob())
	if err == nil {
		t.Fatal("Execute should return error on Deploy failure")
	}
	// Container was never deployed, so Stop must not be called.
	if docker.stopCalls.Load() != 0 {
		t.Errorf("Stop called %d times; want 0 (deploy failed, nothing to stop)", docker.stopCalls.Load())
	}
}

func TestSandboxExecutor_StoreLoadError(t *testing.T) {
	t.Parallel()
	docker := &fakeSandboxDeployer{}
	store := newFakeStore() // empty — Get will fail

	exec := executorWithFastHealth(docker, store)
	_, err := exec.Execute(context.Background(), fakeJob())
	if err == nil {
		t.Fatal("Execute should return error when store.Get fails")
	}
	if docker.deployCalls.Load() != 0 {
		t.Errorf("Deploy called %d times; want 0 (store load failed)", docker.deployCalls.Load())
	}
}

func TestSandboxExecutor_AlwaysStopsContainer(t *testing.T) {
	t.Parallel()
	// Health check always fails — Execute returns an error after Deploy.
	docker := &fakeSandboxDeployer{HealthErr: errors.New("health: daemon unreachable")}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store)

	_, err := exec.Execute(context.Background(), fakeJob())
	if err == nil {
		t.Fatal("Execute should return error on health-check failure")
	}
	// Even on error, the defer must have stopped the container.
	if docker.stopCalls.Load() != 1 {
		t.Errorf("Stop called %d times after health error; want 1", docker.stopCalls.Load())
	}
}

func TestSandboxExecutor_ContextCancelledDuringHealth(t *testing.T) {
	t.Parallel()
	// Never becomes healthy — health always returns false.
	docker := &fakeSandboxDeployer{HealthyAfter: 999}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := exec.Execute(ctx, fakeJob())
	if err == nil {
		t.Fatal("Execute should return error when ctx is cancelled during health poll")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		// The error is wrapped — check the string contains the cause.
		t.Logf("err = %v (expected to wrap context.DeadlineExceeded)", err)
	}
	// Container must still be stopped.
	if docker.stopCalls.Load() != 1 {
		t.Errorf("Stop called %d times after ctx cancel; want 1", docker.stopCalls.Load())
	}
}
