package worker_test

// executor_test.go tests SandboxExecutor in isolation using a
// fakeSandboxDeployer and a local httptest.Server as the sandbox endpoint.
// No Docker daemon required.
//
// Tests:
//   TestSandboxExecutor_ReturnsRealResults    — happy path: fleet runs, real BenchmarkResults populated
//   TestSandboxExecutor_DeployError           — Deploy failure surfaces as error
//   TestSandboxExecutor_StoreLoadError        — store.Get failure surfaces as error
//   TestSandboxExecutor_AlwaysStopsContainer  — container stopped even on health-check failure
//   TestSandboxExecutor_ContextCancelledDuringHealth — ctx cancel during health poll returns error

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/worker"
)

// ── fakeSandboxDeployer ───────────────────────────────────────────────────────

type fakeSandboxDeployer struct {
	DeployErr    error
	DeployedID   string
	DeployedPort int
	HealthyAfter int
	HealthErr    error

	deployCalls atomic.Int32
	stopCalls   atomic.Int32
	healthCalls atomic.Int32
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
	return id, f.DeployedPort, nil
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

// miniFleetConfig returns a tiny fleet config suitable for fast tests.
func miniFleetConfig() botfleet.FleetConfig {
	return botfleet.FleetConfig{
		BotCount:          2,
		RampUpDuration:    0,
		TestDuration:      50 * time.Millisecond,
		PerBotHTTPTimeout: time.Second,
		GeneratorConfig:   botfleet.DefaultRandomGeneratorConfig(),
	}
}

// echoSandboxServer starts an httptest.Server that accepts all orders and
// returns a valid fill response. requestCount is incremented on each request.
func echoSandboxServer(t *testing.T, requestCount *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount != nil {
			requestCount.Add(1)
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		orderID, _ := req["order_id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order_id":       orderID,
			"accepted":       true,
			"executed_price": int64(10_000),
			"executed_qty":   int64(10),
		})
	}))
}

// serverPort parses the TCP port from an httptest.Server URL.
func serverPort(t *testing.T, srv *httptest.Server) int {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse server URL %q: %v", srv.URL, err)
	}
	_, portStr, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split host/port from %q: %v", u.Host, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse port %q: %v", portStr, err)
	}
	return port
}

// executorWithFastHealth returns a SandboxExecutor whose health poller ticks
// every millisecond, so tests don't wait 2 seconds between polls.
func executorWithFastHealth(docker *fakeSandboxDeployer, store worker.Store, fleetCfg botfleet.FleetConfig) *worker.SandboxExecutor {
	return worker.NewSandboxExecutor(
		docker,
		store,
		worker.WithHealthPollInterval(time.Millisecond),
		worker.WithFleetConfig(fleetCfg),
	)
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

func TestSandboxExecutor_ReturnsRealResults(t *testing.T) {
	t.Parallel()

	var reqCount atomic.Int64
	srv := echoSandboxServer(t, &reqCount)
	defer srv.Close()

	port := serverPort(t, srv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
		ArchivePath: "/submissions/exec-test-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store, miniFleetConfig())

	results, err := exec.Execute(context.Background(), fakeJob())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if results == nil {
		t.Fatal("Execute returned nil results")
	}
	if reqCount.Load() == 0 {
		t.Error("echo server received no requests — fleet did not run")
	}
	if results.TotalOrders == 0 {
		t.Error("TotalOrders = 0, expected > 0 after fleet run")
	}
	if results.CompositeScore < 0 || results.CompositeScore > 100 {
		t.Errorf("CompositeScore = %f, want in [0,100]", results.CompositeScore)
	}
	if results.BenchmarkDuration == "" {
		t.Error("BenchmarkDuration must not be empty")
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
	exec := executorWithFastHealth(docker, store, miniFleetConfig())

	_, err := exec.Execute(context.Background(), fakeJob())
	if err == nil {
		t.Fatal("Execute should return error on Deploy failure")
	}
	if docker.stopCalls.Load() != 0 {
		t.Errorf("Stop called %d times; want 0 (deploy failed, nothing to stop)", docker.stopCalls.Load())
	}
}

func TestSandboxExecutor_StoreLoadError(t *testing.T) {
	t.Parallel()
	docker := &fakeSandboxDeployer{}
	store := newFakeStore() // empty — Get will fail

	exec := executorWithFastHealth(docker, store, miniFleetConfig())
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
	docker := &fakeSandboxDeployer{HealthErr: errors.New("health: daemon unreachable")}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store, miniFleetConfig())

	_, err := exec.Execute(context.Background(), fakeJob())
	if err == nil {
		t.Fatal("Execute should return error on health-check failure")
	}
	if docker.stopCalls.Load() != 1 {
		t.Errorf("Stop called %d times after health error; want 1", docker.stopCalls.Load())
	}
}

func TestSandboxExecutor_ContextCancelledDuringHealth(t *testing.T) {
	t.Parallel()
	docker := &fakeSandboxDeployer{HealthyAfter: 999}
	sub := &models.Submission{
		ID: "exec-test-sub", Status: models.StatusPending,
		Language: models.LangGo, Protocol: models.ProtocolREST,
	}
	store := newFakeStore(sub)
	exec := executorWithFastHealth(docker, store, miniFleetConfig())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := exec.Execute(ctx, fakeJob())
	if err == nil {
		t.Fatal("Execute should return error when ctx is cancelled during health poll")
	}
	if docker.stopCalls.Load() != 1 {
		t.Errorf("Stop called %d times after ctx cancel; want 1", docker.stopCalls.Load())
	}
}
