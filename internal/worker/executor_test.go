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
	"github.com/nexusbench/nexusbench/internal/validator"
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
		TestDuration:      500 * time.Millisecond,
		PerBotHTTPTimeout: time.Second,
		GeneratorConfig:   botfleet.DefaultRandomGeneratorConfig(),
	}
}

// echoSandboxServer starts an httptest.Server that accepts all orders and
// returns a valid fill response. requestCount is incremented on each request.
func echoSandboxServer(t *testing.T, requestCount *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			return
		}
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
		worker.WithContestStore(&fakeContestQuerier{contest: miniContest(fleetCfg.TestDuration)}),
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
		t.Fatal("Execute should return error when ctx is canceled during health poll")
	}
	if docker.stopCalls.Load() != 1 {
		t.Errorf("Stop called %d times after ctx cancel; want 1", docker.stopCalls.Load())
	}
}

// ── Stage 5: Pre-flight validator tests ───────────────────────────────────────
//
// These tests exercise runPreflightValidator wired into Execute via
// WithPreflightValidator. They use an httptest.Server as the sandbox endpoint
// so no Docker daemon is required.

// fakeContestQuerier satisfies worker.ContestQuerier and returns a fixed
// contest whose profile durations are set to miniDuration, preventing the
// executor from running a full-length production benchmark during tests.
// If GetActive is called and noActiveContest is true it returns
// models.ErrNoActiveContest instead, causing the executor to fall back to the
// default profile.
type fakeContestQuerier struct {
	contest         *models.Contest
	noActiveContest bool
}

func (f *fakeContestQuerier) GetActive(_ context.Context) (*models.Contest, error) {
	if f.noActiveContest {
		return nil, models.ErrNoActiveContest
	}
	return f.contest, nil
}

// miniContest returns a contest whose Low/Medium/High profiles all use
// miniDuration so fleet runs in tests finish in under a second.
func miniContest(miniDuration time.Duration) *models.Contest {
	miniProfile := func(label string) models.VolatilityProfile {
		return models.VolatilityProfile{
			Label:             label,
			BotCount:          2,
			TestDuration:      miniDuration,
			LimitRatio:        0.80,
			MarketRatio:       0.10,
			CancelRatio:       0.10,
			PriceSpreadCents:  100,
			MaxQuantity:       10,
			TargetP99Ns:       10_000_000,
			TargetSustainTPS:  5_000,
			LatencyWeight:     0.20,
			ThroughputWeight:  0.30,
			CorrectnessWeight: 0.50,
		}
	}
	return &models.Contest{
		ID:            "contest-test",
		Name:          "Test Contest",
		Status:        models.ContestStatusActive,
		LowProfile:    miniProfile("low"),
		MediumProfile: miniProfile("medium"),
		HighProfile:   miniProfile("high"),
		LowWeight:     0.20,
		MediumWeight:  0.35,
		HighWeight:    0.45,
	}
}

// makePreflightJob constructs a Phase-5 'low' profile job so the executor
// activates the pre-flight gate.
func makePreflightJob(sub *models.Submission) queue.Job {
	// RemainingProfiles is empty so Execute reaches computeAndWriteFinalScore
	// instead of trying to enqueue a next job (which would fail without a real
	// queue). For testing the pre-flight path specifically the "low" label is
	// all that matters; the pipeline beyond that is covered by other tests.
	return queue.NewProfileJob(sub, "contest-preflight-test", "low", []string{})
}

// alwaysFailServer rejects every order unconditionally.
func alwaysFailServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OrderID string `json:"order_id"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order_id": req.OrderID,
			"accepted": false,
		})
	}))
}

// executorWithPreflight builds an executor wired with a pre-flight factory
// that always targets a fixed URL (the test server). A miniContest querier is
// injected so that any labeled fleet run uses miniDuration, not the production
// 60–180 s default profiles.
func executorWithPreflight(
	docker *fakeSandboxDeployer,
	store worker.Store,
	fleetCfg botfleet.FleetConfig,
	validatorURL string,
) *worker.SandboxExecutor {
	factory := func(_ string) *validator.Validator {
		// Always point at the validator test server regardless of targetURL so
		// the fleet sandbox host does not interfere with the validator test.
		transport := botfleet.NewRESTTransport(validatorURL, &http.Client{Timeout: 5 * time.Second})
		return validator.New(transport)
	}
	return worker.NewSandboxExecutor(
		docker, store,
		worker.WithHealthPollInterval(time.Millisecond),
		worker.WithFleetConfig(fleetCfg), // Phase 1-4 fallback only
		worker.WithContestStore(&fakeContestQuerier{contest: miniContest(fleetCfg.TestDuration)}),
		worker.WithPreflightValidator(factory),
	)
}

// TestPreflightGate_FailsAndMarksSubmission verifies that when the pre-flight
// server returns failures the executor marks the submission as StatusFailed and
// populates DryRunResult.AllPassed==false, and the bot fleet is never invoked.
func TestPreflightGate_FailsAndMarksSubmission(t *testing.T) {
	t.Parallel()

	validatorSrv := alwaysFailServer(t)
	defer validatorSrv.Close()

	// Fleet server: must NOT be reached if pre-flight fails.
	var fleetReqs atomic.Int64
	fleetSrv := echoSandboxServer(t, &fleetReqs)
	defer fleetSrv.Close()

	port := serverPort(t, fleetSrv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}

	sub := &models.Submission{
		ID:          "preflight-fail-sub",
		Status:      models.StatusPending,
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/preflight-fail-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)

	exec := executorWithPreflight(docker, store, miniFleetConfig(), validatorSrv.URL)

	_, err := exec.Execute(context.Background(), makePreflightJob(sub))

	if err == nil {
		t.Fatal("Execute should return non-nil error when pre-flight fails")
	}
	if fleetReqs.Load() > 0 {
		t.Errorf("fleet server received %d requests; want 0 (bot fleet must not run when preflight fails)",
			fleetReqs.Load())
	}

	fresh, getErr := store.Get(sub.ID)
	if getErr != nil {
		t.Fatalf("store.Get after Execute: %v", getErr)
	}
	if fresh.Status != models.StatusFailed {
		t.Errorf("submission status = %q, want %q", fresh.Status, models.StatusFailed)
	}
	if fresh.DryRunResult == nil {
		t.Fatal("DryRunResult must not be nil after pre-flight failure")
	}
	if fresh.DryRunResult.AllPassed {
		t.Error("DryRunResult.AllPassed must be false after pre-flight failure")
	}
	if fresh.DryRunResult.FailSummary == "" {
		t.Error("DryRunResult.FailSummary must not be empty after pre-flight failure")
	}
}

// TestPreflightGate_PassesAndProceedsToFleet verifies that when the pre-flight
// server returns correct fills, the executor writes DryRunResult.AllPassed==true
// and then proceeds to run the bot fleet.
func TestPreflightGate_PassesAndProceedsToFleet(t *testing.T) {
	t.Parallel()

	book := newOrderbook()
	acceptAllSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OrderID  string `json:"order_id"`
			Kind     string `json:"kind"`
			Side     string `json:"side"`
			Price    int64  `json:"price"`
			Quantity int64  `json:"quantity"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		w.Header().Set("Content-Type", "application/json")

		fill := book.apply(req.OrderID, req.Kind, req.Side, req.Price, req.Quantity)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"order_id":       fill.OrderID,
			"accepted":       fill.Accepted,
			"executed_price": fill.ExecutedPrice,
			"executed_qty":   fill.ExecutedQty,
		})
	}))
	defer acceptAllSrv.Close()

	// The fleet also hits the same server. We reuse it for simplicity—the
	// echo server and the "accept-all" server both accept limit orders.
	port := serverPort(t, acceptAllSrv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}

	sub := &models.Submission{
		ID:          "preflight-pass-sub",
		Status:      models.StatusPending,
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/preflight-pass-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)

	// Count fleet requests via middleware wrapping the accept-all server.
	// We do this by checking AllResults after the run.

	exec := executorWithPreflight(docker, store, miniFleetConfig(), acceptAllSrv.URL)

	_, err := exec.Execute(context.Background(), makePreflightJob(sub))
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}

	fresh, getErr := store.Get(sub.ID)
	if getErr != nil {
		t.Fatalf("store.Get after Execute: %v", getErr)
	}

	// Pre-flight must have written a passing DryRunResult.
	if fresh.DryRunResult == nil {
		t.Fatal("DryRunResult must not be nil after pre-flight pass")
	}
	if !fresh.DryRunResult.AllPassed {
		var failedNames []string
		for _, sc := range fresh.DryRunResult.Scenarios {
			if !sc.Passed {
				failedNames = append(failedNames, sc.Name+": "+sc.Reason)
			}
		}
		t.Errorf("DryRunResult.AllPassed must be true; failed scenarios: %v", failedNames)
	}

	// The bot fleet must have run (AllResults populated).
	if len(fresh.AllResults) == 0 {
		t.Error("AllResults must be non-empty after a successful preflight + fleet run")
	}
}

// TestPreflightGate_SkippedWhenNoFactory verifies that when no factory is
// configured the executor skips validation and calls the bot fleet directly.
func TestPreflightGate_SkippedWhenNoFactory(t *testing.T) {
	t.Parallel()

	var fleetReqs atomic.Int64
	fleetSrv := echoSandboxServer(t, &fleetReqs)
	defer fleetSrv.Close()

	port := serverPort(t, fleetSrv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}

	sub := &models.Submission{
		ID:          "preflight-skip-sub",
		Status:      models.StatusPending,
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/preflight-skip-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)

	// No WithPreflightValidator → factory is nil → gate is skipped.
	exec := executorWithFastHealth(docker, store, miniFleetConfig())

	_, err := exec.Execute(context.Background(), queue.NewProfileJob(sub, "contest-preflight-skip", "low", []string{}))
	if err != nil {
		t.Fatalf("Execute returned unexpected error: %v", err)
	}
	if fleetReqs.Load() == 0 {
		t.Error("fleet server should have received requests when pre-flight gate is skipped")
	}

	fresh, _ := store.Get(sub.ID)
	if fresh != nil && fresh.DryRunResult != nil {
		t.Error("DryRunResult should be nil when no factory is configured")
	}
}

// TestPreflightGate_SkippedForPhase14Job verifies that Phase 1–4 jobs
// (VolatilityLabel=="") skip the pre-flight gate entirely.
func TestPreflightGate_SkippedForPhase14Job(t *testing.T) {
	t.Parallel()

	// Validator server that would cause failures if called.
	validatorSrv := alwaysFailServer(t)
	defer validatorSrv.Close()

	var fleetReqs atomic.Int64
	fleetSrv := echoSandboxServer(t, &fleetReqs)
	defer fleetSrv.Close()

	port := serverPort(t, fleetSrv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}

	sub := &models.Submission{
		ID:          "exec-test-sub",
		Status:      models.StatusPending,
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/exec-test-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)

	exec := executorWithPreflight(docker, store, miniFleetConfig(), validatorSrv.URL)

	// Use the Phase 1–4 job (no VolatilityLabel).
	_, err := exec.Execute(context.Background(), fakeJob()) // fakeJob: VolatilityLabel == ""
	if err != nil {
		t.Fatalf("Execute returned unexpected error for Phase 1-4 job: %v", err)
	}
	if fleetReqs.Load() == 0 {
		t.Error("fleet server should have received requests for Phase 1-4 job")
	}
}

// TestPreflightGate_SkippedForMediumJob verifies that medium/high profile jobs
// skip the pre-flight gate (only 'low' activates it).
//
// The executor is given a fakeContestQuerier so it uses a 500ms fleet duration
// rather than the production 120s default for medium. The job has empty
// RemainingProfiles so Execute computes FinalScore in-process rather than
// trying to enqueue a next job to a nil queue.
func TestPreflightGate_SkippedForMediumJob(t *testing.T) {
	t.Parallel()

	// Validator server that would fail every scenario — must NOT be called.
	validatorSrv := alwaysFailServer(t)
	defer validatorSrv.Close()

	var fleetReqs atomic.Int64
	fleetSrv := echoSandboxServer(t, &fleetReqs)
	defer fleetSrv.Close()

	port := serverPort(t, fleetSrv)
	docker := &fakeSandboxDeployer{HealthyAfter: 0, DeployedPort: port}

	sub := &models.Submission{
		ID:          "medium-skip-sub",
		Status:      models.StatusPending,
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/medium-skip-sub/archive.tar.gz",
	}
	store := newFakeStore(sub)

	exec := executorWithPreflight(docker, store, miniFleetConfig(), validatorSrv.URL)

	// Medium job with empty RemainingProfiles — last in chain, no queue needed.
	j := queue.NewProfileJob(sub, "contest-medium", "medium", []string{})
	_, err := exec.Execute(context.Background(), j)
	if err != nil {
		t.Fatalf("Execute returned unexpected error for medium job: %v", err)
	}
	if fleetReqs.Load() == 0 {
		t.Error("fleet server should have received requests for medium job")
	}
}
