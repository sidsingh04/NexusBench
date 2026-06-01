package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// mockValidatorRunner implements api.ValidatorRunner for testing.
type mockValidatorRunner struct {
	result *api.ValidatorResult
	err    error
}

func (m *mockValidatorRunner) Run(ctx context.Context, submissionID string) (*api.ValidatorResult, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.result.SubmissionID = submissionID
	return m.result, nil
}

// setupValidationRouter builds a test router with a mocked ValidatorFactory.
func setupValidationRouter(t *testing.T, runner api.ValidatorRunner) (http.Handler, *submission.DiskStore) {
	t.Helper()
	cfg := &config.Config{
		SubmissionDir:  t.TempDir(),
		MaxUploadBytes: 64 << 20,
	}

	store := submission.NewDiskStore(cfg.SubmissionDir)
	svc := submission.NewService(store, nil, cfg)
	reg := metrics.New()
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil)

	factory := func(targetURL string) api.ValidatorRunner {
		return runner
	}

	router := api.NewRouter(svc, cfg, reg, nil, contestSvc, factory, nil)
	return router, store
}

func TestValidate_Success(t *testing.T) {
	t.Parallel()

	expectedResult := &api.ValidatorResult{
		AllPassed: true,
		Scenarios: []api.ScenarioResult{
			{Name: "Limit Order", Passed: true},
		},
		TestedAt: time.Now().UTC(),
	}

	runner := &mockValidatorRunner{result: expectedResult}
	router, store := setupValidationRouter(t, runner)

	// Create a dummy submission and simulate it being running with an exposed port.
	sub := &models.Submission{
		ID:          "test-sub-success",
		TeamName:    "alpha",
		Status:      models.StatusPending,
		ExposedPort: 8080,
	}
	if err := store.Save(sub); err != nil {
		t.Fatalf("failed to save submission: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/api/v1/submissions/test-sub-success/validate", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rr.Code)
	}

	var result api.ValidatorResult
	if err := json.NewDecoder(rr.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if result.SubmissionID != "test-sub-success" {
		t.Errorf("expected SubmissionID test-sub-success, got %s", result.SubmissionID)
	}
	if !result.AllPassed {
		t.Errorf("expected AllPassed=true")
	}
}

func TestValidate_BenchmarkingConflict(t *testing.T) {
	t.Parallel()

	router, store := setupValidationRouter(t, nil)

	sub := &models.Submission{
		ID:          "test-sub-conflict",
		TeamName:    "alpha",
		Status:      models.StatusBenchmarking,
		ExposedPort: 8080,
	}
	_ = store.Save(sub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/submissions/test-sub-conflict/validate", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d", rr.Code)
	}
	var apiErr models.APIError
	_ = json.NewDecoder(rr.Body).Decode(&apiErr)
	if apiErr.Code != "VALIDATION_CONFLICT" {
		t.Errorf("expected VALIDATION_CONFLICT, got %s", apiErr.Code)
	}
}

func TestValidate_NotReady(t *testing.T) {
	t.Parallel()

	router, store := setupValidationRouter(t, nil)

	sub := &models.Submission{
		ID:          "test-sub-not-ready",
		TeamName:    "alpha",
		Status:      models.StatusPending,
		ExposedPort: 0, // no port exposed
	}
	_ = store.Save(sub)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/submissions/test-sub-not-ready/validate", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict, got %d", rr.Code)
	}
	var apiErr models.APIError
	_ = json.NewDecoder(rr.Body).Decode(&apiErr)
	if apiErr.Code != "CONTAINER_NOT_READY" {
		t.Errorf("expected CONTAINER_NOT_READY, got %s", apiErr.Code)
	}
}

func TestValidate_RateLimited(t *testing.T) {
	t.Parallel()

	runner := &mockValidatorRunner{result: &api.ValidatorResult{AllPassed: true}}
	router, store := setupValidationRouter(t, runner)

	sub := &models.Submission{
		ID:          "test-sub-ratelimit",
		TeamName:    "alpha",
		Status:      models.StatusPending,
		ExposedPort: 8080,
	}
	_ = store.Save(sub)

	// First request should succeed
	req := httptest.NewRequest(http.MethodPost, "/api/v1/submissions/test-sub-ratelimit/validate", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("first request expected 200, got %d", rr.Code)
	}

	// Second request within 2 minutes should fail with 429
	req2 := httptest.NewRequest(http.MethodPost, "/api/v1/submissions/test-sub-ratelimit/validate", nil)
	rr2 := httptest.NewRecorder()
	router.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request expected 429, got %d", rr2.Code)
	}
	var apiErr models.APIError
	_ = json.NewDecoder(rr2.Body).Decode(&apiErr)
	if apiErr.Code != "TOO_MANY_REQUESTS" {
		t.Errorf("expected TOO_MANY_REQUESTS, got %s", apiErr.Code)
	}
}
