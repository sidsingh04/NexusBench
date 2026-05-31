package api_test

// router_phase5_test.go tests the Stage 5.2 additions to the HTTP router:
//
//  1. TestAdminMiddleware_RejectsWrongKey   — wrong Bearer → 401
//  2. TestAdminMiddleware_RejectsMissing    — no header → 401
//  3. TestAdminMiddleware_AcceptsCorrectKey — correct Bearer → handler runs
//  4. TestAdminRoutes_NotMountedWithoutKey  — nil/empty AdminAPIKey → 404
//  5. TestCreateContest_Returns201          — full round-trip: create → 201
//  6. TestActivateContest_Returns200        — create → activate → 200
//  7. TestActivate_AlreadyActive_Returns409 — activate twice → 409
//  8. TestCloseContest_Returns200           — create → activate → close → 200

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

const testAdminKey = "nexusbench-test-key-2026"

// newTestRouter builds a router wired with a real ContestService backed by
// MemoryContestStore. No sandbox, no queue, no orchestrator.
func newTestRouter(t *testing.T) http.Handler {
	t.Helper()

	cfg := &config.Config{
		AdminAPIKey:    testAdminKey,
		SubmissionDir:  t.TempDir(),
		MaxUploadBytes: 64 << 20,
	}

	// Submission service with no Docker manager (local mode, nil dockerMgr).
	store := submission.NewDiskStore(cfg.SubmissionDir)
	svc := submission.NewService(store, nil, cfg)
	reg := metrics.New()

	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil)

	return api.NewRouter(svc, cfg, reg, nil, contestSvc)
}

// newTestRouterNoAdminKey builds a router with no admin key → routes not mounted.
func newTestRouterNoAdminKey(t *testing.T) http.Handler {
	t.Helper()
	cfg := &config.Config{
		AdminAPIKey:    "", // no key → routes not mounted
		SubmissionDir:  t.TempDir(),
		MaxUploadBytes: 64 << 20,
	}
	store := submission.NewDiskStore(cfg.SubmissionDir)
	svc := submission.NewService(store, nil, cfg)
	reg := metrics.New()
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil)
	return api.NewRouter(svc, cfg, reg, nil, contestSvc)
}

func adminDo(t *testing.T, router http.Handler, method, path, key string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var req *http.Request
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		req = httptest.NewRequest(method, path, bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	return rr
}

// createContestViaAPI creates a draft contest and returns its ID.
func createContestViaAPI(t *testing.T, router http.Handler) string {
	t.Helper()
	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", testAdminKey,
		contest.CreateContestRequest{Name: "test-contest", UseDefaults: true})
	if rr.Code != http.StatusCreated {
		t.Fatalf("createContest: got %d, want 201. body: %s", rr.Code, rr.Body.String())
	}
	var c models.Contest
	if err := json.NewDecoder(rr.Body).Decode(&c); err != nil {
		t.Fatalf("decode contest: %v", err)
	}
	return c.ID
}

// ── 1. TestAdminMiddleware_RejectsWrongKey ────────────────────────────────────

func TestAdminMiddleware_RejectsWrongKey(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", "wrong-key",
		contest.CreateContestRequest{Name: "x", UseDefaults: true})

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}

	var apiErr models.APIError
	_ = json.NewDecoder(rr.Body).Decode(&apiErr)
	if apiErr.Code != "UNAUTHORIZED" {
		t.Errorf("code = %q, want UNAUTHORIZED", apiErr.Code)
	}
}

// ── 2. TestAdminMiddleware_RejectsMissing ─────────────────────────────────────

func TestAdminMiddleware_RejectsMissing(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", "", /* no key */
		contest.CreateContestRequest{Name: "x", UseDefaults: true})

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", rr.Code)
	}
}

// ── 3. TestAdminMiddleware_AcceptsCorrectKey ──────────────────────────────────

func TestAdminMiddleware_AcceptsCorrectKey(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", testAdminKey,
		contest.CreateContestRequest{Name: "valid-contest", UseDefaults: true})

	// Correct key → must not be 401.
	if rr.Code == http.StatusUnauthorized {
		t.Errorf("got 401 with correct key — middleware wrongly rejected it")
	}
}

// ── 4. TestAdminRoutes_NotMountedWithoutKey ───────────────────────────────────

func TestAdminRoutes_NotMountedWithoutKey(t *testing.T) {
	t.Parallel()
	router := newTestRouterNoAdminKey(t)

	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", testAdminKey,
		contest.CreateContestRequest{Name: "x", UseDefaults: true})

	// Routes not mounted → gorilla/mux returns 404 or 405.
	if rr.Code == http.StatusCreated {
		t.Errorf("routes should not be mounted when AdminAPIKey is empty, got 201")
	}
}

// ── 5. TestCreateContest_Returns201 ──────────────────────────────────────────

func TestCreateContest_Returns201(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	rr := adminDo(t, router, http.MethodPost, "/api/v1/admin/contests", testAdminKey,
		contest.CreateContestRequest{Name: "IICPC 2026", UseDefaults: true})

	if rr.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201. body: %s", rr.Code, rr.Body.String())
	}

	var c models.Contest
	if err := json.NewDecoder(rr.Body).Decode(&c); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if c.ID == "" {
		t.Error("response.id should be non-empty")
	}
	if c.Status != models.ContestStatusDraft {
		t.Errorf("status = %q, want %q", c.Status, models.ContestStatusDraft)
	}
	if c.Name != "IICPC 2026" {
		t.Errorf("name = %q, want %q", c.Name, "IICPC 2026")
	}
}

// ── 6. TestActivateContest_Returns200 ────────────────────────────────────────

func TestActivateContest_Returns200(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)
	id := createContestViaAPI(t, router)

	rr := adminDo(t, router, http.MethodPost,
		"/api/v1/admin/contests/"+id+"/activate", testAdminKey, nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("activate status = %d, want 200. body: %s", rr.Code, rr.Body.String())
	}

	var c models.Contest
	if err := json.NewDecoder(rr.Body).Decode(&c); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if c.Status != models.ContestStatusActive {
		t.Errorf("status = %q, want %q", c.Status, models.ContestStatusActive)
	}
}

// ── 7. TestActivate_AlreadyActive_Returns409 ──────────────────────────────────

func TestActivate_AlreadyActive_Returns409(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	first := createContestViaAPI(t, router)
	adminDo(t, router, http.MethodPost,
		"/api/v1/admin/contests/"+first+"/activate", testAdminKey, nil)

	second := createContestViaAPI(t, router)
	rr := adminDo(t, router, http.MethodPost,
		"/api/v1/admin/contests/"+second+"/activate", testAdminKey, nil)

	if rr.Code != http.StatusConflict {
		t.Errorf("second activate = %d, want 409", rr.Code)
	}
	var apiErr models.APIError
	_ = json.NewDecoder(rr.Body).Decode(&apiErr)
	if apiErr.Code != "CONTEST_ALREADY_ACTIVE" {
		t.Errorf("code = %q, want CONTEST_ALREADY_ACTIVE", apiErr.Code)
	}
}

// ── 8. TestCloseContest_Returns200 ───────────────────────────────────────────

func TestCloseContest_Returns200(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)
	id := createContestViaAPI(t, router)

	adminDo(t, router, http.MethodPost,
		"/api/v1/admin/contests/"+id+"/activate", testAdminKey, nil)

	rr := adminDo(t, router, http.MethodPost,
		"/api/v1/admin/contests/"+id+"/close", testAdminKey, nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("close status = %d, want 200. body: %s", rr.Code, rr.Body.String())
	}
}

// ── 9. TestListContests_ReturnsAll ────────────────────────────────────────────

func TestListContests_ReturnsAll(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	createContestViaAPI(t, router)
	createContestViaAPI(t, router)

	rr := adminDo(t, router, http.MethodGet, "/api/v1/admin/contests", testAdminKey, nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("list status = %d, want 200", rr.Code)
	}

	var resp struct {
		Count    int              `json:"count"`
		Contests []models.Contest `json:"contests"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 2 {
		t.Errorf("count = %d, want 2", resp.Count)
	}
}

// ── 10. TestExistingRoutes_StillWork ─────────────────────────────────────────

// Regression test: the Phase 1-4 routes must still respond correctly after
// the admin subrouter is added.
func TestExistingRoutes_StillWork(t *testing.T) {
	t.Parallel()
	router := newTestRouter(t)

	// GET /health should still return 200.
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET /health = %d, want 200", rr.Code)
	}

	// GET /api/v1/leaderboard should still return 200.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/leaderboard", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET /api/v1/leaderboard = %d, want 200", rr.Code)
	}

	// GET /api/v1/submissions should still return 200.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/submissions", nil)
	rr = httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("GET /api/v1/submissions = %d, want 200", rr.Code)
	}
}

// ── 11. TestGetContestLeaderboard_ReturnsSnapshot ─────────────────────────────

func TestGetContestLeaderboard_ReturnsSnapshot(t *testing.T) {
	t.Parallel()

	// Wire directly against the service so we can inject a snapshot without
	// running a real benchmark.
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil)

	ctx := context.Background()
	c, _ := contestSvc.Create(ctx, contest.CreateContestRequest{Name: "snap-test", UseDefaults: true})
	contestSvc.Activate(ctx, c.ID) //nolint:errcheck
	entries := []*models.LeaderboardEntry{
		{SubmissionID: "s1", TeamName: "team-alpha", FinalScore: 88.0},
	}
	contestSvc.Close(ctx, c.ID, entries) //nolint:errcheck

	cfg := &config.Config{
		AdminAPIKey:    testAdminKey,
		SubmissionDir:  t.TempDir(),
		MaxUploadBytes: 64 << 20,
	}
	store := submission.NewDiskStore(cfg.SubmissionDir)
	svc := submission.NewService(store, nil, cfg)
	router := api.NewRouter(svc, cfg, metrics.New(), nil, contestSvc)

	rr := adminDo(t, router, http.MethodGet,
		"/api/v1/admin/contests/"+c.ID+"/leaderboard", testAdminKey, nil)

	if rr.Code != http.StatusOK {
		t.Fatalf("leaderboard status = %d, want 200. body: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Count   int                        `json:"count"`
		Entries []*models.LeaderboardEntry `json:"entries"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 {
		t.Errorf("count = %d, want 1", resp.Count)
	}
	if resp.Entries[0].TeamName != "team-alpha" {
		t.Errorf("entry team = %q, want team-alpha", resp.Entries[0].TeamName)
	}
}
