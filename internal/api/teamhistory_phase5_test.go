package api_test

// teamhistory_phase5_test.go tests Stage 5.2 sub-task 5.2.6:
// GET /api/v1/teams/{name}/submissions — team history view.
//
// Tests:
//   TestTeamHistory_ReturnsAllSubmissions          — only queried team's subs returned
//   TestTeamHistory_EmptyForUnknownTeam            — unknown team → 200 with count=0
//   TestTeamHistory_AllStatusesIncluded            — all statuses appear (not just completed)
//   TestTeamHistory_RouteNotMountedWithoutContest  — nil contestSvc → route absent

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// newTeamHistoryRouter reuses newLeaderboardRouter — both wire contestSvc in.
func newTeamHistoryRouter(t *testing.T) (http.Handler, string) {
	t.Helper()
	return newLeaderboardRouter(t)
}

// newTeamHistoryRouterNoContest builds a router with contestSvc=nil
// so the team history route is NOT mounted.
func newTeamHistoryRouterNoContest(t *testing.T) http.Handler {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		AdminAPIKey:    testAdminKey,
		SubmissionDir:  dir,
		MaxUploadBytes: 64 << 20,
	}
	store := submission.NewDiskStore(dir)
	svc := submission.NewService(store, nil, cfg)
	return api.NewRouter(svc, cfg, metrics.New(), nil, nil, nil) // nil contestSvc, nil validator
}

func getTeamHistory(t *testing.T, router http.Handler, teamName string) (int, []models.Submission) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/teams/"+teamName+"/submissions", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/teams/%s/submissions = %d, body: %s",
			teamName, rr.Code, rr.Body.String())
	}
	var resp struct {
		TeamName    string              `json:"team_name"`
		Count       int                 `json:"count"`
		Submissions []models.Submission `json:"submissions"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode team history: %v", err)
	}
	return resp.Count, resp.Submissions
}

// ── TestTeamHistory_ReturnsAllSubmissions ─────────────────────────────────────

func TestTeamHistory_ReturnsAllSubmissions(t *testing.T) {
	t.Parallel()
	router, dir := newTeamHistoryRouter(t)
	now := time.Now().UTC()

	// delta has 2 submissions; epsilon has 1.
	for i := range 2 {
		seedSubmission(t, dir, &models.Submission{
			ID:        fmt.Sprintf("delta-%d", i),
			TeamName:  "delta",
			Language:  models.LangGo,
			Protocol:  models.ProtocolREST,
			Status:    models.StatusCompleted,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}
	seedSubmission(t, dir, &models.Submission{
		ID:        "epsilon-0",
		TeamName:  "epsilon",
		Language:  models.LangRust,
		Protocol:  models.ProtocolREST,
		Status:    models.StatusCompleted,
		CreatedAt: now,
		UpdatedAt: now,
	})

	count, subs := getTeamHistory(t, router, "delta")

	if count != 2 {
		t.Errorf("count = %d, want 2", count)
	}
	if len(subs) != 2 {
		t.Errorf("len(subs) = %d, want 2", len(subs))
	}
	for _, s := range subs {
		if s.TeamName != "delta" {
			t.Errorf("submission %s has TeamName=%q, want delta", s.ID, s.TeamName)
		}
	}
}

// ── TestTeamHistory_EmptyForUnknownTeam ──────────────────────────────────────

func TestTeamHistory_EmptyForUnknownTeam(t *testing.T) {
	t.Parallel()
	router, _ := newTeamHistoryRouter(t)

	count, subs := getTeamHistory(t, router, "no-such-team")

	if count != 0 {
		t.Errorf("count = %d, want 0 for unknown team", count)
	}
	if len(subs) != 0 {
		t.Errorf("len(subs) = %d, want 0", len(subs))
	}
}

// ── TestTeamHistory_AllStatusesIncluded ──────────────────────────────────────

// Team history must return ALL submissions regardless of status —
// a team should see failed and pending runs alongside completed ones.
func TestTeamHistory_AllStatusesIncluded(t *testing.T) {
	t.Parallel()
	router, dir := newTeamHistoryRouter(t)
	now := time.Now().UTC()

	statuses := []models.SubmissionStatus{
		models.StatusPending,
		models.StatusRunning,
		models.StatusCompleted,
		models.StatusFailed,
	}
	for i, status := range statuses {
		seedSubmission(t, dir, &models.Submission{
			ID:        fmt.Sprintf("zeta-%d", i),
			TeamName:  "zeta",
			Language:  models.LangGo,
			Protocol:  models.ProtocolREST,
			Status:    status,
			CreatedAt: now,
			UpdatedAt: now,
		})
	}

	count, _ := getTeamHistory(t, router, "zeta")

	if count != len(statuses) {
		t.Errorf("count = %d, want %d (all statuses)", count, len(statuses))
	}
}

// ── TestTeamHistory_RouteNotMountedWithoutContest ─────────────────────────────

func TestTeamHistory_RouteNotMountedWithoutContest(t *testing.T) {
	t.Parallel()
	router := newTeamHistoryRouterNoContest(t)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/teams/alpha/submissions", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)

	if rr.Code == http.StatusOK {
		t.Errorf("team history route returned 200 with nil contestSvc — must not be mounted")
	}
}
