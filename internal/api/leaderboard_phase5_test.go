package api_test

// leaderboard_phase5_test.go tests Stage 5.2 sub-task 5.2.5:
// leaderboard deduplication — one row per team, best score wins.
//
// Tests:
//   TestLeaderboard_DeduplicatesPerTeam     — multi-submission teams → one row each
//   TestLeaderboard_BestScoreWins           — lower-score sub is dropped, higher kept
//   TestLeaderboard_Phase4Compat_NoContest  — Phase 1–4 CompositeScore path still works
//   TestLeaderboard_EmptyWhenNoCompleted    — no completed subs → empty entries array

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/api"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// seedSubmission writes a completed submission meta.json directly into the
// DiskStore directory. This lets us inject submissions with controlled score
// values without running a Docker sandbox or bot fleet.
func seedSubmission(t *testing.T, dir string, sub *models.Submission) {
	t.Helper()
	subDir := filepath.Join(dir, sub.ID)
	if err := os.MkdirAll(subDir, 0o750); err != nil {
		t.Fatalf("seedSubmission: mkdir: %v", err)
	}
	f, err := os.Create(filepath.Join(subDir, "meta.json"))
	if err != nil {
		t.Fatalf("seedSubmission: create: %v", err)
	}
	defer func() { _ = f.Close() }()
	if err := json.NewEncoder(f).Encode(sub); err != nil {
		t.Fatalf("seedSubmission: encode: %v", err)
	}
}

func newLeaderboardRouter(t *testing.T) (http.Handler, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := &config.Config{
		AdminAPIKey:    testAdminKey,
		SubmissionDir:  dir,
		MaxUploadBytes: 64 << 20,
	}
	store := submission.NewDiskStore(dir)
	svc := submission.NewService(store, nil, cfg)
	contestStore := contest.NewMemoryContestStore()
	contestSvc := contest.NewContestService(contestStore, nil)
	router := api.NewRouter(svc, cfg, metrics.New(), nil, contestSvc)
	return router, dir
}

func getLeaderboard(t *testing.T, router http.Handler) []models.LeaderboardEntry {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/leaderboard", nil)
	rr := httptest.NewRecorder()
	router.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /api/v1/leaderboard = %d, body: %s", rr.Code, rr.Body.String())
	}
	var resp struct {
		Count   int                       `json:"count"`
		Entries []models.LeaderboardEntry `json:"entries"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode leaderboard: %v", err)
	}
	return resp.Entries
}

// ── TestLeaderboard_DeduplicatesPerTeam ──────────────────────────────────────

func TestLeaderboard_DeduplicatesPerTeam(t *testing.T) {
	t.Parallel()
	router, dir := newLeaderboardRouter(t)
	now := time.Now().UTC()

	// alpha submits 3 times (scores: 80, 60, 40) — should appear once at 80.
	for i, score := range []float64{80, 60, 40} {
		sub := &models.Submission{
			ID:       fmt.Sprintf("alpha-%d", i),
			TeamName: "alpha",
			Language: models.LangGo,
			Protocol: models.ProtocolREST,
			Status:   models.StatusCompleted,
			Results: &models.BenchmarkResults{
				CompositeScore: score,
			},
			CreatedAt:   now,
			UpdatedAt:   now,
			CompletedAt: &now,
		}
		seedSubmission(t, dir, sub)
	}

	// beta submits once (score: 70) — rank 2 behind alpha's best of 80.
	betaSub := &models.Submission{
		ID:          "beta-0",
		TeamName:    "beta",
		Language:    models.LangRust,
		Protocol:    models.ProtocolREST,
		Status:      models.StatusCompleted,
		Results:     &models.BenchmarkResults{CompositeScore: 70},
		CreatedAt:   now,
		UpdatedAt:   now,
		CompletedAt: &now,
	}
	seedSubmission(t, dir, betaSub)

	entries := getLeaderboard(t, router)

	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2 (one per team)", len(entries))
	}
	if entries[0].TeamName != "alpha" {
		t.Errorf("rank 1 team = %q, want alpha", entries[0].TeamName)
	}
	if entries[0].Rank != 1 {
		t.Errorf("rank-1 entry Rank = %d, want 1", entries[0].Rank)
	}
	if entries[1].TeamName != "beta" {
		t.Errorf("rank 2 team = %q, want beta", entries[1].TeamName)
	}
	if entries[1].Rank != 2 {
		t.Errorf("rank-2 entry Rank = %d, want 2", entries[1].Rank)
	}
}

// ── TestLeaderboard_BestScoreWins ─────────────────────────────────────────────

func TestLeaderboard_BestScoreWins(t *testing.T) {
	t.Parallel()
	router, dir := newLeaderboardRouter(t)
	now := time.Now().UTC()

	// gamma submits twice: first poor (30), then improved (95).
	for i, score := range []float64{30, 95} {
		sub := &models.Submission{
			ID:          fmt.Sprintf("gamma-%d", i),
			TeamName:    "gamma",
			Language:    models.LangGo,
			Protocol:    models.ProtocolREST,
			Status:      models.StatusCompleted,
			Results:     &models.BenchmarkResults{CompositeScore: score},
			CreatedAt:   now,
			UpdatedAt:   now,
			CompletedAt: &now,
		}
		seedSubmission(t, dir, sub)
	}

	entries := getLeaderboard(t, router)

	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	// The leaderboard must show 95, not 30.
	if entries[0].CompositeScore != 95 {
		t.Errorf("CompositeScore = %.1f, want 95 (best score wins)", entries[0].CompositeScore)
	}
}

// ── TestLeaderboard_Phase4Compat_NoContest ────────────────────────────────────

// Verifies that Phase 1–4 submissions (Results.CompositeScore, no FinalScore,
// no ContestID) still appear on the leaderboard with all legacy columns intact.
func TestLeaderboard_Phase4Compat_NoContest(t *testing.T) {
	t.Parallel()
	router, dir := newLeaderboardRouter(t)
	now := time.Now().UTC()

	sub := &models.Submission{
		ID:       "legacy-sub-1",
		TeamName: "legacy-team",
		Language: models.LangCpp,
		Protocol: models.ProtocolREST,
		Status:   models.StatusCompleted,
		Results: &models.BenchmarkResults{
			CompositeScore:   55.0,
			P99LatencyMs:     2.5,
			MaxTPS:           12000,
			CorrectnessScore: 0.98,
		},
		CreatedAt:   now,
		UpdatedAt:   now,
		CompletedAt: &now,
	}
	seedSubmission(t, dir, sub)

	entries := getLeaderboard(t, router)

	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	e := entries[0]
	if e.TeamName != "legacy-team" {
		t.Errorf("TeamName = %q, want legacy-team", e.TeamName)
	}
	if e.CompositeScore != 55.0 {
		t.Errorf("CompositeScore = %.1f, want 55.0", e.CompositeScore)
	}
	if e.P99LatencyMs != 2.5 {
		t.Errorf("P99LatencyMs = %.2f, want 2.5", e.P99LatencyMs)
	}
	if e.MaxTPS != 12000 {
		t.Errorf("MaxTPS = %.0f, want 12000", e.MaxTPS)
	}
	if e.CorrectnessScore != 0.98 {
		t.Errorf("CorrectnessScore = %.2f, want 0.98", e.CorrectnessScore)
	}
}

// ── TestLeaderboard_EmptyWhenNoCompleted ──────────────────────────────────────

func TestLeaderboard_EmptyWhenNoCompleted(t *testing.T) {
	t.Parallel()
	router, dir := newLeaderboardRouter(t)
	now := time.Now().UTC()

	for _, status := range []models.SubmissionStatus{
		models.StatusPending,
		models.StatusRunning,
		models.StatusBenchmarking,
	} {
		sub := &models.Submission{
			ID:        string(status) + "-sub",
			TeamName:  "invisible-team",
			Language:  models.LangGo,
			Protocol:  models.ProtocolREST,
			Status:    status,
			CreatedAt: now,
			UpdatedAt: now,
		}
		seedSubmission(t, dir, sub)
	}

	entries := getLeaderboard(t, router)

	if len(entries) != 0 {
		t.Errorf("len(entries) = %d, want 0", len(entries))
	}
}
