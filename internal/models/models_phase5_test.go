package models_test

// models_phase5_test.go tests the Phase 5 additions to models:
//   - SubmissionStatus.IsTerminal
//   - Submission.ResultByLabel
//   - Contest.ProfileByLabel
//   - Sentinel error values
//
// No infrastructure required — all tests are pure in-memory.

import (
	"errors"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
)

// ── SubmissionStatus.IsTerminal ───────────────────────────────────────────────

func TestSubmissionStatus_IsTerminal(t *testing.T) {
	t.Parallel()

	terminal := []models.SubmissionStatus{
		models.StatusCompleted,
		models.StatusFailed,
	}
	nonTerminal := []models.SubmissionStatus{
		models.StatusPending,
		models.StatusBuilding,
		models.StatusDeploying,
		models.StatusRunning,
		models.StatusBenchmarking,
	}

	for _, s := range terminal {
		if !s.IsTerminal() {
			t.Errorf("IsTerminal(%q) = false, want true", s)
		}
	}
	for _, s := range nonTerminal {
		if s.IsTerminal() {
			t.Errorf("IsTerminal(%q) = true, want false", s)
		}
	}
}

// ── Submission.ResultByLabel ──────────────────────────────────────────────────

func TestSubmission_ResultByLabel_ReturnsCorrectResult(t *testing.T) {
	t.Parallel()

	low := &models.BenchmarkResults{VolatilityLabel: "low", P99LatencyMs: 1.0}
	med := &models.BenchmarkResults{VolatilityLabel: "medium", P99LatencyMs: 2.0}
	high := &models.BenchmarkResults{VolatilityLabel: "high", P99LatencyMs: 3.0}

	sub := &models.Submission{
		ID:         "sub-label-test",
		AllResults: []*models.BenchmarkResults{low, med, high},
	}

	if got := sub.ResultByLabel("low"); got != low {
		t.Errorf("ResultByLabel(low) returned wrong pointer")
	}
	if got := sub.ResultByLabel("medium"); got != med {
		t.Errorf("ResultByLabel(medium) returned wrong pointer")
	}
	if got := sub.ResultByLabel("high"); got != high {
		t.Errorf("ResultByLabel(high) returned wrong pointer")
	}
}

func TestSubmission_ResultByLabel_MissingLabel(t *testing.T) {
	t.Parallel()

	sub := &models.Submission{
		ID:         "sub-missing-label",
		AllResults: []*models.BenchmarkResults{
			{VolatilityLabel: "low"},
		},
	}

	if got := sub.ResultByLabel("medium"); got != nil {
		t.Errorf("ResultByLabel(medium) = %v, want nil for missing label", got)
	}
	if got := sub.ResultByLabel(""); got != nil {
		t.Errorf("ResultByLabel('') = %v, want nil", got)
	}
}

func TestSubmission_ResultByLabel_NilAllResults(t *testing.T) {
	t.Parallel()

	sub := &models.Submission{ID: "sub-nil-results", AllResults: nil}

	if got := sub.ResultByLabel("low"); got != nil {
		t.Errorf("ResultByLabel on nil AllResults = %v, want nil", got)
	}
}

// ── Contest.ProfileByLabel ────────────────────────────────────────────────────

func TestContest_ProfileByLabel(t *testing.T) {
	t.Parallel()

	c := &models.Contest{
		ID:            "contest-test",
		Name:          "Test Contest",
		Status:        models.ContestStatusActive,
		LowProfile:    models.VolatilityProfile{Label: "low", BotCount: 10},
		MediumProfile: models.VolatilityProfile{Label: "medium", BotCount: 100},
		HighProfile:   models.VolatilityProfile{Label: "high", BotCount: 1000},
		LowWeight:     0.20,
		MediumWeight:  0.35,
		HighWeight:    0.45,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}

	for _, tc := range []struct {
		label    string
		wantBot  int
		wantFound bool
	}{
		{"low", 10, true},
		{"medium", 100, true},
		{"high", 1000, true},
		{"unknown", 0, false},
		{"", 0, false},
	} {
		tc := tc
		t.Run(tc.label, func(t *testing.T) {
			t.Parallel()
			profile, found := c.ProfileByLabel(tc.label)
			if found != tc.wantFound {
				t.Errorf("ProfileByLabel(%q) found=%v, want %v", tc.label, found, tc.wantFound)
			}
			if found && profile.BotCount != tc.wantBot {
				t.Errorf("ProfileByLabel(%q).BotCount = %d, want %d",
					tc.label, profile.BotCount, tc.wantBot)
			}
		})
	}
}

// ── Sentinel errors ───────────────────────────────────────────────────────────

func TestSentinelErrors_AreDistinct(t *testing.T) {
	t.Parallel()

	errs := []error{
		models.ErrNoActiveContest,
		models.ErrSubmissionInProgress,
		models.ErrContestNotActive,
	}

	for i, a := range errs {
		for j, b := range errs {
			if i == j {
				continue
			}
			if errors.Is(a, b) {
				t.Errorf("errors.Is(%v, %v) = true, sentinel errors must be distinct", a, b)
			}
		}
	}
}

func TestSentinelErrors_UseErrorsIs(t *testing.T) {
	t.Parallel()

	// Ensure callers can use errors.Is for precise matching.
	if !errors.Is(models.ErrNoActiveContest, models.ErrNoActiveContest) {
		t.Error("errors.Is(ErrNoActiveContest, ErrNoActiveContest) = false")
	}
	if !errors.Is(models.ErrSubmissionInProgress, models.ErrSubmissionInProgress) {
		t.Error("errors.Is(ErrSubmissionInProgress, ErrSubmissionInProgress) = false")
	}
	if !errors.Is(models.ErrContestNotActive, models.ErrContestNotActive) {
		t.Error("errors.Is(ErrContestNotActive, ErrContestNotActive) = false")
	}
}

// ── ContestStatus ─────────────────────────────────────────────────────────────

func TestContestStatus_Values(t *testing.T) {
	t.Parallel()

	// Verify the string values match the spec — these are used in DB queries
	// and JSON, so they must be stable.
	if string(models.ContestStatusDraft) != "draft" {
		t.Errorf("ContestStatusDraft = %q, want %q", models.ContestStatusDraft, "draft")
	}
	if string(models.ContestStatusActive) != "active" {
		t.Errorf("ContestStatusActive = %q, want %q", models.ContestStatusActive, "active")
	}
	if string(models.ContestStatusClosed) != "closed" {
		t.Errorf("ContestStatusClosed = %q, want %q", models.ContestStatusClosed, "closed")
	}
}

// ── VolatilityProfile ─────────────────────────────────────────────────────────

func TestVolatilityProfile_ZeroValue(t *testing.T) {
	t.Parallel()
	// Zero value must be a valid struct (no panics on field access).
	var vp models.VolatilityProfile
	if vp.Label != "" {
		t.Errorf("zero VolatilityProfile.Label = %q, want empty", vp.Label)
	}
	if vp.BotCount != 0 {
		t.Errorf("zero VolatilityProfile.BotCount = %d, want 0", vp.BotCount)
	}
}
