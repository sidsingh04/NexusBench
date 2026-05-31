package contest_test

// service_test.go covers the 8 unit tests required by Stage 5.2.
//
// All tests use MemoryContestStore — no database, no Docker, no I/O.
// Tests run with -race: all MemoryContestStore methods hold RWMutex correctly
// so the race detector should stay silent.
//
// Test index:
//   1. TestCreate_Draft
//   2. TestActivate_Transitions
//   3. TestActivate_RejectsIfAlreadyActive
//   4. TestClose_Snapshots
//   5. TestClose_Idempotent
//   6. TestGetActive_ErrWhenNone
//   7. TestGetActive_ErrAfterClose
//   8. TestCreate_UseDefaults_PopulatesProfiles

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/models"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newSvc() *contest.ContestService {
	store := contest.NewMemoryContestStore()
	return contest.NewContestService(store, nil) // nil bus = no SSE in tests
}

func mustCreate(t *testing.T, svc *contest.ContestService, name string) *models.Contest {
	t.Helper()
	c, err := svc.Create(context.Background(), contest.CreateContestRequest{
		Name:        name,
		UseDefaults: true,
	})
	if err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	return c
}

// ── 1. TestCreate_Draft ────────────────────────────────────────────────────────

func TestCreate_Draft(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	c, err := svc.Create(context.Background(), contest.CreateContestRequest{
		Name:        "IICPC 2026",
		UseDefaults: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if c.ID == "" {
		t.Error("ID should be non-empty UUID")
	}
	if c.Status != models.ContestStatusDraft {
		t.Errorf("Status = %q, want %q", c.Status, models.ContestStatusDraft)
	}
	if c.Name != "IICPC 2026" {
		t.Errorf("Name = %q, want %q", c.Name, "IICPC 2026")
	}
	if c.LowProfile.BotCount == 0 {
		t.Error("LowProfile.BotCount should be non-zero when use_defaults=true")
	}
	if c.MediumProfile.BotCount == 0 {
		t.Error("MediumProfile.BotCount should be non-zero when use_defaults=true")
	}
	if c.HighProfile.BotCount == 0 {
		t.Error("HighProfile.BotCount should be non-zero when use_defaults=true")
	}
	if c.LowWeight == 0 || c.MediumWeight == 0 || c.HighWeight == 0 {
		t.Error("aggregate weights should be non-zero when use_defaults=true")
	}
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

// ── 2. TestActivate_Transitions ───────────────────────────────────────────────

func TestActivate_Transitions(t *testing.T) {
	t.Parallel()
	svc := newSvc()
	c := mustCreate(t, svc, "activate-test")

	active, err := svc.Activate(context.Background(), c.ID)
	if err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if active.Status != models.ContestStatusActive {
		t.Errorf("Status = %q, want %q", active.Status, models.ContestStatusActive)
	}

	// GetActive should now return this contest.
	got, err := svc.GetActive(context.Background())
	if err != nil {
		t.Fatalf("GetActive: %v", err)
	}
	if got.ID != c.ID {
		t.Errorf("GetActive().ID = %q, want %q", got.ID, c.ID)
	}
}

// ── 3. TestActivate_RejectsIfAlreadyActive ────────────────────────────────────

func TestActivate_RejectsIfAlreadyActive(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	first := mustCreate(t, svc, "first-contest")
	if _, err := svc.Activate(context.Background(), first.ID); err != nil {
		t.Fatalf("Activate first: %v", err)
	}

	second := mustCreate(t, svc, "second-contest")
	_, err := svc.Activate(context.Background(), second.ID)
	if err == nil {
		t.Fatal("expected error activating second contest while first is active, got nil")
	}
	if !errors.Is(err, contest.ErrAlreadyActive) {
		t.Errorf("error = %v, want ErrAlreadyActive", err)
	}
}

// ── 4. TestClose_Snapshots ────────────────────────────────────────────────────

func TestClose_Snapshots(t *testing.T) {
	t.Parallel()
	svc := newSvc()
	c := mustCreate(t, svc, "close-snapshot-test")
	if _, err := svc.Activate(context.Background(), c.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	entries := []*models.LeaderboardEntry{
		{SubmissionID: "sub-1", TeamName: "alpha", FinalScore: 90.0},
		{SubmissionID: "sub-2", TeamName: "beta", FinalScore: 75.0},
	}

	if err := svc.Close(context.Background(), c.ID, entries); err != nil {
		t.Fatalf("Close: %v", err)
	}

	snapshot, err := svc.GetLeaderboardSnapshot(context.Background(), c.ID)
	if err != nil {
		t.Fatalf("GetLeaderboardSnapshot: %v", err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("snapshot len = %d, want 2", len(snapshot))
	}
	// rankEntries sorts descending — alpha (90) should be rank 1.
	if snapshot[0].Rank != 1 || snapshot[0].TeamName != "alpha" {
		t.Errorf("snapshot[0] = {Rank:%d Team:%s}, want {Rank:1 Team:alpha}",
			snapshot[0].Rank, snapshot[0].TeamName)
	}
	if snapshot[1].Rank != 2 || snapshot[1].TeamName != "beta" {
		t.Errorf("snapshot[1] = {Rank:%d Team:%s}, want {Rank:2 Team:beta}",
			snapshot[1].Rank, snapshot[1].TeamName)
	}
}

// ── 5. TestClose_Idempotent ───────────────────────────────────────────────────

func TestClose_Idempotent(t *testing.T) {
	t.Parallel()
	svc := newSvc()
	c := mustCreate(t, svc, "idempotent-close")
	if _, err := svc.Activate(context.Background(), c.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	entries := []*models.LeaderboardEntry{
		{SubmissionID: "sub-1", TeamName: "gamma", FinalScore: 50.0},
	}

	// First close.
	if err := svc.Close(context.Background(), c.ID, entries); err != nil {
		t.Fatalf("Close (first): %v", err)
	}

	// Second close must be a no-op and return nil.
	if err := svc.Close(context.Background(), c.ID, entries); err != nil {
		t.Errorf("Close (second, idempotent): %v — want nil", err)
	}
}

// ── 6. TestGetActive_ErrWhenNone ──────────────────────────────────────────────

func TestGetActive_ErrWhenNone(t *testing.T) {
	t.Parallel()
	svc := newSvc() // fresh store with no contests

	_, err := svc.GetActive(context.Background())
	if err == nil {
		t.Fatal("expected ErrNoActiveContest, got nil")
	}
	if !errors.Is(err, models.ErrNoActiveContest) {
		t.Errorf("error = %v, want models.ErrNoActiveContest", err)
	}
}

// ── 7. TestGetActive_ErrAfterClose ───────────────────────────────────────────

func TestGetActive_ErrAfterClose(t *testing.T) {
	t.Parallel()
	svc := newSvc()
	c := mustCreate(t, svc, "close-then-none")
	if _, err := svc.Activate(context.Background(), c.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}
	if err := svc.Close(context.Background(), c.ID, nil); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// After closing, no contest is active.
	_, err := svc.GetActive(context.Background())
	if !errors.Is(err, models.ErrNoActiveContest) {
		t.Errorf("error = %v, want models.ErrNoActiveContest after close", err)
	}
}

// ── 8. TestCreate_UseDefaults_PopulatesProfiles ───────────────────────────────

func TestCreate_UseDefaults_PopulatesProfiles(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	c, err := svc.Create(context.Background(), contest.CreateContestRequest{
		Name:        "defaults-check",
		UseDefaults: true,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Verify the defaults match the spec in PROGRESS.md.
	low := contest.DefaultLowProfile()
	if c.LowProfile.BotCount != low.BotCount {
		t.Errorf("LowProfile.BotCount = %d, want %d", c.LowProfile.BotCount, low.BotCount)
	}
	if c.LowProfile.TestDuration != low.TestDuration {
		t.Errorf("LowProfile.TestDuration = %v, want %v", c.LowProfile.TestDuration, low.TestDuration)
	}

	med := contest.DefaultMediumProfile()
	if c.MediumProfile.BotCount != med.BotCount {
		t.Errorf("MediumProfile.BotCount = %d, want %d", c.MediumProfile.BotCount, med.BotCount)
	}

	high := contest.DefaultHighProfile()
	if c.HighProfile.BotCount != high.BotCount {
		t.Errorf("HighProfile.BotCount = %d, want %d", c.HighProfile.BotCount, high.BotCount)
	}

	// Aggregate weights should sum to ≈1.0.
	total := c.LowWeight + c.MediumWeight + c.HighWeight
	if total < 0.999 || total > 1.001 {
		t.Errorf("aggregate weights sum = %.4f, want ≈1.0 (got low=%.2f med=%.2f high=%.2f)",
			total, c.LowWeight, c.MediumWeight, c.HighWeight)
	}

	// Label fields must be set correctly.
	if c.LowProfile.Label != "low" {
		t.Errorf("LowProfile.Label = %q, want %q", c.LowProfile.Label, "low")
	}
	if c.MediumProfile.Label != "medium" {
		t.Errorf("MediumProfile.Label = %q, want %q", c.MediumProfile.Label, "medium")
	}
	if c.HighProfile.Label != "high" {
		t.Errorf("HighProfile.Label = %q, want %q", c.HighProfile.Label, "high")
	}
}

// ── 9. TestCreate_RequiresName ────────────────────────────────────────────────

func TestCreate_RequiresName(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	_, err := svc.Create(context.Background(), contest.CreateContestRequest{
		Name:        "",
		UseDefaults: true,
	})
	if err == nil {
		t.Fatal("expected error for empty name, got nil")
	}
}

// ── 10. TestClose_DraftContest_Rejected ───────────────────────────────────────

func TestClose_DraftContest_Rejected(t *testing.T) {
	t.Parallel()
	svc := newSvc()
	c := mustCreate(t, svc, "never-activated")

	// Closing a draft contest (never activated) must be rejected.
	err := svc.Close(context.Background(), c.ID, nil)
	if err == nil {
		t.Fatal("expected error closing draft contest, got nil")
	}
	if !errors.Is(err, contest.ErrWrongStatus) {
		t.Errorf("error = %v, want ErrWrongStatus", err)
	}
}

// ── 11. TestListPast_OnlyReturnsClosed ────────────────────────────────────────

func TestListPast_OnlyReturnsClosed(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	draft := mustCreate(t, svc, "draft-contest")
	_ = draft

	active := mustCreate(t, svc, "active-contest")
	if _, err := svc.Activate(context.Background(), active.ID); err != nil {
		t.Fatalf("Activate: %v", err)
	}

	closedA := mustCreate(t, svc, "closed-a")
	// Can't activate closedA while active is up; close active first.
	if err := svc.Close(context.Background(), active.ID, nil); err != nil {
		t.Fatalf("Close active: %v", err)
	}
	if _, err := svc.Activate(context.Background(), closedA.ID); err != nil {
		t.Fatalf("Activate closedA: %v", err)
	}
	if err := svc.Close(context.Background(), closedA.ID, nil); err != nil {
		t.Fatalf("Close closedA: %v", err)
	}

	past, err := svc.ListPast(context.Background())
	if err != nil {
		t.Fatalf("ListPast: %v", err)
	}
	if len(past) != 2 {
		t.Errorf("ListPast len = %d, want 2 (active + closedA both closed)", len(past))
	}
	for _, p := range past {
		if p.Status != models.ContestStatusClosed {
			t.Errorf("ListPast returned contest with status %q, want %q", p.Status, models.ContestStatusClosed)
		}
	}
}

// ── 12. TestCreate_EndsAt_StoredCorrectly ─────────────────────────────────────

func TestCreate_EndsAt_StoredCorrectly(t *testing.T) {
	t.Parallel()
	svc := newSvc()

	future := time.Now().Add(24 * time.Hour).UTC().Truncate(time.Second)
	c, err := svc.Create(context.Background(), contest.CreateContestRequest{
		Name:        "timed-contest",
		UseDefaults: true,
		EndsAt:      &future,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if c.EndsAt == nil {
		t.Fatal("EndsAt should be set")
	}
	if !c.EndsAt.Equal(future) {
		t.Errorf("EndsAt = %v, want %v", c.EndsAt, future)
	}
}
