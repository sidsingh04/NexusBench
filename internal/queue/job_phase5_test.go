package queue_test

// job_phase5_test.go tests the Phase 5 additions to the Job type:
// NewProfileJob constructor and IsLastProfile predicate.
//
// Tests:
//   TestNewProfileJob_SetsLabelsCorrectly    — first job has correct label and remaining
//   TestNewProfileJob_LastJob_EmptyRemaining — last job has empty RemainingProfiles
//   TestNewProfileJob_ImmutableRemaining     — mutating caller slice does not affect job
//   TestNewProfileJob_ID_Format             — ID is "<submissionID>/<label>"
//   TestJob_IsLastProfile_Phase4Job          — Phase 1-4 NewJob always returns true
//   TestNewProfileJob_JSONRoundTrip          — Phase 5 fields survive JSON marshal/unmarshal

import (
	"encoding/json"
	"testing"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
)

func profileSub() *models.Submission {
	return &models.Submission{
		ID:          "sub-profile-001",
		TeamName:    "profile-team",
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/sub-profile-001/archive.tar.gz",
		ContestID:   "contest-abc",
	}
}

func TestNewProfileJob_SetsLabelsCorrectly(t *testing.T) {
	t.Parallel()
	sub := profileSub()
	remaining := []string{"medium", "high"}

	j := queue.NewProfileJob(sub, sub.ContestID, "low", remaining)

	if j.VolatilityLabel != "low" {
		t.Errorf("VolatilityLabel = %q, want %q", j.VolatilityLabel, "low")
	}
	if j.ContestID != "contest-abc" {
		t.Errorf("ContestID = %q, want %q", j.ContestID, "contest-abc")
	}
	if len(j.RemainingProfiles) != 2 {
		t.Fatalf("len(RemainingProfiles) = %d, want 2", len(j.RemainingProfiles))
	}
	if j.RemainingProfiles[0] != "medium" {
		t.Errorf("RemainingProfiles[0] = %q, want %q", j.RemainingProfiles[0], "medium")
	}
	if j.RemainingProfiles[1] != "high" {
		t.Errorf("RemainingProfiles[1] = %q, want %q", j.RemainingProfiles[1], "high")
	}
	if j.SubmissionID != sub.ID {
		t.Errorf("SubmissionID = %q, want %q", j.SubmissionID, sub.ID)
	}
}

func TestNewProfileJob_LastJob_EmptyRemaining(t *testing.T) {
	t.Parallel()
	sub := profileSub()

	// High profile — no more after it.
	j := queue.NewProfileJob(sub, sub.ContestID, "high", nil)

	if len(j.RemainingProfiles) != 0 {
		t.Errorf("RemainingProfiles = %v, want empty slice", j.RemainingProfiles)
	}
	if !j.IsLastProfile() {
		t.Error("IsLastProfile() = false for last profile job, want true")
	}
}

func TestNewProfileJob_ImmutableRemaining(t *testing.T) {
	t.Parallel()
	sub := profileSub()
	remaining := []string{"medium", "high"}

	j := queue.NewProfileJob(sub, sub.ContestID, "low", remaining)

	// Mutate the caller's slice after construction.
	remaining[0] = "MUTATED"

	// The job's internal slice must be unaffected.
	if j.RemainingProfiles[0] != "medium" {
		t.Errorf("RemainingProfiles[0] = %q after caller mutation, want %q (should be immutable)",
			j.RemainingProfiles[0], "medium")
	}
}

func TestNewProfileJob_ID_Format(t *testing.T) {
	t.Parallel()
	sub := profileSub()

	j := queue.NewProfileJob(sub, sub.ContestID, "medium", []string{"high"})

	wantID := sub.ID + "/medium"
	if j.ID != wantID {
		t.Errorf("ID = %q, want %q", j.ID, wantID)
	}
}

func TestJob_IsLastProfile_Phase4Job(t *testing.T) {
	t.Parallel()
	// Phase 1-4 jobs created with NewJob have nil RemainingProfiles,
	// so IsLastProfile must return true (there is no chaining).
	sub := &models.Submission{
		ID:          "legacy-sub",
		TeamName:    "legacy-team",
		Language:    models.LangBinary,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/legacy-sub/archive.tar.gz",
	}
	j := queue.NewJob(sub)

	if !j.IsLastProfile() {
		t.Error("IsLastProfile() = false for Phase 1-4 NewJob, want true (nil remaining)")
	}
}

func TestNewProfileJob_JSONRoundTrip(t *testing.T) {
	t.Parallel()
	sub := profileSub()
	original := queue.NewProfileJob(sub, "contest-xyz", "low", []string{"medium", "high"})

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded queue.Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.VolatilityLabel != original.VolatilityLabel {
		t.Errorf("VolatilityLabel: got %q, want %q", decoded.VolatilityLabel, original.VolatilityLabel)
	}
	if decoded.ContestID != original.ContestID {
		t.Errorf("ContestID: got %q, want %q", decoded.ContestID, original.ContestID)
	}
	if len(decoded.RemainingProfiles) != len(original.RemainingProfiles) {
		t.Errorf("len(RemainingProfiles): got %d, want %d",
			len(decoded.RemainingProfiles), len(original.RemainingProfiles))
	}
	for i, got := range decoded.RemainingProfiles {
		if got != original.RemainingProfiles[i] {
			t.Errorf("RemainingProfiles[%d]: got %q, want %q", i, got, original.RemainingProfiles[i])
		}
	}
}
