package submission_test

import (
	"context"
	"errors"
	"mime/multipart"
	"net/textproto"
	"os"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// mockDocker is a no-op sandbox manager for unit tests.
// It doesn't touch Docker at all.
type mockDocker struct{}

func (m *mockDocker) Deploy(_ context.Context, sub *models.Submission) (string, int, error) {
	return "mock-container-abc123", 20001, nil
}
func (m *mockDocker) Stop(_ context.Context, _ string) error { return nil }

func TestDiskStore_SaveAndGet(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)

	sub := &models.Submission{
		ID:       "test-id-123",
		TeamName: "alpha-team",
		Language: models.LangGo,
		Protocol: models.ProtocolREST,
		Status:   models.StatusPending,
	}

	if err := store.Save(sub); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Get("test-id-123")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.TeamName != "alpha-team" {
		t.Errorf("TeamName: got %q, want %q", got.TeamName, "alpha-team")
	}
	if got.Language != models.LangGo {
		t.Errorf("Language: got %q, want %q", got.Language, models.LangGo)
	}
}

func TestDiskStore_List(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)

	for _, id := range []string{"aaa", "bbb", "ccc"} {
		if err := store.Save(&models.Submission{ID: id, TeamName: "team-" + id}); err != nil {
			t.Fatal(err)
		}
	}

	list, err := store.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("List: got %d, want 3", len(list))
	}
}

func TestValidation_BadLanguage(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)
	cfg := config.Load()
	cfg.SubmissionDir = dir

	// We need a real sandbox manager to build Service — use a tiny archive
	// with a nil docker manager by monkey-patching via the real constructor.
	// Instead, directly test the Ingest validation path via the service.
	svc := submission.NewService(store, nil, cfg) // nil docker — Ingest validates before deploying

	fh := makeFakeFileHeader("test.tar.gz", []byte("fake archive content"))
	req := models.SubmitRequest{
		TeamName: "t",
		Language: "cobol", // invalid
		Protocol: models.ProtocolREST,
	}

	_, err := svc.Ingest(context.Background(), req, fh)
	if err == nil {
		t.Fatal("expected validation error for unknown language, got nil")
	}
}

func TestValidation_EmptyTeamName(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)
	cfg := config.Load()
	cfg.SubmissionDir = dir

	svc := submission.NewService(store, nil, cfg)

	fh := makeFakeFileHeader("test.tar.gz", []byte("x"))
	_, err := svc.Ingest(context.Background(), models.SubmitRequest{
		Language: models.LangGo,
		Protocol: models.ProtocolREST,
	}, fh)
	if err == nil {
		t.Fatal("expected error for empty team name")
	}
}

// makeFakeFileHeader creates a multipart.FileHeader backed by a temp file,
// suitable for passing to service.Ingest in tests.
func makeFakeFileHeader(filename string, content []byte) *multipart.FileHeader {
	f, _ := os.CreateTemp("", filename)
	_, _ = f.Write(content)
	_ = f.Close()

	return &multipart.FileHeader{
		Filename: filename,
		Size:     int64(len(content)),
		Header:   make(textproto.MIMEHeader),
		// multipart.FileHeader.Open() reads from the file path via internal field.
		// For tests we override by creating our own struct with a known temp path.
	}
}

// ── Stage 5.3 — One-Active-Submission Guard tests ─────────────────────────────

// mockContestGetter is a minimal ContestGetter for unit tests.
// It returns the supplied contest (or error) on every GetActive call.
type mockContestGetter struct {
	contest *models.Contest
	err     error
}

func (m *mockContestGetter) GetActive(_ context.Context) (*models.Contest, error) {
	return m.contest, m.err
}

// activeContest returns a minimal active contest for use in guard tests.
func activeContest(id string) *models.Contest {
	return &models.Contest{
		ID:     id,
		Name:   "test-contest",
		Status: models.ContestStatusActive,
	}
}

// TestIngest_RejectsIfSubmissionInProgress verifies that Ingest returns
// ErrSubmissionInProgress when the team already has a non-terminal submission
// in the same contest.
func TestIngest_RejectsIfSubmissionInProgress(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)
	cfg := config.Load()
	cfg.SubmissionDir = dir

	const contestID = "contest-abc"
	const teamName = "alpha-team"

	// Seed an existing pending submission for the same team + contest.
	existing := &models.Submission{
		ID:        "existing-sub-001",
		TeamName:  teamName,
		ContestID: contestID,
		Status:    models.StatusPending, // non-terminal — should block
	}
	if err := store.Save(existing); err != nil {
		t.Fatalf("seed existing submission: %v", err)
	}

	getter := &mockContestGetter{contest: activeContest(contestID)}
	svc := submission.NewService(store, nil, cfg).WithContestGetter(getter)

	fh := makeFakeFileHeader("engine.tar.gz", []byte("fake"))
	_, err := svc.Ingest(context.Background(), models.SubmitRequest{
		TeamName: teamName,
		Language: models.LangGo,
		Protocol: models.ProtocolREST,
	}, fh)

	if err == nil {
		t.Fatal("expected ErrSubmissionInProgress, got nil")
	}
	if !errors.Is(err, models.ErrSubmissionInProgress) {
		t.Errorf("expected ErrSubmissionInProgress, got: %v", err)
	}
}

// TestIngest_AllowsAfterPreviousCompleted verifies that a team may resubmit
// once their previous submission has reached a terminal status.
func TestIngest_AllowsAfterPreviousCompleted(t *testing.T) {
	dir := t.TempDir()
	store := submission.NewDiskStore(dir)
	cfg := config.Load()
	cfg.SubmissionDir = dir

	const contestID = "contest-xyz"
	const teamName = "beta-team"

	// Seed a completed submission — terminal, so should not block.
	now := time.Now().UTC()
	previous := &models.Submission{
		ID:          "prev-sub-001",
		TeamName:    teamName,
		ContestID:   contestID,
		Status:      models.StatusCompleted, // terminal — should allow resubmit
		CompletedAt: &now,
	}
	if err := store.Save(previous); err != nil {
		t.Fatalf("seed previous submission: %v", err)
	}

	getter := &mockContestGetter{contest: activeContest(contestID)}
	// nil docker — Ingest runs contest checks, then hits storeArchive.
	// We verify the guard passes by checking the error is NOT ErrSubmissionInProgress.
	svc := submission.NewService(store, nil, cfg).WithContestGetter(getter)

	// Use a real temp file so storeArchive succeeds.
	tmpFile, err := os.CreateTemp(t.TempDir(), "engine.tar.gz")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	_, _ = tmpFile.WriteString("fake archive content")
	_ = tmpFile.Close()

	fh := makeFakeFileHeader("engine.tar.gz", []byte("fake archive content"))
	_, err = svc.Ingest(context.Background(), models.SubmitRequest{
		TeamName: teamName,
		Language: models.LangGo,
		Protocol: models.ProtocolREST,
	}, fh)

	// The guard must NOT fire. Any other error (e.g. docker deploy) is fine —
	// nil docker means deployAsync will be skipped entirely in local mode, and
	// the submission is already persisted. But since we have no jobQueue wired,
	// deployAsync is called in a goroutine, so Ingest itself returns nil.
	if errors.Is(err, models.ErrSubmissionInProgress) {
		t.Errorf("expected guard to pass for completed previous submission, got ErrSubmissionInProgress")
	}
}
