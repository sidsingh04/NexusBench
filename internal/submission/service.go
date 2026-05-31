// Package submission handles the full lifecycle of a contestant's code upload:
// validation, storage on disk, and spinning up a language-specific sandbox.
package submission

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"mime/multipart"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/sandbox"
)

// ContestGetter is satisfied by *contest.ContestService.
// Defined here (not in internal/contest) so that internal/submission can call
// GetActive without importing internal/contest and creating an import cycle.
// Injected via Service.WithContestGetter; nil in Phase 1–4 local mode.
type ContestGetter interface {
	// GetActive returns the currently active contest.
	// Returns models.ErrNoActiveContest when no contest is active.
	GetActive(ctx context.Context) (*models.Contest, error)
}

// Store is the persistence interface for submissions.
type Store interface {
	Save(sub *models.Submission) error
	Get(id string) (*models.Submission, error)
	List() ([]*models.Submission, error)
	Update(sub *models.Submission) error
}

// DiskStore implements Store using the local filesystem.
// Layout: <baseDir>/<submissionID>/meta.json + archive.*
type DiskStore struct {
	baseDir string
	mu      sync.RWMutex
}

func NewDiskStore(baseDir string) *DiskStore {
	if err := os.MkdirAll(baseDir, 0o750); err != nil {
		slog.Error("failed to create submission dir", "path", baseDir, "err", err)
	}
	return &DiskStore{baseDir: baseDir}
}

func (s *DiskStore) metaPath(id string) string {
	return filepath.Join(s.baseDir, id, "meta.json")
}

func (s *DiskStore) Save(sub *models.Submission) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := filepath.Join(s.baseDir, sub.ID)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("mkdir: %w", err)
	}
	return writeJSON(s.metaPath(sub.ID), sub)
}

func (s *DiskStore) Get(id string) (*models.Submission, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var sub models.Submission
	if err := readJSON(s.metaPath(id), &sub); err != nil {
		return nil, fmt.Errorf("submission %s not found: %w", id, err)
	}
	return &sub, nil
}

func (s *DiskStore) Update(sub *models.Submission) error {
	sub.UpdatedAt = time.Now().UTC()
	return s.Save(sub)
}

func (s *DiskStore) List() ([]*models.Submission, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, err := os.ReadDir(s.baseDir)
	if err != nil {
		return nil, err
	}
	var subs []*models.Submission
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		var sub models.Submission
		if err := readJSON(filepath.Join(s.baseDir, e.Name(), "meta.json"), &sub); err != nil {
			slog.Warn("skipping corrupt submission", "id", e.Name(), "err", err)
			continue
		}
		subs = append(subs, &sub)
	}
	sort.Slice(subs, func(i, j int) bool {
		return subs[i].CreatedAt.After(subs[j].CreatedAt)
	})
	return subs, nil
}

// ── Service ───────────────────────────────────────────────────────────────────

// Service orchestrates ingestion and on-demand container deployment.
//
// Phase 3 dispatch modes (controlled by the jobQueue field):
//
//   - jobQueue == nil (Phase 1/2 mode):
//     Ingest calls deployAsync directly — runs the full sandbox lifecycle
//     in-process. Identical to the original behavior; no existing tests break.
//
//   - jobQueue != nil (Phase 3 distributed mode):
//     Ingest enqueues a Job to the queue. A separate worker process picks it
//     up, runs the sandbox, and writes results back via the shared Store.
//     The control plane stays stateless between submission and completion.
//
// Phase 5 additions (controlled by the contestGetter field):
//
//   - contestGetter == nil (Phase 1–4 compatibility mode):
//     Ingest skips all contest-scoped checks. Teams may submit freely.
//     No ContestID is attached to the submission.
//
//   - contestGetter != nil (Phase 5 contest mode):
//     Ingest requires an active contest, enforces the one-active-submission
//     guard, and rejects uploads after SubmissionsClosedAt.
type Service struct {
	store         Store
	docker        *sandbox.DockerManager
	cfg           *config.Config
	jobQueue      queue.Queue   // nil = local mode (Phase 1/2), non-nil = distributed mode (Phase 3+)
	contestGetter ContestGetter // nil = Phase 1–4 compat (no contest checks), non-nil = Phase 5
}

// NewService creates a Service in local (Phase 1/2) mode.
// No queue is wired — Ingest deploys sandboxes directly via docker.
func NewService(store Store, docker *sandbox.DockerManager, cfg *config.Config) *Service {
	return &Service{store: store, docker: docker, cfg: cfg}
}

// WithQueue returns a copy of s with a job queue wired in, enabling
// distributed (Phase 3+) mode. The original Service is not modified.
//
// Usage in cmd/server/main.go:
//
//	svc := submission.NewService(store, docker, cfg).WithQueue(jobQueue)
func (s *Service) WithQueue(q queue.Queue) *Service {
	return &Service{
		store:         s.store,
		docker:        s.docker,
		cfg:           s.cfg,
		jobQueue:      q,
		contestGetter: s.contestGetter,
	}
}

// WithContestGetter returns a copy of s with a ContestGetter wired in,
// enabling Phase 5 contest-scoped submission behavior. The original Service
// is not modified.
//
// Usage in cmd/server/main.go:
//
//	svc := submission.NewService(store, docker, cfg).
//	    WithQueue(jobQueue).
//	    WithContestGetter(contestSvc)
func (s *Service) WithContestGetter(cg ContestGetter) *Service {
	return &Service{
		store:         s.store,
		docker:        s.docker,
		cfg:           s.cfg,
		jobQueue:      s.jobQueue,
		contestGetter: cg,
	}
}

// Ingest validates the upload, persists the archive, records the submission,
// then either enqueues a job (distributed mode) or deploys the sandbox
// directly (local mode).
//
// The returned Submission is always in StatusPending — callers poll
// GET /submissions/{id} to observe status transitions.
//
// Phase 5 contest checks (only when contestGetter is non-nil):
//
//  1. Requires an active contest; returns ErrContestNotActive if none.
//  2. Rejects uploads after contest.SubmissionsClosedAt; returns ErrContestNotActive.
//  3. One-active-submission guard: returns ErrSubmissionInProgress if the team
//     already has a non-terminal submission in the same contest.
func (s *Service) Ingest(
	ctx context.Context,
	req models.SubmitRequest,
	fileHeader *multipart.FileHeader,
) (*models.Submission, error) {
	if err := validateLanguage(req.Language); err != nil {
		return nil, err
	}
	if err := validateProtocol(req.Protocol); err != nil {
		return nil, err
	}
	if req.TeamName == "" {
		return nil, fmt.Errorf("team_name is required")
	}

	if _, ok := s.cfg.ImageForLanguage(string(req.Language)); !ok {
		return nil, fmt.Errorf("no sandbox image configured for language %q", req.Language)
	}

	// ── Phase 5 contest checks ────────────────────────────────────────────────
	// These checks are skipped when contestGetter is nil (Phase 1–4 compat).
	var contestID string
	if s.contestGetter != nil {
		contest, err := s.contestGetter.GetActive(ctx)
		if err != nil {
			// ErrNoActiveContest and any other error both map to ErrContestNotActive
			// from the caller's perspective: you cannot submit without an active contest.
			return nil, models.ErrContestNotActive
		}

		// Gate 1: submissions-closed check.
		// If the admin has set SubmissionsClosedAt and that time has passed,
		// reject new uploads so in-flight jobs can drain cleanly.
		if contest.SubmissionsClosedAt != nil && time.Now().UTC().After(*contest.SubmissionsClosedAt) {
			return nil, models.ErrContestNotActive
		}

		// Gate 2: one-active-submission guard.
		// A team may not flood the queue by submitting while their previous
		// submission is still being evaluated.
		if err := s.checkNoActiveSubmission(req.TeamName, contest.ID); err != nil {
			return nil, err
		}

		contestID = contest.ID
	}

	id := uuid.New().String()
	now := time.Now().UTC()

	sub := &models.Submission{
		ID:          id,
		TeamName:    req.TeamName,
		Language:    req.Language,
		Protocol:    req.Protocol,
		Status:      models.StatusPending,
		ArchiveSize: fileHeader.Size,
		ContestID:   contestID,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	archivePath, err := s.storeArchive(id, fileHeader)
	if err != nil {
		return nil, fmt.Errorf("store archive: %w", err)
	}
	sub.ArchivePath = archivePath

	if err := s.store.Save(sub); err != nil {
		return nil, fmt.Errorf("save metadata: %w", err)
	}

	image, _ := s.cfg.ImageForLanguage(string(req.Language))
	slog.Info("submission ingested",
		"id", id,
		"team", req.TeamName,
		"language", req.Language,
		"image", image,
		"archive_path", archivePath,
		"size_bytes", fileHeader.Size,
		"contest_id", contestID,
	)

	// ── dispatch ──────────────────────────────────────────────────────────────
	if s.jobQueue != nil {
		// Distributed mode: hand the job to the worker fleet via the queue.
		// We enqueue synchronously so any broker failure surfaces immediately
		// to the caller as a 500 rather than silently losing the job.
		j := queue.NewJob(sub)
		if err := s.jobQueue.Enqueue(ctx, j); err != nil {
			// Roll back the submission to failed so the client knows to retry.
			s.setStatus(sub, models.StatusFailed, fmt.Sprintf("enqueue error: %v", err))
			return nil, fmt.Errorf("enqueue job: %w", err)
		}
		slog.Info("submission dispatched to worker queue", "id", id, "job_id", j.ID)
	} else {
		// Local mode (Phase 1/2): deploy in this process. Unchanged behavior.
		go s.deployAsync(context.WithoutCancel(ctx), sub)
	}

	return sub, nil
}

// checkNoActiveSubmission returns ErrSubmissionInProgress if the given team
// already has a non-terminal submission associated with contestID.
//
// Terminal statuses (StatusCompleted, StatusFailed) are allowed — a team may
// resubmit once their previous run finishes. Only non-terminal statuses
// (pending, building, deploying, running, benchmarking) block re-ingestion.
//
// Design note: this iterates the full submission list, which is acceptable
// because the list is bounded by the number of submissions per contest
// (typically tens to low hundreds). A database index on (team_name, contest_id,
// status) would be the correct optimisation if the list grew to thousands.
func (s *Service) checkNoActiveSubmission(teamName, contestID string) error {
	all, err := s.store.List()
	if err != nil {
		// Treat a store error conservatively: block the submission rather than
		// accidentally allowing a flood if the store is temporarily unavailable.
		return fmt.Errorf("submission guard: list failed: %w", err)
	}

	for _, sub := range all {
		if sub.TeamName != teamName {
			continue
		}
		if sub.ContestID != contestID {
			continue
		}
		if !sub.Status.IsTerminal() {
			slog.Warn("submission guard: team has in-progress submission",
				"team", teamName,
				"contest_id", contestID,
				"blocking_submission_id", sub.ID,
				"blocking_status", sub.Status,
			)
			return models.ErrSubmissionInProgress
		}
	}
	return nil
}

// deployAsync picks the pre-built image for this submission's language
// and spins it up as a container. Only called in local (Phase 1/2) mode.
func (s *Service) deployAsync(ctx context.Context, sub *models.Submission) {
	timeoutCtx, cancel := context.WithTimeout(ctx, s.cfg.SandboxTimeout)
	defer cancel()

	s.setStatus(sub, models.StatusDeploying, fmt.Sprintf("starting %s container", sub.Language))

	containerID, exposedPort, err := s.docker.Deploy(timeoutCtx, sub)
	if err != nil {
		slog.Error("sandbox deploy failed", "id", sub.ID, "err", err)
		s.setStatus(sub, models.StatusFailed, fmt.Sprintf("deploy error: %v", err))
		return
	}

	sub.ContainerID = containerID
	sub.ContainerName = fmt.Sprintf("nexusbench-%s", sub.ID[:8])
	sub.ExposedPort = exposedPort

	s.setStatus(sub, models.StatusRunning, fmt.Sprintf("container live on port %d", exposedPort))

	slog.Info("submission live",
		"id", sub.ID,
		"container", containerID[:12],
		"language", sub.Language,
		"port", exposedPort,
	)
}

func (s *Service) setStatus(sub *models.Submission, status models.SubmissionStatus, msg string) {
	sub.Status = status
	sub.StatusMsg = msg
	if err := s.store.Update(sub); err != nil {
		slog.Error("failed to persist status update", "id", sub.ID, "err", err)
	}
}

func (s *Service) Get(id string) (*models.Submission, error) {
	return s.store.Get(id)
}

func (s *Service) List() ([]*models.Submission, error) {
	return s.store.List()
}

func (s *Service) StopContainer(ctx context.Context, id string) error {
	sub, err := s.store.Get(id)
	if err != nil {
		return err
	}
	if sub.ContainerID == "" {
		return fmt.Errorf("no container running for submission %s", id)
	}
	if err := s.docker.Stop(ctx, sub.ContainerID); err != nil {
		return err
	}
	s.setStatus(sub, models.StatusCompleted, "manually stopped")
	return nil
}

// storeArchive persists the uploaded file to <submissionDir>/<id>/archive.<ext>
//
// Bug fix 1 — extension detection:
//
//	filepath.Ext("file.tar.gz") returns ".gz", not ".tar.gz".
//	We check the full filename suffix manually to preserve compound extensions.
func (s *Service) storeArchive(id string, fh *multipart.FileHeader) (string, error) {
	dir := filepath.Join(s.cfg.SubmissionDir, id)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}

	ext := archiveExt(fh.Filename)
	dest := filepath.Join(dir, "archive"+ext)

	src, err := fh.Open()
	if err != nil {
		return "", err
	}
	defer src.Close()

	out, err := os.Create(dest)
	if err != nil {
		return "", err
	}
	defer func() { _ = out.Close() }()

	if _, err := io.Copy(out, src); err != nil {
		return "", err
	}
	return dest, nil
}

// archiveExt returns the full compound extension for an archive filename.
// filepath.Ext only returns the last segment (.gz instead of .tar.gz),
// so we check known compound extensions explicitly.
func archiveExt(filename string) string {
	lower := strings.ToLower(filename)
	for _, ext := range []string{".tar.gz", ".tar.bz2", ".tar.xz", ".tar.zst"} {
		if strings.HasSuffix(lower, ext) {
			return ext
		}
	}
	if ext := filepath.Ext(filename); ext != "" {
		return ext
	}
	return "" // Return empty string if no extension is provided
}

// ── Validators ────────────────────────────────────────────────────────────────

func validateLanguage(l models.Language) error {
	switch l {
	case models.LangGo, models.LangRust, models.LangCpp, models.LangPython, models.LangBinary:
		return nil
	default:
		return fmt.Errorf("unsupported language %q (valid: go, rust, cpp, python, binary)", l)
	}
}

func validateProtocol(p models.Protocol) error {
	switch p {
	case models.ProtocolREST, models.ProtocolWebSocket, models.ProtocolFIX:
		return nil
	default:
		return fmt.Errorf("unsupported protocol %q (valid: rest, websocket, fix)", p)
	}
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

func writeJSON(path string, v any) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return json.NewDecoder(f).Decode(v)
}
