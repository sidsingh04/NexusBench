// Package submission handles the full lifecycle of a contestant's code upload:
// validation, storage on disk, and spinning up a language-specific sandbox.
package submission

import (
	"context"
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

type Service struct {
	store         Store
	docker        *sandbox.DockerManager
	cfg           *config.Config
	jobQueue      queue.Queue
	contestGetter ContestGetter
}

func NewService(store Store, docker *sandbox.DockerManager, cfg *config.Config) *Service {
	return &Service{store: store, docker: docker, cfg: cfg}
}

func (s *Service) WithQueue(q queue.Queue) *Service {
	return &Service{
		store:         s.store,
		docker:        s.docker,
		cfg:           s.cfg,
		jobQueue:      q,
		contestGetter: s.contestGetter,
	}
}

func (s *Service) WithContestGetter(cg ContestGetter) *Service {
	return &Service{
		store:         s.store,
		docker:        s.docker,
		cfg:           s.cfg,
		jobQueue:      s.jobQueue,
		contestGetter: cg,
	}
}

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

	var contestID string
	if s.contestGetter != nil {
		contest, err := s.contestGetter.GetActive(ctx)
		if err != nil {
			return nil, models.ErrContestNotActive
		}

		if contest.SubmissionsClosedAt != nil && time.Now().UTC().After(*contest.SubmissionsClosedAt) {
			return nil, models.ErrContestNotActive
		}

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

	if s.jobQueue != nil {
		var j queue.Job
		if contestID != "" {
			j = queue.NewProfileJob(sub, contestID, "low", []string{"medium", "high"})
		} else {
			j = queue.NewJob(sub)
		}
		if err := s.jobQueue.Enqueue(ctx, j); err != nil {
			s.setStatus(sub, models.StatusFailed, fmt.Sprintf("enqueue error: %v", err))
			return nil, fmt.Errorf("enqueue job: %w", err)
		}
		slog.Info("submission dispatched to worker queue",
			"id", id,
			"job_id", j.ID,
			"volatility_label", j.VolatilityLabel,
			"remaining_profiles", j.RemainingProfiles,
		)
	} else {
		go s.deployAsync(context.WithoutCancel(ctx), sub)
	}

	return sub, nil
}

func (s *Service) checkNoActiveSubmission(teamName, contestID string) error {
	all, err := s.store.List()
	if err != nil {
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
	return ""
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

// ── JSON helpers: writeJSON and readJSON are in writejson.go ─────────────────
