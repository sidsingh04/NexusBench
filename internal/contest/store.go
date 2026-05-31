// Package contest owns the full lifecycle of a benchmark contest:
// creation, activation, closing, leaderboard snapshotting, and default
// volatility profile construction.
//
// Dependency rules (enforced by go build — never relax these):
//   - contest may import models, config.
//   - contest must NOT import api, submission, worker, sandbox, or queue.
//   - The LeaderboardBroadcaster interface is defined here (not in api) so
//     contest can fire events without importing api and creating a cycle.
//
// Implementations of ContestStore:
//   - MemoryContestStore  — in-memory, used in tests and local dev (this file).
//   - PostgresContestStore — PostgreSQL-backed, added in Stage 5.9.
package contest

import (
	"errors"
	"sync"

	"github.com/nexusbench/nexusbench/internal/models"
)

// ── ContestStore interface ─────────────────────────────────────────────────────

// ContestStore is the persistence interface for contests.
//
// All methods must be safe for concurrent use.
//
// Error contract:
//   - Get/GetActive/GetLeaderboardSnapshot return a wrapped ErrNotFound /
//     models.ErrNoActiveContest when the requested record does not exist.
//   - Save returns ErrDuplicateContest if a contest with the same ID already
//     exists (MemoryContestStore) or if the INSERT conflicts (Postgres).
//   - All other errors are opaque infrastructure errors; callers should treat
//     them as internal server errors.
type ContestStore interface {
	// Save persists a new contest. Returns ErrDuplicateContest if a contest
	// with the same ID already exists.
	Save(c *models.Contest) error

	// Get returns the contest with the given ID.
	// Returns ErrNotFound if no contest has that ID.
	Get(id string) (*models.Contest, error)

	// GetActive returns the single contest whose Status is ContestStatusActive.
	// Returns models.ErrNoActiveContest if no contest is currently active.
	// There is at most one active contest at any time; this invariant is
	// enforced by ContestService.Activate.
	GetActive() (*models.Contest, error)

	// List returns all contests in insertion order.
	// Returns an empty slice (not nil) when no contests exist.
	List() ([]*models.Contest, error)

	// Update overwrites all mutable fields of an existing contest.
	// Returns ErrNotFound if no contest with c.ID exists.
	Update(c *models.Contest) error

	// SnapshotLeaderboard archives the final ranked leaderboard for a closed
	// contest. Called exactly once per contest, when it transitions to
	// StatusClosed. Overwrites any previous snapshot for the same contestID
	// (idempotent — safe to call again if the first attempt failed mid-way).
	SnapshotLeaderboard(contestID string, entries []*models.LeaderboardEntry) error

	// GetLeaderboardSnapshot returns the archived snapshot for a closed
	// contest. Returns ErrNotFound if no snapshot has been written yet.
	GetLeaderboardSnapshot(contestID string) ([]*models.LeaderboardEntry, error)
}

// ── Store-level sentinel errors ────────────────────────────────────────────────

var (
	// ErrNotFound is returned by Get, Update, and GetLeaderboardSnapshot when
	// the requested record does not exist.
	ErrNotFound = errors.New("contest: record not found")

	// ErrDuplicateContest is returned by Save when a contest with the same ID
	// already exists.
	ErrDuplicateContest = errors.New("contest: duplicate contest ID")
)

// ── MemoryContestStore ─────────────────────────────────────────────────────────

// MemoryContestStore is a thread-safe in-memory ContestStore implementation.
//
// It is used in all unit tests (no database required) and in local dev mode
// (DISTRIBUTED_MODE=false). It stores shallow copies of contest structs so
// that mutations to returned pointers do not silently corrupt store state —
// the same contract a real database provides.
type MemoryContestStore struct {
	mu        sync.RWMutex
	contests  map[string]*models.Contest            // id → contest
	order     []string                              // insertion order for List()
	snapshots map[string][]*models.LeaderboardEntry // contestID → snapshot
}

// NewMemoryContestStore returns a ready-to-use MemoryContestStore.
func NewMemoryContestStore() *MemoryContestStore {
	return &MemoryContestStore{
		contests:  make(map[string]*models.Contest),
		snapshots: make(map[string][]*models.LeaderboardEntry),
	}
}

// Save persists c. Returns ErrDuplicateContest if c.ID already exists.
func (s *MemoryContestStore) Save(c *models.Contest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.contests[c.ID]; exists {
		return ErrDuplicateContest
	}
	clone := *c
	s.contests[c.ID] = &clone
	s.order = append(s.order, c.ID)
	return nil
}

// Get returns a copy of the stored contest. Mutations to the returned value
// do not affect store state.
func (s *MemoryContestStore) Get(id string) (*models.Contest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	c, ok := s.contests[id]
	if !ok {
		return nil, ErrNotFound
	}
	clone := *c
	return &clone, nil
}

// GetActive returns the single active contest.
// Returns models.ErrNoActiveContest if none is active.
func (s *MemoryContestStore) GetActive() (*models.Contest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, c := range s.contests {
		if c.Status == models.ContestStatusActive {
			clone := *c
			return &clone, nil
		}
	}
	return nil, models.ErrNoActiveContest
}

// List returns shallow copies of all contests in insertion order.
func (s *MemoryContestStore) List() ([]*models.Contest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make([]*models.Contest, 0, len(s.order))
	for _, id := range s.order {
		c := s.contests[id]
		clone := *c
		out = append(out, &clone)
	}
	return out, nil
}

// Update overwrites the stored contest. Returns ErrNotFound if c.ID is unknown.
func (s *MemoryContestStore) Update(c *models.Contest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.contests[c.ID]; !ok {
		return ErrNotFound
	}
	clone := *c
	s.contests[c.ID] = &clone
	return nil
}

// SnapshotLeaderboard writes (or overwrites) the leaderboard snapshot for
// contestID. The snapshot is a defensive copy of entries.
func (s *MemoryContestStore) SnapshotLeaderboard(contestID string, entries []*models.LeaderboardEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	cp := make([]*models.LeaderboardEntry, len(entries))
	for i, e := range entries {
		ec := *e
		cp[i] = &ec
	}
	s.snapshots[contestID] = cp
	return nil
}

// GetLeaderboardSnapshot returns a copy of the snapshot. Returns ErrNotFound
// if no snapshot exists for contestID.
func (s *MemoryContestStore) GetLeaderboardSnapshot(contestID string) ([]*models.LeaderboardEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entries, ok := s.snapshots[contestID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := make([]*models.LeaderboardEntry, len(entries))
	for i, e := range entries {
		ec := *e
		cp[i] = &ec
	}
	return cp, nil
}
