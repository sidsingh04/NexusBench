package contest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/nexusbench/nexusbench/internal/models"
)

// ── Broadcaster contract ───────────────────────────────────────────────────────

// LeaderboardBroadcaster is satisfied by the leaderboardBus in internal/api.
//
// It is defined here (not in internal/api) so that internal/contest can fire
// broadcast events without importing internal/api and creating an import cycle.
// The api package provides the concrete implementation; cmd/server wires them
// together at startup.
//
// Tests that do not care about SSE behavior pass a nil bus to
// NewContestService — all Broadcast calls are no-ops when bus is nil.
type LeaderboardBroadcaster interface {
	Broadcast(event LeaderboardEvent)
}

// LeaderboardEvent is the payload pushed to every connected SSE subscriber
// whenever the live leaderboard changes or a contest is frozen.
//
// Type values:
//   - "update"  — one or more scores changed; Entries is the full ranked list.
//   - "frozen"  — the contest just closed; Entries is the final snapshot.
type LeaderboardEvent struct {
	Type      string // "update" | "frozen"
	ContestID string
	Entries   []*models.LeaderboardEntry
}

// ── Request / Response DTOs ────────────────────────────────────────────────────

// CreateContestRequest is decoded from the admin API body for
// POST /api/v1/admin/contests.
//
// If UseDefaults is true, the three volatility profiles are populated from
// DefaultLowProfile / DefaultMediumProfile / DefaultHighProfile and the
// aggregate weights default to 0.20 / 0.35 / 0.45. Any explicit profile
// fields in the request override the defaults field-by-field — this lets
// an admin say "use defaults but change BotCount on high to 500" without
// spelling out every field.
type CreateContestRequest struct {
	Name          string                    `json:"name"`
	UseDefaults   bool                      `json:"use_defaults"`
	LowProfile    *models.VolatilityProfile `json:"low_profile,omitempty"`
	MediumProfile *models.VolatilityProfile `json:"medium_profile,omitempty"`
	HighProfile   *models.VolatilityProfile `json:"high_profile,omitempty"`
	LowWeight     float64                   `json:"low_weight"`
	MediumWeight  float64                   `json:"medium_weight"`
	HighWeight    float64                   `json:"high_weight"`
	// EndsAt, when set, triggers automatic closing. Nil = manual close only.
	EndsAt *time.Time `json:"ends_at,omitempty"`
}

// ── Service-level sentinel errors ─────────────────────────────────────────────

var (
	// ErrAlreadyActive is returned by Activate when another contest is
	// already active. At most one contest may be active at any time.
	ErrAlreadyActive = errors.New("contest: another contest is already active")

	// ErrWrongStatus is returned by Activate when the target contest is not
	// in StatusDraft (e.g. it is already active or closed).
	ErrWrongStatus = errors.New("contest: contest is not in the required status for this transition")
)

// ── ContestService ─────────────────────────────────────────────────────────────

// ContestService implements the full contest lifecycle.
//
// It is the single authoritative owner of all contest state transitions:
//
//	draft ──activate──► active ──close──► closed
//
// All transitions are guarded so no two goroutines can produce inconsistent
// state (e.g. two simultaneous Activate calls each believing they won the race).
// This is achieved via the store's own mutex; the service does not add a
// second lock layer.
//
// ContestService has no dependency on submission, worker, sandbox, or api.
// The cmd/server wiring layer connects those.
type ContestService struct {
	store ContestStore
	// bus may be nil in tests that do not exercise SSE behavior.
	// All broadcast calls are guarded by a nil check.
	bus LeaderboardBroadcaster

	// now is a hook for deterministic time in tests.
	// Production code leaves it nil; the service uses time.Now().UTC().
	now func() time.Time
}

// NewContestService returns a ContestService backed by store.
// bus may be nil — Broadcast calls are silently skipped when bus is nil.
func NewContestService(store ContestStore, bus LeaderboardBroadcaster) *ContestService {
	return &ContestService{store: store, bus: bus}
}

// withNow overrides the time source. Used only in unit tests.
func (s *ContestService) withNow(fn func() time.Time) *ContestService {
	s.now = fn
	return s
}

func (s *ContestService) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now().UTC()
}

// ── Lifecycle methods ──────────────────────────────────────────────────────────

// Create persists a new contest in StatusDraft.
//
// If req.UseDefaults is true, default profiles are used as the base and any
// non-nil profile overrides in req are merged on top. Aggregate weights
// default to 0.20 / 0.35 / 0.45 when all three are zero (unset).
//
// Returns an error if:
//   - req.Name is empty.
//   - The store rejects the save (rare; only if a UUID collision occurs).
func (s *ContestService) Create(_ context.Context, req CreateContestRequest) (*models.Contest, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("contest name is required")
	}

	low := DefaultLowProfile()
	med := DefaultMediumProfile()
	high := DefaultHighProfile()

	if !req.UseDefaults {
		// Caller must supply all three profiles explicitly.
		if req.LowProfile == nil || req.MediumProfile == nil || req.HighProfile == nil {
			return nil, fmt.Errorf("all three volatility profiles are required when use_defaults=false")
		}
	}

	// Merge explicit overrides on top of defaults.
	if req.LowProfile != nil {
		low = mergeProfile(low, *req.LowProfile)
	}
	if req.MediumProfile != nil {
		med = mergeProfile(med, *req.MediumProfile)
	}
	if req.HighProfile != nil {
		high = mergeProfile(high, *req.HighProfile)
	}

	// Aggregate weight defaults: 0.20 / 0.35 / 0.45.
	lowW, medW, highW := req.LowWeight, req.MediumWeight, req.HighWeight
	if lowW == 0 && medW == 0 && highW == 0 {
		lowW, medW, highW = 0.20, 0.35, 0.45
	}

	now := s.clock()
	c := &models.Contest{
		ID:            uuid.New().String(),
		Name:          req.Name,
		Status:        models.ContestStatusDraft,
		LowProfile:    low,
		MediumProfile: med,
		HighProfile:   high,
		LowWeight:     lowW,
		MediumWeight:  medW,
		HighWeight:    highW,
		EndsAt:        req.EndsAt,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.store.Save(c); err != nil {
		return nil, fmt.Errorf("contest: save: %w", err)
	}

	slog.Info("contest: created",
		"id", c.ID,
		"name", c.Name,
		"use_defaults", req.UseDefaults,
	)
	return c, nil
}

// Activate transitions a draft contest to StatusActive.
//
// Returns ErrAlreadyActive if another contest is already active.
// Returns ErrWrongStatus if the target contest is not StatusDraft.
// Returns ErrNotFound (wrapped) if no contest with id exists.
func (s *ContestService) Activate(_ context.Context, id string) (*models.Contest, error) {
	// Guard: no other contest may be active.
	if _, err := s.store.GetActive(); err == nil {
		// GetActive succeeded — there IS an active contest.
		return nil, ErrAlreadyActive
	} else if !errors.Is(err, models.ErrNoActiveContest) {
		// Unexpected store error.
		return nil, fmt.Errorf("contest: check active: %w", err)
	}

	c, err := s.store.Get(id)
	if err != nil {
		return nil, fmt.Errorf("contest: get %s: %w", id, err)
	}
	if c.Status != models.ContestStatusDraft {
		return nil, fmt.Errorf("%w: contest %s has status %s, need draft", ErrWrongStatus, id, c.Status)
	}

	now := s.clock()
	c.Status = models.ContestStatusActive
	c.UpdatedAt = now

	if err := s.store.Update(c); err != nil {
		return nil, fmt.Errorf("contest: update %s: %w", id, err)
	}

	slog.Info("contest: activated", "id", id, "name", c.Name)
	return c, nil
}

// Close transitions an active contest to StatusClosed, snapshots the
// leaderboard, and broadcasts a "frozen" event.
//
// Idempotent: if the contest is already StatusClosed, Close returns nil
// without modifying state or re-broadcasting.
//
// entries is the ranked leaderboard at the moment of closing. It is the
// caller's responsibility to compute and sort this slice before calling Close.
// Typically the caller is the auto-close goroutine in cmd/server, which
// iterates completed submissions and builds the list.
//
// Returns ErrNotFound (wrapped) if no contest with id exists.
// Returns ErrWrongStatus if the contest is StatusDraft (cannot close a
// contest that was never activated).
func (s *ContestService) Close(_ context.Context, id string, entries []*models.LeaderboardEntry) error {
	c, err := s.store.Get(id)
	if err != nil {
		return fmt.Errorf("contest: get %s: %w", id, err)
	}

	// Idempotent: already closed.
	if c.Status == models.ContestStatusClosed {
		slog.Debug("contest: Close called on already-closed contest — no-op", "id", id)
		return nil
	}

	if c.Status == models.ContestStatusDraft {
		return fmt.Errorf("%w: contest %s is draft, must be activated before closing", ErrWrongStatus, id)
	}

	now := s.clock()
	c.Status = models.ContestStatusClosed
	c.ContestClosedAt = &now
	c.UpdatedAt = now

	if err := s.store.Update(c); err != nil {
		return fmt.Errorf("contest: update %s: %w", id, err)
	}

	// Sort entries by FinalScore descending and assign ranks.
	ranked := rankEntries(entries)

	if err := s.store.SnapshotLeaderboard(id, ranked); err != nil {
		// Log but don't fail the close — the contest is already marked closed.
		slog.Error("contest: snapshot leaderboard failed", "id", id, "err", err)
	}

	slog.Info("contest: closed", "id", id, "entries", len(ranked))

	s.broadcast(LeaderboardEvent{
		Type:      "frozen",
		ContestID: id,
		Entries:   ranked,
	})
	return nil
}

// GetActive returns the currently active contest.
// Returns models.ErrNoActiveContest if no contest is active.
func (s *ContestService) GetActive(_ context.Context) (*models.Contest, error) {
	return s.store.GetActive()
}

// ListAll returns all contests (draft + active + closed) in insertion order.
func (s *ContestService) ListAll(_ context.Context) ([]*models.Contest, error) {
	return s.store.List()
}

// ListPast returns all closed contests in insertion order.
func (s *ContestService) ListPast(_ context.Context) ([]*models.Contest, error) {
	all, err := s.store.List()
	if err != nil {
		return nil, err
	}
	var closed []*models.Contest
	for _, c := range all {
		if c.Status == models.ContestStatusClosed {
			closed = append(closed, c)
		}
	}
	if closed == nil {
		closed = []*models.Contest{} // never return nil
	}
	return closed, nil
}

// GetLeaderboardSnapshot returns the archived leaderboard for a closed contest.
// Returns ErrNotFound if no snapshot has been written yet.
func (s *ContestService) GetLeaderboardSnapshot(_ context.Context, contestID string) ([]*models.LeaderboardEntry, error) {
	entries, err := s.store.GetLeaderboardSnapshot(contestID)
	if err != nil {
		return nil, fmt.Errorf("contest: get snapshot %s: %w", contestID, err)
	}
	return entries, nil
}

// ── Helpers ───────────────────────────────────────────────────────────────────

// broadcast fires an event on the bus if one is wired in.
// Tests that pass nil bus skip broadcast silently.
func (s *ContestService) broadcast(event LeaderboardEvent) {
	if s.bus == nil {
		return
	}
	s.bus.Broadcast(event)
}

// rankEntries sorts entries by FinalScore descending and assigns 1-based ranks.
// Returns a new slice; does not mutate the input.
func rankEntries(entries []*models.LeaderboardEntry) []*models.LeaderboardEntry {
	cp := make([]*models.LeaderboardEntry, len(entries))
	for i, e := range entries {
		ec := *e
		cp[i] = &ec
	}
	sort.Slice(cp, func(i, j int) bool {
		return cp[i].FinalScore > cp[j].FinalScore
	})
	for i, e := range cp {
		e.Rank = i + 1
	}
	return cp
}

// mergeProfile applies non-zero fields from override on top of base.
// Zero-value override fields leave the base field intact, so callers can
// say "use defaults but set BotCount=500 on the high profile" without
// repeating every field.
func mergeProfile(base, override models.VolatilityProfile) models.VolatilityProfile {
	if override.Label != "" {
		base.Label = override.Label
	}
	if override.BotCount != 0 {
		base.BotCount = override.BotCount
	}
	if override.TestDuration != 0 {
		base.TestDuration = override.TestDuration
	}
	if override.MarketDataPath != "" {
		base.MarketDataPath = override.MarketDataPath
	}
	if override.LimitRatio != 0 {
		base.LimitRatio = override.LimitRatio
	}
	if override.MarketRatio != 0 {
		base.MarketRatio = override.MarketRatio
	}
	if override.CancelRatio != 0 {
		base.CancelRatio = override.CancelRatio
	}
	if override.PriceSpreadCents != 0 {
		base.PriceSpreadCents = override.PriceSpreadCents
	}
	if override.MaxQuantity != 0 {
		base.MaxQuantity = override.MaxQuantity
	}
	if override.TargetP99Ns != 0 {
		base.TargetP99Ns = override.TargetP99Ns
	}
	if override.TargetSustainTPS != 0 {
		base.TargetSustainTPS = override.TargetSustainTPS
	}
	if override.LatencyWeight != 0 {
		base.LatencyWeight = override.LatencyWeight
	}
	if override.ThroughputWeight != 0 {
		base.ThroughputWeight = override.ThroughputWeight
	}
	if override.CorrectnessWeight != 0 {
		base.CorrectnessWeight = override.CorrectnessWeight
	}
	return base
}
