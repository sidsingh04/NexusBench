// Package models provides the core data structures for NexusBench.
//
// Design rules (enforced by CI):
//   - No imports from other internal packages — models is the base of the
//     dependency graph and must remain import-cycle free.
//   - No business logic — methods may only format or retrieve field values;
//     they must never mutate state or perform I/O.
//   - All new types required by a phase are added here atomically before any
//     other package references them, so the entire codebase compiles against
//     stable types from the first commit of each phase.
package models

import (
	"errors"
	"time"
)

// ── Sentinel errors ───────────────────────────────────────────────────────────

// Sentinel errors let callers use errors.Is rather than string matching.
// They are defined here so any package can import models and check against them
// without depending on contest, submission, or any other domain package.
var (
	// ErrNoActiveContest is returned by ContestStore.GetActive when no contest
	// currently has Status=active.
	ErrNoActiveContest = errors.New("no active contest")

	// ErrSubmissionInProgress is returned by submission.Service.Ingest when the
	// team already has a non-terminal submission in the same contest.
	ErrSubmissionInProgress = errors.New("a submission from this team is already being evaluated")

	// ErrContestNotActive is returned by submission.Service.Ingest when there
	// is no active contest, or when submissions have closed for the active one.
	ErrContestNotActive = errors.New("contest is not accepting submissions")
)

// ── Submission lifecycle ──────────────────────────────────────────────────────

// SubmissionStatus represents the lifecycle state of a submission.
type SubmissionStatus string

const (
	StatusPending      SubmissionStatus = "pending"      // uploaded, not yet processed
	StatusBuilding     SubmissionStatus = "building"     // being compiled / image built
	StatusDeploying    SubmissionStatus = "deploying"    // container starting
	StatusRunning      SubmissionStatus = "running"      // container healthy, endpoints live
	StatusBenchmarking SubmissionStatus = "benchmarking" // bot fleet actively testing
	StatusCompleted    SubmissionStatus = "completed"    // all profile runs done, results available
	StatusFailed       SubmissionStatus = "failed"       // any terminal error
)

// IsTerminal reports whether s is a terminal status (no further transitions).
// Terminal submissions may be resubmitted; non-terminal ones block re-ingestion
// under the one-active-submission guard.
func (s SubmissionStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed
}

// ── Language and Protocol ─────────────────────────────────────────────────────

// Language is the programming language of the submission.
type Language string

const (
	LangGo     Language = "go"
	LangRust   Language = "rust"
	LangCpp    Language = "cpp"
	LangPython Language = "python"
	LangBinary Language = "binary" // pre-compiled binary submitted directly
)

// Protocol is the wire protocol the contestant's engine speaks.
type Protocol string

const (
	ProtocolREST      Protocol = "rest"
	ProtocolWebSocket Protocol = "websocket"
	ProtocolFIX       Protocol = "fix"
)

// ── Contest lifecycle ─────────────────────────────────────────────────────────

// ContestStatus is the lifecycle state of a contest.
type ContestStatus string

const (
	// ContestStatusDraft is the initial state. A draft contest is not yet
	// accepting submissions and does not appear on the public leaderboard.
	ContestStatusDraft ContestStatus = "draft"

	// ContestStatusActive means the contest is open: submissions are accepted,
	// benchmark jobs are dispatched, and the live leaderboard is streaming.
	// At most one contest may be active at a time.
	ContestStatusActive ContestStatus = "active"

	// ContestStatusClosed is the terminal state. No new submissions are
	// accepted. The final leaderboard snapshot has been persisted.
	ContestStatusClosed ContestStatus = "closed"
)

// VolatilityProfile configures one benchmark run within a contest.
// The admin sets these when creating the contest; defaults are provided by
// internal/contest.DefaultLowProfile(), DefaultMediumProfile(), and
// DefaultHighProfile().
//
// All ratio fields (LimitRatio, MarketRatio, CancelRatio) must sum to 1.0.
// All weight fields (LatencyWeight, ThroughputWeight, CorrectnessWeight) must
// sum to 1.0.
type VolatilityProfile struct {
	// Label is the human-readable name: "low" | "medium" | "high".
	// Set by the defaults package; not user-configurable at profile level.
	Label string `json:"label"`

	// BotCount is the number of concurrent virtual traders during this run.
	BotCount int `json:"bot_count"`

	// TestDuration is how long the bot fleet runs at full capacity.
	// Stored as nanoseconds in JSON for lossless round-tripping.
	TestDuration time.Duration `json:"test_duration_ns"`

	// MarketDataPath is the path to a historical market data segment on the
	// shared PVC for replay. Empty string means synthetic-only generation.
	MarketDataPath string `json:"market_data_path,omitempty"`

	// Order mix ratios — must sum to 1.0.
	LimitRatio  float64 `json:"limit_ratio"`  // fraction of limit orders
	MarketRatio float64 `json:"market_ratio"` // fraction of market orders
	CancelRatio float64 `json:"cancel_ratio"` // fraction of cancel orders

	// PriceSpreadCents is the maximum deviation from mid-price in cents used
	// by the order generator when producing limit order prices.
	PriceSpreadCents int64 `json:"price_spread_cents"`

	// MaxQuantity is the maximum order size in units for the order generator.
	MaxQuantity int64 `json:"max_quantity"`

	// Scoring targets — define the ceiling at which normalised scores reach 1.0.

	// TargetP99Ns is the p99 latency target in nanoseconds.
	// An engine at or below this latency receives normP99 = 1.0.
	TargetP99Ns int64 `json:"target_p99_ns"`

	// TargetSustainTPS is the sustained TPS target (full-run average, not burst).
	// An engine at or above this rate receives normTPS = 1.0.
	TargetSustainTPS float64 `json:"target_sustain_tps"`

	// Scoring weights — must sum to 1.0.
	LatencyWeight     float64 `json:"latency_weight"`
	ThroughputWeight  float64 `json:"throughput_weight"`
	CorrectnessWeight float64 `json:"correctness_weight"`
}

// Contest is the top-level entity governing a timed benchmark competition.
// At most one Contest may have Status=active at any time (enforced by
// ContestService).
//
// All submissions, benchmark jobs, and leaderboard entries are scoped to a
// Contest via ContestID.
type Contest struct {
	ID     string        `json:"id"`
	Name   string        `json:"name"`
	Status ContestStatus `json:"status"`

	// Volatility profiles — one per benchmark run, executed sequentially.
	LowProfile    VolatilityProfile `json:"low_profile"`
	MediumProfile VolatilityProfile `json:"medium_profile"`
	HighProfile   VolatilityProfile `json:"high_profile"`

	// Aggregate weights for the final leaderboard FinalScore.
	// Must sum to 1.0. Defaults: 0.20 / 0.35 / 0.45.
	LowWeight    float64 `json:"low_weight"`
	MediumWeight float64 `json:"medium_weight"`
	HighWeight   float64 `json:"high_weight"`

	// SubmissionsClosedAt, when set, is the timestamp after which Ingest
	// rejects new uploads. May be set earlier than ContestClosedAt to freeze
	// the entry list while in-flight benchmarks complete.
	SubmissionsClosedAt *time.Time `json:"submissions_closed_at,omitempty"`

	// ContestClosedAt is set when the contest transitions to StatusClosed.
	ContestClosedAt *time.Time `json:"contest_closed_at,omitempty"`

	// EndsAt, when set, triggers automatic closing by the auto-close
	// background goroutine. Nil means manual close only.
	EndsAt *time.Time `json:"ends_at,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// ProfileByLabel returns the VolatilityProfile for the given label
// ("low", "medium", "high"). Returns the zero value and false if not found.
func (c *Contest) ProfileByLabel(label string) (VolatilityProfile, bool) {
	switch label {
	case "low":
		return c.LowProfile, true
	case "medium":
		return c.MediumProfile, true
	case "high":
		return c.HighProfile, true
	default:
		return VolatilityProfile{}, false
	}
}

// ── Submission ────────────────────────────────────────────────────────────────

// Submission is the core domain object representing one contestant's entry.
type Submission struct {
	ID        string           `json:"id"`
	TeamName  string           `json:"team_name"`
	Language  Language         `json:"language"`
	Protocol  Protocol         `json:"protocol"`
	Status    SubmissionStatus `json:"status"`
	StatusMsg string           `json:"status_message,omitempty"`

	// ContestID links this submission to its governing contest.
	// Empty in Phase 1–4 local-mode submissions (backward compatibility).
	ContestID string `json:"contest_id,omitempty"`

	// File info
	ArchivePath string `json:"archive_path,omitempty"` // path on disk
	ArchiveSize int64  `json:"archive_size_bytes"`

	// Sandbox runtime info (populated once container is running)
	ContainerID   string `json:"container_id,omitempty"`
	ContainerName string `json:"container_name,omitempty"`
	ExposedPort   int    `json:"exposed_port,omitempty"` // host port mapped to container

	// Timestamps
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Phase 1–4 single-run result (kept for backward compatibility).
	// Deprecated: use AllResults indexed by VolatilityLabel for Phase 5+.
	// Remains non-nil for submissions created before Phase 5.
	Results *BenchmarkResults `json:"results,omitempty"`

	// AllResults holds one BenchmarkResults per completed volatility profile
	// run, in the order they completed. Phase 5+.
	AllResults []*BenchmarkResults `json:"all_results,omitempty"`

	// FinalScore is the weighted aggregate across all three profile runs,
	// scaled 0–100. Set by computeAndWriteFinalScore after the last profile
	// job commits. Zero for in-progress submissions.
	FinalScore float64 `json:"final_score,omitempty"`
}

// ResultByLabel returns the BenchmarkResults for the given volatility label
// from AllResults. Returns nil if no result for that label exists yet.
func (s *Submission) ResultByLabel(label string) *BenchmarkResults {
	for _, r := range s.AllResults {
		if r.VolatilityLabel == label {
			return r
		}
	}
	return nil
}

// ── BenchmarkResults ──────────────────────────────────────────────────────────

// BenchmarkResults holds the scored output of one completed benchmark profile run.
type BenchmarkResults struct {
	// VolatilityLabel identifies which profile produced this result.
	// "low" | "medium" | "high". Empty for Phase 1–4 single-run results.
	VolatilityLabel string `json:"volatility_label,omitempty"`

	// Latency percentiles (milliseconds)
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P90LatencyMs float64 `json:"p90_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`

	// Throughput
	MaxTPS       float64 `json:"max_tps"`       // peak 100ms window TPS (diagnostic only)
	SustainedTPS float64 `json:"sustained_tps"` // full-run average TPS (used for scoring)

	// Correctness (0.0 – 1.0)
	CorrectnessScore float64 `json:"correctness_score"`
	TotalOrders      int64   `json:"total_orders_sent"`
	CorrectFills     int64   `json:"correct_fills"`
	IncorrectFills   int64   `json:"incorrect_fills"`

	// RunScore is the per-profile composite score (0.0–1.0) computed from
	// the profile's weighting formula. Zero for Phase 1–4 results.
	RunScore float64 `json:"run_score,omitempty"`

	// CompositeScore is the legacy single-run score (0–100), kept for
	// backward compatibility with Phase 1–4 leaderboard consumers.
	// For Phase 5+ submissions, use LeaderboardEntry.FinalScore instead.
	CompositeScore float64 `json:"composite_score"`

	BenchmarkDuration string    `json:"benchmark_duration"`
	CompletedAt       time.Time `json:"completed_at"`
}

// ── LeaderboardEntry ──────────────────────────────────────────────────────────

// LeaderboardEntry is a projection of Submission used in list/leaderboard views.
//
// Phase 5 extensions add per-profile scores and the weighted FinalScore.
// The legacy CompositeScore field is retained for Phase 1–4 backward compatibility;
// for Phase 5+ entries it equals FinalScore.
type LeaderboardEntry struct {
	Rank         int              `json:"rank"`
	SubmissionID string           `json:"submission_id"`
	TeamName     string           `json:"team_name"`
	Language     Language         `json:"language"`
	Protocol     Protocol         `json:"protocol"`
	Status       SubmissionStatus `json:"status"`

	// Phase 1–4 legacy score. For Phase 5+ entries this equals FinalScore.
	CompositeScore float64 `json:"composite_score"`

	// Phase 5 per-profile run scores (0.0–1.0 each).
	LowScore    float64 `json:"low_score,omitempty"`
	MediumScore float64 `json:"medium_score,omitempty"`
	HighScore   float64 `json:"high_score,omitempty"`

	// FinalScore is the weighted aggregate scaled 0–100.
	// This is the primary ranking key for Phase 5+ contests.
	FinalScore float64 `json:"final_score"`

	// Diagnostic columns shown on the leaderboard.
	BestP99Ms        float64 `json:"best_p99_ms,omitempty"`
	PeakSustainedTPS float64 `json:"peak_sustained_tps,omitempty"`
	AvgCorrectness   float64 `json:"avg_correctness,omitempty"`

	// Phase 1–4 legacy columns (kept for backward compatibility).
	P99LatencyMs     float64 `json:"p99_latency_ms"`
	MaxTPS           float64 `json:"max_tps"`
	CorrectnessScore float64 `json:"correctness_score"`

	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// ── Request DTOs ──────────────────────────────────────────────────────────────

// SubmitRequest is decoded from the multipart upload form.
type SubmitRequest struct {
	TeamName string   `json:"team_name"`
	Language Language `json:"language"`
	Protocol Protocol `json:"protocol"`
}

// APIError is the standard error envelope returned by the API.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
