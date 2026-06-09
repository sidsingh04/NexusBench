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

var (
	ErrNoActiveContest      = errors.New("no active contest")
	ErrSubmissionInProgress = errors.New("a submission from this team is already being evaluated")
	ErrContestNotActive     = errors.New("contest is not accepting submissions")
)

// ── Submission lifecycle ──────────────────────────────────────────────────────

type SubmissionStatus string

const (
	StatusPending      SubmissionStatus = "pending"
	StatusBuilding     SubmissionStatus = "building"
	StatusDeploying    SubmissionStatus = "deploying"
	StatusRunning      SubmissionStatus = "running"
	StatusBenchmarking SubmissionStatus = "benchmarking"
	StatusCompleted    SubmissionStatus = "completed"
	StatusFailed       SubmissionStatus = "failed"
)

func (s SubmissionStatus) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed
}

// ── Language and Protocol ─────────────────────────────────────────────────────

type Language string

const (
	LangGo     Language = "go"
	LangRust   Language = "rust"
	LangCpp    Language = "cpp"
	LangPython Language = "python"
	LangBinary Language = "binary"
)

type Protocol string

const (
	ProtocolREST      Protocol = "rest"
	ProtocolWebSocket Protocol = "websocket"
	ProtocolFIX       Protocol = "fix"
)

// ── Contest lifecycle ─────────────────────────────────────────────────────────

type ContestStatus string

const (
	ContestStatusDraft  ContestStatus = "draft"
	ContestStatusActive ContestStatus = "active"
	ContestStatusClosed ContestStatus = "closed"
)

type VolatilityProfile struct {
	Label             string        `json:"label"`
	BotCount          int           `json:"bot_count"`
	TestDuration      time.Duration `json:"test_duration_ns"`
	MarketDataPath    string        `json:"market_data_path,omitempty"`
	LimitRatio        float64       `json:"limit_ratio"`
	MarketRatio       float64       `json:"market_ratio"`
	CancelRatio       float64       `json:"cancel_ratio"`
	PriceSpreadCents  int64         `json:"price_spread_cents"`
	MaxQuantity       int64         `json:"max_quantity"`
	TargetP99Ns       int64         `json:"target_p99_ns"`
	TargetSustainTPS  float64       `json:"target_sustain_tps"`
	LatencyWeight     float64       `json:"latency_weight"`
	ThroughputWeight  float64       `json:"throughput_weight"`
	CorrectnessWeight float64       `json:"correctness_weight"`
}

type Contest struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Status              ContestStatus     `json:"status"`
	LowProfile          VolatilityProfile `json:"low_profile"`
	MediumProfile       VolatilityProfile `json:"medium_profile"`
	HighProfile         VolatilityProfile `json:"high_profile"`
	LowWeight           float64           `json:"low_weight"`
	MediumWeight        float64           `json:"medium_weight"`
	HighWeight          float64           `json:"high_weight"`
	SubmissionsClosedAt *time.Time        `json:"submissions_closed_at,omitempty"`
	ContestClosedAt     *time.Time        `json:"contest_closed_at,omitempty"`
	EndsAt              *time.Time        `json:"ends_at,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
}

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

type Submission struct {
	ID        string           `json:"id"`
	TeamName  string           `json:"team_name"`
	Language  Language         `json:"language"`
	Protocol  Protocol         `json:"protocol"`
	Status    SubmissionStatus `json:"status"`
	StatusMsg string           `json:"status_message,omitempty"`

	ContestID string `json:"contest_id,omitempty"`

	ArchivePath string `json:"archive_path,omitempty"`
	ArchiveSize int64  `json:"archive_size_bytes"`

	ContainerID   string `json:"container_id,omitempty"`
	ContainerName string `json:"container_name,omitempty"`
	ExposedPort   int    `json:"exposed_port,omitempty"`

	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`

	// Phase 1–4 single-run result (kept for backward compatibility).
	Results *BenchmarkResults `json:"results,omitempty"`

	// Phase 5+: one entry per completed volatility profile.
	AllResults []*BenchmarkResults `json:"all_results,omitempty"`

	// FinalScore is the weighted aggregate across all three profile runs,
	// scaled 0–100. Set by computeAndWriteFinalScore after the last profile
	// job commits. Zero for in-progress submissions.
	FinalScore float64 `json:"final_score"`

	// DryRunResult holds the output of the worker-side pre-flight validator
	// run before the bot fleet starts. Nil for Phase 1–6 submissions and for
	// any submission processed by a worker that predates this feature.
	// omitempty keeps Phase 1–6 submission JSON byte-identical to before.
	DryRunResult *DryRunResult `json:"dry_run_result,omitempty"`
}

func (s *Submission) ResultByLabel(label string) *BenchmarkResults {
	for _, r := range s.AllResults {
		if r.VolatilityLabel == label {
			return r
		}
	}
	return nil
}

// ── DryRunResult ──────────────────────────────────────────────────────────────

// DryRunScenarioResult is one scenario's outcome from the pre-flight validator.
// Mirrors validator.ScenarioResult but lives in models to avoid import cycles.
type DryRunScenarioResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	// Reason is the enriched failure string. Empty when Passed is true.
	// Format: `order "<id>" [<kind> <side> qty=<n> price=$<x.xx>]:
	//          <field> mismatch: got <actual>, want <expected>
	//          book state: <context>`
	Reason string `json:"reason,omitempty"`
}

// DryRunResult is the stored output of the worker-side pre-flight validator.
// Non-nil only on submissions processed by workers with the pre-flight gate
// enabled (Phase 7+).
type DryRunResult struct {
	AllPassed bool                   `json:"all_passed"`
	Scenarios []DryRunScenarioResult `json:"scenarios"`
	RanAt     time.Time              `json:"ran_at"`
	// FailSummary is a single human-readable string for the UI status panel.
	// Empty when AllPassed is true.
	// Example: "3/21 scenarios failed: [limit_sell_crosses_buy_partial_fill,
	//           cancel_of_unknown_id_rejected, concurrent_burst_10]"
	FailSummary string `json:"fail_summary,omitempty"`
}

// ── BenchmarkResults ──────────────────────────────────────────────────────────

type BenchmarkResults struct {
	VolatilityLabel   string    `json:"volatility_label,omitempty"`
	P50LatencyMs      float64   `json:"p50_latency_ms"`
	P90LatencyMs      float64   `json:"p90_latency_ms"`
	P99LatencyMs      float64   `json:"p99_latency_ms"`
	MaxTPS            float64   `json:"max_tps"`
	SustainedTPS      float64   `json:"sustained_tps"`
	CorrectnessScore  float64   `json:"correctness_score"`
	TotalOrders       int64     `json:"total_orders_sent"`
	CorrectFills      int64     `json:"correct_fills"`
	IncorrectFills    int64     `json:"incorrect_fills"`
	RunScore          float64   `json:"run_score"`
	CompositeScore    float64   `json:"composite_score"`
	BenchmarkDuration string    `json:"benchmark_duration"`
	CompletedAt       time.Time `json:"completed_at"`
}

// ── LeaderboardEntry ──────────────────────────────────────────────────────────

type LeaderboardEntry struct {
	Rank         int              `json:"rank"`
	SubmissionID string           `json:"submission_id"`
	TeamName     string           `json:"team_name"`
	Language     Language         `json:"language"`
	Protocol     Protocol         `json:"protocol"`
	Status       SubmissionStatus `json:"status"`

	CompositeScore float64 `json:"composite_score"`
	LowScore       float64 `json:"low_score"`
	MediumScore    float64 `json:"medium_score"`
	HighScore      float64 `json:"high_score"`
	FinalScore     float64 `json:"final_score"`

	BestP99Ms        float64 `json:"best_p99_ms,omitempty"`
	PeakSustainedTPS float64 `json:"peak_sustained_tps,omitempty"`
	AvgCorrectness   float64 `json:"avg_correctness"`

	P99LatencyMs     float64 `json:"p99_latency_ms"`
	MaxTPS           float64 `json:"max_tps"`
	CorrectnessScore float64 `json:"correctness_score"`

	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

// ── Request DTOs ──────────────────────────────────────────────────────────────

type SubmitRequest struct {
	TeamName string   `json:"team_name"`
	Language Language `json:"language"`
	Protocol Protocol `json:"protocol"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
