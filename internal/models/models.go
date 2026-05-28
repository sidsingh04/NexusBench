package models

import "time"

// SubmissionStatus represents the lifecycle state of a submission.
type SubmissionStatus string

const (
	StatusPending      SubmissionStatus = "pending"      // uploaded, not yet processed
	StatusBuilding     SubmissionStatus = "building"     // being compiled / image built
	StatusDeploying    SubmissionStatus = "deploying"    // container starting
	StatusRunning      SubmissionStatus = "running"      // container healthy, endpoints live
	StatusBenchmarking SubmissionStatus = "benchmarking" // bot fleet actively testing
	StatusCompleted    SubmissionStatus = "completed"    // benchmark finished, results available
	StatusFailed       SubmissionStatus = "failed"       // any terminal error
)

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

// Submission is the core domain object representing one contestant's entry.
type Submission struct {
	ID        string           `json:"id"`
	TeamName  string           `json:"team_name"`
	Language  Language         `json:"language"`
	Protocol  Protocol         `json:"protocol"`
	Status    SubmissionStatus `json:"status"`
	StatusMsg string           `json:"status_message,omitempty"`

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

	// Results (populated after benchmarking)
	Results *BenchmarkResults `json:"results,omitempty"`
}

// BenchmarkResults holds the scored output of a completed benchmark run.
type BenchmarkResults struct {
	// Latency percentiles (milliseconds)
	P50LatencyMs float64 `json:"p50_latency_ms"`
	P90LatencyMs float64 `json:"p90_latency_ms"`
	P99LatencyMs float64 `json:"p99_latency_ms"`

	// Throughput
	MaxTPS       float64 `json:"max_tps"`
	SustainedTPS float64 `json:"sustained_tps"`

	// Correctness (0.0 – 1.0)
	CorrectnessScore float64 `json:"correctness_score"`
	TotalOrders      int64   `json:"total_orders_sent"`
	CorrectFills     int64   `json:"correct_fills"`
	IncorrectFills   int64   `json:"incorrect_fills"`

	// Composite score used for leaderboard ranking
	CompositeScore float64 `json:"composite_score"`

	BenchmarkDuration string    `json:"benchmark_duration"`
	CompletedAt       time.Time `json:"completed_at"`
}

// LeaderboardEntry is a projection of Submission used in list/leaderboard views.
type LeaderboardEntry struct {
	Rank             int              `json:"rank"`
	SubmissionID     string           `json:"submission_id"`
	TeamName         string           `json:"team_name"`
	Language         Language         `json:"language"`
	Protocol         Protocol         `json:"protocol"`
	Status           SubmissionStatus `json:"status"`
	CompositeScore   float64          `json:"composite_score"`
	P99LatencyMs     float64          `json:"p99_latency_ms"`
	MaxTPS           float64          `json:"max_tps"`
	CorrectnessScore float64          `json:"correctness_score"`
	CompletedAt      *time.Time       `json:"completed_at,omitempty"`
}

// APIError is the standard error envelope returned by the API.
type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// SubmitRequest is decoded from the multipart upload form.
type SubmitRequest struct {
	TeamName string   `json:"team_name"`
	Language Language `json:"language"`
	Protocol Protocol `json:"protocol"`
}
