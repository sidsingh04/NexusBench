// Package consumer implements the metrics consumer pipeline for NexusBench
// Phase 2, Step 3.
//
// Pipeline:
//
//	Redpanda (metrics.latency) → Consumer → LatencyWindow → Store (TimescaleDB)
//
// Design rules:
//  1. percentile.go  — pure math, zero imports outside stdlib. Testable alone.
//  2. store.go       — all SQL lives here. Consumer never touches SQL directly.
//  3. consumer.go    — orchestration only: read, compute, write, repeat.
package consumer

import (
	"math"
	"sort"
	"time"
)

// LatencyWindow is one computed 5-second window of latency statistics.
// This is the row written into TimescaleDB by the Store.
//
// All latency values are in nanoseconds — the same unit as Event.LatencyNs.
// Consumers of the DB (Grafana, the leaderboard API) convert to µs or ms
// as needed at query time, keeping the source of truth lossless.
type LatencyWindow struct {
	// WindowStart is the UTC timestamp of the first event in this window.
	// TimescaleDB partitions by this column.
	WindowStart time.Time

	// SubmissionID identifies which contestant's engine produced these events.
	SubmissionID string

	// P50Ns, P90Ns, P99Ns are the 50th, 90th, and 99th percentile latencies
	// in nanoseconds, computed over all events in the window.
	P50Ns int64
	P90Ns int64
	P99Ns int64

	// MinNs and MaxNs are the minimum and maximum observed latencies.
	// Useful for spotting outliers that percentiles smooth over.
	MinNs int64
	MaxNs int64

	// MeanNs is the arithmetic mean latency.
	MeanNs int64

	// TPS is the throughput: number of events divided by window duration.
	// Stored as float64 to preserve fractional transactions per second.
	TPS float64

	// SampleN is the number of events this window was computed from.
	// A window with SampleN < MinSamples should be treated as unreliable.
	SampleN int
}

// WindowDuration is the fixed width of each latency window.
// 5 seconds balances granularity (enough events per window for stable
// percentiles) against write volume to TimescaleDB.
const WindowDuration = 5 * time.Second

// MinSamples is the minimum number of events required to produce a reliable
// percentile estimate. Windows below this threshold are still written but
// flagged implicitly by their SampleN value.
const MinSamples = 10

// Accumulator collects raw latency samples within a single time window
// and computes the LatencyWindow when the window closes.
//
// Design: we store raw samples (not a sketch) because window sizes are
// small (≤ a few thousand events per 5s window in dev). For Phase 5
// stress-testing at millions of TPS, swap the []int64 for an HDR histogram.
// The interface (Add / Compute) does not change.
//
// The zero value is NOT valid. Use NewAccumulator.
type Accumulator struct {
	submissionID string
	windowStart  time.Time
	samples      []int64 // raw LatencyNs values, unsorted until Compute()
}

// NewAccumulator creates an Accumulator for the given submission and window.
func NewAccumulator(submissionID string, windowStart time.Time) *Accumulator {
	return &Accumulator{
		submissionID: submissionID,
		windowStart:  windowStart,
		// Pre-allocate for 1024 samples — avoids repeated re-allocation for
		// typical window sizes without wasting memory for quiet submissions.
		samples: make([]int64, 0, 1024),
	}
}

// Add records one latency sample. Safe to call many times; not concurrent-safe
// (the Consumer calls Add from a single goroutine per window).
func (a *Accumulator) Add(latencyNs int64) {
	a.samples = append(a.samples, latencyNs)
}

// Len returns the number of samples added so far.
func (a *Accumulator) Len() int { return len(a.samples) }

// Compute finalises the window and returns a LatencyWindow.
// It sorts the samples in place (O(n log n)) and computes all statistics.
// After Compute(), the Accumulator should be discarded.
//
// Returns an empty LatencyWindow (SampleN=0) if no samples were added.
func (a *Accumulator) Compute() LatencyWindow {
	w := LatencyWindow{
		WindowStart:  a.windowStart,
		SubmissionID: a.submissionID,
		SampleN:      len(a.samples),
	}

	if len(a.samples) == 0 {
		return w
	}

	// Sort in place — percentile() requires sorted input.
	sort.Slice(a.samples, func(i, j int) bool {
		return a.samples[i] < a.samples[j]
	})

	w.P50Ns = percentile(a.samples, 0.50)
	w.P90Ns = percentile(a.samples, 0.90)
	w.P99Ns = percentile(a.samples, 0.99)
	w.MinNs = a.samples[0]
	w.MaxNs = a.samples[len(a.samples)-1]
	w.MeanNs = mean(a.samples)
	w.TPS = float64(len(a.samples)) / WindowDuration.Seconds()

	return w
}

// percentile returns the p-th percentile (0.0–1.0) of a sorted slice.
// Uses the "nearest rank" method: index = ceil(p * N) - 1, clamped to [0, N-1].
// This is the same method used by most monitoring systems (Prometheus, Grafana).
//
// Precondition: samples must be sorted ascending and non-empty.
func percentile(sorted []int64, p float64) int64 {
	n := len(sorted)
	if n == 0 {
		return 0
	}
	// math.Ceil(p * float64(n)) gives the 1-based rank.
	rank := int(math.Ceil(p * float64(n)))
	// Convert to 0-based index and clamp.
	idx := rank - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// mean returns the arithmetic mean of a non-empty slice.
func mean(samples []int64) int64 {
	if len(samples) == 0 {
		return 0
	}
	var sum int64
	for _, v := range samples {
		sum += v
	}
	return sum / int64(len(samples))
}
