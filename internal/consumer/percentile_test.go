package consumer

// percentile_test.go tests the pure-math layer in isolation.
// No Redpanda, no TimescaleDB, no network. Runs in milliseconds.
//
// Test structure:
//   TestPercentile_KnownValues      — spot-check against hand-computed answers
//   TestPercentile_SingleSample     — edge case: N=1
//   TestPercentile_TwoSamples       — edge case: N=2
//   TestPercentile_AllSame          — all samples identical
//   TestAccumulator_Empty           — Compute() on zero samples
//   TestAccumulator_Ordering        — Add() in random order, Compute() sorts
//   TestAccumulator_Statistics      — P50/P90/P99/Min/Max/Mean/TPS correctness
//   TestAccumulator_TPS             — TPS = N / WindowDuration.Seconds()
//   TestAccumulator_LargeN          — 10 000 samples, verify P99 is reasonable

import (
	"testing"
	"time"
)

// ── percentile() (package-private, tested via Accumulator) ────────────────────

func TestPercentile_KnownValues(t *testing.T) {
	t.Parallel()
	// sorted [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
	// P50 → ceil(0.50 * 10) = 5 → index 4 → value 5
	// P90 → ceil(0.90 * 10) = 9 → index 8 → value 9
	// P99 → ceil(0.99 * 10) = 10 → index 9 → value 10
	samples := []int64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	cases := []struct {
		p    float64
		want int64
	}{
		{0.50, 5},
		{0.90, 9},
		{0.99, 10},
		{0.00, 1},  // 0th percentile → first element
		{1.00, 10}, // 100th percentile → last element
	}
	for _, tc := range cases {
		got := percentile(samples, tc.p)
		if got != tc.want {
			t.Errorf("percentile(samples, %.2f) = %d, want %d", tc.p, got, tc.want)
		}
	}
}

func TestPercentile_SingleSample(t *testing.T) {
	t.Parallel()
	samples := []int64{42}
	for _, p := range []float64{0.0, 0.5, 0.99, 1.0} {
		if got := percentile(samples, p); got != 42 {
			t.Errorf("percentile([42], %.2f) = %d, want 42", p, got)
		}
	}
}

func TestPercentile_TwoSamples(t *testing.T) {
	t.Parallel()
	// [10, 20]
	// P50 → ceil(0.50 * 2) = 1 → index 0 → value 10
	// P99 → ceil(0.99 * 2) = 2 → index 1 → value 20
	samples := []int64{10, 20}
	if got := percentile(samples, 0.50); got != 10 {
		t.Errorf("P50 = %d, want 10", got)
	}
	if got := percentile(samples, 0.99); got != 20 {
		t.Errorf("P99 = %d, want 20", got)
	}
}

func TestPercentile_AllSame(t *testing.T) {
	t.Parallel()
	samples := []int64{100, 100, 100, 100, 100}
	for _, p := range []float64{0.50, 0.90, 0.99} {
		if got := percentile(samples, p); got != 100 {
			t.Errorf("percentile(all-100, %.2f) = %d, want 100", p, got)
		}
	}
}

// ── Accumulator ───────────────────────────────────────────────────────────────

func TestAccumulator_Empty(t *testing.T) {
	t.Parallel()
	acc := NewAccumulator("sub-1", time.Now().UTC())
	w := acc.Compute()

	if w.SampleN != 0 {
		t.Errorf("SampleN = %d, want 0", w.SampleN)
	}
	if w.P50Ns != 0 || w.P90Ns != 0 || w.P99Ns != 0 {
		t.Errorf("empty window has non-zero percentiles: p50=%d p90=%d p99=%d",
			w.P50Ns, w.P90Ns, w.P99Ns)
	}
	if w.TPS != 0 {
		t.Errorf("TPS = %f, want 0", w.TPS)
	}
}

func TestAccumulator_Ordering(t *testing.T) {
	t.Parallel()
	// Add samples in reverse order; Compute() must sort them.
	acc := NewAccumulator("sub-1", time.Now().UTC())
	for i := int64(100); i >= 1; i-- {
		acc.Add(i)
	}
	w := acc.Compute()

	if w.MinNs != 1 {
		t.Errorf("MinNs = %d, want 1", w.MinNs)
	}
	if w.MaxNs != 100 {
		t.Errorf("MaxNs = %d, want 100", w.MaxNs)
	}
	// P50 of [1..100] → ceil(0.50*100)=50 → index 49 → value 50
	if w.P50Ns != 50 {
		t.Errorf("P50Ns = %d, want 50", w.P50Ns)
	}
}

func TestAccumulator_Statistics(t *testing.T) {
	t.Parallel()
	// Use samples [1, 2, ..., 100] — hand-computable ground truth.
	acc := NewAccumulator("sub-stat", time.Now().UTC())
	for i := int64(1); i <= 100; i++ {
		acc.Add(i)
	}
	w := acc.Compute()

	// SampleN
	if w.SampleN != 100 {
		t.Errorf("SampleN = %d, want 100", w.SampleN)
	}

	// Min / Max
	if w.MinNs != 1 {
		t.Errorf("MinNs = %d, want 1", w.MinNs)
	}
	if w.MaxNs != 100 {
		t.Errorf("MaxNs = %d, want 100", w.MaxNs)
	}

	// Mean of [1..100] = 50 (integer division: 5050/100 = 50)
	if w.MeanNs != 50 {
		t.Errorf("MeanNs = %d, want 50", w.MeanNs)
	}

	// Percentiles for [1..100]:
	// P50 → ceil(0.50*100)=50 → index 49 → value 50
	if w.P50Ns != 50 {
		t.Errorf("P50Ns = %d, want 50", w.P50Ns)
	}
	// P90 → ceil(0.90*100)=90 → index 89 → value 90
	if w.P90Ns != 90 {
		t.Errorf("P90Ns = %d, want 90", w.P90Ns)
	}
	// P99 → ceil(0.99*100)=99 → index 98 → value 99
	if w.P99Ns != 99 {
		t.Errorf("P99Ns = %d, want 99", w.P99Ns)
	}

	// SubmissionID must survive through to the window.
	if w.SubmissionID != "sub-stat" {
		t.Errorf("SubmissionID = %q, want %q", w.SubmissionID, "sub-stat")
	}
}

func TestAccumulator_TPS(t *testing.T) {
	t.Parallel()
	// 50 samples in a 5-second window → TPS = 50 / 5.0 = 10.0
	acc := NewAccumulator("sub-tps", time.Now().UTC())
	for i := 0; i < 50; i++ {
		acc.Add(int64(i + 1))
	}
	w := acc.Compute()

	wantTPS := float64(50) / WindowDuration.Seconds() // 50 / 5 = 10.0
	if w.TPS != wantTPS {
		t.Errorf("TPS = %f, want %f", w.TPS, wantTPS)
	}
}

func TestAccumulator_LargeN(t *testing.T) {
	t.Parallel()
	// 10 000 samples, latency values 1..10000 ns.
	// P99 → ceil(0.99 * 10000) = 9900 → index 9899 → value 9900
	acc := NewAccumulator("sub-large", time.Now().UTC())
	for i := int64(1); i <= 10_000; i++ {
		acc.Add(i)
	}
	w := acc.Compute()

	if w.SampleN != 10_000 {
		t.Errorf("SampleN = %d, want 10000", w.SampleN)
	}
	if w.P99Ns != 9900 {
		t.Errorf("P99Ns = %d, want 9900", w.P99Ns)
	}
	if w.MinNs != 1 {
		t.Errorf("MinNs = %d, want 1", w.MinNs)
	}
	if w.MaxNs != 10_000 {
		t.Errorf("MaxNs = %d, want 10000", w.MaxNs)
	}
}

func TestAccumulator_WindowStartPreserved(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	acc := NewAccumulator("sub-ts", ts)
	acc.Add(100)
	w := acc.Compute()

	if !w.WindowStart.Equal(ts) {
		t.Errorf("WindowStart = %v, want %v", w.WindowStart, ts)
	}
}
