package worker

// executor_scoring_test.go — white-box unit tests for the buildResults
// scoring function (Stage 5.4).
//
// These tests live in package worker (not worker_test) so they can call the
// unexported buildResults directly. This is the standard Go pattern for
// white-box tests of pure functions that are intentionally unexported.
//
// Tests:
//   TestBuildResults_LowProfile_PerfectScore    — P99=target, TPS=target, correctness=1.0 → RunScore=1.0
//   TestBuildResults_HighProfile_WorstLatency   — P99>>target → normP99≈0, RunScore driven by TPS+correctness
//   TestBuildResults_ZeroCorrectness_ZerosAll   — correctness=0.0 → RunScore=0.0 regardless of TPS/P99
//   TestBuildResults_LabelPropagated            — VolatilityLabel set correctly from label arg

import (
	"math"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
	"github.com/nexusbench/nexusbench/internal/models"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// makeFleetResult constructs a minimal FleetResult with the given BotStats.
// All other fields are set to zero/sensible defaults.
func makeFleetResult(stats botfleet.BotStats) *botfleet.FleetResult {
	now := time.Now()
	return &botfleet.FleetResult{
		Stats:      stats,
		StartedAt:  now.Add(-time.Second),
		FinishedAt: now,
	}
}

// makeCorrectnessResult constructs a CorrectnessResult with a given Score.
func makeCorrectnessResult(score float64) *correctness.CorrectnessResult {
	total := int64(100)
	correct := int64(math.Round(score * float64(total)))
	return &correctness.CorrectnessResult{
		Score:          score,
		TotalFills:     total,
		CorrectFills:   correct,
		IncorrectFills: total - correct,
	}
}

// standardProfile returns a VolatilityProfile with round numbers that make
// expected scores easy to compute by hand.
//
//	TargetP99Ns      = 1_000_000 (1 ms)
//	TargetSustainTPS = 10_000
//	LatencyWeight    = 0.5
//	ThroughputWeight = 0.3
//	CorrectnessWeight= 0.2
//
// With correctness=1.0, normP99=1.0, normTPS=1.0:
//
//	runScore = 1.0*(0.5*1.0 + 0.3*1.0) + 0.2*1.0 = 0.8 + 0.2 = 1.0
func standardProfile() models.VolatilityProfile {
	return models.VolatilityProfile{
		Label:             "low",
		BotCount:          50,
		TestDuration:      30 * time.Second,
		LimitRatio:        0.60,
		MarketRatio:       0.30,
		CancelRatio:       0.10,
		PriceSpreadCents:  200,
		MaxQuantity:       50,
		TargetP99Ns:       1_000_000, // 1 ms
		TargetSustainTPS:  10_000,
		LatencyWeight:     0.5,
		ThroughputWeight:  0.3,
		CorrectnessWeight: 0.2,
	}
}

const approxTolerance = 1e-9 // floating-point comparison tolerance

func approxEqual(a, b, tol float64) bool {
	return math.Abs(a-b) <= tol
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestBuildResults_LowProfile_PerfectScore verifies that an engine hitting
// exactly the target P99 and TPS with perfect correctness receives RunScore=1.0.
//
// Formula check:
//
//	normP99  = 1.0  (P99Ns == TargetP99Ns)
//	normTPS  = 1.0  (SustainedTPS == TargetSustainTPS)
//	runScore = 1.0*(0.5*1.0 + 0.3*1.0) + 0.2*1.0 = 1.0
func TestBuildResults_LowProfile_PerfectScore(t *testing.T) {
	t.Parallel()

	p := standardProfile()

	stats := botfleet.BotStats{
		P99Ns:            float64(p.TargetP99Ns), // exactly at target
		SustainedTPS:     p.TargetSustainTPS,     // exactly at target
		TotalOrders:      1000,
		SuccessfulOrders: 1000,
	}

	fr := makeFleetResult(stats)
	cr := makeCorrectnessResult(1.0) // perfect

	result := buildResults(fr, cr, p, "low")

	if result.VolatilityLabel != "low" {
		t.Errorf("VolatilityLabel = %q, want %q", result.VolatilityLabel, "low")
	}
	if !approxEqual(result.RunScore, 1.0, approxTolerance) {
		t.Errorf("RunScore = %.6f, want 1.0 (perfect score)", result.RunScore)
	}
	// CompositeScore should be RunScore×100 for Phase 5 labeled entries.
	if !approxEqual(result.CompositeScore, 100.0, approxTolerance) {
		t.Errorf("CompositeScore = %.6f, want 100.0", result.CompositeScore)
	}
}

// TestBuildResults_HighProfile_WorstLatency verifies that when P99 is well
// above 10×TargetP99Ns the latency contribution is zero, and RunScore is
// driven only by TPS and correctness.
//
// Setup:
//
//	P99Ns         = TargetP99Ns * 100  (well past the 10× worst-case floor)
//	SustainedTPS  = TargetSustainTPS   (full TPS)
//	correctness   = 1.0
//
// Expected:
//
//	normP99  = 0.0  (P99 ≥ 10×target)
//	normTPS  = 1.0
//	runScore = 1.0*(0.5*0.0 + 0.3*1.0) + 0.2*1.0 = 0.3 + 0.2 = 0.5
func TestBuildResults_HighProfile_WorstLatency(t *testing.T) {
	t.Parallel()

	p := standardProfile()
	p.Label = "high"

	stats := botfleet.BotStats{
		P99Ns:            float64(p.TargetP99Ns) * 100, // 100× target — floor
		SustainedTPS:     p.TargetSustainTPS,
		TotalOrders:      1000,
		SuccessfulOrders: 1000,
	}

	fr := makeFleetResult(stats)
	cr := makeCorrectnessResult(1.0)

	result := buildResults(fr, cr, p, "high")

	want := p.ThroughputWeight*1.0 + p.CorrectnessWeight*1.0 // 0.3 + 0.2 = 0.5
	if !approxEqual(result.RunScore, want, 1e-6) {
		t.Errorf("RunScore = %.6f, want %.6f (latency floor: only TPS+correctness)", result.RunScore, want)
	}
	if result.VolatilityLabel != "high" {
		t.Errorf("VolatilityLabel = %q, want %q", result.VolatilityLabel, "high")
	}
}

// TestBuildResults_ZeroCorrectness_ZerosAll verifies that an engine with 0%
// correctness receives RunScore=0.0 regardless of latency or throughput.
//
// This is the "correctness multiplier" property: a fast but wrong engine should
// never rank above a correct one.
//
// Setup:
//
//	P99Ns        = TargetP99Ns  (perfect latency)
//	SustainedTPS = TargetSustainTPS (perfect TPS)
//	correctness  = 0.0
//
// Expected:
//
//	runScore = 0.0*(0.5*1.0 + 0.3*1.0) + 0.2*0.0 = 0.0
func TestBuildResults_ZeroCorrectness_ZerosAll(t *testing.T) {
	t.Parallel()

	p := standardProfile()

	stats := botfleet.BotStats{
		P99Ns:            float64(p.TargetP99Ns), // perfect latency
		SustainedTPS:     p.TargetSustainTPS,     // perfect TPS
		TotalOrders:      1000,
		SuccessfulOrders: 1000,
	}

	fr := makeFleetResult(stats)
	cr := makeCorrectnessResult(0.0) // zero correctness

	result := buildResults(fr, cr, p, "medium")

	if result.RunScore != 0.0 {
		t.Errorf("RunScore = %.6f, want 0.0 (zero correctness must zero RunScore)", result.RunScore)
	}
	if result.CompositeScore != 0.0 {
		t.Errorf("CompositeScore = %.6f, want 0.0", result.CompositeScore)
	}
}

// TestBuildResults_LabelPropagated verifies that the VolatilityLabel argument
// is set verbatim on the returned BenchmarkResults, and that the Phase 5 path
// is taken (RunScore is set, VolatilityLabel is non-empty).
//
// This also verifies that the legacy Phase 1–4 path (label=="") is NOT taken
// when a label is provided, i.e. we are not falling back to hardcoded constants.
func TestBuildResults_LabelPropagated(t *testing.T) {
	t.Parallel()

	for _, label := range []string{"low", "medium", "high"} {
		label := label // capture range variable
		t.Run(label, func(t *testing.T) {
			t.Parallel()

			p := standardProfile()
			p.Label = label

			stats := botfleet.BotStats{
				P99Ns:            float64(p.TargetP99Ns),
				SustainedTPS:     p.TargetSustainTPS,
				TotalOrders:      100,
				SuccessfulOrders: 100,
			}

			fr := makeFleetResult(stats)
			cr := makeCorrectnessResult(1.0)

			result := buildResults(fr, cr, p, label)

			if result.VolatilityLabel != label {
				t.Errorf("VolatilityLabel = %q, want %q", result.VolatilityLabel, label)
			}
			// RunScore must be set (non-zero) because correctness=1, TPS=target, P99=target.
			if result.RunScore == 0.0 {
				t.Errorf("RunScore = 0.0 for label=%q with perfect inputs; Phase 5 path not taken", label)
			}
		})
	}
}

// TestBuildResults_LegacyPath_NoLabel verifies that when label is empty the
// legacy Phase 1–4 scoring path is taken: CompositeScore is populated,
// RunScore remains zero, and VolatilityLabel is empty.
//
// This guards backward compatibility: Phase 1–4 workers that set no label
// must produce identical results before and after Stage 5.4.
func TestBuildResults_LegacyPath_NoLabel(t *testing.T) {
	t.Parallel()

	// P99 exactly at the legacy 1ms target → normP99=1.0
	// MaxTPS at legacy 50_000 target → normTPS=1.0
	// correctness=1.0
	// legacy formula: (0.5*1.0 + 0.3*1.0 + 0.2*1.0)*100 = 100.0
	stats := botfleet.BotStats{
		P99Ns:            1_000_000, // 1ms — legacy target
		MaxTPS:           50_000,
		SustainedTPS:     50_000,
		TotalOrders:      1000,
		SuccessfulOrders: 1000,
	}

	fr := makeFleetResult(stats)
	cr := makeCorrectnessResult(1.0)

	result := buildResults(fr, cr, models.VolatilityProfile{}, "") // empty label → legacy

	if result.VolatilityLabel != "" {
		t.Errorf("VolatilityLabel = %q, want empty for legacy path", result.VolatilityLabel)
	}
	if result.RunScore != 0.0 {
		t.Errorf("RunScore = %.6f, want 0.0 for legacy path", result.RunScore)
	}
	if !approxEqual(result.CompositeScore, 100.0, 1e-6) {
		t.Errorf("CompositeScore = %.6f, want 100.0 for perfect legacy score", result.CompositeScore)
	}
}
