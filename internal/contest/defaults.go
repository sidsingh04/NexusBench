package contest

import (
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
)

// DefaultLowProfile returns the default VolatilityProfile for the "low"
// benchmark tier.
//
// Design intent — low tier:
//   - Gentle load (10 bots) to verify basic correctness under minimal pressure.
//   - Long test window (60s) gives a stable sustained-TPS baseline.
//   - Order mix is heavily limit-weighted (80%) to keep the order book deep
//     and stress price-time priority logic.
//   - Correctness carries the heaviest weight (0.50) at this tier because a
//     slow-but-correct engine beats a fast-but-wrong one in the low bracket.
func DefaultLowProfile() models.VolatilityProfile {
	return models.VolatilityProfile{
		Label:        "low",
		BotCount:     10,
		TestDuration: 60 * time.Second,

		LimitRatio:  0.80,
		MarketRatio: 0.10,
		CancelRatio: 0.10,

		PriceSpreadCents: 100, // ±$1.00 from mid
		MaxQuantity:      10,

		TargetP99Ns:      10_000_000, // 10 ms — achievable by any correct engine
		TargetSustainTPS: 5_000,      // 5k orders/s — baseline throughput

		LatencyWeight:     0.20,
		ThroughputWeight:  0.30,
		CorrectnessWeight: 0.50,
	}
}

// DefaultMediumProfile returns the default VolatilityProfile for the "medium"
// benchmark tier.
//
// Design intent — medium tier:
//   - 100 bots — real concurrency pressure starts here.
//   - More market orders (30%) and cancels (10%) create aggressive order-book
//     churn that stresses lock contention in naive implementations.
//   - Tighter p99 target (5ms) rewards optimized hot paths.
//   - Equal latency/throughput weights (0.35 each) with correctness backing
//     off to 0.30 — the engine is assumed correct; now rank on speed.
func DefaultMediumProfile() models.VolatilityProfile {
	return models.VolatilityProfile{
		Label:        "medium",
		BotCount:     100,
		TestDuration: 120 * time.Second,

		LimitRatio:  0.60,
		MarketRatio: 0.30,
		CancelRatio: 0.10,

		PriceSpreadCents: 500, // ±$5.00 — moderate volatility
		MaxQuantity:      100,

		TargetP99Ns:      5_000_000, // 5 ms
		TargetSustainTPS: 20_000,    // 20k orders/s

		LatencyWeight:     0.35,
		ThroughputWeight:  0.35,
		CorrectnessWeight: 0.30,
	}
}

// DefaultHighProfile returns the default VolatilityProfile for the "high"
// benchmark tier.
//
// Design intent — high tier:
//   - 1000 bots — extreme concurrency mimicking peak exchange load.
//   - Balanced order mix with significant cancels (20%) churns the resting
//     book aggressively and stresses cancel-path correctness.
//   - Very tight p99 target (1ms) — only lock-free or highly optimized engines
//     will achieve normP99 ≈ 1.0.
//   - Throughput weighted highest (0.50) — at this tier raw TPS separates
//     the top of the leaderboard. Correctness still acts as a multiplier.
func DefaultHighProfile() models.VolatilityProfile {
	return models.VolatilityProfile{
		Label:        "high",
		BotCount:     1_000,
		TestDuration: 180 * time.Second,

		LimitRatio:  0.40,
		MarketRatio: 0.40,
		CancelRatio: 0.20,

		PriceSpreadCents: 2_000, // ±$20.00 — extreme volatility
		MaxQuantity:      1_000,

		TargetP99Ns:      1_000_000, // 1 ms
		TargetSustainTPS: 50_000,    // 50k orders/s

		LatencyWeight:     0.20,
		ThroughputWeight:  0.50,
		CorrectnessWeight: 0.30,
	}
}
