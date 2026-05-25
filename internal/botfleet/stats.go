package botfleet

import (
	"sort"
)

// BotStats holds the aggregated performance metrics computed from a fleet run.
// All latency fields are in nanoseconds internally; callers convert to ms as
// needed using the helper methods.
type BotStats struct {
	// Latency percentiles (nanoseconds).
	P50Ns float64
	P90Ns float64
	P99Ns float64

	// MaxTPS is the peak observed transactions per second, computed from the
	// busiest 100ms window in the result set.
	MaxTPS float64

	// SustainedTPS is total successful orders divided by elapsed wall-clock
	// seconds of the fleet run.
	SustainedTPS float64

	// TotalOrders is the number of orders attempted (including errors).
	TotalOrders int64

	// SuccessfulOrders is the number of orders that received a valid response
	// (Err == nil). This is the denominator for TPS calculations.
	SuccessfulOrders int64

	// ErrorOrders is the number of transport-level errors (timeouts, refused).
	ErrorOrders int64
}

// P50Ms returns the p50 latency in milliseconds.
func (s BotStats) P50Ms() float64 { return s.P50Ns / 1e6 }

// P90Ms returns the p90 latency in milliseconds.
func (s BotStats) P90Ms() float64 { return s.P90Ns / 1e6 }

// P99Ms returns the p99 latency in milliseconds.
func (s BotStats) P99Ms() float64 { return s.P99Ns / 1e6 }

// ComputeStats aggregates a slice of OrderResults into a BotStats summary.
//
// Algorithm:
//   - Latency percentiles: sort the successful latency samples, read at the
//     index corresponding to each percentile using the "nearest rank" method.
//     This is O(n log n) and requires no external library.
//   - MaxTPS: slide a 100ms window over the sorted SentAt timestamps and find
//     the maximum count. This is O(n) after the sort.
//   - SustainedTPS: total successful orders / (lastAckedAt - firstSentAt).
//
// Edge cases:
//   - Empty results → zero BotStats.
//   - All errors → zero latency stats, zero TPS.
//   - Single sample → p50 = p90 = p99 = that sample's latency.
func ComputeStats(results []OrderResult) BotStats {
	if len(results) == 0 {
		return BotStats{}
	}

	var (
		latencies        []int64   // nanoseconds, successful orders only
		sentAts          []int64   // Unix nanoseconds, for TPS window
		totalOrders      int64
		successfulOrders int64
		errorOrders      int64
		firstSentNs      int64 = -1
		lastAckedNs      int64
	)

	for i := range results {
		r := &results[i]
		totalOrders++

		if r.Err != nil {
			errorOrders++
			continue
		}

		successfulOrders++
		latencies = append(latencies, r.LatencyNs)

		sentNs := r.SentAt.UnixNano()
		ackedNs := r.AckedAt.UnixNano()

		sentAts = append(sentAts, sentNs)

		if firstSentNs == -1 || sentNs < firstSentNs {
			firstSentNs = sentNs
		}
		if ackedNs > lastAckedNs {
			lastAckedNs = ackedNs
		}
	}

	stats := BotStats{
		TotalOrders:      totalOrders,
		SuccessfulOrders: successfulOrders,
		ErrorOrders:      errorOrders,
	}

	if len(latencies) == 0 {
		return stats
	}

	// ── Latency percentiles ───────────────────────────────────────────────────
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	stats.P50Ns = float64(percentileValue(latencies, 50))
	stats.P90Ns = float64(percentileValue(latencies, 90))
	stats.P99Ns = float64(percentileValue(latencies, 99))

	// ── SustainedTPS ──────────────────────────────────────────────────────────
	elapsedNs := lastAckedNs - firstSentNs
	if elapsedNs > 0 {
		stats.SustainedTPS = float64(successfulOrders) / (float64(elapsedNs) / 1e9)
	}

	// ── MaxTPS — 100ms sliding window ─────────────────────────────────────────
	stats.MaxTPS = computeMaxTPS(sentAts)

	return stats
}

// percentileValue returns the value at the given percentile (0–100) using the
// "nearest rank" method. The input slice must be sorted in ascending order.
func percentileValue(sorted []int64, pct int) int64 {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	// Nearest rank: ceil(pct/100 * n), then clamp to [1, n].
	rank := (pct*n + 99) / 100 // integer ceiling of pct/100 * n
	if rank < 1 {
		rank = 1
	}
	if rank > n {
		rank = n
	}
	return sorted[rank-1]
}

// computeMaxTPS finds the maximum number of order sends that occurred within
// any 100ms window, then converts to a per-second rate.
//
// sentAts: Unix nanosecond timestamps of order sends (unsorted OK — we sort).
// Returns 0 if sentAts is empty.
func computeMaxTPS(sentAts []int64) float64 {
	if len(sentAts) == 0 {
		return 0
	}

	sort.Slice(sentAts, func(i, j int) bool { return sentAts[i] < sentAts[j] })

	const windowNs = 100_000_000 // 100ms in nanoseconds

	maxCount := 0
	left := 0
	for right := 0; right < len(sentAts); right++ {
		// Advance left until the window [left, right] fits within 100ms.
		for sentAts[right]-sentAts[left] > windowNs {
			left++
		}
		count := right - left + 1
		if count > maxCount {
			maxCount = count
		}
	}

	// maxCount orders occurred in 100ms → maxCount * 10 orders/second.
	return float64(maxCount) * 10.0
}
