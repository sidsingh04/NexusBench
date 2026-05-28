package telemetry

// TopicForKind maps an event Kind to its canonical Redpanda topic name.
// All topic names are defined here — a single source of truth that both
// the producer (RedpandaEmitter) and the consumer (Step 3) import.
//
// Topic layout:
//
//	metrics.latency     — order_ack, fill, cancel_ack, reject events (carry LatencyNs)
//	metrics.heartbeat   — heartbeat events (liveness signals, no latency)
//	metrics.dlq         — dead-letter: events that could not be delivered after retries
//
// Why separate latency from heartbeat?
//
//	The consumer in Step 3 will compute p50/p90/p99 from metrics.latency.
//	Mixing heartbeats into that topic would pollute the percentile calculation
//	with zero-latency rows that don't represent real order processing.
const (
	TopicLatency   = "metrics.latency"
	TopicHeartbeat = "metrics.heartbeat"
	TopicDLQ       = "metrics.dlq"
)

// TopicForKind returns the correct topic for a given event Kind.
// Returns TopicDLQ for any unrecognized Kind — belt-and-suspenders guard
// so a future Kind that hasn't been wired up here doesn't silently drop events.
func TopicForKind(k Kind) string {
	switch k {
	case KindOrderAck, KindFill, KindCancelAck, KindReject:
		return TopicLatency
	case KindHeartbeat:
		return TopicHeartbeat
	default:
		return TopicDLQ
	}
}

// AllTopics returns every topic the RedpandaEmitter will create on startup.
// The bootstrap logic iterates this slice — adding a new topic only requires
// adding it here and to TopicForKind above.
func AllTopics() []string {
	return []string{TopicLatency, TopicHeartbeat, TopicDLQ}
}
