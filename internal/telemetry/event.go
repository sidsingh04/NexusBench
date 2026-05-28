// Package telemetry defines the structured event model that every component
// in NexusBench emits, and the Emitter interface that decouples event
// production from event transport.
//
// Design rule: this file has NO external dependencies — only the standard
// library. Any component (matching engine, replay engine, bot fleet) can
// import this package without pulling in Kafka, Redpanda, or any I/O library.
// Transport backends live in separate files and implement the Emitter interface.
package telemetry

import "time"

// Kind classifies what happened in a single event.
// Using typed constants (not bare strings) means a mistyped kind is a
// compile-time error, not a silent data quality problem at query time.
type Kind string

const (
	// KindOrderAck is emitted when the matching engine acknowledges receipt
	// of a new order. Latency = time from order arrival to ack sent.
	KindOrderAck Kind = "order_ack"

	// KindFill is emitted when an order is fully or partially filled.
	// Latency = time from order arrival to fill execution.
	KindFill Kind = "fill"

	// KindCancelAck is emitted when a cancel request is acknowledged.
	// Latency = time from cancel request arrival to ack sent.
	KindCancelAck Kind = "cancel_ack"

	// KindReject is emitted when an order is rejected (e.g. invalid price,
	// insufficient balance). Latency is still meaningful — it measures how
	// quickly the engine detected and reported the rejection.
	KindReject Kind = "reject"

	// KindHeartbeat is emitted periodically by a running engine to signal
	// liveness. It carries no order ID and zero latency.
	KindHeartbeat Kind = "heartbeat"
)

// Event is the atomic unit of telemetry emitted by a matching engine or
// any other NexusBench component.
//
// Serialization contract (NDJSON on stdout, Kafka message body in Step 2):
//
//	{
//	  "kind":          "order_ack",
//	  "submission_id": "abc-123",
//	  "timestamp":     "2026-05-17T12:00:00.000000123Z",
//	  "order_id":      "ord-456",
//	  "latency_ns":    312000,
//	  "meta":          {"side": "buy", "quantity": "100"}
//	}
//
// Rules:
//   - Timestamp is always UTC and always nanosecond-precision.
//   - LatencyNs is the raw nanosecond value; consumers compute ms/µs as needed.
//   - OrderID is empty for KindHeartbeat events.
//   - Meta carries optional, event-specific key/value pairs.
//     Keep Meta entries to ≤ 5 keys — they are not indexed.
type Event struct {
	Kind         Kind              `json:"kind"`
	SubmissionID string            `json:"submission_id"`
	Timestamp    time.Time         `json:"timestamp"`
	OrderID      string            `json:"order_id,omitempty"`
	LatencyNs    int64             `json:"latency_ns"`
	Meta         map[string]string `json:"meta,omitempty"`
}

// Validate returns a non-nil error if the event is structurally invalid.
// Emitters call this before attempting to send, so broken events are caught
// as close to the source as possible.
func (e *Event) Validate() error {
	if e.Kind == "" {
		return errorf("event.Kind must not be empty")
	}
	if !isKnownKind(e.Kind) {
		return errorf("unknown event Kind %q", e.Kind)
	}
	if e.SubmissionID == "" {
		return errorf("event.SubmissionID must not be empty")
	}
	if e.Timestamp.IsZero() {
		return errorf("event.Timestamp must not be zero")
	}
	if e.LatencyNs < 0 {
		return errorf("event.LatencyNs must be >= 0, got %d", e.LatencyNs)
	}
	// KindHeartbeat deliberately has no OrderID — that's fine.
	// All other kinds should carry an OrderID for traceability.
	if e.Kind != KindHeartbeat && e.OrderID == "" {
		return errorf("event.OrderID must not be empty for Kind %q", e.Kind)
	}
	return nil
}

// isKnownKind returns true for any Kind constant defined in this package.
// Centralizing this check means adding a new Kind only requires editing one
// place (the const block above) — Validate and any switch statements that
// exhaustively cover Kinds will break at compile time if you forget to update.
func isKnownKind(k Kind) bool {
	switch k {
	case KindOrderAck, KindFill, KindCancelAck, KindReject, KindHeartbeat:
		return true
	}
	return false
}
