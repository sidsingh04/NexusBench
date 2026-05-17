package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
)

// Emitter is the single abstraction every event producer depends on.
// Keeping this interface minimal (two methods) means:
//
//  1. StdoutEmitter, RedpandaEmitter, and test fakes all implement it trivially.
//  2. The matching engine has zero dependency on any transport library.
//  3. Swapping transports in Step 2 requires zero changes to engine code.
//
// Contract:
//   - Emit validates the event before sending; it returns an error if the
//     event is malformed or if the underlying transport fails.
//   - Emit MUST NOT block the caller for more than a few milliseconds.
//     Implementations that need buffering (e.g. Redpanda batch producer)
//     must do so internally without holding up the hot path.
//   - Close flushes any buffered events and releases resources.
//     It is safe to call Close more than once; subsequent calls are no-ops.
type Emitter interface {
	Emit(ctx context.Context, e Event) error
	Close() error
}

// ── StdoutEmitter ─────────────────────────────────────────────────────────────

// StdoutEmitter serialises every Event as a JSON object followed by a newline
// (NDJSON / JSON Lines format) and writes it to an io.Writer.
//
// Why NDJSON?
//   - One event per line → trivially parseable with `jq` or any log shipper.
//   - No framing overhead.
//   - `cat output.ndjson | jq .` gives pretty-printed inspection for free.
//
// The zero value is NOT valid. Use NewStdoutEmitter.
type StdoutEmitter struct {
	mu  sync.Mutex // protects enc; json.Encoder is not concurrent-safe
	enc *json.Encoder
	w   io.Writer
}

// NewStdoutEmitter returns a StdoutEmitter that writes to w.
// Pass os.Stdout for production use; pass a *bytes.Buffer in tests.
func NewStdoutEmitter(w io.Writer) *StdoutEmitter {
	enc := json.NewEncoder(w)
	// DisableHTMLEscaping preserves raw angle brackets in Meta values, which
	// is important for FIX protocol tags that use < and >.
	enc.SetEscapeHTML(false)
	return &StdoutEmitter{enc: enc, w: w}
}

// DefaultStdoutEmitter returns a StdoutEmitter writing to os.Stdout.
// Convenience constructor for use in cmd/ entrypoints.
func DefaultStdoutEmitter() *StdoutEmitter {
	return NewStdoutEmitter(os.Stdout)
}

// Emit validates e, then writes it as a single JSON line.
// It is safe to call Emit concurrently from multiple goroutines.
func (s *StdoutEmitter) Emit(_ context.Context, e Event) error {
	// Validate before acquiring the lock — validation is CPU-only and we
	// don't want to hold the writer mutex during it.
	if err := e.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(e)
}

// Close is a no-op for StdoutEmitter (the caller owns the writer).
// Implemented so StdoutEmitter satisfies the Emitter interface and can be
// swapped for RedpandaEmitter (which does flush on close) without caller changes.
func (s *StdoutEmitter) Close() error {
	return nil
}

// ── NoopEmitter ───────────────────────────────────────────────────────────────

// NoopEmitter discards every event silently.
// Use in unit tests where the test is not about telemetry output.
type NoopEmitter struct{}

func (n NoopEmitter) Emit(_ context.Context, _ Event) error { return nil }
func (n NoopEmitter) Close() error                          { return nil }

// ── RecordingEmitter ─────────────────────────────────────────────────────────

// RecordingEmitter buffers every emitted Event in memory.
// Use in tests that need to assert on what was emitted.
//
// Example:
//
//	rec := telemetry.NewRecordingEmitter()
//	engine.SetEmitter(rec)
//	engine.ProcessOrder(ctx, order)
//	events := rec.Events()
//	// assert events[0].Kind == telemetry.KindOrderAck, etc.
type RecordingEmitter struct {
	mu     sync.Mutex
	events []Event
}

func NewRecordingEmitter() *RecordingEmitter {
	return &RecordingEmitter{}
}

// Emit validates and records the event. Returns an error if invalid.
func (r *RecordingEmitter) Emit(_ context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// Close is a no-op; implemented to satisfy Emitter.
func (r *RecordingEmitter) Close() error { return nil }

// Events returns a snapshot of all recorded events in emission order.
// Safe to call concurrently with Emit.
func (r *RecordingEmitter) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

// EventsOfKind returns only events matching the given Kind.
func (r *RecordingEmitter) EventsOfKind(k Kind) []Event {
	all := r.Events()
	var out []Event
	for _, e := range all {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

// Reset clears all recorded events. Useful between sub-tests.
func (r *RecordingEmitter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = r.events[:0]
}

// Len returns the total number of recorded events.
func (r *RecordingEmitter) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}
