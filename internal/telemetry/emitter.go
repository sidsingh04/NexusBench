package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"sync"
)

// Emitter is the single abstraction every event producer depends on.
//
// Contract:
//   - Emit validates the event before sending; it returns an error if the
//     event is malformed or if the underlying transport fails.
//   - BatchEmit is the preferred path for high-throughput producers (e.g.
//     the bot fleet). Implementations must buffer internally and not block
//     the caller longer than a single round-trip to the transport.
//   - Emit and BatchEmit MUST be safe to call concurrently.
//   - Close flushes any buffered events and releases resources.
//     It is safe to call Close more than once; subsequent calls are no-ops.
type Emitter interface {
	Emit(ctx context.Context, e Event) error

	// BatchEmit sends a slice of events in one logical operation.
	// Callers should batch events in slices of ~100 before calling — this
	// avoids per-event lock/unlock overhead and (for Redpanda) amortises
	// network round-trips across multiple records.
	//
	// BatchEmit validates every event before sending. If any event is invalid
	// it is skipped (not sent) and a combined error is returned after all
	// valid events have been sent. Partial sends are therefore possible; the
	// caller should log the error but need not treat it as fatal.
	BatchEmit(ctx context.Context, events []Event) error

	Close() error
}

// ── StdoutEmitter ─────────────────────────────────────────────────────────────

// StdoutEmitter serialises every Event as NDJSON (one JSON object per line).
// The zero value is NOT valid. Use NewStdoutEmitter.
type StdoutEmitter struct {
	mu  sync.Mutex
	enc *json.Encoder
	w   io.Writer
}

func NewStdoutEmitter(w io.Writer) *StdoutEmitter {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return &StdoutEmitter{enc: enc, w: w}
}

func DefaultStdoutEmitter() *StdoutEmitter {
	return NewStdoutEmitter(os.Stdout)
}

func (s *StdoutEmitter) Emit(_ context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enc.Encode(e)
}

// BatchEmit writes each event as a separate JSON line, holding the lock for
// the whole batch so no interleaving can occur between concurrent callers.
// Invalid events are skipped and reported in the returned error.
func (s *StdoutEmitter) BatchEmit(_ context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Validate all events before acquiring the lock.
	var errs []error
	valid := make([]Event, 0, len(events))
	for i := range events {
		if err := events[i].Validate(); err != nil {
			errs = append(errs, err)
			continue
		}
		valid = append(valid, events[i])
	}

	if len(valid) > 0 {
		s.mu.Lock()
		for i := range valid {
			if encErr := s.enc.Encode(valid[i]); encErr != nil {
				errs = append(errs, encErr)
			}
		}
		s.mu.Unlock()
	}

	return joinErrors(errs)
}

func (s *StdoutEmitter) Close() error { return nil }

// ── NoopEmitter ───────────────────────────────────────────────────────────────

// NoopEmitter discards every event silently. Use in unit tests.
type NoopEmitter struct{}

func (n NoopEmitter) Emit(_ context.Context, _ Event) error              { return nil }
func (n NoopEmitter) BatchEmit(_ context.Context, _ []Event) error       { return nil }
func (n NoopEmitter) Close() error                                        { return nil }

// ── RecordingEmitter ─────────────────────────────────────────────────────────

// RecordingEmitter buffers every emitted Event in memory.
// Use in tests that need to assert on what was emitted.
type RecordingEmitter struct {
	mu     sync.Mutex
	events []Event
}

func NewRecordingEmitter() *RecordingEmitter {
	return &RecordingEmitter{}
}

func (r *RecordingEmitter) Emit(_ context.Context, e Event) error {
	if err := e.Validate(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

// BatchEmit validates and records all valid events; returns combined error for
// any invalid ones.
func (r *RecordingEmitter) BatchEmit(_ context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	var errs []error
	valid := make([]Event, 0, len(events))
	for i := range events {
		if err := events[i].Validate(); err != nil {
			errs = append(errs, err)
			continue
		}
		valid = append(valid, events[i])
	}

	if len(valid) > 0 {
		r.mu.Lock()
		r.events = append(r.events, valid...)
		r.mu.Unlock()
	}

	return joinErrors(errs)
}

func (r *RecordingEmitter) Close() error { return nil }

func (r *RecordingEmitter) Events() []Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Event, len(r.events))
	copy(out, r.events)
	return out
}

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

func (r *RecordingEmitter) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = r.events[:0]
}

func (r *RecordingEmitter) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.events)
}

// ── joinErrors ────────────────────────────────────────────────────────────────

// joinErrors combines multiple errors into one. Returns nil if the slice is
// empty or contains only nils. Uses errors.Join when available (Go 1.20+).
func joinErrors(errs []error) error {
	var nonNil []error
	for _, e := range errs {
		if e != nil {
			nonNil = append(nonNil, e)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	// errors.Join is stdlib since Go 1.20 — safe to use (go.mod requires 1.25).
	return errorf("batch: %d event(s) failed: %v", len(nonNil), nonNil[0])
}
