package telemetry_test

// This file tests the telemetry package in isolation — no Docker, no network,
// no filesystem writes (other than what t.TempDir gives us).
//
// Test structure:
//   TestEvent_Validate          — table-driven: every valid and invalid shape
//   TestStdoutEmitter_Emit      — golden output + concurrency safety
//   TestStdoutEmitter_HTMLEscape — angle brackets must NOT be escaped
//   TestNoopEmitter             — smoke test (always passes, never panics)
//   TestRecordingEmitter        — record, query, reset lifecycle
//   TestRecordingEmitter_Concurrent — race-detector check

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// validEvent returns a well-formed Event for reuse across tests.
// Tests that want a specific shape clone this and mutate it.
func validEvent() telemetry.Event {
	return telemetry.Event{
		Kind:         telemetry.KindOrderAck,
		SubmissionID: "sub-abc-123",
		Timestamp:    time.Now().UTC(),
		OrderID:      "ord-xyz-456",
		LatencyNs:    312_000, // 312 µs
		Meta:         map[string]string{"side": "buy"},
	}
}

// ── TestEvent_Validate ────────────────────────────────────────────────────────

func TestEvent_Validate(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()

	cases := []struct {
		name    string
		event   telemetry.Event
		wantErr bool // true → Validate must return non-nil
	}{
		// ── valid events ──────────────────────────────────────────────────────
		{
			name: "valid order_ack",
			event: telemetry.Event{
				Kind: telemetry.KindOrderAck, SubmissionID: "s1",
				Timestamp: now, OrderID: "o1", LatencyNs: 1000,
			},
		},
		{
			name: "valid fill with meta",
			event: telemetry.Event{
				Kind: telemetry.KindFill, SubmissionID: "s2",
				Timestamp: now, OrderID: "o2", LatencyNs: 0,
				Meta: map[string]string{"qty": "100"},
			},
		},
		{
			name: "valid cancel_ack",
			event: telemetry.Event{
				Kind: telemetry.KindCancelAck, SubmissionID: "s3",
				Timestamp: now, OrderID: "o3", LatencyNs: 500,
			},
		},
		{
			name: "valid reject",
			event: telemetry.Event{
				Kind: telemetry.KindReject, SubmissionID: "s4",
				Timestamp: now, OrderID: "o4", LatencyNs: 200,
			},
		},
		{
			name: "valid heartbeat — no order_id required",
			event: telemetry.Event{
				Kind: telemetry.KindHeartbeat, SubmissionID: "s5",
				Timestamp: now, LatencyNs: 0,
				// OrderID intentionally absent
			},
		},
		{
			name: "zero latency is valid",
			event: telemetry.Event{
				Kind: telemetry.KindOrderAck, SubmissionID: "s6",
				Timestamp: now, OrderID: "o6", LatencyNs: 0,
			},
		},

		// ── invalid events ────────────────────────────────────────────────────
		{
			name:    "empty Kind",
			event:   telemetry.Event{SubmissionID: "s", Timestamp: now, OrderID: "o", LatencyNs: 1},
			wantErr: true,
		},
		{
			name: "unknown Kind",
			event: telemetry.Event{
				Kind: "made_up_kind", SubmissionID: "s", Timestamp: now, OrderID: "o", LatencyNs: 1,
			},
			wantErr: true,
		},
		{
			name: "empty SubmissionID",
			event: telemetry.Event{
				Kind: telemetry.KindOrderAck, Timestamp: now, OrderID: "o", LatencyNs: 1,
			},
			wantErr: true,
		},
		{
			name: "zero Timestamp",
			event: telemetry.Event{
				Kind: telemetry.KindOrderAck, SubmissionID: "s", OrderID: "o", LatencyNs: 1,
				// Timestamp is zero value
			},
			wantErr: true,
		},
		{
			name: "negative LatencyNs",
			event: telemetry.Event{
				Kind: telemetry.KindOrderAck, SubmissionID: "s",
				Timestamp: now, OrderID: "o", LatencyNs: -1,
			},
			wantErr: true,
		},
		{
			name: "non-heartbeat missing OrderID",
			event: telemetry.Event{
				Kind: telemetry.KindFill, SubmissionID: "s",
				Timestamp: now, LatencyNs: 100,
				// OrderID absent
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.event.Validate()
			if tc.wantErr && err == nil {
				t.Errorf("Validate() = nil, want error")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("Validate() = %v, want nil", err)
			}
		})
	}
}

// ── TestStdoutEmitter_Emit ────────────────────────────────────────────────────

func TestStdoutEmitter_Emit_ValidEvent(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)

	e := validEvent()
	if err := emitter.Emit(context.Background(), e); err != nil {
		t.Fatalf("Emit returned unexpected error: %v", err)
	}

	// Must be valid JSON
	line := strings.TrimSpace(buf.String())
	if line == "" {
		t.Fatal("Emit wrote nothing to the buffer")
	}

	var got map[string]any
	if err := json.Unmarshal([]byte(line), &got); err != nil {
		t.Fatalf("output is not valid JSON: %v\noutput: %s", err, line)
	}

	// Spot-check required fields are present
	for _, field := range []string{"kind", "submission_id", "timestamp", "order_id", "latency_ns"} {
		if _, ok := got[field]; !ok {
			t.Errorf("output JSON missing field %q\noutput: %s", field, line)
		}
	}

	// Kind must match
	if got["kind"] != string(e.Kind) {
		t.Errorf("kind: got %v, want %v", got["kind"], e.Kind)
	}

	// SubmissionID must match
	if got["submission_id"] != e.SubmissionID {
		t.Errorf("submission_id: got %v, want %v", got["submission_id"], e.SubmissionID)
	}
}

func TestStdoutEmitter_Emit_InvalidEventReturnsError(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)

	bad := telemetry.Event{} // zero value — Kind and SubmissionID are empty
	err := emitter.Emit(context.Background(), bad)
	if err == nil {
		t.Fatal("Emit(invalid event) = nil, want error")
	}
	if buf.Len() > 0 {
		t.Errorf("Emit wrote output for an invalid event: %q", buf.String())
	}
}

func TestStdoutEmitter_HTMLEscaping_Disabled(t *testing.T) {
	t.Parallel()
	// FIX protocol tags use angle brackets — they must NOT be \u003c-escaped.
	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)

	e := validEvent()
	e.Meta = map[string]string{"fix_tag": "<Price>100.5</Price>"}

	if err := emitter.Emit(context.Background(), e); err != nil {
		t.Fatalf("Emit: %v", err)
	}

	line := buf.String()
	if strings.Contains(line, `\u003c`) || strings.Contains(line, `\u003e`) {
		t.Errorf("HTML escaping is ON — angle brackets were escaped in output:\n%s", line)
	}
	if !strings.Contains(line, "<Price>") {
		t.Errorf("angle brackets missing from output:\n%s", line)
	}
}

func TestStdoutEmitter_Emit_Concurrent(t *testing.T) {
	t.Parallel()
	// Run with -race — if the mutex in StdoutEmitter is wrong, the race
	// detector will catch a data race on the json.Encoder's internal buffer.

	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)

	const goroutines = 20
	const eventsEach = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsEach; j++ {
				e := validEvent()
				if err := emitter.Emit(context.Background(), e); err != nil {
					t.Errorf("concurrent Emit error: %v", err)
				}
			}
		}()
	}
	wg.Wait()

	// Every line must be valid JSON
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if want := goroutines * eventsEach; len(lines) != want {
		t.Errorf("line count: got %d, want %d", len(lines), want)
	}
	for i, line := range lines {
		var obj map[string]any
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Errorf("line %d is not valid JSON: %v\n%s", i, err, line)
		}
	}
}

func TestStdoutEmitter_Close_IsNoop(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)
	if err := emitter.Close(); err != nil {
		t.Errorf("Close() = %v, want nil", err)
	}
	// Second close must also be a no-op (not panic)
	if err := emitter.Close(); err != nil {
		t.Errorf("second Close() = %v, want nil", err)
	}
}

// ── TestNoopEmitter ───────────────────────────────────────────────────────────

func TestNoopEmitter(t *testing.T) {
	t.Parallel()
	var n telemetry.NoopEmitter

	// Valid event — must silently succeed
	if err := n.Emit(context.Background(), validEvent()); err != nil {
		t.Errorf("NoopEmitter.Emit(valid) = %v, want nil", err)
	}

	// NoopEmitter skips validation by design — it is intentionally lenient.
	// (It's used in tests where telemetry output is irrelevant.)
	if err := n.Close(); err != nil {
		t.Errorf("NoopEmitter.Close() = %v, want nil", err)
	}
}

// ── TestRecordingEmitter ──────────────────────────────────────────────────────

func TestRecordingEmitter_RecordsEvents(t *testing.T) {
	t.Parallel()
	rec := telemetry.NewRecordingEmitter()

	e1 := validEvent()
	e1.Kind = telemetry.KindOrderAck
	e1.OrderID = "o1"

	e2 := validEvent()
	e2.Kind = telemetry.KindFill
	e2.OrderID = "o2"

	_ = rec.Emit(context.Background(), e1)
	_ = rec.Emit(context.Background(), e2)

	if got := rec.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}

	all := rec.Events()
	if all[0].OrderID != "o1" {
		t.Errorf("Events()[0].OrderID = %q, want %q", all[0].OrderID, "o1")
	}
	if all[1].OrderID != "o2" {
		t.Errorf("Events()[1].OrderID = %q, want %q", all[1].OrderID, "o2")
	}
}

func TestRecordingEmitter_EventsOfKind(t *testing.T) {
	t.Parallel()
	rec := telemetry.NewRecordingEmitter()

	ack := validEvent()
	ack.Kind = telemetry.KindOrderAck
	ack.OrderID = "ack-1"

	fill := validEvent()
	fill.Kind = telemetry.KindFill
	fill.OrderID = "fill-1"

	hb := telemetry.Event{
		Kind: telemetry.KindHeartbeat, SubmissionID: "s", Timestamp: time.Now().UTC(),
	}

	_ = rec.Emit(context.Background(), ack)
	_ = rec.Emit(context.Background(), fill)
	_ = rec.Emit(context.Background(), hb)

	acks := rec.EventsOfKind(telemetry.KindOrderAck)
	if len(acks) != 1 {
		t.Errorf("EventsOfKind(OrderAck): got %d, want 1", len(acks))
	}
	fills := rec.EventsOfKind(telemetry.KindFill)
	if len(fills) != 1 {
		t.Errorf("EventsOfKind(Fill): got %d, want 1", len(fills))
	}
	hbs := rec.EventsOfKind(telemetry.KindHeartbeat)
	if len(hbs) != 1 {
		t.Errorf("EventsOfKind(Heartbeat): got %d, want 1", len(hbs))
	}
}

func TestRecordingEmitter_InvalidEventReturnsError(t *testing.T) {
	t.Parallel()
	rec := telemetry.NewRecordingEmitter()

	bad := telemetry.Event{} // zero value
	err := rec.Emit(context.Background(), bad)
	if err == nil {
		t.Fatal("Emit(invalid) = nil, want error")
	}
	if rec.Len() != 0 {
		t.Errorf("invalid event was recorded; Len() = %d, want 0", rec.Len())
	}
}

func TestRecordingEmitter_Reset(t *testing.T) {
	t.Parallel()
	rec := telemetry.NewRecordingEmitter()

	_ = rec.Emit(context.Background(), validEvent())
	_ = rec.Emit(context.Background(), validEvent())
	if rec.Len() != 2 {
		t.Fatalf("before Reset: Len() = %d, want 2", rec.Len())
	}

	rec.Reset()
	if rec.Len() != 0 {
		t.Errorf("after Reset: Len() = %d, want 0", rec.Len())
	}
}

func TestRecordingEmitter_Concurrent(t *testing.T) {
	t.Parallel()
	rec := telemetry.NewRecordingEmitter()

	const goroutines = 10
	const eventsEach = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < eventsEach; j++ {
				_ = rec.Emit(context.Background(), validEvent())
			}
		}()
	}
	wg.Wait()

	if got, want := rec.Len(), goroutines*eventsEach; got != want {
		t.Errorf("concurrent Len() = %d, want %d", got, want)
	}
}

func TestRecordingEmitter_EventsReturnsCopy(t *testing.T) {
	t.Parallel()
	// Mutating the returned slice must not affect internal state.
	rec := telemetry.NewRecordingEmitter()
	_ = rec.Emit(context.Background(), validEvent())

	snapshot := rec.Events()
	snapshot[0].OrderID = "tampered"

	fresh := rec.Events()
	if fresh[0].OrderID == "tampered" {
		t.Error("Events() returned a direct reference to internal slice — mutation leaked")
	}
}
