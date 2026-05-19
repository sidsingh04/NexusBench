package consumer

// consumer_test.go tests the Consumer pipeline logic in isolation.
// No Redpanda, no TimescaleDB — uses MemoryStore and calls handleRecord()
// directly, bypassing the kgo poll loop entirely.
//
// Tests:
//   TestConsumer_SingleWindow               — events in one bucket → one window
//   TestConsumer_WindowRollover             — two buckets → two windows flushed
//   TestConsumer_HeartbeatIgnored           — heartbeat events produce no window
//   TestConsumer_MultipleSubmissions        — two submissions → independent windows
//   TestConsumer_EmptyWindowNotStored       — no events → no DB write
//   TestConsumer_StoreReceivesCorrectStats  — p99/tps values flow through correctly
//   TestConsumer_FlushAll                   — shutdown flushes all open windows

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// makeRecord converts a telemetry.Event into a *kgo.Record as the Redpanda
// broker would deliver it — simulating what PollFetches returns.
func makeRecord(t *testing.T, e telemetry.Event) *kgo.Record {
	t.Helper()
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("makeRecord: marshal: %v", err)
	}
	return &kgo.Record{
		Topic: telemetry.TopicLatency,
		Key:   []byte(e.SubmissionID),
		Value: b,
	}
}

// newTestConsumer builds a Consumer wired to a MemoryStore, bypassing the
// kgo client construction (which would dial Redpanda). The kgo.Client field
// is nil — safe as long as tests call handleRecord() directly and never Run().
func newTestConsumer(store *MemoryStore) *Consumer {
	return &Consumer{
		client:  nil, // not needed — tests call handleRecord() directly
		store:   store,
		windows: make(map[string]*windowState),
	}
}

// eventAt creates a valid latency event at a specific timestamp.
func eventAt(subID, orderID string, ts time.Time, latencyNs int64) telemetry.Event {
	return telemetry.Event{
		Kind:         telemetry.KindOrderAck,
		SubmissionID: subID,
		Timestamp:    ts,
		OrderID:      orderID,
		LatencyNs:    latencyNs,
	}
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestConsumer_SingleWindow(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	// All three events share the same 5-second bucket.
	base := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	events := []telemetry.Event{
		eventAt("sub-1", "o1", base.Add(0*time.Second), 100),
		eventAt("sub-1", "o2", base.Add(1*time.Second), 200),
		eventAt("sub-1", "o3", base.Add(2*time.Second), 300),
	}

	for i, e := range events {
		r := makeRecord(t, e)
		if err := c.handleRecord(ctx, r); err != nil {
			t.Fatalf("handleRecord[%d]: %v", i, err)
		}
	}

	// Window is still open — not yet flushed (no new bucket arrived).
	if store.Len() != 0 {
		t.Errorf("store.Len() = %d after same-bucket events, want 0 (window still open)", store.Len())
	}

	// Force flush by sending an event in the next bucket.
	nextBucket := base.Add(WindowDuration)
	trigger := eventAt("sub-1", "o4", nextBucket, 50)
	if err := c.handleRecord(ctx, makeRecord(t, trigger)); err != nil {
		t.Fatalf("handleRecord(trigger): %v", err)
	}

	// Now the first window should have been written.
	if store.Len() != 1 {
		t.Errorf("store.Len() = %d after rollover, want 1", store.Len())
	}
}

func TestConsumer_WindowRollover(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	base := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)

	// Bucket 0: 12:00:00 – 12:00:05
	for i := 0; i < 3; i++ {
		e := eventAt("sub-1", "o1", base.Add(time.Duration(i)*time.Second), int64(i+1)*100)
		if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
			t.Fatalf("bucket0 event %d: %v", i, err)
		}
	}

	// Bucket 1: 12:00:05 – 12:00:10 — triggers flush of bucket 0
	bucket1 := base.Add(WindowDuration)
	for i := 0; i < 3; i++ {
		e := eventAt("sub-1", "o2", bucket1.Add(time.Duration(i)*time.Second), int64(i+1)*50)
		if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
			t.Fatalf("bucket1 event %d: %v", i, err)
		}
	}

	// After bucket 1 events, bucket 0 is flushed, bucket 1 is still open.
	if store.Len() != 1 {
		t.Errorf("store.Len() = %d after bucket1 started, want 1", store.Len())
	}

	// Bucket 2 triggers flush of bucket 1.
	bucket2 := base.Add(2 * WindowDuration)
	e := eventAt("sub-1", "o3", bucket2, 10)
	if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
		t.Fatalf("bucket2 trigger: %v", err)
	}

	if store.Len() != 2 {
		t.Errorf("store.Len() = %d after bucket2 started, want 2", store.Len())
	}
}

func TestConsumer_HeartbeatIgnored(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	hb := telemetry.Event{
		Kind:         telemetry.KindHeartbeat,
		SubmissionID: "sub-1",
		Timestamp:    time.Now().UTC(),
		// No OrderID — valid for heartbeat
	}
	r := makeRecord(t, hb)
	if err := c.handleRecord(ctx, r); err != nil {
		t.Fatalf("handleRecord(heartbeat): %v", err)
	}

	// No window opened, no store write.
	if len(c.windows) != 0 {
		t.Errorf("heartbeat opened a window; len(c.windows) = %d, want 0", len(c.windows))
	}
	if store.Len() != 0 {
		t.Errorf("heartbeat caused a store write; store.Len() = %d, want 0", store.Len())
	}
}

func TestConsumer_MultipleSubmissions(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	base := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)

	// Interleave events from two submissions in the same bucket.
	for i := 0; i < 3; i++ {
		eA := eventAt("sub-A", "oA", base.Add(time.Duration(i)*time.Second), 100)
		eB := eventAt("sub-B", "oB", base.Add(time.Duration(i)*time.Second), 200)
		if err := c.handleRecord(ctx, makeRecord(t, eA)); err != nil {
			t.Fatalf("sub-A event %d: %v", i, err)
		}
		if err := c.handleRecord(ctx, makeRecord(t, eB)); err != nil {
			t.Fatalf("sub-B event %d: %v", i, err)
		}
	}

	// Two windows open, nothing flushed yet.
	if len(c.windows) != 2 {
		t.Errorf("len(c.windows) = %d, want 2", len(c.windows))
	}
	if store.Len() != 0 {
		t.Errorf("store.Len() = %d before rollover, want 0", store.Len())
	}

	// Trigger rollover for both.
	next := base.Add(WindowDuration)
	for _, sub := range []string{"sub-A", "sub-B"} {
		e := eventAt(sub, "trigger", next, 50)
		if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
			t.Fatalf("rollover trigger for %s: %v", sub, err)
		}
	}

	if store.Len() != 2 {
		t.Errorf("store.Len() = %d after rollover, want 2", store.Len())
	}
}

func TestConsumer_StoreReceivesCorrectStats(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	// Add events with latencies 1..100 ns — known ground truth for percentiles.
	base := time.Date(2026, 5, 17, 12, 0, 0, 0, time.UTC)
	for i := int64(1); i <= 100; i++ {
		e := eventAt("sub-stat", "o", base, i)
		if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
			t.Fatalf("event %d: %v", i, err)
		}
	}

	// Trigger flush.
	trigger := eventAt("sub-stat", "trigger", base.Add(WindowDuration), 1)
	if err := c.handleRecord(ctx, makeRecord(t, trigger)); err != nil {
		t.Fatalf("trigger: %v", err)
	}

	if store.Len() != 1 {
		t.Fatalf("store.Len() = %d, want 1", store.Len())
	}

	w := store.WrittenWindows()[0]

	if w.SampleN != 100 {
		t.Errorf("SampleN = %d, want 100", w.SampleN)
	}
	// P99 of [1..100]: ceil(0.99*100)=99 → index 98 → value 99
	if w.P99Ns != 99 {
		t.Errorf("P99Ns = %d, want 99", w.P99Ns)
	}
	// P50 of [1..100]: ceil(0.50*100)=50 → index 49 → value 50
	if w.P50Ns != 50 {
		t.Errorf("P50Ns = %d, want 50", w.P50Ns)
	}
	// TPS = 100 / 5.0 = 20.0
	wantTPS := float64(100) / WindowDuration.Seconds()
	if w.TPS != wantTPS {
		t.Errorf("TPS = %f, want %f", w.TPS, wantTPS)
	}
	if w.SubmissionID != "sub-stat" {
		t.Errorf("SubmissionID = %q, want %q", w.SubmissionID, "sub-stat")
	}
}

func TestConsumer_FlushAll(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(WindowDuration)

	// Open windows for three submissions without triggering rollover.
	for _, sub := range []string{"sub-1", "sub-2", "sub-3"} {
		e := eventAt(sub, "o", base, 100)
		if err := c.handleRecord(ctx, makeRecord(t, e)); err != nil {
			t.Fatalf("handleRecord(%s): %v", sub, err)
		}
	}

	if len(c.windows) != 3 {
		t.Fatalf("expected 3 open windows before flushAll, got %d", len(c.windows))
	}

	// Simulate shutdown.
	c.flushAll(ctx)

	if store.Len() != 3 {
		t.Errorf("store.Len() = %d after flushAll, want 3", store.Len())
	}
	if len(c.windows) != 0 {
		t.Errorf("len(c.windows) = %d after flushAll, want 0", len(c.windows))
	}
}

func TestConsumer_MalformedRecordDoesNotCrash(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	c := newTestConsumer(store)
	ctx := context.Background()

	// Inject a record with invalid JSON.
	bad := &kgo.Record{
		Topic: telemetry.TopicLatency,
		Key:   []byte("sub-bad"),
		Value: []byte("this is not json {{{"),
	}

	err := c.handleRecord(ctx, bad)
	// Must return an error, not panic.
	if err == nil {
		t.Error("handleRecord(malformed JSON) = nil, want error")
	}
	// Store must be untouched.
	if store.Len() != 0 {
		t.Errorf("store.Len() = %d after malformed record, want 0", store.Len())
	}
}
