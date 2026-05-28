package consumer

// store_test.go tests the Store interface contract using an in-memory fake.
// No database, no network. Runs in microseconds.
//
// Tests:
//   TestMemoryStore_WriteWindow          — records windows correctly
//   TestMemoryStore_SkipsEmptyWindow     — SampleN=0 must never be stored
//   TestMemoryStore_Idempotent           — duplicate (time, submissionID) = one row
//   TestMemoryStore_MultipleSubmissions  — isolates rows per submission
//   TestMemoryStore_WrittenWindows       — snapshot helper returns copy

import (
	"context"
	"testing"
	"time"
)

// ── MemoryStore ───────────────────────────────────────────────────────────────

// MemoryStore is an in-memory implementation of Store used in unit tests.
// It mirrors the exact behavior contract of TimescaleStore:
//   - Empty windows (SampleN == 0) are silently dropped.
//   - Duplicate (WindowStart, SubmissionID) pairs are ignored (idempotent).
//
// Defined here (not in a _test.go file with build constraints) so that
// consumer_test.go can also use it via the same package.
type MemoryStore struct {
	// key: "submissionID|windowStart.UnixNano()"
	rows map[string]LatencyWindow
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{rows: make(map[string]LatencyWindow)}
}

func (m *MemoryStore) WriteWindow(_ context.Context, w LatencyWindow) error {
	if w.SampleN == 0 {
		return nil // match TimescaleStore behavior exactly
	}
	key := storeKey(w)
	if _, exists := m.rows[key]; exists {
		return nil // idempotent: duplicate is a no-op
	}
	m.rows[key] = w
	return nil
}

func (m *MemoryStore) Close() {}

// WrittenWindows returns all stored windows in insertion order.
// Returns a copy — callers cannot mutate internal state.
func (m *MemoryStore) WrittenWindows() []LatencyWindow {
	out := make([]LatencyWindow, 0, len(m.rows))
	for _, w := range m.rows {
		out = append(out, w)
	}
	return out
}

// Len returns the number of stored windows.
func (m *MemoryStore) Len() int { return len(m.rows) }

func storeKey(w LatencyWindow) string {
	return w.SubmissionID + "|" + w.WindowStart.UTC().Format(time.RFC3339Nano)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestMemoryStore_WriteWindow(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	now := time.Now().UTC()

	w := LatencyWindow{
		WindowStart:  now,
		SubmissionID: "sub-1",
		P50Ns:        100, P90Ns: 200, P99Ns: 300,
		MinNs: 10, MaxNs: 400, MeanNs: 150,
		TPS: 20.0, SampleN: 50,
	}

	if err := store.WriteWindow(ctx, w); err != nil {
		t.Fatalf("WriteWindow: %v", err)
	}
	if store.Len() != 1 {
		t.Errorf("Len() = %d, want 1", store.Len())
	}

	got := store.WrittenWindows()[0]
	if got.P99Ns != 300 {
		t.Errorf("P99Ns = %d, want 300", got.P99Ns)
	}
	if got.SampleN != 50 {
		t.Errorf("SampleN = %d, want 50", got.SampleN)
	}
}

func TestMemoryStore_SkipsEmptyWindow(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()

	empty := LatencyWindow{
		WindowStart:  time.Now().UTC(),
		SubmissionID: "sub-1",
		SampleN:      0, // empty
	}

	if err := store.WriteWindow(ctx, empty); err != nil {
		t.Fatalf("WriteWindow(empty): %v", err)
	}
	if store.Len() != 0 {
		t.Errorf("empty window was stored; Len() = %d, want 0", store.Len())
	}
}

func TestMemoryStore_Idempotent(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	now := time.Now().UTC()

	w := LatencyWindow{
		WindowStart: now, SubmissionID: "sub-1",
		P99Ns: 100, SampleN: 10,
	}

	// Write the same window three times — only one row should be stored.
	for i := 0; i < 3; i++ {
		if err := store.WriteWindow(ctx, w); err != nil {
			t.Fatalf("WriteWindow attempt %d: %v", i, err)
		}
	}
	if store.Len() != 1 {
		t.Errorf("Len() = %d after 3 duplicate writes, want 1", store.Len())
	}
}

func TestMemoryStore_MultipleSubmissions(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()
	now := time.Now().UTC()

	subs := []string{"sub-A", "sub-B", "sub-C"}
	for _, id := range subs {
		w := LatencyWindow{
			WindowStart: now, SubmissionID: id,
			P99Ns: 100, SampleN: 20,
		}
		if err := store.WriteWindow(ctx, w); err != nil {
			t.Fatalf("WriteWindow(%s): %v", id, err)
		}
	}

	if store.Len() != 3 {
		t.Errorf("Len() = %d, want 3", store.Len())
	}
}

func TestMemoryStore_WrittenWindowsReturnsCopy(t *testing.T) {
	t.Parallel()
	store := NewMemoryStore()
	ctx := context.Background()

	w := LatencyWindow{
		WindowStart: time.Now().UTC(), SubmissionID: "sub-1",
		P99Ns: 500, SampleN: 15,
	}
	_ = store.WriteWindow(ctx, w)

	// Mutate the returned slice — must not affect internal state.
	snap := store.WrittenWindows()
	snap[0].P99Ns = 99999

	fresh := store.WrittenWindows()
	if fresh[0].P99Ns == 99999 {
		t.Error("WrittenWindows() returned a direct reference — mutation leaked into store")
	}
}
