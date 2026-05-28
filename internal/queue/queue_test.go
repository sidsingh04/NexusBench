package queue_test

// queue_test.go tests the queue package logic in isolation.
// No Redpanda required — uses MemoryQueue exclusively.
//
// Tests:
//   TestMemoryQueue_EnqueueDequeue           — basic round-trip
//   TestMemoryQueue_DequeueBlocksUntilJob    — Dequeue blocks; unblocks when job arrives
//   TestMemoryQueue_CancelledDequeue         — ctx cancel unblocks Dequeue
//   TestMemoryQueue_CloseUnblocksDequeue     — Close unblocks a waiting Dequeue
//   TestMemoryQueue_BufferFull               — Enqueue returns error when buffer full
//   TestMemoryQueue_CommitJobIsNoOp          — CommitJob never errors
//   TestMemoryQueue_QueueDepth_Empty         — QueueDepth returns 0 on empty queue
//   TestMemoryQueue_QueueDepth_AfterEnqueue  — QueueDepth tracks enqueued jobs
//   TestMemoryQueue_QueueDepth_AfterDequeue  — QueueDepth decrements after Dequeue
//   TestMemoryQueue_QueueDepth_Unbuffered    — QueueDepth always 0 for unbuffered queue
//   TestMemoryQueue_QueueDepth_CancelledCtx  — QueueDepth returns 0, nil on canceled ctx
//   TestNewJob_FieldMapping                  — NewJob copies all fields from Submission
//   TestJob_JSONRoundTrip                    — Job survives JSON marshal/unmarshal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func makeSubmission(id, team string, lang models.Language) *models.Submission {
	return &models.Submission{
		ID:          id,
		TeamName:    team,
		Language:    lang,
		Protocol:    models.ProtocolREST,
		ArchivePath: "/submissions/" + id + "/archive.tar.gz",
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestMemoryQueue_EnqueueDequeue(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(4)
	ctx := context.Background()

	sub := makeSubmission("sub-001", "alpha", models.LangGo)
	j := queue.NewJob(sub)

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	got, err := q.Dequeue(ctx)
	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if got.SubmissionID != j.SubmissionID {
		t.Errorf("SubmissionID: got %q, want %q", got.SubmissionID, j.SubmissionID)
	}
	if got.TeamName != "alpha" {
		t.Errorf("TeamName: got %q, want %q", got.TeamName, "alpha")
	}
	if got.Language != models.LangGo {
		t.Errorf("Language: got %q, want %q", got.Language, models.LangGo)
	}
}

func TestMemoryQueue_DequeueBlocksUntilJob(t *testing.T) {
	t.Parallel()
	// Synchronous channel (bufSize=0): Dequeue must block until Enqueue.
	q := queue.NewMemoryQueue(0)
	ctx := context.Background()

	sub := makeSubmission("sub-002", "beta", models.LangRust)
	j := queue.NewJob(sub)

	// Enqueue in a goroutine after a short delay.
	go func() {
		time.Sleep(20 * time.Millisecond)
		if err := q.Enqueue(ctx, j); err != nil {
			_ = err
		}
	}()

	start := time.Now()
	got, err := q.Dequeue(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Dequeue: %v", err)
	}
	if got.SubmissionID != j.SubmissionID {
		t.Errorf("SubmissionID: got %q, want %q", got.SubmissionID, j.SubmissionID)
	}
	if elapsed < 15*time.Millisecond {
		t.Errorf("Dequeue returned too quickly (%v); expected blocking", elapsed)
	}
}

func TestMemoryQueue_CancelledDequeue(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(4)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, err := q.Dequeue(ctx)
	if err == nil {
		t.Fatal("Dequeue on empty queue with canceled ctx should return error")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("err = %v, want context.DeadlineExceeded", err)
	}
}

func TestMemoryQueue_CloseUnblocksDequeue(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(4)
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		_, err := q.Dequeue(ctx)
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	if err := q.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Error("Dequeue after Close should return an error, got nil")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("Dequeue did not unblock after Close within 200ms")
	}
}

func TestMemoryQueue_BufferFull(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(1)
	ctx := context.Background()

	sub := makeSubmission("sub-full", "gamma", models.LangCpp)
	j := queue.NewJob(sub)

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if err := q.Enqueue(ctx, j); err == nil {
		t.Error("second Enqueue on full buffer should return error, got nil")
	}
}

func TestMemoryQueue_CommitJobIsNoOp(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(4)
	ctx := context.Background()

	if err := q.CommitJob(ctx); err != nil {
		t.Errorf("CommitJob (no-op): unexpected error: %v", err)
	}
}

// ── QueueDepth tests ──────────────────────────────────────────────────────────

// TestMemoryQueue_QueueDepth_Empty verifies that a freshly created queue
// reports depth 0 before any jobs are enqueued.
func TestMemoryQueue_QueueDepth_Empty(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(8)
	ctx := context.Background()

	depth, err := q.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth on empty queue: unexpected error: %v", err)
	}
	if depth != 0 {
		t.Errorf("QueueDepth = %d on empty queue, want 0", depth)
	}
}

// TestMemoryQueue_QueueDepth_AfterEnqueue verifies that QueueDepth increases
// by 1 for each job enqueued and matches the enqueued count exactly.
func TestMemoryQueue_QueueDepth_AfterEnqueue(t *testing.T) {
	t.Parallel()
	const jobCount = 5
	q := queue.NewMemoryQueue(jobCount + 2) // extra headroom
	ctx := context.Background()

	for i := 0; i < jobCount; i++ {
		sub := makeSubmission("sub-depth-enq", "team-enq", models.LangGo)
		if err := q.Enqueue(ctx, queue.NewJob(sub)); err != nil {
			t.Fatalf("Enqueue[%d]: %v", i, err)
		}

		depth, err := q.QueueDepth(ctx)
		if err != nil {
			t.Fatalf("QueueDepth after enqueue[%d]: %v", i, err)
		}
		want := int64(i + 1)
		if depth != want {
			t.Errorf("after enqueue[%d]: QueueDepth = %d, want %d", i, depth, want)
		}
	}
}

// TestMemoryQueue_QueueDepth_AfterDequeue verifies that QueueDepth decrements
// correctly as jobs are consumed.
func TestMemoryQueue_QueueDepth_AfterDequeue(t *testing.T) {
	t.Parallel()
	const jobCount = 4
	q := queue.NewMemoryQueue(jobCount)
	ctx := context.Background()

	// Fill the queue.
	for i := 0; i < jobCount; i++ {
		sub := makeSubmission("sub-depth-deq", "team-deq", models.LangRust)
		if err := q.Enqueue(ctx, queue.NewJob(sub)); err != nil {
			t.Fatalf("Enqueue[%d]: %v", i, err)
		}
	}

	// Verify starting depth.
	depth, err := q.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth (full): %v", err)
	}
	if depth != int64(jobCount) {
		t.Errorf("QueueDepth (full) = %d, want %d", depth, jobCount)
	}

	// Consume one by one and verify depth decrements.
	for i := 0; i < jobCount; i++ {
		if _, err := q.Dequeue(ctx); err != nil {
			t.Fatalf("Dequeue[%d]: %v", i, err)
		}
		depth, err := q.QueueDepth(ctx)
		if err != nil {
			t.Fatalf("QueueDepth after dequeue[%d]: %v", i, err)
		}
		want := int64(jobCount - i - 1)
		if depth != want {
			t.Errorf("after dequeue[%d]: QueueDepth = %d, want %d", i, depth, want)
		}
	}
}

// TestMemoryQueue_QueueDepth_Unbuffered verifies that an unbuffered (cap=0)
// MemoryQueue always reports depth 0 — there is no buffer to measure.
func TestMemoryQueue_QueueDepth_Unbuffered(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(0) // unbuffered
	ctx := context.Background()

	depth, err := q.QueueDepth(ctx)
	if err != nil {
		t.Fatalf("QueueDepth on unbuffered queue: %v", err)
	}
	if depth != 0 {
		t.Errorf("QueueDepth on unbuffered queue = %d, want 0", depth)
	}
}

// TestMemoryQueue_QueueDepth_CancelledCtx verifies that QueueDepth does not
// block on a canceled context — the MemoryQueue implementation is non-blocking
// and should return (0, nil) even when ctx is already canceled.
func TestMemoryQueue_QueueDepth_CancelledCtx(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(4)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	// QueueDepth must not block even with a canceled ctx. It should return
	// quickly because MemoryQueue's implementation uses len(ch) — no I/O.
	done := make(chan struct{})
	go func() {
		depth, err := q.QueueDepth(ctx)
		if err != nil {
			t.Errorf("QueueDepth with canceled ctx: unexpected error: %v", err)
		}
		if depth != 0 {
			t.Errorf("QueueDepth with canceled ctx = %d, want 0", depth)
		}
		close(done)
	}()

	select {
	case <-done:
		// passed
	case <-time.After(100 * time.Millisecond):
		t.Error("QueueDepth blocked with canceled ctx — should be non-blocking")
	}
}

// TestMemoryQueue_QueueDepth_SatisfiesInterface ensures *MemoryQueue can be
// assigned to the Queue interface — a compile-time check that QueueDepth is
// correctly implemented. If this file compiles, the test trivially passes.
func TestMemoryQueue_QueueDepth_SatisfiesInterface(t *testing.T) {
	t.Parallel()
	var _ queue.Queue = queue.NewMemoryQueue(1)
}

// ── Existing tests (unchanged) ────────────────────────────────────────────────

func TestNewJob_FieldMapping(t *testing.T) {
	t.Parallel()
	sub := &models.Submission{
		ID:          "sub-field-test",
		TeamName:    "delta",
		Language:    models.LangPython,
		Protocol:    models.ProtocolWebSocket,
		ArchivePath: "/data/sub-field-test/archive.zip",
	}

	before := time.Now()
	j := queue.NewJob(sub)
	after := time.Now()

	if j.ID != sub.ID {
		t.Errorf("ID: got %q, want %q", j.ID, sub.ID)
	}
	if j.SubmissionID != sub.ID {
		t.Errorf("SubmissionID: got %q, want %q", j.SubmissionID, sub.ID)
	}
	if j.TeamName != sub.TeamName {
		t.Errorf("TeamName: got %q, want %q", j.TeamName, sub.TeamName)
	}
	if j.Language != sub.Language {
		t.Errorf("Language: got %q, want %q", j.Language, sub.Language)
	}
	if j.Protocol != sub.Protocol {
		t.Errorf("Protocol: got %q, want %q", j.Protocol, sub.Protocol)
	}
	if j.ArchivePath != sub.ArchivePath {
		t.Errorf("ArchivePath: got %q, want %q", j.ArchivePath, sub.ArchivePath)
	}
	if j.EnqueuedAt.Before(before) || j.EnqueuedAt.After(after) {
		t.Errorf("EnqueuedAt %v outside expected range [%v, %v]",
			j.EnqueuedAt, before, after)
	}
}

func TestJob_JSONRoundTrip(t *testing.T) {
	t.Parallel()
	sub := makeSubmission("sub-json", "epsilon", models.LangBinary)
	original := queue.NewJob(sub)

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded queue.Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.TeamName != original.TeamName {
		t.Errorf("TeamName: got %q, want %q", decoded.TeamName, original.TeamName)
	}
	if decoded.Language != original.Language {
		t.Errorf("Language: got %q, want %q", decoded.Language, original.Language)
	}
	if decoded.ArchivePath != original.ArchivePath {
		t.Errorf("ArchivePath: got %q, want %q", decoded.ArchivePath, original.ArchivePath)
	}
	if !decoded.EnqueuedAt.Equal(original.EnqueuedAt) {
		t.Errorf("EnqueuedAt: got %v, want %v", decoded.EnqueuedAt, original.EnqueuedAt)
	}
}

func TestMemoryQueue_Len(t *testing.T) {
	t.Parallel()
	q := queue.NewMemoryQueue(8)
	ctx := context.Background()

	if q.Len() != 0 {
		t.Errorf("Len() = %d before any enqueue, want 0", q.Len())
	}

	for i := 0; i < 3; i++ {
		sub := makeSubmission("sub-len", "zeta", models.LangGo)
		if err := q.Enqueue(ctx, queue.NewJob(sub)); err != nil {
			t.Fatalf("Enqueue[%d]: %v", i, err)
		}
	}

	if q.Len() != 3 {
		t.Errorf("Len() = %d after 3 enqueues, want 3", q.Len())
	}

	if _, err := q.Dequeue(ctx); err != nil {
		t.Fatalf("Dequeue: %v", err)
	}

	if q.Len() != 2 {
		t.Errorf("Len() = %d after 1 dequeue, want 2", q.Len())
	}
}
