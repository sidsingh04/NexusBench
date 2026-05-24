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
			// t.Errorf is not goroutine-safe after t returns; best effort.
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
	// Must have blocked for at least 15ms (goroutine delay is 20ms).
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
		t.Fatal("Dequeue on empty queue with cancelled ctx should return error")
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
	// Buffer of 1: second Enqueue should fail immediately.
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

	// CommitJob without a preceding Dequeue must be a no-op, not a panic.
	if err := q.CommitJob(ctx); err != nil {
		t.Errorf("CommitJob (no-op): unexpected error: %v", err)
	}
}

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
	// Time comparison: JSON marshals to RFC3339Nano; verify round-trip is lossless.
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
