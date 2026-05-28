package queue

import (
	"context"
	"fmt"
	"sync"
)

// MemoryQueue is a thread-safe, in-memory implementation of Queue.
// Use it in unit tests instead of RedpandaQueue — no broker required.
//
// Enqueue behavior depends on buffer capacity:
//   - bufSize > 0: non-blocking send. Returns an error immediately if the
//     buffer is full, which lets tests verify backpressure without deadlocking.
//   - bufSize == 0: synchronous (unbuffered) channel. Enqueue blocks until
//     a Dequeue call is ready to receive, or ctx is canceled. This matches
//     Go channel semantics and is useful for tightly-synchronized tests.
//
// CommitJob is a no-op — in-memory delivery has no offset to commit.
// QueueDepth returns len(ch) — the current number of buffered jobs.
type MemoryQueue struct {
	ch     chan Job
	mu     sync.Mutex
	closed bool
}

// NewMemoryQueue returns a MemoryQueue with the given buffer depth.
//   - bufSize == 0: synchronous (unbuffered); Enqueue blocks until Dequeue.
//   - bufSize > 0:  asynchronous; Enqueue returns immediately until the buffer
//     is full, then returns an error.
func NewMemoryQueue(bufSize int) *MemoryQueue {
	return &MemoryQueue{ch: make(chan Job, bufSize)}
}

// Enqueue adds j to the queue.
//
// Buffered queues (cap > 0): non-blocking. Returns an error if the buffer
// is full so callers fail fast rather than deadlocking.
//
// Unbuffered queues (cap == 0): blocking. Blocks until a Dequeue is ready
// or ctx is canceled, mirroring bare Go channel semantics.
func (m *MemoryQueue) Enqueue(ctx context.Context, j Job) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("queue: MemoryQueue is closed")
	}
	m.mu.Unlock()

	if cap(m.ch) == 0 {
		// Synchronous path: block until a receiver is ready or ctx is done.
		select {
		case m.ch <- j:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Buffered path: fail fast if the buffer is full.
	select {
	case m.ch <- j:
		return nil
	default:
		return fmt.Errorf("queue: MemoryQueue buffer full (size=%d)", cap(m.ch))
	}
}

// Dequeue blocks until a job is available or ctx is canceled.
func (m *MemoryQueue) Dequeue(ctx context.Context) (Job, error) {
	select {
	case j, ok := <-m.ch:
		if !ok {
			return Job{}, fmt.Errorf("queue: MemoryQueue is closed")
		}
		return j, nil
	case <-ctx.Done():
		return Job{}, ctx.Err()
	}
}

// CommitJob is a no-op for MemoryQueue — in-memory delivery is inherently
// at-most-once and there is no offset to commit.
func (m *MemoryQueue) CommitJob(_ context.Context) error {
	return nil
}

// QueueDepth returns the number of jobs currently buffered in the channel.
//
// For unbuffered queues (cap == 0) this is always 0 — no buffering exists.
// This is safe to call concurrently and never blocks.
func (m *MemoryQueue) QueueDepth(_ context.Context) (int64, error) {
	return int64(len(m.ch)), nil
}

// Close closes the underlying channel. Subsequent Dequeue calls return an
// error immediately. Safe to call multiple times.
func (m *MemoryQueue) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.closed {
		m.closed = true
		close(m.ch)
	}
	return nil
}

// Len returns the number of jobs currently in the buffer.
// Always 0 for unbuffered queues. Useful for test assertions.
func (m *MemoryQueue) Len() int {
	return len(m.ch)
}
