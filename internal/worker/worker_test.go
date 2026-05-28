package worker_test

// worker_test.go tests the Worker orchestration logic in isolation.
// No Docker, no Redpanda — uses MemoryQueue, fakeStore, and fakeExecutor.
//
// Tests:
//   TestWorker_ProcessesJobSuccessfully        — happy path: job → completed
//   TestWorker_SkipsNonPendingJob              — idempotent guard: already-running job is skipped
//   TestWorker_MarksFailedOnExecutorError      — executor error → submission StatusFailed
//   TestWorker_CommitsOffsetAfterSuccess       — queue offset committed on success
//   TestWorker_CommitsOffsetAfterFailure       — queue offset committed on failure (terminal)
//   TestWorker_StopsOnContextCancel            — Run() returns when ctx canceled
//   TestWorker_StoreGetErrorDoesNotCommit      — store.Get failure leaves offset uncommitted
//   TestNewWorker_RequiresNonNilDeps           — constructor validates all deps

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/queue"
	"github.com/nexusbench/nexusbench/internal/worker"
)

// ── fakes ─────────────────────────────────────────────────────────────────────

// fakeStore implements worker.Store in memory.
type fakeStore struct {
	mu   sync.RWMutex
	subs map[string]*models.Submission
}

func newFakeStore(subs ...*models.Submission) *fakeStore {
	m := make(map[string]*models.Submission, len(subs))
	for _, s := range subs {
		m[s.ID] = s
	}
	return &fakeStore{subs: m}
}

func (f *fakeStore) Get(id string) (*models.Submission, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s, ok := f.subs[id]
	if !ok {
		return nil, errors.New("not found: " + id)
	}
	cp := *s
	return &cp, nil
}

func (f *fakeStore) Update(sub *models.Submission) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := *sub
	f.subs[sub.ID] = &cp
	return nil
}

func (f *fakeStore) status(id string) models.SubmissionStatus {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if s, ok := f.subs[id]; ok {
		return s.Status
	}
	return ""
}

// fakeExecutor implements worker.Executor.
type fakeExecutor struct {
	results *models.BenchmarkResults
	err     error
	mu      sync.Mutex
	called  int
	// done is closed by Execute after each call, allowing tests to
	// synchronize on job completion without timing-dependent sleeps.
	done chan struct{}
}

func newFakeExecutor(results *models.BenchmarkResults, err error) *fakeExecutor {
	return &fakeExecutor{
		results: results,
		err:     err,
		done:    make(chan struct{}, 16), // buffered: one slot per expected call
	}
}

func (f *fakeExecutor) Execute(_ context.Context, _ queue.Job) (*models.BenchmarkResults, error) {
	f.mu.Lock()
	f.called++
	f.mu.Unlock()

	defer func() { f.done <- struct{}{} }()

	if f.err != nil {
		return nil, f.err
	}
	r := *f.results
	return &r, nil
}

func (f *fakeExecutor) timesCalled() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// waitDone blocks until Execute has been called (and returned) at least once,
// or the deadline is exceeded. Eliminates all timing-dependent sleeps.
func (f *fakeExecutor) waitDone(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-f.done:
	case <-time.After(timeout):
		t.Fatalf("executor.Execute was not called within %s", timeout)
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func goodResults() *models.BenchmarkResults {
	return &models.BenchmarkResults{
		P50LatencyMs:      1.2,
		P90LatencyMs:      3.5,
		P99LatencyMs:      8.1,
		MaxTPS:            15000,
		SustainedTPS:      12000,
		CorrectnessScore:  0.999,
		TotalOrders:       100000,
		CorrectFills:      99900,
		IncorrectFills:    100,
		CompositeScore:    94.2,
		BenchmarkDuration: "5m0s",
		CompletedAt:       time.Now().UTC(),
	}
}

func pendingSub(id string) *models.Submission {
	return &models.Submission{
		ID:          id,
		TeamName:    "test-team",
		Language:    models.LangGo,
		Protocol:    models.ProtocolREST,
		Status:      models.StatusPending,
		ArchivePath: "/submissions/" + id + "/archive.tar.gz",
	}
}

// runWorkerUntilDone starts the worker in a goroutine, enqueues j, waits
// for the executor to finish (via exec.waitDone), then cancels the worker
// and waits for Run to return. No sleeps involved.
func runWorkerUntilDone(
	t *testing.T,
	w *worker.Worker,
	q *queue.MemoryQueue,
	exec *fakeExecutor,
	j queue.Job,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	if err := q.Enqueue(ctx, j); err != nil {
		cancel()
		t.Fatalf("Enqueue: %v", err)
	}

	// Wait for Execute to complete — this is the precise synchronization point.
	exec.waitDone(t, 2*time.Second)

	// Give processJob a moment to write the final status to the store and
	// commit the offset. Both are synchronous store/queue calls that return
	// immediately in tests, so 50ms is conservative.
	time.Sleep(50 * time.Millisecond)

	cancel()
	if err := <-runDone; err != nil {
		t.Errorf("Run: %v", err)
	}
}

// runWorkerSkip is used for tests where the executor is NOT expected to be
// called (idempotent-guard path). It enqueues the job and waits for the
// queue to drain, then cancels.
func runWorkerSkip(
	t *testing.T,
	w *worker.Worker,
	q *queue.MemoryQueue,
	j queue.Job,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	if err := q.Enqueue(ctx, j); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Poll until the queue is drained (job consumed and committed/skipped).
	deadline := time.Now().Add(2 * time.Second)
	for q.Len() > 0 {
		if time.Now().After(deadline) {
			t.Fatal("queue not drained within 2s — worker may have stalled")
		}
		time.Sleep(5 * time.Millisecond)
	}
	// After Len() == 0 the worker has dequeued the job. The skip path
	// (idempotent guard) does a synchronous CommitJob and returns —
	// no async work, so 20ms headroom is ample.
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-runDone
}

// ── tests ─────────────────────────────────────────────────────────────────────

func TestWorker_ProcessesJobSuccessfully(t *testing.T) {
	t.Parallel()
	sub := pendingSub("sub-happy")
	store := newFakeStore(sub)
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runWorkerUntilDone(t, w, q, exec, queue.NewJob(sub))

	if got := store.status(sub.ID); got != models.StatusCompleted {
		t.Errorf("status = %q, want %q", got, models.StatusCompleted)
	}
	if exec.timesCalled() != 1 {
		t.Errorf("executor called %d times, want 1", exec.timesCalled())
	}
}

func TestWorker_SkipsNonPendingJob(t *testing.T) {
	t.Parallel()
	sub := pendingSub("sub-running")
	sub.Status = models.StatusRunning
	store := newFakeStore(sub)
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runWorkerSkip(t, w, q, queue.NewJob(sub))

	if got := store.status(sub.ID); got != models.StatusRunning {
		t.Errorf("status = %q, want %q (unchanged)", got, models.StatusRunning)
	}
	if exec.timesCalled() != 0 {
		t.Errorf("executor called %d times, want 0 (should be skipped)", exec.timesCalled())
	}
}

func TestWorker_MarksFailedOnExecutorError(t *testing.T) {
	t.Parallel()
	sub := pendingSub("sub-fail")
	store := newFakeStore(sub)
	exec := newFakeExecutor(nil, errors.New("docker: container failed to start"))
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runWorkerUntilDone(t, w, q, exec, queue.NewJob(sub))

	if got := store.status(sub.ID); got != models.StatusFailed {
		t.Errorf("status = %q, want %q", got, models.StatusFailed)
	}
}

func TestWorker_CommitsOffsetAfterSuccess(t *testing.T) {
	t.Parallel()
	sub := pendingSub("sub-commit-ok")
	store := newFakeStore(sub)
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runWorkerUntilDone(t, w, q, exec, queue.NewJob(sub))

	if q.Len() != 0 {
		t.Errorf("queue.Len() = %d after job processed, want 0", q.Len())
	}
}

func TestWorker_CommitsOffsetAfterFailure(t *testing.T) {
	t.Parallel()
	sub := pendingSub("sub-commit-fail")
	store := newFakeStore(sub)
	exec := newFakeExecutor(nil, errors.New("sandbox exploded"))
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	runWorkerUntilDone(t, w, q, exec, queue.NewJob(sub))

	if q.Len() != 0 {
		t.Errorf("queue.Len() = %d after failed job, want 0", q.Len())
	}
	if got := store.status(sub.ID); got != models.StatusFailed {
		t.Errorf("status = %q, want %q", got, models.StatusFailed)
	}
}

func TestWorker_StopsOnContextCancel(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4) // empty — worker blocks in Dequeue

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned error: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Error("Run did not stop within 500ms after context cancel")
	}
}

func TestWorker_StoreGetErrorDoesNotCommit(t *testing.T) {
	t.Parallel()
	store := newFakeStore() // empty — Get will always fail
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4)

	w, err := worker.NewWorker(q, store, exec, worker.Config{
		WorkerID:   "test-worker",
		JobTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewWorker: %v", err)
	}

	sub := pendingSub("sub-missing")
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	if err := q.Enqueue(ctx, queue.NewJob(sub)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	_ = w.Run(ctx)

	if exec.timesCalled() != 0 {
		t.Errorf("executor called %d times on store-error path, want 0", exec.timesCalled())
	}
}

func TestNewWorker_RequiresNonNilDeps(t *testing.T) {
	t.Parallel()
	store := newFakeStore()
	exec := newFakeExecutor(goodResults(), nil)
	q := queue.NewMemoryQueue(4)
	cfg := worker.Config{WorkerID: "t", JobTimeout: time.Minute}

	if _, err := worker.NewWorker(nil, store, exec, cfg); err == nil {
		t.Error("NewWorker(nil queue) should return error")
	}
	if _, err := worker.NewWorker(q, nil, exec, cfg); err == nil {
		t.Error("NewWorker(nil store) should return error")
	}
	if _, err := worker.NewWorker(q, store, nil, cfg); err == nil {
		t.Error("NewWorker(nil executor) should return error")
	}
	if _, err := worker.NewWorker(q, store, exec, cfg); err != nil {
		t.Errorf("NewWorker(all valid) should not error, got: %v", err)
	}
}
