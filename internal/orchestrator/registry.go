// Package orchestrator manages the worker fleet: registration, heartbeats,
// liveness tracking, and re-queue of orphaned jobs from dead workers.
//
// Architecture:
//
//	Control plane (cmd/server)
//	  └─ Orchestrator
//	       └─ WorkerRegistry  (in-memory, goroutine-safe)
//
//	Worker (cmd/worker)
//	  └─ Heartbeater  → POST /internal/workers/{id}/heartbeat  every 5s
//
// The Orchestrator does NOT dispatch jobs — that is the queue's job.
// It only tracks which workers are alive so the control plane can:
//
//	a) Surface worker fleet status via GET /internal/workers
//	b) Detect dead workers and re-queue their in-progress jobs
//
// Re-queue strategy (at-least-once):
//
//	Workers commit the queue offset only after writing results to the store.
//	If a worker dies mid-job, its partition offset is uncommitted.
//	Redpanda will re-deliver that job to another worker after the consumer
//	group session timeout (default 45s in franz-go). The orchestrator's
//	TTL check (15s) catches this earlier and surfaces it in the status API
//	so operators can see dead workers without waiting for Redpanda's timeout.
//
//	The orchestrator does NOT need to explicitly re-enqueue jobs — Redpanda
//	handles that automatically via the uncommitted offset. The registry just
//	marks the worker dead so the API reflects reality.
package orchestrator

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// WorkerStatus is the lifecycle state of a registered worker.
type WorkerStatus string

const (
	// WorkerStatusIdle means the worker is registered and healthy but not
	// currently processing a job.
	WorkerStatusIdle WorkerStatus = "idle"

	// WorkerStatusBusy means the worker has dequeued a job and is processing it.
	WorkerStatusBusy WorkerStatus = "busy"

	// WorkerStatusDead means the worker has not sent a heartbeat within the TTL.
	// Its in-progress job (if any) will be re-delivered by Redpanda automatically.
	WorkerStatusDead WorkerStatus = "dead"
)

const (
	// HeartbeatTTL is the maximum time between heartbeats before a worker is
	// considered dead. Must be > HeartbeatInterval (5s) with enough headroom
	// for one missed heartbeat under load.
	HeartbeatTTL = 15 * time.Second
)

// WorkerRecord holds the last-known state of a registered worker.
type WorkerRecord struct {
	// ID is the worker's unique identifier (hostname or WORKER_ID env var).
	ID string `json:"id"`

	// Status is the worker's current lifecycle state.
	Status WorkerStatus `json:"status"`

	// CurrentJobID is the submission ID the worker is currently processing.
	// Empty when Status == WorkerStatusIdle or WorkerStatusDead.
	CurrentJobID string `json:"current_job_id,omitempty"`

	// RegisteredAt is when the worker first called POST /internal/workers/register.
	RegisteredAt time.Time `json:"registered_at"`

	// LastHeartbeat is the wall-clock time of the most recent heartbeat.
	// Used to compute staleness.
	LastHeartbeat time.Time `json:"last_heartbeat"`

	// JobsCompleted is the total number of jobs this worker has completed
	// since registration. Incremented by the Heartbeat call.
	JobsCompleted int `json:"jobs_completed"`
}

// IsAlive returns true if the worker's last heartbeat is within HeartbeatTTL.
func (w *WorkerRecord) IsAlive() bool {
	return time.Since(w.LastHeartbeat) <= HeartbeatTTL
}

// HeartbeatUpdate is the payload sent by the worker on each heartbeat.
type HeartbeatUpdate struct {
	// Status is the worker's self-reported status.
	Status WorkerStatus `json:"status"`
	// CurrentJobID is the submission ID being processed, or empty if idle.
	CurrentJobID string `json:"current_job_id,omitempty"`
	// JobsCompleted is the total jobs finished since worker startup.
	JobsCompleted int `json:"jobs_completed"`
}

// WorkerRegistry is the in-memory, goroutine-safe store of registered workers.
// It is the single source of truth for worker fleet status within a control
// plane instance.
//
// In a multi-control-plane deployment (Phase 3.5+) this would be backed by
// Redis or a shared database. For Stage 3.2 in-memory is correct because we
// run one control plane.
type WorkerRegistry struct {
	mu      sync.RWMutex
	workers map[string]*WorkerRecord // keyed by worker ID
}

// NewWorkerRegistry returns an empty, ready-to-use WorkerRegistry.
func NewWorkerRegistry() *WorkerRegistry {
	return &WorkerRegistry{
		workers: make(map[string]*WorkerRecord),
	}
}

// Register adds or re-registers a worker. Re-registration (e.g. after a
// worker restart) resets the worker's state while preserving its ID.
// Returns the WorkerRecord as stored.
func (r *WorkerRegistry) Register(id string) (*WorkerRecord, error) {
	if id == "" {
		return nil, fmt.Errorf("orchestrator: worker ID must not be empty")
	}

	now := time.Now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.workers[id]; ok {
		// Re-registration: worker restarted. Reset status but keep the ID.
		existing.Status = WorkerStatusIdle
		existing.CurrentJobID = ""
		existing.LastHeartbeat = now
		existing.RegisteredAt = now
		existing.JobsCompleted = 0
		slog.Info("orchestrator: worker re-registered", "worker_id", id)
		return existing, nil
	}

	rec := &WorkerRecord{
		ID:            id,
		Status:        WorkerStatusIdle,
		RegisteredAt:  now,
		LastHeartbeat: now,
	}
	r.workers[id] = rec
	slog.Info("orchestrator: worker registered", "worker_id", id, "total_workers", len(r.workers))
	return rec, nil
}

// Heartbeat updates the last-seen time and status for a worker.
// Returns an error if the worker is not registered (worker must call
// Register before Heartbeat).
func (r *WorkerRegistry) Heartbeat(id string, update HeartbeatUpdate) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	rec, ok := r.workers[id]
	if !ok {
		return fmt.Errorf("orchestrator: worker %q not registered; call register first", id)
	}

	rec.LastHeartbeat = time.Now().UTC()
	rec.Status = update.Status
	rec.CurrentJobID = update.CurrentJobID
	rec.JobsCompleted = update.JobsCompleted

	return nil
}

// List returns a snapshot of all registered workers, with liveness recomputed.
// Workers whose LastHeartbeat exceeds HeartbeatTTL are returned with
// Status == WorkerStatusDead in the snapshot (the registry record is updated
// in-place so subsequent List calls reflect the change).
func (r *WorkerRegistry) List() []WorkerRecord {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]WorkerRecord, 0, len(r.workers))
	for _, rec := range r.workers {
		// Update dead status in-place so it's persisted for next call.
		if !rec.IsAlive() && rec.Status != WorkerStatusDead {
			slog.Warn("orchestrator: worker missed heartbeat TTL, marking dead",
				"worker_id", rec.ID,
				"last_heartbeat", rec.LastHeartbeat,
				"ttl", HeartbeatTTL,
				"current_job_id", rec.CurrentJobID,
			)
			rec.Status = WorkerStatusDead
			rec.CurrentJobID = "" // job will be re-delivered by Redpanda
		}
		out = append(out, *rec) // copy, not pointer
	}
	return out
}

// Get returns the WorkerRecord for a specific worker ID.
// The second return value is false if the worker is not registered.
func (r *WorkerRegistry) Get(id string) (WorkerRecord, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rec, ok := r.workers[id]
	if !ok {
		return WorkerRecord{}, false
	}
	return *rec, true
}

// Stats returns aggregate counts across all registered workers.
func (r *WorkerRegistry) Stats() RegistryStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	var stats RegistryStats
	stats.Total = len(r.workers)
	for _, rec := range r.workers {
		if !rec.IsAlive() {
			rec.Status = WorkerStatusDead
		}
		switch rec.Status {
		case WorkerStatusIdle:
			stats.Idle++
		case WorkerStatusBusy:
			stats.Busy++
		case WorkerStatusDead:
			stats.Dead++
		}
	}
	return stats
}

// RegistryStats is a summary of the current worker fleet state.
type RegistryStats struct {
	Total int `json:"total"`
	Idle  int `json:"idle"`
	Busy  int `json:"busy"`
	Dead  int `json:"dead"`
}
