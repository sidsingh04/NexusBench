// Package queue defines the job contract between the control plane and the
// distributed worker fleet.
//
// Design principles:
//
//  1. A Job is a self-contained snapshot — workers operate on the Job value
//     alone. They never need to call back into the submission store to start
//     work. This makes the queue protocol stable: adding fields to Submission
//     does not break in-flight jobs.
//
//  2. The Queue interface is intentionally narrow: Enqueue, Dequeue, CommitJob,
//     QueueDepth, Close. Richer operations (cancel, inspect, prioritise) belong
//     to a scheduler layer built on top, not in this package.
//
//  3. This file has NO external dependencies — only stdlib. Any component
//     can import queue.Job without pulling in Redpanda or any I/O library.
//     Transport implementations live in separate files.
package queue

import (
	"context"
	"time"

	"github.com/nexusbench/nexusbench/internal/models"
)

// Job is the atomic unit of work dispatched to a worker.
//
// It embeds a snapshot of the Submission at the moment of dispatch so the
// worker can operate fully offline — reading the archive, spinning up the
// sandbox, running the bot fleet — without any further store lookups.
//
// Serialisation: JSON. Workers and the control plane may be different
// processes on different machines; JSON gives us human-readable messages
// and schema evolution without a code-gen step.
type Job struct {
	// ID is a stable identifier for this job, used for deduplication and
	// logging. Set to the submission ID — one submission produces exactly
	// one job.
	ID string `json:"id"`

	// SubmissionID is redundant with ID but kept explicit so log lines and
	// store queries are unambiguous ("job 123" vs "submission 123").
	SubmissionID string `json:"submission_id"`

	// TeamName is carried for structured logging on the worker side.
	TeamName string `json:"team_name"`

	// Language determines which sandbox image the worker will launch.
	Language models.Language `json:"language"`

	// Protocol determines how the bot fleet will communicate with the engine.
	Protocol models.Protocol `json:"protocol"`

	// ArchivePath is the absolute host path to the submission archive.
	// Workers resolve this path relative to the shared volume mount.
	// On a multi-machine cluster this would be an object-store URL instead.
	ArchivePath string `json:"archive_path"`

	// EnqueuedAt is the wall-clock time the control plane enqueued this job.
	// Workers use it to compute scheduling latency (time-in-queue).
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// NewJob constructs a Job from a persisted Submission.
// EnqueuedAt is always set by the caller at production time.
func NewJob(sub *models.Submission) Job {
	return Job{
		ID:           sub.ID,
		SubmissionID: sub.ID,
		TeamName:     sub.TeamName,
		Language:     sub.Language,
		Protocol:     sub.Protocol,
		ArchivePath:  sub.ArchivePath,
		EnqueuedAt:   time.Now().UTC(),
	}
}

// Queue is the interface between the control plane (producer) and the worker
// fleet (consumers).
//
// Delivery guarantee: at-least-once.
// A dequeued job is not considered "done" until CommitJob is called.
// If a worker crashes before committing, the job is re-delivered to the next
// available worker. Workers must therefore be idempotent — they check
// submission status before starting work to guard against double-execution.
//
// All implementations must be safe for concurrent use by multiple goroutines.
type Queue interface {
	// Enqueue publishes a job to the queue. Returns an error if the
	// underlying transport is unavailable or the context is cancelled.
	// Enqueue must not modify j.
	Enqueue(ctx context.Context, j Job) error

	// Dequeue blocks until a job is available, then returns it.
	// The job's offset is NOT committed until CommitJob is called.
	// Returns ctx.Err() when the context is done.
	Dequeue(ctx context.Context) (Job, error)

	// CommitJob advances the consumer group's offset past the most recently
	// dequeued job, preventing re-delivery on restart.
	// Must be called after the job has been durably completed.
	// Calling CommitJob without a preceding Dequeue is a safe no-op.
	CommitJob(ctx context.Context) error

	// QueueDepth returns the number of unconsumed (pending) jobs in the queue.
	//
	// For RedpandaQueue this is the consumer-group lag on the jobs.benchmark
	// topic: the number of messages produced but not yet committed by any
	// worker. KEDA reads the same lag metric directly from the broker; this
	// method gives the control plane an in-process view for Prometheus.
	//
	// For MemoryQueue this is the number of jobs currently sitting in the
	// in-memory channel buffer (len of the channel).
	//
	// Implementations must be non-blocking and return within the context
	// deadline. A best-effort estimate is acceptable (e.g. the lag value
	// may lag real-time by a few seconds).
	QueueDepth(ctx context.Context) (int64, error)

	// Close flushes any buffered produce records, commits pending offsets,
	// and releases transport resources.
	// Safe to call multiple times; subsequent calls are no-ops.
	Close() error
}
