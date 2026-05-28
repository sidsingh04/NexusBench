// RedpandaQueue implements Queue using Redpanda (Kafka-compatible) as the
// durable job bus.
//
// Topic: jobs.benchmark
//   - One partition per worker is the ideal steady-state. For Stage 3.1
//     we default to 4 partitions — enough for up to 4 concurrent workers
//     without re-partitioning.
//   - Partition key = SubmissionID. This ensures all retries of a given
//     submission land on the same partition (and thus the same worker),
//     avoiding duplicate-worker races on the same job.
//
// Consumer group: nexusbench-workers
//   - All workers share one group. Redpanda assigns each partition to
//     exactly one worker, giving us natural load-balancing with no
//     custom scheduler needed at this stage.
//   - On worker restart, the committed offset is resumed — no job is lost.
//
// Delivery guarantee: at-least-once.
//   - A job is committed (offset advanced) only after the worker calls
//     CommitJob. If the worker crashes mid-job, the job is re-delivered
//     to the next available worker.
//   - Workers guard against duplicate execution by checking submission
//     status before starting work (idempotent guard in worker.go).
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// TopicJobs is the single Redpanda topic used for all benchmark jobs.
	// Using a named constant here means a typo is a compile error, not
	// a silent topic mismatch.
	TopicJobs = "jobs.benchmark"

	// workerGroupID is the consumer group shared by all worker instances.
	// Redpanda assigns partitions across the group automatically.
	workerGroupID = "nexusbench-workers"

	// defaultPartitions and defaultReplFactor are used by Bootstrap()
	// when creating the topic. Both can be overridden via Config.
	defaultJobPartitions int32 = 4
	defaultJobReplFactor int16 = 1

	// pollTimeout is how long Dequeue waits for a new record before
	// returning (with no job) so the caller can check ctx.
	pollTimeout = 5 * time.Second
)

// RedpandaConfig holds tunable parameters for RedpandaQueue.
// Use DefaultRedpandaQueueConfig() for local development defaults.
type RedpandaConfig struct {
	// Brokers is the list of Redpanda broker addresses ("host:port").
	Brokers []string

	// Partitions is the number of partitions for the jobs.benchmark topic.
	// Set equal to the maximum number of concurrent workers you intend to run.
	Partitions int32

	// ReplicationFactor is the number of broker replicas per partition.
	// Must be ≤ the number of brokers. Use 1 for single-broker dev setups.
	ReplicationFactor int16
}

// DefaultRedpandaQueueConfig returns a Config suitable for local development
// with a single-node Redpanda on 127.0.0.1:19092.
func DefaultRedpandaQueueConfig() RedpandaConfig {
	return RedpandaConfig{
		Brokers:           []string{"127.0.0.1:19092"},
		Partitions:        defaultJobPartitions,
		ReplicationFactor: defaultJobReplFactor,
	}
}

// RedpandaQueue implements the Queue interface on top of Redpanda.
// Use NewRedpandaQueue to construct one; the zero value is invalid.
type RedpandaQueue struct {
	producer *kgo.Client
	consumer *kgo.Client
	adminCl  *kadm.Client
	cfg      RedpandaConfig

	// pendingRecord holds the most recently polled kgo.Record that has not
	// yet been committed. CommitJob advances the offset for this record.
	// Guarded by mu.
	mu            sync.Mutex
	pendingRecord *kgo.Record

	closeOnce sync.Once
}

// NewRedpandaQueue constructs a RedpandaQueue with separate producer and
// consumer clients. Using separate clients avoids the producer's linger
// timer from blocking the consumer's poll loop.
//
// Call Bootstrap(ctx) before the first Enqueue or Dequeue to ensure the
// jobs.benchmark topic exists.
func NewRedpandaQueue(cfg RedpandaConfig) (*RedpandaQueue, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("queue: RedpandaConfig.Brokers must not be empty")
	}

	// ── Producer client ───────────────────────────────────────────────────────
	// AllISRAcks + idempotent production: a job is never silently lost on
	// broker failover. The control plane blocks in Enqueue until the broker
	// acknowledges — acceptable because job dispatch is not on the hot path.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	)
	if err != nil {
		return nil, fmt.Errorf("queue: create producer client: %w", err)
	}

	// ── Consumer client ───────────────────────────────────────────────────────
	// DisableAutoCommit is intentional: we commit manually via CommitJob
	// only after the worker has durably recorded its results. This gives
	// at-least-once delivery with a clear commit boundary.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(workerGroupID),
		kgo.ConsumeTopics(TopicJobs),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(pollTimeout),
	)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("queue: create consumer client: %w", err)
	}

	return &RedpandaQueue{
		producer: producer,
		consumer: consumer,
		adminCl:  kadm.NewClient(producer),
		cfg:      cfg,
	}, nil
}

// Bootstrap creates the jobs.benchmark topic if it does not already exist.
// It is idempotent and safe to call on every startup.
func (q *RedpandaQueue) Bootstrap(ctx context.Context) error {
	slog.Info("queue: bootstrapping jobs.benchmark topic", "brokers", q.cfg.Brokers)

	// Verify broker connectivity before proceeding.
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := q.adminCl.ListTopics(pingCtx); err != nil {
		return fmt.Errorf("queue: broker unreachable at %v: %w", q.cfg.Brokers, err)
	}

	results, err := q.adminCl.CreateTopics(
		ctx,
		q.cfg.Partitions,
		q.cfg.ReplicationFactor,
		nil,
		TopicJobs,
	)
	if err != nil {
		return fmt.Errorf("queue: CreateTopics RPC: %w", err)
	}

	for _, res := range results {
		if res.Err != nil && !errors.Is(res.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("queue: create topic %q: %w", res.Topic, res.Err)
		}
		if res.Err == nil {
			slog.Info("queue: created topic", "topic", res.Topic, "partitions", q.cfg.Partitions)
		} else {
			slog.Info("queue: topic already exists", "topic", res.Topic)
		}
	}

	return nil
}

// Enqueue serializes j as JSON and produces it to jobs.benchmark.
// The submission ID is used as the partition key to ensure all retries
// for the same submission land on the same partition.
//
// Enqueue blocks until the broker acknowledges the write (AllISRAcks),
// so the caller can treat a nil return as a durable enqueue guarantee.
func (q *RedpandaQueue) Enqueue(ctx context.Context, j Job) error {
	payload, err := json.Marshal(j)
	if err != nil {
		// Job contains only JSON-safe types; this is a safety net.
		return fmt.Errorf("queue: marshal job %s: %w", j.ID, err)
	}

	record := &kgo.Record{
		Topic: TopicJobs,
		Key:   []byte(j.SubmissionID),
		Value: payload,
	}

	if err := q.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("queue: produce job %s: %w", j.ID, err)
	}

	slog.Info("queue: enqueued job",
		"job_id", j.ID,
		"submission_id", j.SubmissionID,
		"team", j.TeamName,
		"language", j.Language,
		"topic", TopicJobs,
	)
	return nil
}

// Dequeue blocks until a Job is available or ctx is canceled.
//
// The returned Job has been polled from Redpanda but its offset has NOT been
// committed. The caller must call CommitJob(ctx, j) after processing to
// advance the consumer group's offset and prevent re-delivery.
//
// If ctx is canceled, Dequeue returns (Job{}, ctx.Err()).
func (q *RedpandaQueue) Dequeue(ctx context.Context) (Job, error) {
	for {
		// Check for cancellation before blocking on poll.
		select {
		case <-ctx.Done():
			return Job{}, ctx.Err()
		default:
		}

		fetches := q.consumer.PollFetches(ctx)

		// Context canceled during poll.
		if ctx.Err() != nil {
			return Job{}, ctx.Err()
		}

		// Log partition-level errors (broker disconnect, leader election)
		// but do not crash — franz-go retries automatically.
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("queue: fetch error",
				"topic", topic,
				"partition", partition,
				"err", err,
			)
		})

		// Collect records from this fetch. We take exactly the first one
		// and cache it as pendingRecord; the rest stay in the client's
		// internal buffer and will be returned on the next PollFetches call.
		var found *kgo.Record
		fetches.EachRecord(func(r *kgo.Record) {
			if found == nil {
				found = r
			}
		})

		if found == nil {
			// Poll timed out (FetchMaxWait) with no records — loop and retry.
			continue
		}

		var j Job
		if err := json.Unmarshal(found.Value, &j); err != nil {
			// Malformed record: log and commit past it so it doesn't block the queue.
			slog.Error("queue: malformed job record; skipping",
				"offset", found.Offset,
				"err", err,
			)
			q.consumer.MarkCommitRecords(found)
			if err := q.consumer.CommitMarkedOffsets(ctx); err != nil {
				slog.Warn("queue: failed to commit past malformed record", "err", err)
			}
			continue
		}

		q.mu.Lock()
		q.pendingRecord = found
		q.mu.Unlock()

		slog.Info("queue: dequeued job",
			"job_id", j.ID,
			"submission_id", j.SubmissionID,
			"team", j.TeamName,
			"language", j.Language,
			"queue_latency_ms", time.Since(j.EnqueuedAt).Milliseconds(),
		)

		return j, nil
	}
}

// CommitJob marks the offset of the most recently dequeued job as processed,
// advancing the consumer group's committed offset.
//
// Call this after the job has been durably completed (results written to the
// submission store). Failing to call CommitJob after Dequeue means the job
// will be re-delivered on the next worker restart — this is intentional
// at-least-once behavior.
//
// Calling CommitJob without a preceding Dequeue is a no-op.
func (q *RedpandaQueue) CommitJob(ctx context.Context) error {
	q.mu.Lock()
	r := q.pendingRecord
	q.pendingRecord = nil
	q.mu.Unlock()

	if r == nil {
		return nil
	}

	q.consumer.MarkCommitRecords(r)
	if err := q.consumer.CommitMarkedOffsets(ctx); err != nil {
		return fmt.Errorf("queue: commit job offset: %w", err)
	}
	return nil
}

// QueueDepth returns the total consumer-group lag for the nexusbench-workers
// group on the jobs.benchmark topic.
//
// Lag = sum over all partitions of (latest_offset - committed_offset).
// This is the exact value KEDA reads from the broker to drive the HPA;
// exposing it as a Prometheus gauge gives the control-plane an in-process
// view without duplicating broker RPC calls from a second scraper.
//
// Implementation notes:
//   - Two admin RPCs are required: ListEndOffsets (head of each partition)
//     and FetchOffsets (the consumer group's committed position).
//   - Both calls are bounded by ctx, so a short deadline is safe.
//   - A best-effort estimate is acceptable: lag may trail reality by a few
//     seconds, which is fine for a 15s scrape interval.
//   - If a partition has no committed offset yet (group just started) the
//     lag for that partition is treated as the full end offset (worst case).
func (q *RedpandaQueue) QueueDepth(ctx context.Context) (int64, error) {
	// Step 1: fetch the latest (end) offset for each partition of the topic.
	endOffsets, err := q.adminCl.ListEndOffsets(ctx, TopicJobs)
	if err != nil {
		return 0, fmt.Errorf("queue: ListEndOffsets: %w", err)
	}

	// Step 2: fetch the consumer group's committed offsets.
	committedOffsets, err := q.adminCl.FetchOffsets(ctx, workerGroupID)
	if err != nil {
		return 0, fmt.Errorf("queue: FetchOffsets for group %q: %w", workerGroupID, err)
	}

	// Step 3: compute lag = sum(end - committed) across all partitions.
	var totalLag int64
	endOffsets.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			// Skip partitions we cannot read; log at call site if needed.
			return
		}
		// Look up the committed offset for this partition.
		committed, ok := committedOffsets.Lookup(o.Topic, o.Partition)
		if !ok || committed.At < 0 {
			// No committed offset: the group hasn't consumed from this
			// partition yet. Treat the full end offset as pending lag.
			totalLag += o.Offset
			return
		}
		lag := o.Offset - committed.At
		if lag < 0 {
			lag = 0 // committed offset can race ahead of end in edge cases
		}
		totalLag += lag
	})

	return totalLag, nil
}

// Close flushes the producer, commits the consumer's pending offsets, and
// closes both clients. Safe to call multiple times.
func (q *RedpandaQueue) Close() error {
	var firstErr error
	q.closeOnce.Do(func() {
		// Flush buffered producer records before closing.
		if err := q.producer.Flush(context.Background()); err != nil {
			slog.Warn("queue: producer flush error on close", "err", err)
			firstErr = err
		}
		q.producer.Close()
		q.consumer.Close()
		slog.Info("queue: RedpandaQueue closed")
	})
	return firstErr
}
