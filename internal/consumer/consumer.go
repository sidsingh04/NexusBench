package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// Consumer is the metrics pipeline: it reads events from the metrics.latency
// Redpanda topic, accumulates them into 5-second windows per submission,
// and writes each completed window to the Store.
//
// Architecture:
//
//	┌─────────────┐    poll    ┌──────────────────┐   WriteWindow   ┌───────┐
//	│  Redpanda   │ ────────► │    Consumer       │ ──────────────► │ Store │
//	│metrics.lat. │           │ (window buckets)  │                 │  DB   │
//	└─────────────┘           └──────────────────┘                 └───────┘
//
// Window bucketing:
//
//	Events are grouped by (submissionID, 5-second bucket). The bucket is
//	computed as: truncate(event.Timestamp, 5s). When we receive an event
//	whose timestamp falls into a NEW bucket for a given submission, we
//	close and flush the previous bucket first, then start the new one.
//	This means windows close lazily (on the next event) rather than on a
//	wall-clock ticker — correct for a single-partition ordered stream.
//
// Consumer group:
//
//	We use a named consumer group ("nexusbench-consumer") so that on restart
//	the consumer resumes from where it left off (committed offset) rather
//	than re-reading from the start. This is what makes WriteWindow's
//	ON CONFLICT DO NOTHING meaningful — it's a last-resort guard.
//
// The zero value is NOT valid. Use NewConsumer.
type Consumer struct {
	client *kgo.Client
	store  Store

	// windows holds the in-progress Accumulator for each submission.
	// key: submissionID
	// We track one window per submission simultaneously.
	windows map[string]*windowState
}

// windowState holds the Accumulator and the current bucket start time
// for one submission.
type windowState struct {
	acc         *Accumulator
	bucketStart time.Time
}

// Config holds all configuration for the Consumer.
type Config struct {
	// Brokers is the list of Redpanda broker addresses.
	Brokers []string

	// GroupID is the Kafka consumer group ID. Use a stable name so offsets
	// are committed and the consumer resumes on restart.
	GroupID string

	// CommitInterval is how often the consumer commits its Redpanda offset.
	// Lower = less re-processing on restart. Default: 5 seconds.
	CommitInterval time.Duration
}

// DefaultConfig returns a Config suitable for local development.
func DefaultConfig() Config {
	return Config{
		Brokers:        []string{"127.0.0.1:19092"},
		GroupID:        "nexusbench-consumer",
		CommitInterval: 5 * time.Second,
	}
}

// NewConsumer creates a Consumer connected to Redpanda, subscribed to the
// metrics.latency topic. Call Run() to start processing.
func NewConsumer(cfg Config, store Store) (*Consumer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("consumer: Config.Brokers must not be empty")
	}
	if store == nil {
		return nil, fmt.Errorf("consumer: Store must not be nil")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("consumer: Config.GroupID must not be empty")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(telemetry.TopicLatency),
		// Start from the earliest available offset the first time this group
		// is seen. On restart, the committed offset takes precedence.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Commit offsets automatically on the given interval.
		kgo.AutoCommitInterval(cfg.CommitInterval),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer: create kgo client: %w", err)
	}

	return &Consumer{
		client:  client,
		store:   store,
		windows: make(map[string]*windowState),
	}, nil
}

// Run starts the consume loop. It blocks until ctx is canceled.
// On cancellation it flushes all open windows before returning.
//
// Typical usage:
//
//	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
//	defer cancel()
//	if err := consumer.Run(ctx); err != nil {
//	    log.Fatal(err)
//	}
func (c *Consumer) Run(ctx context.Context) error {
	slog.Info("consumer: starting consume loop",
		"topic", telemetry.TopicLatency,
		"window_duration", WindowDuration,
	)

	for {
		// PollFetches blocks until at least one record is available or ctx
		// is canceled. It never returns nil.
		fetches := c.client.PollFetches(ctx)

		// ctx canceled — flush open windows and exit cleanly.
		if ctx.Err() != nil {
			slog.Info("consumer: context canceled, flushing open windows")
			c.flushAll(context.Background())
			return ctx.Err()
		}

		// Log fetch-level errors (partition errors, broker disconnects) but
		// do not crash — franz-go will retry automatically.
		fetches.EachError(func(topic string, partition int32, err error) {
			slog.Error("consumer: fetch error",
				"topic", topic,
				"partition", partition,
				"err", err,
			)
		})

		// Process each record in the fetch batch.
		fetches.EachRecord(func(r *kgo.Record) {
			if err := c.handleRecord(ctx, r); err != nil {
				slog.Error("consumer: handle record error",
					"topic", r.Topic,
					"offset", r.Offset,
					"err", err,
				)
				// Don't stop the consumer on a single bad record.
				// The record is still marked — it will be committed and
				// not retried, which is the correct behavior for a
				// malformed event.
			}
			// Mark this record's offset as ready to commit.
			// franz-go batches the actual commit per AutoCommitInterval.
			c.client.MarkCommitRecords(r)
		})
	}
}

// handleRecord deserialises one Redpanda record into a telemetry.Event,
// routes it into the correct window accumulator, and flushes completed
// windows to the store.
func (c *Consumer) handleRecord(ctx context.Context, r *kgo.Record) error {
	var e telemetry.Event
	if err := json.Unmarshal(r.Value, &e); err != nil {
		return fmt.Errorf("unmarshal event at offset %d: %w", r.Offset, err)
	}

	// Ignore heartbeats — they carry no latency data and should not appear
	// on metrics.latency (topics.go routes them to metrics.heartbeat), but
	// we guard defensively.
	if e.Kind == telemetry.KindHeartbeat {
		return nil
	}

	// Compute which 5-second bucket this event belongs to.
	bucket := e.Timestamp.UTC().Truncate(WindowDuration)

	state, exists := c.windows[e.SubmissionID]
	if !exists {
		// First event for this submission — open a new window.
		c.windows[e.SubmissionID] = &windowState{
			acc:         NewAccumulator(e.SubmissionID, bucket),
			bucketStart: bucket,
		}
		state = c.windows[e.SubmissionID]
	}

	if bucket != state.bucketStart {
		// This event belongs to a new bucket — close the old one first.
		if err := c.flushWindow(ctx, e.SubmissionID, state); err != nil {
			return fmt.Errorf("flush window for %s: %w", e.SubmissionID, err)
		}
		// Open a fresh accumulator for the new bucket.
		state = &windowState{
			acc:         NewAccumulator(e.SubmissionID, bucket),
			bucketStart: bucket,
		}
		c.windows[e.SubmissionID] = state
	}

	state.acc.Add(e.LatencyNs)
	return nil
}

// flushWindow computes the LatencyWindow for the given state, writes it to
// the store, and removes the entry from c.windows.
func (c *Consumer) flushWindow(ctx context.Context, submissionID string, state *windowState) error {
	w := state.acc.Compute()

	slog.Info("consumer: flushing window",
		"submission_id", submissionID,
		"bucket_start", state.bucketStart,
		"sample_n", w.SampleN,
		"p99_ns", w.P99Ns,
		"tps", w.TPS,
	)

	if err := c.store.WriteWindow(ctx, w); err != nil {
		return err
	}
	delete(c.windows, submissionID)
	return nil
}

// flushAll flushes every open window. Called on shutdown.
func (c *Consumer) flushAll(ctx context.Context) {
	for id, state := range c.windows {
		if err := c.flushWindow(ctx, id, state); err != nil {
			slog.Error("consumer: error flushing window on shutdown",
				"submission_id", id,
				"err", err,
			)
		}
	}
}

// Close releases the Redpanda client. Call after Run() returns.
func (c *Consumer) Close() {
	c.client.Close()
	slog.Info("consumer: closed")
}
