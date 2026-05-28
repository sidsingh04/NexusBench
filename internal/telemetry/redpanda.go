package telemetry

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

// RedpandaEmitter implements Emitter by producing events to Redpanda topics
// over the Kafka protocol using franz-go.
//
// Architecture decisions:
//
//  1. Non-blocking hot path — Emit() serializes and hands the record to the
//     franz-go client's internal buffer, then returns immediately. The client
//     flushes in the background. This means Emit() is ~microseconds even under
//     high load, never blocking the matching engine.
//
//  2. Per-Kind topic routing — events are routed to different topics based on
//     their Kind (see topics.go). This lets the Step 3 consumer subscribe only
//     to metrics.latency without receiving heartbeat noise.
//
//  3. Dead-letter queue — if a record cannot be delivered after the client's
//     internal retry budget is exhausted, the delivery hook logs the failure.
//     Full DLQ routing (writing failed records to metrics.dlq) is added in
//     Phase 3, once the consumer exists to read from it.
//
//  4. Idempotent producer — RequiredAcks(AllISRAcks) prevents duplicate
//     records on broker-side retries.
//
//  5. Topic auto-creation — Bootstrap() creates all topics on startup if they
//     don't exist, removing manual setup from the dev workflow.
//
// The zero value is NOT valid. Use NewRedpandaEmitter.
type RedpandaEmitter struct {
	client  *kgo.Client
	adminCl *kadm.Client
	cfg     RedpandaConfig // stored so Bootstrap() can use partition/replication values

	closeOnce sync.Once
	closed    chan struct{}
}

// RedpandaConfig holds all tunable parameters for the RedpandaEmitter.
// All fields have sane defaults via DefaultRedpandaConfig().
type RedpandaConfig struct {
	// Brokers is the list of Redpanda broker addresses.
	// Format: "host:port". Use 127.0.0.1:19092 for local dev.
	Brokers []string

	// TopicPartitions is the number of partitions to create for each topic
	// during Bootstrap(). More partitions = more parallelism for consumers.
	// Default: 3 (enough for local dev; scale up in production).
	TopicPartitions int32

	// TopicReplicationFactor is how many broker replicas each partition has.
	// Must be ≤ the number of brokers. Default: 1 (single-broker dev setup).
	TopicReplicationFactor int16

	// ProducerBatchMaxBytes caps the size of a single produce batch.
	// Default: 1 MiB.
	ProducerBatchMaxBytes int32

	// ProducerLingerDuration is how long the producer waits to accumulate
	// records into a batch before flushing.
	// Higher = better throughput at cost of higher emit-to-broker latency.
	// Default: 5ms — a good balance for a benchmarking platform.
	ProducerLingerDuration time.Duration
}

// DefaultRedpandaConfig returns a RedpandaConfig suitable for local development
// with a single-node Redpanda running on 127.0.0.1:19092.
func DefaultRedpandaConfig() RedpandaConfig {
	return RedpandaConfig{
		Brokers:                []string{"127.0.0.1:19092"},
		TopicPartitions:        3,
		TopicReplicationFactor: 1,
		ProducerBatchMaxBytes:  1 << 20, // 1 MiB
		ProducerLingerDuration: 5 * time.Millisecond,
	}
}

// NewRedpandaEmitter constructs a RedpandaEmitter and establishes a connection
// to the broker pool. It does NOT create topics — call Bootstrap() before
// the first Emit() to ensure topics exist.
//
// Typical usage:
//
//	emitter, err := telemetry.NewRedpandaEmitter(telemetry.DefaultRedpandaConfig())
//	if err != nil { log.Fatal(err) }
//	defer emitter.Close()
//	if err := emitter.Bootstrap(ctx); err != nil { log.Fatal(err) }
func NewRedpandaEmitter(cfg RedpandaConfig) (*RedpandaEmitter, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("telemetry: RedpandaConfig.Brokers must not be empty")
	}

	hook := &deliveryHook{}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ProducerBatchMaxBytes(cfg.ProducerBatchMaxBytes),
		kgo.ProducerLinger(cfg.ProducerLingerDuration),

		// StickyKeyPartitioner ensures all records with the same key (SubmissionID)
		// go to the same partition, preserving per-submission event order.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),

		// AllISRAcks waits for all in-sync replicas to acknowledge the write.
		// This prevents data loss on broker failover and enables idempotent
		// production (franz-go enables idempotency automatically with this setting).
		kgo.RequiredAcks(kgo.AllISRAcks()),

		// The delivery hook is called after every record is either successfully
		// persisted or permanently failed. We use it to log failures.
		kgo.WithHooks(hook),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("telemetry: create franz-go client: %w", err)
	}

	return &RedpandaEmitter{
		client:  client,
		adminCl: kadm.NewClient(client),
		cfg:     cfg,
		closed:  make(chan struct{}),
	}, nil
}

// Bootstrap creates all NexusBench topics if they don't already exist.
// It is idempotent — calling it on a broker that already has the topics
// is safe and fast (one ListTopics call, then early return).
// Must be called before the first Emit().
func (r *RedpandaEmitter) Bootstrap(ctx context.Context) error {
	slog.Info("telemetry: bootstrapping Redpanda topics",
		"brokers", r.cfg.Brokers,
		"topics", AllTopics(),
	)

	// Fail fast — surface a clear "broker unreachable" error rather than
	// a cryptic context deadline exceeded from within the produce path.
	if err := r.ping(ctx); err != nil {
		return fmt.Errorf("telemetry: broker unreachable at %v: %w", r.cfg.Brokers, err)
	}

	// Fetch the current topic list so we only create what's missing.
	topicDetails, err := r.adminCl.ListTopics(ctx)
	if err != nil {
		return fmt.Errorf("telemetry: list topics: %w", err)
	}
	existing := make(map[string]bool, len(topicDetails))
	for _, td := range topicDetails {
		existing[td.Topic] = true
	}

	var toCreate []string
	for _, t := range AllTopics() {
		if !existing[t] {
			toCreate = append(toCreate, t)
		}
	}

	if len(toCreate) == 0 {
		slog.Info("telemetry: all topics already exist, skipping creation")
		return nil
	}

	// CreateTopics uses the partition/replication values from cfg, not
	// hardcoded constants. This is intentional — integration tests set
	// TopicPartitions=1 for deterministic ordering.
	results, err := r.adminCl.CreateTopics(
		ctx,
		r.cfg.TopicPartitions,
		r.cfg.TopicReplicationFactor,
		nil, // no extra topic configs
		toCreate...,
	)
	if err != nil {
		return fmt.Errorf("telemetry: CreateTopics RPC: %w", err)
	}

	for _, res := range results {
		if res.Err != nil {
			// TOPIC_ALREADY_EXISTS is benign — a concurrent instance may have
			// created it between our ListTopics call and CreateTopics call.
			// kerr.TopicAlreadyExists is the typed error sentinel from franz-go.
			if errors.Is(res.Err, kerr.TopicAlreadyExists) {
				slog.Info("telemetry: topic already exists (race)", "topic", res.Topic)
				continue
			}
			return fmt.Errorf("telemetry: create topic %q: %w", res.Topic, res.Err)
		}
		slog.Info("telemetry: created topic",
			"topic", res.Topic,
			"partitions", r.cfg.TopicPartitions,
		)
	}

	return nil
}

// Emit validates e and produces it to the correct Redpanda topic.
//
// Hot path contract:
//   - Validation is synchronous and CPU-only (~nanoseconds).
//   - JSON serialization is synchronous (~microseconds).
//   - ProduceSync hands the record to the client's internal batch buffer and
//     returns. Network I/O to the broker happens asynchronously in the
//     background. This means Emit() adds < 10µs to the caller's critical path
//     under normal broker conditions.
//
// Emit is safe to call concurrently from multiple goroutines.
func (r *RedpandaEmitter) Emit(ctx context.Context, e Event) error {
	// Reject new events after Close() — reading from a closed channel is
	// always non-blocking and returns immediately.
	select {
	case <-r.closed:
		return fmt.Errorf("telemetry: emitter is closed")
	default:
	}

	if err := e.Validate(); err != nil {
		return err
	}

	payload, err := json.Marshal(e)
	if err != nil {
		// json.Marshal only fails for unmarshalable types (channels, funcs).
		// Event only contains strings, int64, time.Time, and map[string]string —
		// all of which are marshalable. This error path is a safety net.
		return fmt.Errorf("telemetry: marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: TopicForKind(e.Kind),
		// SubmissionID as the partition key guarantees all events for one
		// submission arrive at the consumer in the order they were produced.
		Key:   []byte(e.SubmissionID),
		Value: payload,
	}

	// ProduceSync returns after the record is buffered internally.
	// It does NOT wait for the broker to acknowledge — that happens via the
	// deliveryHook asynchronously.
	if err := r.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("telemetry: produce to %q: %w", record.Topic, err)
	}

	return nil
}

// BatchEmit validates and produces a slice of events in a single produce
// batch for efficiency. This is the preferred path for the bot fleet, which
// may generate thousands of OrderAck events per second.
//
// Implementation:
//   - All valid events are serialized and handed to ProduceSync as a single
//     multi-record batch. franz-go's linger window aggregates them further.
//   - Invalid events are skipped (not sent); a combined error is returned.
//   - The function returns after all valid records are buffered — not after
//     the broker has acknowledged them (same non-blocking contract as Emit).
//
// BatchEmit is safe to call concurrently.
func (r *RedpandaEmitter) BatchEmit(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	select {
	case <-r.closed:
		return fmt.Errorf("telemetry: emitter is closed")
	default:
	}

	var errs []error
	records := make([]*kgo.Record, 0, len(events))

	for i := range events {
		e := &events[i]
		if err := e.Validate(); err != nil {
			errs = append(errs, err)
			continue
		}
		payload, err := json.Marshal(e)
		if err != nil {
			errs = append(errs, fmt.Errorf("marshal event %s: %w", e.OrderID, err))
			continue
		}
		records = append(records, &kgo.Record{
			Topic: TopicForKind(e.Kind),
			Key:   []byte(e.SubmissionID),
			Value: payload,
		})
	}

	if len(records) > 0 {
		if err := r.client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			errs = append(errs, fmt.Errorf("produce batch: %w", err))
		}
	}

	return joinErrors(errs)
}

// Close flushes all buffered records to the broker, waits for in-flight
// produce requests to complete, then tears down the client connection.
// Safe to call multiple times — subsequent calls are no-ops.
func (r *RedpandaEmitter) Close() error {
	var closeErr error
	r.closeOnce.Do(func() {
		close(r.closed)

		// Flush blocks until every buffered record has either been delivered
		// or permanently failed (delivery hook is called for each).
		if err := r.client.Flush(context.Background()); err != nil {
			slog.Warn("telemetry: flush error during Close", "err", err)
			closeErr = err
		}
		r.client.Close()
		slog.Info("telemetry: RedpandaEmitter closed")
	})
	return closeErr
}

// ping performs a lightweight ListTopics call with a 5-second timeout to
// verify broker connectivity. Used as a fast-fail check in Bootstrap().
func (r *RedpandaEmitter) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_, err := r.adminCl.ListTopics(pingCtx)
	return err
}

// ── deliveryHook ─────────────────────────────────────────────────────────────

// deliveryHook is called by franz-go after every record is either successfully
// persisted to the broker or permanently failed after all retries.
//
// Implements kgo.HookProduceRecordUnbuffered.
type deliveryHook struct{}

// OnProduceRecordUnbuffered logs permanently-failed records.
// We intentionally do NOT write to the DLQ topic here because that write
// could itself fail, and the hook must not block or panic. DLQ routing
// is implemented in Phase 3 via a separate error-capture goroutine.
func (h *deliveryHook) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	if err != nil {
		slog.Error("telemetry: record permanently failed",
			"topic", r.Topic,
			"submission_id", string(r.Key),
			"err", err,
		)
	}
}
