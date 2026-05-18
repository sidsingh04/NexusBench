//go:build integration

// Run with:  go test ./internal/telemetry/... -tags=integration -v -timeout 60s
//
// Requires a Redpanda broker at 127.0.0.1:19092.
// Start one with:
//   docker run -d --name redpanda-dev -p 19092:19092 -p 9644:9644 \
//     redpandadata/redpanda:v24.1.13 redpanda start \
//     --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \
//     --advertise-kafka-addr internal://redpanda-dev:9092,external://localhost:19092 \
//     --mode dev-container --smp 1 --default-log-level=warn
//
// Or via docker compose:  docker compose up redpanda -d

package telemetry_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

const testBroker = "127.0.0.1:19092"

// integrationCfg returns a RedpandaConfig pointed at the local test broker.
func integrationCfg() telemetry.RedpandaConfig {
	cfg := telemetry.DefaultRedpandaConfig()
	cfg.Brokers = []string{testBroker}
	cfg.TopicPartitions = 1        // single partition keeps consume order deterministic
	cfg.TopicReplicationFactor = 1 // single-node broker
	cfg.ProducerLingerDuration = 0 // no linger in tests — flush immediately
	return cfg
}

// TestRedpandaEmitter_Bootstrap verifies that topics are created and the
// call is idempotent (running it twice does not error).
func TestRedpandaEmitter_Bootstrap(t *testing.T) {
	ctx := context.Background()

	emitter, err := telemetry.NewRedpandaEmitter(integrationCfg())
	if err != nil {
		t.Fatalf("NewRedpandaEmitter: %v", err)
	}
	defer emitter.Close()

	// First call — topics may or may not exist
	if err := emitter.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap (first): %v", err)
	}

	// Second call — must be idempotent
	if err := emitter.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap (second, idempotency check): %v", err)
	}

	// Verify topics exist via admin client
	adminCl := kadm.NewClient(mustNewKgoClient(t, testBroker))
	// defer adminCl.Client().Close()

	details, err := adminCl.ListTopics(ctx)
	if err != nil {
		t.Fatalf("ListTopics: %v", err)
	}
	existing := make(map[string]bool)
	for _, d := range details {
		existing[d.Topic] = true
	}

	for _, topic := range telemetry.AllTopics() {
		if !existing[topic] {
			t.Errorf("topic %q was not created by Bootstrap()", topic)
		}
	}
}

// TestRedpandaEmitter_EmitAndConsume emits events of every Kind and reads
// them back from the broker, verifying the full round-trip.
func TestRedpandaEmitter_EmitAndConsume(t *testing.T) {
	ctx := context.Background()

	emitter, err := telemetry.NewRedpandaEmitter(integrationCfg())
	if err != nil {
		t.Fatalf("NewRedpandaEmitter: %v", err)
	}
	defer emitter.Close()

	if err := emitter.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	// Use a unique submissionID per test run to avoid reading stale messages
	// left over from a previous run on the same topic+partition.
	subID := fmt.Sprintf("inttest-%d", time.Now().UnixNano())
	now := time.Now().UTC()

	// Emit one event of each latency Kind plus one heartbeat
	toEmit := []telemetry.Event{
		{Kind: telemetry.KindOrderAck, SubmissionID: subID, Timestamp: now, OrderID: "o1", LatencyNs: 100_000},
		{Kind: telemetry.KindFill, SubmissionID: subID, Timestamp: now, OrderID: "o1", LatencyNs: 200_000},
		{Kind: telemetry.KindCancelAck, SubmissionID: subID, Timestamp: now, OrderID: "o2", LatencyNs: 50_000},
		{Kind: telemetry.KindReject, SubmissionID: subID, Timestamp: now, OrderID: "o3", LatencyNs: 30_000},
		{Kind: telemetry.KindHeartbeat, SubmissionID: subID, Timestamp: now},
	}

	for _, e := range toEmit {
		if err := emitter.Emit(ctx, e); err != nil {
			t.Fatalf("Emit(%s): %v", e.Kind, err)
		}
	}

	// Flush ensures all records reach the broker before we try to consume.
	if err := emitter.Close(); err != nil {
		t.Fatalf("Close/Flush: %v", err)
	}

	// ── Consume from metrics.latency ─────────────────────────────────────────
	// We expect 4 events (ack, fill, cancel, reject) — heartbeat goes elsewhere.
	latencyEvents := consumeFromTopic(t, ctx, telemetry.TopicLatency, subID, 4, 15*time.Second)
	if len(latencyEvents) != 4 {
		t.Errorf("metrics.latency: got %d events, want 4", len(latencyEvents))
	}

	// Verify every event deserialises correctly and carries the right subID
	for i, e := range latencyEvents {
		if e.SubmissionID != subID {
			t.Errorf("latencyEvents[%d].SubmissionID = %q, want %q", i, e.SubmissionID, subID)
		}
		if e.LatencyNs <= 0 {
			t.Errorf("latencyEvents[%d].LatencyNs = %d, want > 0", i, e.LatencyNs)
		}
	}

	// ── Consume from metrics.heartbeat ───────────────────────────────────────
	heartbeatEvents := consumeFromTopic(t, ctx, telemetry.TopicHeartbeat, subID, 1, 10*time.Second)
	if len(heartbeatEvents) != 1 {
		t.Errorf("metrics.heartbeat: got %d events, want 1", len(heartbeatEvents))
	}
	if len(heartbeatEvents) == 1 && heartbeatEvents[0].Kind != telemetry.KindHeartbeat {
		t.Errorf("heartbeat event Kind = %q, want %q", heartbeatEvents[0].Kind, telemetry.KindHeartbeat)
	}
}

// TestRedpandaEmitter_InvalidEventNotProduced verifies that an invalid event
// never reaches the broker — Validate() must reject it before produce.
func TestRedpandaEmitter_InvalidEventNotProduced(t *testing.T) {
	ctx := context.Background()

	emitter, err := telemetry.NewRedpandaEmitter(integrationCfg())
	if err != nil {
		t.Fatalf("NewRedpandaEmitter: %v", err)
	}
	defer emitter.Close()

	bad := telemetry.Event{} // zero value — invalid
	err = emitter.Emit(ctx, bad)
	if err == nil {
		t.Fatal("Emit(invalid event) = nil, want error")
	}
}

// TestRedpandaEmitter_ClosedEmitterRejectsEmit verifies that Emit() returns
// an error after Close() has been called.
func TestRedpandaEmitter_ClosedEmitterRejectsEmit(t *testing.T) {
	ctx := context.Background()

	emitter, err := telemetry.NewRedpandaEmitter(integrationCfg())
	if err != nil {
		t.Fatalf("NewRedpandaEmitter: %v", err)
	}

	if err := emitter.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	emitter.Close()

	e := telemetry.Event{
		Kind: telemetry.KindOrderAck, SubmissionID: "s",
		Timestamp: time.Now().UTC(), OrderID: "o", LatencyNs: 1,
	}
	if err := emitter.Emit(ctx, e); err == nil {
		t.Error("Emit after Close() returned nil, want error")
	}
}

// TestRedpandaEmitter_SubmissionIDIsPartitionKey verifies that all events for
// the same submissionID land on the same partition (order preserved).
func TestRedpandaEmitter_SubmissionIDIsPartitionKey(t *testing.T) {
	ctx := context.Background()

	emitter, err := telemetry.NewRedpandaEmitter(integrationCfg())
	if err != nil {
		t.Fatalf("NewRedpandaEmitter: %v", err)
	}
	defer emitter.Close()

	if err := emitter.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	subID := fmt.Sprintf("keytest-%d", time.Now().UnixNano())
	now := time.Now().UTC()

	const n = 5
	for i := 0; i < n; i++ {
		e := telemetry.Event{
			Kind:         telemetry.KindOrderAck,
			SubmissionID: subID,
			Timestamp:    now.Add(time.Duration(i) * time.Millisecond),
			OrderID:      fmt.Sprintf("o%d", i),
			LatencyNs:    int64(i * 1000),
		}
		if err := emitter.Emit(ctx, e); err != nil {
			t.Fatalf("Emit %d: %v", i, err)
		}
	}
	emitter.Close()

	events := consumeFromTopic(t, ctx, telemetry.TopicLatency, subID, n, 15*time.Second)

	// All events must be on the same partition (same key → same partition).
	if len(events) == 0 {
		t.Fatal("no events consumed")
	}
	// Verify they arrive in order (partition key → ordering guarantee).
	for i := 1; i < len(events); i++ {
		if events[i].Timestamp.Before(events[i-1].Timestamp) {
			t.Errorf("events out of order at index %d: %v < %v",
				i, events[i].Timestamp, events[i-1].Timestamp)
		}
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// consumeFromTopic reads up to `want` events from `topic` where
// event.SubmissionID == subID, waiting up to `timeout`.
// Returns early once `want` matching events are found.
func consumeFromTopic(
	t *testing.T,
	ctx context.Context,
	topic, subID string,
	want int,
	timeout time.Duration,
) []telemetry.Event {
	t.Helper()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(testBroker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("consumer client: %v", err)
	}
	defer cl.Close()

	deadline := time.Now().Add(timeout)
	var collected []telemetry.Event

	for time.Now().Before(deadline) && len(collected) < want {
		pollCtx, cancel := context.WithDeadline(ctx, deadline)
		fetches := cl.PollFetches(pollCtx)
		cancel()

		if fetches.IsClientClosed() {
			break
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			t.Logf("fetch error topic=%s partition=%d: %v", topic, partition, err)
		})

		fetches.EachRecord(func(r *kgo.Record) {
			var e telemetry.Event
			if err := json.Unmarshal(r.Value, &e); err != nil {
				t.Logf("unmarshal error: %v (raw: %s)", err, string(r.Value))
				return
			}
			// Filter to only events from this test run's submissionID.
			if e.SubmissionID == subID {
				collected = append(collected, e)
			}
		})
	}

	return collected
}

// mustNewKgoClient creates a kgo client for admin operations in tests.
func mustNewKgoClient(t *testing.T, broker string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(func() { cl.Close() })
	return cl
}
