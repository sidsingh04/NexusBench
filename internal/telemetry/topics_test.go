package telemetry_test

import (
	"testing"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// TestTopicForKind verifies every Kind routes to the correct topic.
// This is the contract the Step 3 consumer depends on — if routing changes,
// the consumer's topic subscription must change too, and this test breaks
// to force that conversation.
func TestTopicForKind(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind  telemetry.Kind
		topic string
	}{
		{telemetry.KindOrderAck, telemetry.TopicLatency},
		{telemetry.KindFill, telemetry.TopicLatency},
		{telemetry.KindCancelAck, telemetry.TopicLatency},
		{telemetry.KindReject, telemetry.TopicLatency},
		{telemetry.KindHeartbeat, telemetry.TopicHeartbeat},
		// Unknown kind must go to DLQ, never be dropped silently.
		{"unknown_future_kind", telemetry.TopicDLQ},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(string(tc.kind), func(t *testing.T) {
			t.Parallel()
			got := telemetry.TopicForKind(tc.kind)
			if got != tc.topic {
				t.Errorf("TopicForKind(%q) = %q, want %q", tc.kind, got, tc.topic)
			}
		})
	}
}

// TestAllTopics verifies the topic list is complete and contains no duplicates.
func TestAllTopics(t *testing.T) {
	t.Parallel()

	topics := telemetry.AllTopics()

	if len(topics) == 0 {
		t.Fatal("AllTopics() returned empty slice")
	}

	// No duplicates
	seen := make(map[string]bool)
	for _, topic := range topics {
		if topic == "" {
			t.Error("AllTopics() contains an empty string")
		}
		if seen[topic] {
			t.Errorf("AllTopics() contains duplicate topic %q", topic)
		}
		seen[topic] = true
	}

	// The three known topics must be present
	required := []string{telemetry.TopicLatency, telemetry.TopicHeartbeat, telemetry.TopicDLQ}
	for _, r := range required {
		if !seen[r] {
			t.Errorf("AllTopics() missing required topic %q", r)
		}
	}
}

// TestTopicConstants verifies topic name strings are stable.
// These values are written into Redpanda and referenced by consumers —
// changing them is a breaking change that requires a data migration.
func TestTopicConstants(t *testing.T) {
	t.Parallel()

	if telemetry.TopicLatency != "metrics.latency" {
		t.Errorf("TopicLatency = %q, want %q", telemetry.TopicLatency, "metrics.latency")
	}
	if telemetry.TopicHeartbeat != "metrics.heartbeat" {
		t.Errorf("TopicHeartbeat = %q, want %q", telemetry.TopicHeartbeat, "metrics.heartbeat")
	}
	if telemetry.TopicDLQ != "metrics.dlq" {
		t.Errorf("TopicDLQ = %q, want %q", telemetry.TopicDLQ, "metrics.dlq")
	}
}
