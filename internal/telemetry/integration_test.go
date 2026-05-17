package telemetry_test

// integration_test.go verifies the full emit → NDJSON → parse pipeline
// that Step 1 of Phase 2 requires.
//
// These tests simulate what you'd do manually with jq:
//
//   go run ./cmd/server 2>&1 | jq -c 'select(.kind != null)'
//   go run ./cmd/server 2>&1 | jq -s 'group_by(.kind) | map({kind:.[0].kind, count:length})'
//
// Running as a test means:
//   - The jq assertions are machine-checked on every `go test` run.
//   - CI catches regressions before you manually pipe output.
//   - No jq binary required on the test machine.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

// TestPipeline_NDJSONRoundtrip simulates a 30-second replay:
// emit N events of mixed kinds, then parse every line back and verify
// the data survives the round-trip without loss.
func TestPipeline_NDJSONRoundtrip(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)
	ctx := context.Background()
	now := time.Now().UTC()

	// Build a representative batch: 3 acks, 2 fills, 1 cancel, 1 reject, 1 heartbeat
	events := []telemetry.Event{
		{Kind: telemetry.KindOrderAck, SubmissionID: "sub-1", Timestamp: now, OrderID: "o1", LatencyNs: 100_000},
		{Kind: telemetry.KindOrderAck, SubmissionID: "sub-1", Timestamp: now, OrderID: "o2", LatencyNs: 110_000},
		{Kind: telemetry.KindOrderAck, SubmissionID: "sub-1", Timestamp: now, OrderID: "o3", LatencyNs: 90_000},
		{Kind: telemetry.KindFill, SubmissionID: "sub-1", Timestamp: now, OrderID: "o1", LatencyNs: 200_000, Meta: map[string]string{"qty": "50"}},
		{Kind: telemetry.KindFill, SubmissionID: "sub-1", Timestamp: now, OrderID: "o2", LatencyNs: 210_000, Meta: map[string]string{"qty": "100"}},
		{Kind: telemetry.KindCancelAck, SubmissionID: "sub-1", Timestamp: now, OrderID: "o3", LatencyNs: 50_000},
		{Kind: telemetry.KindReject, SubmissionID: "sub-1", Timestamp: now, OrderID: "o4", LatencyNs: 30_000, Meta: map[string]string{"reason": "price_out_of_range"}},
		{Kind: telemetry.KindHeartbeat, SubmissionID: "sub-1", Timestamp: now},
	}

	for _, e := range events {
		if err := emitter.Emit(ctx, e); err != nil {
			t.Fatalf("Emit(%s): %v", e.Kind, err)
		}
	}

	// ── Parse every output line back into an Event ────────────────────────────

	type wireEvent struct {
		Kind         string            `json:"kind"`
		SubmissionID string            `json:"submission_id"`
		Timestamp    time.Time         `json:"timestamp"`
		OrderID      string            `json:"order_id"`
		LatencyNs    int64             `json:"latency_ns"`
		Meta         map[string]string `json:"meta"`
	}

	var parsed []wireEvent
	scanner := bufio.NewScanner(&buf)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var we wireEvent
		if err := json.Unmarshal([]byte(line), &we); err != nil {
			t.Errorf("line %d: invalid JSON: %v\n  content: %s", lineNum, err, line)
			continue
		}
		parsed = append(parsed, we)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	// ── Assertions that mirror `jq -s 'length'` ───────────────────────────────

	if got, want := len(parsed), len(events); got != want {
		t.Fatalf("parsed %d events, want %d", got, want)
	}

	// ── Assertions that mirror `jq -s 'group_by(.kind) | map({kind,count})'` ──

	kindCount := make(map[string]int)
	for _, we := range parsed {
		kindCount[we.Kind]++
	}

	wantCounts := map[string]int{
		"order_ack":  3,
		"fill":       2,
		"cancel_ack": 1,
		"reject":     1,
		"heartbeat":  1,
	}
	for kind, want := range wantCounts {
		if got := kindCount[kind]; got != want {
			t.Errorf("kind %q: count = %d, want %d", kind, got, want)
		}
	}

	// ── Data fidelity check ───────────────────────────────────────────────────

	// The first event should be the first ack with latency 100_000 ns.
	first := parsed[0]
	if first.Kind != "order_ack" {
		t.Errorf("parsed[0].kind = %q, want %q", first.Kind, "order_ack")
	}
	if first.LatencyNs != 100_000 {
		t.Errorf("parsed[0].latency_ns = %d, want 100000", first.LatencyNs)
	}
	if first.SubmissionID != "sub-1" {
		t.Errorf("parsed[0].submission_id = %q, want %q", first.SubmissionID, "sub-1")
	}

	// The reject event should carry its meta reason.
	var rejectEvent *wireEvent
	for i := range parsed {
		if parsed[i].Kind == "reject" {
			rejectEvent = &parsed[i]
			break
		}
	}
	if rejectEvent == nil {
		t.Fatal("no reject event in output")
	}
	if rejectEvent.Meta["reason"] != "price_out_of_range" {
		t.Errorf("reject meta[reason] = %q, want %q", rejectEvent.Meta["reason"], "price_out_of_range")
	}

	// The heartbeat must have no order_id in the JSON.
	var hbEvent *wireEvent
	for i := range parsed {
		if parsed[i].Kind == "heartbeat" {
			hbEvent = &parsed[i]
			break
		}
	}
	if hbEvent == nil {
		t.Fatal("no heartbeat event in output")
	}
	if hbEvent.OrderID != "" {
		t.Errorf("heartbeat order_id = %q, want empty", hbEvent.OrderID)
	}
}

// TestPipeline_LatencyStats verifies the latency values survive round-trip
// and that a consumer can compute a simple p99 from them — the same computation
// TimescaleDB will do in Step 3.
func TestPipeline_LatencyStats(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := telemetry.NewStdoutEmitter(&buf)
	ctx := context.Background()
	now := time.Now().UTC()

	// Emit 100 order_ack events with known latency values 1ns..100ns.
	// p99 of [1..100] is 99 (the 99th smallest value in a 100-element set).
	for i := 1; i <= 100; i++ {
		e := telemetry.Event{
			Kind:         telemetry.KindOrderAck,
			SubmissionID: "perf-sub",
			Timestamp:    now,
			OrderID:      "o",
			LatencyNs:    int64(i),
		}
		if err := emitter.Emit(ctx, e); err != nil {
			t.Fatalf("Emit %d: %v", i, err)
		}
	}

	var latencies []int64
	scanner := bufio.NewScanner(&buf)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var obj struct {
			LatencyNs int64 `json:"latency_ns"`
		}
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("parse error: %v", err)
		}
		latencies = append(latencies, obj.LatencyNs)
	}

	if len(latencies) != 100 {
		t.Fatalf("got %d latency values, want 100", len(latencies))
	}

	// Compute p99 the same way a consumer would (sort + index).
	sorted := make([]int64, len(latencies))
	copy(sorted, latencies)
	sortInt64s(sorted)

	p99idx := int(float64(len(sorted))*0.99) - 1
	if p99idx < 0 {
		p99idx = 0
	}
	p99 := sorted[p99idx]
	if p99 != 99 {
		t.Errorf("p99 = %d, want 99", p99)
	}
}

// TestPipeline_EmitterInterfaceCompatibility verifies that StdoutEmitter,
// NoopEmitter, and RecordingEmitter all satisfy the Emitter interface and
// behave consistently on the same input.
func TestPipeline_EmitterInterfaceCompatibility(t *testing.T) {
	t.Parallel()

	e := validEvent()
	ctx := context.Background()

	emitters := []struct {
		name    string
		emitter telemetry.Emitter
	}{
		{"StdoutEmitter", telemetry.NewStdoutEmitter(&bytes.Buffer{})},
		{"NoopEmitter", telemetry.NoopEmitter{}},
		{"RecordingEmitter", telemetry.NewRecordingEmitter()},
	}

	for _, tc := range emitters {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.emitter.Emit(ctx, e); err != nil {
				t.Errorf("%s.Emit(valid) = %v, want nil", tc.name, err)
			}
			if err := tc.emitter.Close(); err != nil {
				t.Errorf("%s.Close() = %v, want nil", tc.name, err)
			}
		})
	}
}

// sortInt64s is a simple insertion sort — good enough for 100 elements in tests.
// We avoid importing sort to keep this file dependency-free.
func sortInt64s(s []int64) {
	for i := 1; i < len(s); i++ {
		key := s[i]
		j := i - 1
		for j >= 0 && s[j] > key {
			s[j+1] = s[j]
			j--
		}
		s[j+1] = key
	}
}
