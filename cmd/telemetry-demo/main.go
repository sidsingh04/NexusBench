// telemetry-demo is a small manual verification tool for Step 1 of Phase 2.
// It emits a batch of mixed events to stdout in NDJSON format so you can
// visually inspect the output and pipe it through jq.
//
// Usage (from project root):
//
//	go run ./cmd/telemetry-demo
//	go run ./cmd/telemetry-demo | jq '.'
//	go run ./cmd/telemetry-demo | jq '{kind, latency_ns, submission_id}'
//	go run ./cmd/telemetry-demo | jq -s 'group_by(.kind) | map({kind: .[0].kind, count: length})'
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/nexusbench/nexusbench/internal/telemetry"
)

func main() {
	emitter := telemetry.NewStdoutEmitter(os.Stdout)
	ctx := context.Background()
	now := time.Now().UTC()
	subID := "demo-sub-001"

	events := []telemetry.Event{
		{
			Kind:         telemetry.KindOrderAck,
			SubmissionID: subID,
			Timestamp:    now,
			OrderID:      "ord-001",
			LatencyNs:    98_000,
			Meta:         map[string]string{"side": "buy", "qty": "100"},
		},
		{
			Kind:         telemetry.KindOrderAck,
			SubmissionID: subID,
			Timestamp:    now.Add(1 * time.Millisecond),
			OrderID:      "ord-002",
			LatencyNs:    143_000,
			Meta:         map[string]string{"side": "sell", "qty": "50"},
		},
		{
			Kind:         telemetry.KindFill,
			SubmissionID: subID,
			Timestamp:    now.Add(2 * time.Millisecond),
			OrderID:      "ord-001",
			LatencyNs:    210_000,
			Meta:         map[string]string{"fill_qty": "100", "fill_price": "99.50"},
		},
		{
			Kind:         telemetry.KindCancelAck,
			SubmissionID: subID,
			Timestamp:    now.Add(3 * time.Millisecond),
			OrderID:      "ord-002",
			LatencyNs:    55_000,
		},
		{
			Kind:         telemetry.KindReject,
			SubmissionID: subID,
			Timestamp:    now.Add(4 * time.Millisecond),
			OrderID:      "ord-003",
			LatencyNs:    31_000,
			Meta:         map[string]string{"reason": "price_out_of_range"},
		},
		{
			Kind:         telemetry.KindHeartbeat,
			SubmissionID: subID,
			Timestamp:    now.Add(5 * time.Millisecond),
		},
	}

	for _, e := range events {
		if err := emitter.Emit(ctx, e); err != nil {
			fmt.Fprintf(os.Stderr, "emit error: %v\n", err)
			os.Exit(1)
		}
	}
}
