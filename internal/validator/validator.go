// Package validator implements the pre-flight correctness gate for NexusBench engines.
package validator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
)

// ScenarioResult is the outcome of one named test scenario.
type ScenarioResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Reason string `json:"reason,omitempty"`
}

// ValidationResult is the complete output of a Validator.Run call.
type ValidationResult struct {
	SubmissionID string           `json:"submission_id"`
	Scenarios    []ScenarioResult `json:"scenarios"`
	AllPassed    bool             `json:"all_passed"`
	TestedAt     time.Time        `json:"tested_at"`
}

// Validator runs the fixed deterministic smoke test against a contestant engine.
// The zero value is NOT valid. Use New.
type Validator struct {
	transport botfleet.BotTransport
}

// New constructs a Validator targeting the engine reachable via transport.
// transport must not be nil.
func New(transport botfleet.BotTransport) *Validator {
	if transport == nil {
		panic("validator.New: transport must not be nil")
	}
	return &Validator{transport: transport}
}

// Run executes all fixed scenarios against the engine and returns a
// ValidationResult. Each scenario is independent: a failure does not abort
// subsequent scenarios, so callers always receive a complete picture.
func (v *Validator) Run(ctx context.Context, submissionID string) (*ValidationResult, error) {
	result := &ValidationResult{
		SubmissionID: submissionID,
		TestedAt:     time.Now().UTC(),
	}

	scenarios := fixedScenarios()
	result.Scenarios = make([]ScenarioResult, 0, len(scenarios))

	allPassed := true

	for i := range scenarios {
		sc := &scenarios[i]

		var sr ScenarioResult
		var err error

		if sc.isConcurrent {
			sr, err = v.runConcurrentScenario(ctx, sc)
		} else {
			sr, err = v.runScenario(ctx, sc)
		}

		if err != nil {
			return result, fmt.Errorf("validator: scenario %q: %w", sc.name, err)
		}

		result.Scenarios = append(result.Scenarios, sr)
		if !sr.Passed {
			allPassed = false
		}
	}

	result.AllPassed = allPassed
	return result, nil
}

// runScenario sends all orders for one sequential scenario and compares fills.
func (v *Validator) runScenario(ctx context.Context, sc *scenario) (ScenarioResult, error) {
	for i, order := range sc.orders {
		select {
		case <-ctx.Done():
			return ScenarioResult{}, ctx.Err()
		default:
		}

		fill, err := v.transport.Send(ctx, order)
		if err != nil {
			if ctx.Err() != nil {
				return ScenarioResult{}, ctx.Err()
			}
			return ScenarioResult{
				Name:   sc.name,
				Passed: false,
				Reason: fmt.Sprintf("order %q: transport error: %v", order.ID, err),
			}, nil
		}

		expected := sc.expected[i]
		if reason := compareFills(order, fill, expected, sc.bookContext); reason != "" {
			return ScenarioResult{
				Name:   sc.name,
				Passed: false,
				Reason: reason,
			}, nil
		}
	}

	return ScenarioResult{Name: sc.name, Passed: true}, nil
}

// concurrentResult holds the result of one goroutine in a concurrent burst.
type concurrentResult struct {
	idx  int
	fill botfleet.Fill
	err  error
}

// runConcurrentScenario sends all orders in sc simultaneously from parallel
// goroutines and verifies all responses arrive within sc.concurrentDeadline.
func (v *Validator) runConcurrentScenario(ctx context.Context, sc *scenario) (ScenarioResult, error) {
	if ctx.Err() != nil {
		return ScenarioResult{}, ctx.Err()
	}

	n := len(sc.orders)
	results := make(chan concurrentResult, n)

	burstCtx, cancel := context.WithTimeout(ctx, sc.concurrentDeadline)
	defer cancel()

	// Each goroutine sends one independent order.
	var wg sync.WaitGroup
	wg.Add(n)
	for i, order := range sc.orders {
		i, order := i, order
		go func() {
			defer wg.Done()
			fill, err := v.transport.Send(burstCtx, order)
			results <- concurrentResult{idx: i, fill: fill, err: err}
		}()
	}

	// Close results channel once all goroutines finish.
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results. The channel is closed when all goroutines finish OR
	// when burstCtx expires (the goroutines return early with a context error).
	collected := make([]concurrentResult, 0, n)
	for r := range results {
		collected = append(collected, r)
	}

	// Check for timeouts: any goroutine whose error is context.DeadlineExceeded
	// or context.Canceled (from burstCtx) is a timeout.
	var timedOutIDs []string
	for i, order := range sc.orders {
		found := false
		for _, r := range collected {
			if r.idx == i {
				found = true
				if r.err != nil && (strings.Contains(r.err.Error(), "deadline") ||
					strings.Contains(r.err.Error(), "context canceled")) {
					timedOutIDs = append(timedOutIDs, order.ID)
				}
				break
			}
		}
		if !found {
			timedOutIDs = append(timedOutIDs, order.ID)
		}
	}

	if len(timedOutIDs) > 0 {
		reason := fmt.Sprintf(
			"concurrent burst of %d orders: only %d/%d responses arrived within %s deadline.\n"+
				"Missing or timed-out order IDs: [%s].\n"+
				"This indicates your engine may be deadlocking or dropping connections under "+
				"concurrent load. Check for: global mutexes held during I/O, unbounded work "+
				"queues, goroutine leaks, or per-request goroutine exhaustion.",
			n, n-len(timedOutIDs), n,
			sc.concurrentDeadline,
			strings.Join(timedOutIDs, ", "),
		)
		return ScenarioResult{Name: sc.name, Passed: false, Reason: reason}, nil
	}

	// Check for transport errors (non-timeout).
	for _, r := range collected {
		if r.err != nil {
			order := sc.orders[r.idx]
			reason := fmt.Sprintf(
				"concurrent burst of %d orders: order %q %s: transport error: %v",
				n, order.ID, describeOrder(order), r.err,
			)
			return ScenarioResult{Name: sc.name, Passed: false, Reason: reason}, nil
		}
	}

	// Sort collected by index to check fills in order.
	sorted := make([]concurrentResult, n)
	for _, r := range collected {
		sorted[r.idx] = r
	}

	// Check each fill against expected.
	for i, r := range sorted {
		order := sc.orders[i]
		expected := sc.expected[i]
		if reason := compareFills(order, r.fill, expected, sc.bookContext); reason != "" {
			fullReason := fmt.Sprintf(
				"concurrent burst of %d orders: %s\n"+
					"Engine may be rejecting or mishandling orders under concurrent load "+
					"(possible race condition in order validation or book state management).",
				n, reason,
			)
			return ScenarioResult{Name: sc.name, Passed: false, Reason: fullReason}, nil
		}
	}

	return ScenarioResult{Name: sc.name, Passed: true}, nil
}

// ── Fill comparison helpers ───────────────────────────────────────────────────

// compareFills returns an enriched human-readable mismatch reason when the
// actual fill does not match expected. Returns empty string on match.
//
// The reason string includes:
//   - The order's kind, side, price (as $X.XX), and quantity
//   - The actual vs expected value with human labels
//   - The bookContext so the contestant knows what book state is expected
func compareFills(order botfleet.Order, actual botfleet.Fill, expected correctness.GoldenFill, bookContext string) string {
	orderDesc := describeOrder(order)

	if actual.Accepted != expected.Accepted {
		gotLabel := "accepted"
		if !actual.Accepted {
			gotLabel = "rejected"
		}
		wantLabel := "accepted"
		if !expected.Accepted {
			wantLabel = "rejected"
		}
		reason := fmt.Sprintf(
			"order %q %s: accepted mismatch: got %v (%s), want %v (%s)",
			order.ID, orderDesc, actual.Accepted, gotLabel, expected.Accepted, wantLabel,
		)
		if bookContext != "" {
			reason += fmt.Sprintf("\n  book state: %s", bookContext)
		}
		return reason
	}

	if actual.ExecutedPrice != expected.ExecutedPrice {
		gotLabel := "(no fill)"
		if actual.ExecutedPrice > 0 {
			gotLabel = fmt.Sprintf("(%s)", formatPrice(actual.ExecutedPrice))
		}
		wantLabel := "(no fill)"
		if expected.ExecutedPrice > 0 {
			wantLabel = fmt.Sprintf("(%s)", formatPrice(expected.ExecutedPrice))
		}
		reason := fmt.Sprintf(
			"order %q %s: executed_price mismatch: got %d %s, want %d %s",
			order.ID, orderDesc,
			actual.ExecutedPrice, gotLabel,
			expected.ExecutedPrice, wantLabel,
		)
		if bookContext != "" {
			reason += fmt.Sprintf("\n  book state: %s", bookContext)
		}
		return reason
	}

	if actual.ExecutedQty != expected.ExecutedQty {
		gotLabel := "(no fill)"
		if actual.ExecutedQty > 0 {
			gotLabel = fmt.Sprintf("(qty %d)", actual.ExecutedQty)
		}
		wantLabel := "(no fill)"
		if expected.ExecutedQty > 0 {
			wantLabel = fmt.Sprintf("(qty %d)", expected.ExecutedQty)
		}
		reason := fmt.Sprintf(
			"order %q %s: executed_qty mismatch: got %d %s, want %d %s",
			order.ID, orderDesc,
			actual.ExecutedQty, gotLabel,
			expected.ExecutedQty, wantLabel,
		)
		if bookContext != "" {
			reason += fmt.Sprintf("\n  book state: %s", bookContext)
		}
		return reason
	}

	return ""
}

// formatPrice converts integer cents to "$X.XX" display format.
func formatPrice(cents int64) string {
	return fmt.Sprintf("$%.2f", float64(cents)/100.0)
}

// describeOrder returns a bracket summary for use in reason strings.
// Examples: "[limit buy qty=10 price=$100.00]", "[cancel]", "[market sell qty=5]"
func describeOrder(o botfleet.Order) string {
	switch o.Kind {
	case botfleet.KindCancel:
		return "[cancel]"
	case botfleet.KindMarket:
		return fmt.Sprintf("[market %s qty=%d]", o.Side, o.Quantity)
	default:
		return fmt.Sprintf("[limit %s qty=%d price=%s]", o.Side, o.Quantity, formatPrice(o.Price))
	}
}
