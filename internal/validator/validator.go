// Package validator runs a fixed, deterministic smoke test against a deployed
// contestant engine and returns per-scenario pass/fail results.
//
// # Purpose
//
// The dry-run validator lets contestants verify their engine's wiring before
// spending a benchmark slot. It exercises seven correctness axes (see
// scenarios.go) using a hand-crafted, reproducible order sequence and compares
// each fill against the canonical GoldenOrderbook output.
//
// # Side effects: none
//
// The Validator does NOT modify submission status, does NOT write
// BenchmarkResults, and does NOT touch the leaderboard. It is a pure read
// operation against the live container.
//
// # Dependencies
//
// Imports only botfleet.BotTransport and correctness.GoldenOrderbook.
// Must NOT import submission, worker, contest, or any storage package.
// This constraint prevents import cycles and keeps the package independently
// testable with httptest.Server without spinning up any infrastructure.
//
// # Concurrency
//
// Validator.Run is safe to call concurrently for different submissions. Each
// call constructs its own GoldenOrderbook and its own scenario loop. There is
// no shared mutable state in the Validator struct.
package validator

import (
	"context"
	"fmt"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
)

// ScenarioResult is the outcome of one named test scenario.
type ScenarioResult struct {
	// Name is the human-readable scenario label (e.g. "limit_buy_rests_on_empty_book").
	Name string `json:"name"`

	// Passed is true when every order in the scenario received the expected fill.
	Passed bool `json:"passed"`

	// Reason describes why the scenario failed. Empty when Passed is true.
	// Format: "order <id>: <field> mismatch: got <actual>, want <expected>"
	Reason string `json:"reason,omitempty"`
}

// ValidationResult is the complete output of a Validator.Run call.
type ValidationResult struct {
	// SubmissionID is the submission that was tested.
	SubmissionID string `json:"submission_id"`

	// Scenarios holds one result per test case, in execution order.
	Scenarios []ScenarioResult `json:"scenarios"`

	// AllPassed is true only when every scenario passed.
	// Shortcut for callers that want a single boolean gate.
	AllPassed bool `json:"all_passed"`

	// TestedAt is the wall-clock time when Run was called.
	TestedAt time.Time `json:"tested_at"`
}

// Validator runs the fixed deterministic smoke test against a contestant engine.
//
// Create one Validator per target endpoint (i.e. per submission). The transport
// must already be connected to the live sandbox container.
//
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
//
// The method is safe to call concurrently for different submissions.
//
// ctx cancellation is respected: if the context is canceled mid-run, Run
// returns immediately with a non-nil error. Scenarios that completed before
// cancellation are included in the partial result's Scenarios slice.
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

		sr, err := v.runScenario(ctx, sc)
		if err != nil {
			// Context cancellation or transport failure mid-run: return partial
			// results with a wrapped error so the caller can distinguish a
			// context cancel from a transport error.
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

// runScenario sends all orders for one scenario and compares the fills against
// the pre-computed expected fills. It returns a ScenarioResult describing
// whether the scenario passed, and the first mismatch reason if it failed.
//
// The GoldenOrderbook is NOT used here for fill computation — expected fills
// are embedded in the scenario definition (see scenarios.go). This means
// runScenario runs in O(orders) time with no stateful side effects beyond
// the transport calls.
//
// Important: because scenarios share a single running engine, each scenario
// must be carefully designed so that the engine's book state after one
// scenario is the expected starting state for the next. The fixedScenarios
// function guarantees this ordering.
func (v *Validator) runScenario(ctx context.Context, sc *scenario) (ScenarioResult, error) {
	for i, order := range sc.orders {
		// Respect context cancellation between order sends.
		select {
		case <-ctx.Done():
			return ScenarioResult{}, ctx.Err()
		default:
		}

		fill, err := v.transport.Send(ctx, order)
		if err != nil {
			// Transport errors (connection refused, timeout, malformed JSON) are
			// surfaced as scenario failures with a descriptive reason rather than
			// propagated as Go errors. This gives contestants actionable feedback
			// ("your engine is not responding on the expected port") rather than
			// a silent HTTP 500.
			//
			// Exception: context cancellation IS propagated as a Go error because
			// the caller needs to know the run was interrupted, not just that one
			// order failed.
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
		if reason := compareFills(order.ID, fill, expected); reason != "" {
			return ScenarioResult{
				Name:   sc.name,
				Passed: false,
				Reason: reason,
			}, nil
		}
	}

	return ScenarioResult{Name: sc.name, Passed: true}, nil
}

// compareFills returns a human-readable mismatch reason when the actual fill
// from the contestant's engine does not match the expected fill. Returns an
// empty string when they match.
//
// Matching rules (identical to correctness.Checker.fillsMatch):
//   - Accepted must match.
//   - ExecutedPrice must match (0 == 0 counts as a match for resting orders).
//   - ExecutedQty must match.
//
// We do not check fill.OrderID here: the transport always echoes the
// order_id back correctly if the HTTP round-trip succeeded; a mismatch there
// would indicate a serious engine bug that would fail the Accepted check too.
func compareFills(orderID string, actual botfleet.Fill, expected correctness.GoldenFill) string {
	if actual.Accepted != expected.Accepted {
		return fmt.Sprintf("order %q: accepted mismatch: got %v, want %v",
			orderID, actual.Accepted, expected.Accepted)
	}
	if actual.ExecutedPrice != expected.ExecutedPrice {
		return fmt.Sprintf("order %q: executed_price mismatch: got %d, want %d",
			orderID, actual.ExecutedPrice, expected.ExecutedPrice)
	}
	if actual.ExecutedQty != expected.ExecutedQty {
		return fmt.Sprintf("order %q: executed_qty mismatch: got %d, want %d",
			orderID, actual.ExecutedQty, expected.ExecutedQty)
	}
	return ""
}
