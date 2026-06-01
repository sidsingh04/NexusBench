package validator

// scenarios.go defines the fixed, deterministic 20-order smoke-test sequence.
//
// Design goals:
//   - Purely data: no I/O, no randomness, no external state.
//   - Self-contained: the golden fills are pre-computed by replaying the order
//     sequence through a GoldenOrderbook; the Validator doesn't need to do that
//     at startup — the expected fills are embedded here.
//   - Comprehensive: covers the seven correctness axes that a correct CLOB must
//     satisfy. See scenarioGroups below for the breakdown.
//
// This file is unexported because callers interact only through Validator.Run.
// Tests that need to inspect the sequence can do so through the returned
// ValidationResult.Scenarios slice.

import (
	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
)

// scenario is one named test case: a sequence of orders and the expected fills
// the contestant's engine must produce.
type scenario struct {
	// name is the human-readable label shown in ValidationResult.Scenarios.
	name string

	// orders is the exact order sequence sent to the engine.
	// All order IDs must be globally unique across the entire 20-order sequence
	// because the Validator sends them all to one running engine instance.
	orders []botfleet.Order

	// expected is the canonical fill for each order, keyed by order index.
	// expected[i] corresponds to orders[i].
	expected []correctness.GoldenFill
}

// fixedScenarios returns the 20-order test sequence covering seven correctness
// axes. The slice is reconstructed on every call — it is not a package-level
// variable — so callers in parallel test runs cannot share state.
//
// Order ID naming convention: "val-<scenarioIndex>-<orderIndex>" ensures
// global uniqueness even when multiple Validators run against the same engine.
//
// Pre-computed expected fills are derived by mentally executing a GoldenOrderbook
// over the sequence. They are verified by the TestValidator_GoldenFillsMatchOrderbook
// test in validator_test.go.
//
// ── Seven correctness axes covered ───────────────────────────────────────────
//
// Axis 1 (orders 0–1):    Limit orders rest when no opposite side is available.
// Axis 2 (order  2):      Limit sell crosses and partially fills resting buy.
// Axis 3 (order  3):      Market buy sweeps remaining sell-side liquidity.
// Axis 4 (order  4):      Cancel of a known resting order is accepted.
// Axis 5 (orders 5–6):    Cancel of unknown / already-filled IDs is rejected.
// Axis 6 (order  7):      Zero-quantity order is rejected (accepted: false).
// Axis 7 (orders 8–19):   Larger mixed sequence preserves price-time priority
//
//	and produces correct partial fills.
func fixedScenarios() []scenario {
	return []scenario{
		// ── Axis 1: resting limit orders ─────────────────────────────────────
		{
			name: "limit_buy_rests_on_empty_book",
			orders: []botfleet.Order{
				{ID: "val-0-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10000, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-0-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "limit_sell_rests_above_best_bid",
			orders: []botfleet.Order{
				// bid is $100.00 (val-0-0); this sell is at $101.00 — no cross.
				{ID: "val-1-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10100, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-1-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},

		// ── Axis 2: limit sell crosses and partially fills resting buy ────────
		{
			name: "limit_sell_crosses_buy_partial_fill",
			orders: []botfleet.Order{
				// Sell at $100.00 (=bid) with qty 5; resting buy has qty 10.
				// Engine should fill 5 units at $100.00 and leave 5 on the buy side.
				{ID: "val-2-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-2-0", Accepted: true, ExecutedPrice: 10000, ExecutedQty: 5},
			},
		},

		// ── Axis 3: market buy sweeps remaining sell-side liquidity ──────────
		{
			name: "market_buy_sweeps_resting_sell",
			orders: []botfleet.Order{
				// Resting sells: val-1-0 at $101.00 qty 10 (still full, not yet touched).
				// Buy up to qty 20 — should sweep val-1-0 entirely (10 units at $101.00).
				// The remaining 10 qty cannot fill (no more sells) so the market order
				// accepts partial: executed_qty=10, executed_price=10100.
				{ID: "val-3-0", Kind: botfleet.KindMarket, Side: botfleet.SideBuy, Quantity: 20},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-3-0", Accepted: true, ExecutedPrice: 10100, ExecutedQty: 10},
			},
		},

		// ── Axis 4: cancel of a known resting order ───────────────────────────
		{
			name: "cancel_of_known_resting_order_accepted",
			orders: []botfleet.Order{
				// val-0-0 placed a buy of 10 at $100.00; partial fill consumed 5 (val-2-0).
				// Remaining qty = 5 — the order is still resting. Cancel must be accepted.
				{ID: "val-0-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-0-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},

		// ── Axis 5: cancel of unknown / already-consumed IDs is rejected ──────
		{
			name: "cancel_of_unknown_id_rejected",
			orders: []botfleet.Order{
				// "val-NEVER-0" was never submitted — engine must reject.
				{ID: "val-NEVER-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-NEVER-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "cancel_of_already_canceled_order_rejected",
			orders: []botfleet.Order{
				// val-0-0 was already canceled in the previous scenario — double cancel rejected.
				{ID: "val-0-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-0-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},

		// ── Axis 6: zero-quantity order is rejected ───────────────────────────
		{
			name: "zero_quantity_order_rejected",
			orders: []botfleet.Order{
				{ID: "val-6-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10000, Quantity: 0},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-6-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},

		// ── Axis 7: mixed sequence tests price-time priority ─────────────────
		// The remaining 13 orders build a realistic order-book scenario:
		// multiple resting bids at two price levels, two aggressive sells that
		// should fill in price-then-time order, and cleanup cancels.
		{
			name: "price_time_priority_setup_buy_100_50",
			orders: []botfleet.Order{
				// First resting buy at $100.50 (10050) qty 5 — arrives first at this price.
				{ID: "val-7-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10050, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-7-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "price_time_priority_setup_buy_100_50_second",
			orders: []botfleet.Order{
				// Second resting buy at same $100.50, qty 3 — arrives second; fills after val-7-0.
				{ID: "val-8-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10050, Quantity: 3},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-8-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "price_time_priority_setup_buy_100_00",
			orders: []botfleet.Order{
				// Buy at lower price $100.00 (10000), qty 10 — lower priority than 10050 bids.
				{ID: "val-9-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10000, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-9-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "price_time_priority_aggressive_sell_fills_best_bid_first",
			orders: []botfleet.Order{
				// Aggressive limit sell at $100.00 (crosses all three resting buys).
				// Qty 5 — should fill entirely against val-7-0 (highest price, earliest time).
				{ID: "val-10-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				// Fills 5 units at $100.50 (the best bid price, not the ask).
				{OrderID: "val-10-0", Accepted: true, ExecutedPrice: 10050, ExecutedQty: 5},
			},
		},
		{
			name: "price_time_priority_aggressive_sell_fills_second_bid",
			orders: []botfleet.Order{
				// Sell qty 5: fills val-8-0 (qty 3 at $100.50) then takes 2 from val-9-0 (at $100.00).
				{ID: "val-11-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				// First 3 fill at $100.50 (val-8-0); next 2 fill at $100.00 (val-9-0).
				// GoldenOrderbook reports the last executed price per order.
				{OrderID: "val-11-0", Accepted: true, ExecutedPrice: 10000, ExecutedQty: 5},
			},
		},
		{
			name: "cancel_remaining_resting_buy",
			orders: []botfleet.Order{
				// val-9-0 had qty 10; 2 were consumed by val-11-0. Remaining = 8.
				{ID: "val-9-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-9-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "resting_sell_at_ask",
			orders: []botfleet.Order{
				// Place a resting sell at $101.00 qty 10.
				{ID: "val-13-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10100, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-13-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "market_sell_rejected_on_empty_buy_side",
			orders: []botfleet.Order{
				// No bids remain after val-9-0 cancel. Market sell has no liquidity → rejected.
				{ID: "val-14-0", Kind: botfleet.KindMarket, Side: botfleet.SideSell, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-14-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "limit_buy_fills_resting_sell",
			orders: []botfleet.Order{
				// Aggressive limit buy at $101.00 qty 3 — crosses val-13-0 (sell at $101.00).
				{ID: "val-15-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10100, Quantity: 3},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-15-0", Accepted: true, ExecutedPrice: 10100, ExecutedQty: 3},
			},
		},
		{
			name: "cancel_partially_consumed_sell",
			orders: []botfleet.Order{
				// val-13-0 had qty 10; 3 consumed. Remaining = 7. Cancel accepted.
				{ID: "val-13-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-13-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
		},
		{
			name: "multiple_unknown_cancels_all_rejected",
			orders: []botfleet.Order{
				{ID: "val-GHOST-1", Kind: botfleet.KindCancel},
				{ID: "val-GHOST-2", Kind: botfleet.KindCancel},
				{ID: "val-GHOST-3", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-GHOST-1", Accepted: false},
				{OrderID: "val-GHOST-2", Accepted: false},
				{OrderID: "val-GHOST-3", Accepted: false},
			},
		},
	}
}
