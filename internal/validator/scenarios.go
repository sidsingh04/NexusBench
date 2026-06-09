// Package validator implements the pre-flight correctness gate for NexusBench engines.
package validator

import (
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/correctness"
)

// scenario is one named test case: a sequence of orders and the expected fills.
type scenario struct {
	name     string
	orders   []botfleet.Order
	expected []correctness.GoldenFill

	// bookContext describes the expected book state BEFORE this scenario's
	// first order is sent. Included in failure reason strings so contestants
	// can understand what state their engine should be in.
	bookContext string

	// isConcurrent, when true, causes Run to dispatch all orders in this
	// scenario simultaneously via runConcurrentScenario instead of the
	// sequential runScenario loop. Used for the concurrency burst scenario.
	isConcurrent bool

	// concurrentDeadline is the maximum time to wait for all concurrent
	// goroutines to respond. Only used when isConcurrent is true.
	concurrentDeadline time.Duration
}

// fixedScenarios returns the 21-scenario test sequence (20 sequential + 1
// concurrent burst) covering eight correctness axes. The slice is
// reconstructed on every call — it is NOT a package-level variable — so
// parallel test runs cannot share state.
//
// ── Eight correctness axes ────────────────────────────────────────────────────
// Axis 1 (orders 0–1):   Limit orders rest when no opposite side is available.
// Axis 2 (order  2):     Limit sell crosses and partially fills resting buy.
// Axis 3 (order  3):     Market buy sweeps remaining sell-side liquidity.
// Axis 4 (order  4):     Cancel of a known resting order is accepted.
// Axis 5 (orders 5–6):   Cancel of unknown / already-filled IDs is rejected.
// Axis 6 (order  7):     Zero-quantity order is rejected.
// Axis 7 (orders 8–19):  Mixed sequence preserves price-time priority.
// Axis 8 (scenario 20):  10 concurrent resting limit orders all accepted within
//
//	3s deadline (catches deadlocks, goroutine starvation, serial bottlenecks).
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
			bookContext: "book: empty",
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
			bookContext: "book: 1 resting buy at $100.00 qty=10 (val-0-0)",
		},

		// ── Axis 2: limit sell crosses and partially fills resting buy ────────
		{
			name: "limit_sell_crosses_buy_partial_fill",
			orders: []botfleet.Order{
				// Sell at $100.00 with qty 5; resting buy has qty 10. Fill 5 units.
				{ID: "val-2-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-2-0", Accepted: true, ExecutedPrice: 10000, ExecutedQty: 5},
			},
			bookContext: "book: 1 resting buy at $100.00 qty=10 (val-0-0); 1 resting sell at $101.00 qty=10 (val-1-0)",
		},

		// ── Axis 3: market buy sweeps remaining sell-side liquidity ──────────
		{
			name: "market_buy_sweeps_resting_sell",
			orders: []botfleet.Order{
				// Resting sell val-1-0 at $101.00 qty=10 (untouched). Market buy qty=20.
				// Should sweep val-1-0 entirely: executed_qty=10 at $101.00.
				{ID: "val-3-0", Kind: botfleet.KindMarket, Side: botfleet.SideBuy, Quantity: 20},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-3-0", Accepted: true, ExecutedPrice: 10100, ExecutedQty: 10},
			},
			bookContext: "book: 1 resting buy at $100.00 qty=5 (val-0-0, partially filled); 1 resting sell at $101.00 qty=10 (val-1-0)",
		},

		// ── Axis 4: cancel of a known resting order ───────────────────────────
		{
			name: "cancel_of_known_resting_order_accepted",
			orders: []botfleet.Order{
				// val-0-0 placed buy qty=10; partial fill consumed 5 (val-2-0).
				// Remaining qty=5 — still resting. Cancel must be accepted.
				{ID: "val-0-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-0-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: 1 resting buy at $100.00 qty=5 (val-0-0, remaining after partial fill)",
		},

		// ── Axis 5: cancel of unknown / already-consumed IDs is rejected ──────
		{
			name: "cancel_of_unknown_id_rejected",
			orders: []botfleet.Order{
				{ID: "val-NEVER-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-NEVER-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: empty (val-0-0 canceled, val-1-0 swept by market buy)",
		},
		{
			name: "cancel_of_already_canceled_order_rejected",
			orders: []botfleet.Order{
				// val-0-0 was already canceled in the previous scenario.
				{ID: "val-0-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-0-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: empty (val-0-0 already canceled)",
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
			bookContext: "book: empty",
		},

		// ── Axis 7: price-time priority sequence ──────────────────────────────
		{
			name: "price_time_priority_setup_buy_100_50",
			orders: []botfleet.Order{
				{ID: "val-7-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10050, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-7-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: empty",
		},
		{
			name: "price_time_priority_setup_buy_100_50_second",
			orders: []botfleet.Order{
				// Same price as val-7-0; arrives second — lower time priority.
				{ID: "val-8-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10050, Quantity: 3},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-8-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: 1 resting buy at $100.50 qty=5 (val-7-0)",
		},
		{
			name: "price_time_priority_setup_buy_100_00",
			orders: []botfleet.Order{
				// Lower price — lower priority than both $100.50 bids.
				{ID: "val-9-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10000, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-9-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: 2 resting buys at $100.50 (val-7-0 qty=5, val-8-0 qty=3); 1 resting buy at $100.00 qty=10 (val-9-0)",
		},
		{
			name: "price_time_priority_aggressive_sell_fills_best_bid_first",
			orders: []botfleet.Order{
				// Aggressive sell qty=5 at $100.00 — should fill entirely against
				// val-7-0 (highest price $100.50, earliest arrival).
				{ID: "val-10-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				// Fill at maker price $100.50 (not the aggressor's ask).
				{OrderID: "val-10-0", Accepted: true, ExecutedPrice: 10050, ExecutedQty: 5},
			},
			bookContext: "book: val-7-0 qty=5 at $100.50 (highest bid, fills first); val-8-0 qty=3 at $100.50; val-9-0 qty=10 at $100.00",
		},
		{
			name: "price_time_priority_aggressive_sell_fills_second_bid",
			orders: []botfleet.Order{
				// Sell qty=5: val-8-0 (qty=3 at $100.50) fills first, then 2 from val-9-0 ($100.00).
				{ID: "val-11-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10000, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				// Last fill price is $100.00 (from val-9-0 partial).
				{OrderID: "val-11-0", Accepted: true, ExecutedPrice: 10000, ExecutedQty: 5},
			},
			bookContext: "book: val-8-0 qty=3 at $100.50; val-9-0 qty=10 at $100.00",
		},
		{
			name: "cancel_remaining_resting_buy",
			orders: []botfleet.Order{
				// val-9-0 had qty=10; 2 consumed by val-11-0. Remaining=8.
				{ID: "val-9-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-9-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: val-9-0 qty=8 remaining at $100.00 (partially filled)",
		},
		{
			name: "resting_sell_at_ask",
			orders: []botfleet.Order{
				{ID: "val-13-0", Kind: botfleet.KindLimit, Side: botfleet.SideSell, Price: 10100, Quantity: 10},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-13-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: empty buy side",
		},
		{
			name: "market_sell_rejected_on_empty_buy_side",
			orders: []botfleet.Order{
				// No bids remain. Market sell has no liquidity → rejected.
				{ID: "val-14-0", Kind: botfleet.KindMarket, Side: botfleet.SideSell, Quantity: 5},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-14-0", Accepted: false, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: empty buy side; 1 resting sell at $101.00 qty=10 (val-13-0)",
		},
		{
			name: "limit_buy_fills_resting_sell",
			orders: []botfleet.Order{
				// Aggressive buy at $101.00 qty=3 — crosses val-13-0.
				{ID: "val-15-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 10100, Quantity: 3},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-15-0", Accepted: true, ExecutedPrice: 10100, ExecutedQty: 3},
			},
			bookContext: "book: 1 resting sell at $101.00 qty=10 (val-13-0)",
		},
		{
			name: "cancel_partially_consumed_sell",
			orders: []botfleet.Order{
				// val-13-0 had qty=10; 3 consumed by val-15-0. Remaining=7. Cancel accepted.
				{ID: "val-13-0", Kind: botfleet.KindCancel},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-13-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext: "book: val-13-0 qty=7 remaining at $101.00 (partially filled)",
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
			bookContext: "book: empty",
		},

		// ── Axis 8: concurrent burst — detects deadlocks and serial bottlenecks
		{
			name: "concurrent_burst_10",
			// 10 independent limit buys at 10 distinct prices far below any
			// resting sell ($90.00–$99.00). All will rest without matching.
			// Sent simultaneously from 10 goroutines.
			// A correct engine must respond to all 10 within 3 seconds.
			// Deadlocks, per-request goroutine exhaustion, or serialized
			// handlers that cannot overlap will fail this scenario.
			orders: []botfleet.Order{
				{ID: "val-cb-0", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9000, Quantity: 1},
				{ID: "val-cb-1", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9100, Quantity: 1},
				{ID: "val-cb-2", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9200, Quantity: 1},
				{ID: "val-cb-3", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9300, Quantity: 1},
				{ID: "val-cb-4", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9400, Quantity: 1},
				{ID: "val-cb-5", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9500, Quantity: 1},
				{ID: "val-cb-6", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9600, Quantity: 1},
				{ID: "val-cb-7", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9700, Quantity: 1},
				{ID: "val-cb-8", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9800, Quantity: 1},
				{ID: "val-cb-9", Kind: botfleet.KindLimit, Side: botfleet.SideBuy, Price: 9900, Quantity: 1},
			},
			expected: []correctness.GoldenFill{
				{OrderID: "val-cb-0", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-1", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-2", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-3", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-4", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-5", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-6", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-7", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-8", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
				{OrderID: "val-cb-9", Accepted: true, ExecutedPrice: 0, ExecutedQty: 0},
			},
			bookContext:        "book: empty (all previous orders canceled or consumed)",
			isConcurrent:       true,
			concurrentDeadline: 3 * time.Second,
		},
	}
}
