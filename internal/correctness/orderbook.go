// Package correctness implements the Golden Orderbook and Correctness Checker
// used to verify that a contestant's matching engine maintains price-time
// priority and produces accurate fill results.
//
// Design constraints:
//   - No external dependencies — stdlib only.
//   - Fully deterministic: given the same order sequence, GoldenOrderbook
//     always produces identical fills. No randomness, no wall-clock tie-breaking.
//   - Zero import cycles: correctness must not import worker, submission, sandbox,
//     or botfleet.
//
// Terminology:
//
//	Price-time priority: among orders at the same price, the one that arrived
//	first (lowest sequence number) is matched first.
//	Fill: the execution of a buy and sell order against each other at a price.
package correctness

import (
	"fmt"
	"sort"
)

// Side is the direction of an order within the orderbook.
type Side string

const (
	SideBuy  Side = "buy"
	SideSell Side = "sell"
)

// OrderKind classifies what the order does.
type OrderKind string

const (
	KindLimit  OrderKind = "limit"
	KindMarket OrderKind = "market"
	KindCancel OrderKind = "cancel"
)

// GoldenOrder is the canonical representation of an order sent to the golden
// orderbook. It mirrors botfleet.Order but is defined here so the correctness
// package has zero dependency on botfleet.
type GoldenOrder struct {
	// ID is the unique order identifier. For cancels, ID is the ID of the
	// order to cancel.
	ID string

	// Kind classifies the order.
	Kind OrderKind

	// Side is buy or sell. Ignored for cancel orders.
	Side Side

	// Price is the limit price in integer cents. Ignored for market orders.
	Price int64

	// Quantity is the number of units. Ignored for cancel orders.
	Quantity int64
}

// GoldenFill is the canonical fill result produced by the GoldenOrderbook.
// The Checker compares contestant fills against these to compute the
// correctness score.
type GoldenFill struct {
	// OrderID is the ID of the order that was acted upon.
	OrderID string

	// ExecutedPrice is the price at which a match occurred. 0 for resting/cancel.
	ExecutedPrice int64

	// ExecutedQty is the quantity matched. 0 for resting/cancel acks.
	ExecutedQty int64

	// Accepted is true when the golden book accepted the order.
	Accepted bool
}

// ── internal order representation ────────────────────────────────────────────

// bookEntry is a resting limit order in the golden orderbook.
type bookEntry struct {
	id         string
	side       Side
	price      int64
	quantity   int64
	seq        uint64 // arrival sequence for price-time priority
	remainingQ int64  // quantity not yet matched
}

// GoldenOrderbook is a deterministic price-time priority matching engine.
// It processes orders in submission order and produces GoldenFills that serve
// as the reference for correctness checking.
//
// The zero value is valid and ready to use.
//
// Concurrency: NOT goroutine-safe. The Checker creates one instance per
// benchmark run and applies orders sequentially.
type GoldenOrderbook struct {
	buys  []*bookEntry // max-heap by price, then min-heap by seq (price-time)
	sells []*bookEntry // min-heap by price, then min-heap by seq
	index map[string]*bookEntry // id → entry for O(1) cancel lookup
	seq   uint64
}

// NewGoldenOrderbook returns an initialised GoldenOrderbook.
func NewGoldenOrderbook() *GoldenOrderbook {
	return &GoldenOrderbook{
		index: make(map[string]*bookEntry),
	}
}

// Apply processes one order and returns the canonical GoldenFill.
// The fill reflects whether the order was accepted, and if so, how much
// was matched against existing resting orders.
func (g *GoldenOrderbook) Apply(o GoldenOrder) (GoldenFill, error) {
	if o.ID == "" {
		return GoldenFill{}, fmt.Errorf("golden orderbook: order ID must not be empty")
	}

	switch o.Kind {
	case KindCancel:
		return g.applyCancel(o)
	case KindLimit:
		return g.applyLimit(o)
	case KindMarket:
		return g.applyMarket(o)
	default:
		return GoldenFill{}, fmt.Errorf("golden orderbook: unknown order kind %q", o.Kind)
	}
}

// applyLimit processes a limit order:
//  1. Try to match against the opposite side (price-time priority).
//  2. Any unmatched remainder rests in the book.
func (g *GoldenOrderbook) applyLimit(o GoldenOrder) (GoldenFill, error) {
	if o.Quantity <= 0 {
		return GoldenFill{OrderID: o.ID, Accepted: false}, nil
	}
	if o.Price <= 0 {
		return GoldenFill{OrderID: o.ID, Accepted: false}, nil
	}

	g.seq++
	remaining := o.Quantity
	var totalExecutedQty int64
	var executedPrice int64

	if o.Side == SideBuy {
		// Match against the ask side (sells), ascending price.
		g.sortSells()
		for len(g.sells) > 0 && remaining > 0 {
			best := g.sells[0]
			if best.price > o.Price { // sell price too high
				break
			}
			matchQty := min64(remaining, best.remainingQ)
			executedPrice = best.price
			totalExecutedQty += matchQty
			remaining -= matchQty
			best.remainingQ -= matchQty
			if best.remainingQ == 0 {
				g.sells = g.sells[1:]
				delete(g.index, best.id)
			}
		}
	} else { // SideSell
		// Match against the bid side (buys), descending price.
		g.sortBuys()
		for len(g.buys) > 0 && remaining > 0 {
			best := g.buys[0]
			if best.price < o.Price { // buy price too low
				break
			}
			matchQty := min64(remaining, best.remainingQ)
			executedPrice = best.price
			totalExecutedQty += matchQty
			remaining -= matchQty
			best.remainingQ -= matchQty
			if best.remainingQ == 0 {
				g.buys = g.buys[1:]
				delete(g.index, best.id)
			}
		}
	}

	// Rest any unmatched quantity.
	if remaining > 0 {
		entry := &bookEntry{
			id:         o.ID,
			side:       o.Side,
			price:      o.Price,
			quantity:   o.Quantity,
			seq:        g.seq,
			remainingQ: remaining,
		}
		if o.Side == SideBuy {
			g.buys = append(g.buys, entry)
		} else {
			g.sells = append(g.sells, entry)
		}
		g.index[o.ID] = entry
	}

	return GoldenFill{
		OrderID:       o.ID,
		ExecutedPrice: executedPrice,
		ExecutedQty:   totalExecutedQty,
		Accepted:      true,
	}, nil
}

// applyMarket processes a market order. Market orders match against the best
// available opposite price. If the book is empty, the order is rejected.
func (g *GoldenOrderbook) applyMarket(o GoldenOrder) (GoldenFill, error) {
	if o.Quantity <= 0 {
		return GoldenFill{OrderID: o.ID, Accepted: false}, nil
	}

	g.seq++
	remaining := o.Quantity
	var totalExecutedQty int64
	var executedPrice int64

	if o.Side == SideBuy {
		g.sortSells()
		for len(g.sells) > 0 && remaining > 0 {
			best := g.sells[0]
			matchQty := min64(remaining, best.remainingQ)
			executedPrice = best.price
			totalExecutedQty += matchQty
			remaining -= matchQty
			best.remainingQ -= matchQty
			if best.remainingQ == 0 {
				g.sells = g.sells[1:]
				delete(g.index, best.id)
			}
		}
	} else {
		g.sortBuys()
		for len(g.buys) > 0 && remaining > 0 {
			best := g.buys[0]
			matchQty := min64(remaining, best.remainingQ)
			executedPrice = best.price
			totalExecutedQty += matchQty
			remaining -= matchQty
			best.remainingQ -= matchQty
			if best.remainingQ == 0 {
				g.buys = g.buys[1:]
				delete(g.index, best.id)
			}
		}
	}

	accepted := totalExecutedQty > 0
	return GoldenFill{
		OrderID:       o.ID,
		ExecutedPrice: executedPrice,
		ExecutedQty:   totalExecutedQty,
		Accepted:      accepted,
	}, nil
}

// applyCancel removes a resting order. The cancel is accepted if the order
// exists and is resting; rejected otherwise.
func (g *GoldenOrderbook) applyCancel(o GoldenOrder) (GoldenFill, error) {
	entry, ok := g.index[o.ID]
	if !ok {
		// Unknown or already filled/cancelled — reject.
		return GoldenFill{OrderID: o.ID, Accepted: false}, nil
	}

	delete(g.index, o.ID)
	if entry.side == SideBuy {
		g.buys = removeEntry(g.buys, o.ID)
	} else {
		g.sells = removeEntry(g.sells, o.ID)
	}

	return GoldenFill{OrderID: o.ID, Accepted: true}, nil
}

// sortBuys sorts buys descending by price, ascending by seq (price-time priority).
func (g *GoldenOrderbook) sortBuys() {
	sort.SliceStable(g.buys, func(i, j int) bool {
		if g.buys[i].price != g.buys[j].price {
			return g.buys[i].price > g.buys[j].price // higher price first
		}
		return g.buys[i].seq < g.buys[j].seq // earlier arrival first
	})
}

// sortSells sorts sells ascending by price, ascending by seq (price-time priority).
func (g *GoldenOrderbook) sortSells() {
	sort.SliceStable(g.sells, func(i, j int) bool {
		if g.sells[i].price != g.sells[j].price {
			return g.sells[i].price < g.sells[j].price // lower price first
		}
		return g.sells[i].seq < g.sells[j].seq // earlier arrival first
	})
}

func removeEntry(entries []*bookEntry, id string) []*bookEntry {
	for i, e := range entries {
		if e.id == id {
			return append(entries[:i], entries[i+1:]...)
		}
	}
	return entries
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
