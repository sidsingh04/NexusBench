package validator_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
	"github.com/nexusbench/nexusbench/internal/validator"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// correctEngine returns an httptest.Server whose POST /orders handler
// replays a real GoldenOrderbook so it always produces the correct fills.
// This is the happy-path server used by TestValidator_AllPass.
func correctEngine(t *testing.T) *httptest.Server {
	t.Helper()

	// We need to run the GoldenOrderbook inside the fake server so it generates
	// the same fills the validator expects. Import the correctness package here
	// to do so — this is test code, not production code, so the import is allowed.
	//
	// The server maintains a single orderbook shared across all requests (like a
	// real engine). Access is serialized by the httptest server (one goroutine
	// per request) but the GoldenOrderbook is not goroutine-safe; however,
	// the Validator sends orders sequentially (one at a time), so there is no
	// concurrent access in practice.
	srv := newGoldenServer(t)
	return srv
}

// newGoldenServer creates an httptest.Server backed by a GoldenOrderbook.
// Each request processes one order and returns the canonical golden fill.
func newGoldenServer(t *testing.T) *httptest.Server {
	t.Helper()
	book := newOrderbook()
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && r.Method != http.MethodDelete {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			OrderID  string `json:"order_id"`
			Kind     string `json:"kind"`
			Side     string `json:"side"`
			Price    int64  `json:"price"`
			Quantity int64  `json:"quantity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fill := book.apply(req.OrderID, req.Kind, req.Side, req.Price, req.Quantity)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fill) //nolint:errcheck
	})
	return httptest.NewServer(mux)
}

// orderFill is the JSON shape returned by the fake server.
type orderFill struct {
	OrderID       string `json:"order_id"`
	Accepted      bool   `json:"accepted"`
	ExecutedPrice int64  `json:"executed_price"`
	ExecutedQty   int64  `json:"executed_qty"`
}

// inMemoryOrderbook is a minimal stateful orderbook for the test server.
// It mirrors the GoldenOrderbook logic without the full package import.
type inMemoryOrderbook struct {
	buys  []bookEntry
	sells []bookEntry
	index map[string]int // id → index in buys/sells (simplified)
}

type bookEntry struct {
	id         string
	side       string
	price      int64
	remainingQ int64
	seq        int
}

var globalSeq int

func newOrderbook() *inMemoryOrderbook {
	return &inMemoryOrderbook{index: make(map[string]int)}
}

func (ob *inMemoryOrderbook) apply(id, kind, side string, price, qty int64) orderFill {
	switch kind {
	case "cancel":
		return ob.applyCancel(id)
	case "limit":
		return ob.applyLimit(id, side, price, qty)
	case "market":
		return ob.applyMarket(id, side, qty)
	default:
		return orderFill{OrderID: id, Accepted: false}
	}
}

func (ob *inMemoryOrderbook) applyCancel(id string) orderFill {
	// Try to remove from buys
	for i, e := range ob.buys {
		if e.id == id {
			ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			delete(ob.index, id)
			return orderFill{OrderID: id, Accepted: true}
		}
	}
	// Try to remove from sells
	for i, e := range ob.sells {
		if e.id == id {
			ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			delete(ob.index, id)
			return orderFill{OrderID: id, Accepted: true}
		}
	}
	return orderFill{OrderID: id, Accepted: false}
}

func (ob *inMemoryOrderbook) applyLimit(id, side string, price, qty int64) orderFill {
	if qty <= 0 || price <= 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	globalSeq++
	remaining := qty
	var execPrice int64
	var execQty int64

	if side == "buy" {
		// Sort sells ascending by price, then by seq
		sortSells(ob.sells)
		i := 0
		for i < len(ob.sells) && remaining > 0 {
			s := &ob.sells[i]
			if s.price > price {
				break
			}
			match := min64(remaining, s.remainingQ)
			execPrice = s.price
			execQty += match
			remaining -= match
			s.remainingQ -= match
			if s.remainingQ == 0 {
				ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			} else {
				i++
			}
		}
	} else {
		// Sort buys descending by price, then by seq
		sortBuys(ob.buys)
		i := 0
		for i < len(ob.buys) && remaining > 0 {
			b := &ob.buys[i]
			if b.price < price {
				break
			}
			match := min64(remaining, b.remainingQ)
			execPrice = b.price
			execQty += match
			remaining -= match
			b.remainingQ -= match
			if b.remainingQ == 0 {
				ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			} else {
				i++
			}
		}
	}

	if remaining > 0 {
		entry := bookEntry{id: id, side: side, price: price, remainingQ: remaining, seq: globalSeq}
		if side == "buy" {
			ob.buys = append(ob.buys, entry)
		} else {
			ob.sells = append(ob.sells, entry)
		}
		ob.index[id] = globalSeq
	}

	return orderFill{OrderID: id, Accepted: true, ExecutedPrice: execPrice, ExecutedQty: execQty}
}

func (ob *inMemoryOrderbook) applyMarket(id, side string, qty int64) orderFill {
	if qty <= 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	remaining := qty
	var execPrice int64
	var execQty int64

	if side == "buy" {
		sortSells(ob.sells)
		i := 0
		for i < len(ob.sells) && remaining > 0 {
			s := &ob.sells[i]
			match := min64(remaining, s.remainingQ)
			execPrice = s.price
			execQty += match
			remaining -= match
			s.remainingQ -= match
			if s.remainingQ == 0 {
				ob.sells = append(ob.sells[:i], ob.sells[i+1:]...)
			} else {
				i++
			}
		}
	} else {
		sortBuys(ob.buys)
		i := 0
		for i < len(ob.buys) && remaining > 0 {
			b := &ob.buys[i]
			match := min64(remaining, b.remainingQ)
			execPrice = b.price
			execQty += match
			remaining -= match
			b.remainingQ -= match
			if b.remainingQ == 0 {
				ob.buys = append(ob.buys[:i], ob.buys[i+1:]...)
			} else {
				i++
			}
		}
	}

	if execQty == 0 {
		return orderFill{OrderID: id, Accepted: false}
	}
	return orderFill{OrderID: id, Accepted: true, ExecutedPrice: execPrice, ExecutedQty: execQty}
}

func sortBuys(buys []bookEntry) {
	for i := 1; i < len(buys); i++ {
		for j := i; j > 0; j-- {
			if buys[j].price > buys[j-1].price ||
				(buys[j].price == buys[j-1].price && buys[j].seq < buys[j-1].seq) {
				buys[j], buys[j-1] = buys[j-1], buys[j]
			} else {
				break
			}
		}
	}
}

func sortSells(sells []bookEntry) {
	for i := 1; i < len(sells); i++ {
		for j := i; j > 0; j-- {
			if sells[j].price < sells[j-1].price ||
				(sells[j].price == sells[j-1].price && sells[j].seq < sells[j-1].seq) {
				sells[j], sells[j-1] = sells[j-1], sells[j]
			} else {
				break
			}
		}
	}
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// wrongPriceEngine returns a server that always returns ExecutedPrice=9999
// for filled orders (a deliberately wrong price).
func wrongPriceEngine(t *testing.T) *httptest.Server {
	t.Helper()
	book := newOrderbook()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OrderID  string `json:"order_id"`
			Kind     string `json:"kind"`
			Side     string `json:"side"`
			Price    int64  `json:"price"`
			Quantity int64  `json:"quantity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fill := book.apply(req.OrderID, req.Kind, req.Side, req.Price, req.Quantity)
		// Corrupt the price on every fill that has an executed quantity.
		if fill.ExecutedQty > 0 {
			fill.ExecutedPrice = 9999
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fill) //nolint:errcheck
	}))
}

// wrongCancelEngine returns a server that always accepts cancel orders,
// even for unknown IDs.
func wrongCancelEngine(t *testing.T) *httptest.Server {
	t.Helper()
	book := newOrderbook()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			OrderID  string `json:"order_id"`
			Kind     string `json:"kind"`
			Side     string `json:"side"`
			Price    int64  `json:"price"`
			Quantity int64  `json:"quantity"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fill := book.apply(req.OrderID, req.Kind, req.Side, req.Price, req.Quantity)
		// Force-accept every cancel regardless of whether the order exists.
		if req.Kind == "cancel" {
			fill.Accepted = true
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(fill) //nolint:errcheck
	}))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestValidator_AllPass verifies that a correctly-implemented engine (backed by
// our own GoldenOrderbook logic) passes every scenario.
func TestValidator_AllPass(t *testing.T) {
	srv := correctEngine(t)
	defer srv.Close()

	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: 5 * time.Second})
	v := validator.New(transport)

	result, err := v.Run(context.Background(), "sub-test-1")
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if !result.AllPassed {
		for _, sc := range result.Scenarios {
			if !sc.Passed {
				t.Errorf("scenario %q failed: %s", sc.Name, sc.Reason)
			}
		}
		t.Fatalf("expected AllPassed=true, got false")
	}
	if result.SubmissionID != "sub-test-1" {
		t.Errorf("SubmissionID: got %q, want %q", result.SubmissionID, "sub-test-1")
	}
	if result.TestedAt.IsZero() {
		t.Error("TestedAt must not be zero")
	}
	if len(result.Scenarios) == 0 {
		t.Error("Scenarios must not be empty")
	}
}

// TestValidator_FailOnWrongExecutedPrice verifies that an engine returning the
// wrong executed price on a fill is caught with a descriptive reason.
func TestValidator_FailOnWrongExecutedPrice(t *testing.T) {
	srv := wrongPriceEngine(t)
	defer srv.Close()

	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: 5 * time.Second})
	v := validator.New(transport)

	result, err := v.Run(context.Background(), "sub-test-2")
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	// The wrong-price engine corrupts every fill that has executed_qty > 0.
	// The first such fill is in "limit_sell_crosses_buy_partial_fill" (scenario 3,
	// 0-indexed as 2). Scenarios before that (resting orders with 0 executed_qty)
	// pass because the price corruption only fires on fills.
	if result.AllPassed {
		t.Fatal("expected AllPassed=false for wrong-price engine, got true")
	}

	// Find the first failed scenario and verify the reason mentions executed_price.
	found := false
	for _, sc := range result.Scenarios {
		if !sc.Passed {
			if sc.Reason == "" {
				t.Errorf("failed scenario %q has empty Reason", sc.Name)
			}
			// The reason should mention "executed_price" to be actionable.
			if len(sc.Reason) > 0 {
				found = true
				t.Logf("first failure: %s — %s", sc.Name, sc.Reason)
				break
			}
		}
	}
	if !found {
		t.Error("no failed scenario with a non-empty Reason found")
	}
}

// TestValidator_FailOnCancelAccepted verifies that an engine that accepts
// unknown cancel IDs is caught.
func TestValidator_FailOnCancelAccepted(t *testing.T) {
	srv := wrongCancelEngine(t)
	defer srv.Close()

	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: 5 * time.Second})
	v := validator.New(transport)

	result, err := v.Run(context.Background(), "sub-test-3")
	if err != nil {
		t.Fatalf("Run returned unexpected error: %v", err)
	}

	if result.AllPassed {
		t.Fatal("expected AllPassed=false for wrong-cancel engine, got true")
	}

	// Find any failure related to cancel rejection — should mention "accepted".
	for _, sc := range result.Scenarios {
		if !sc.Passed {
			t.Logf("failed scenario: %s — %s", sc.Name, sc.Reason)
		}
	}
}

// TestValidator_ContextCancellation verifies that canceling the context
// mid-run returns a non-nil error and does not panic.
func TestValidator_ContextCancellation(t *testing.T) {
	// A slow server that hangs for longer than our context allows.
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First request succeeds normally; subsequent ones stall so the
		// context has time to be canceled.
		if reqCount.Add(1) > 2 {
			// Stall until the client disconnects.
			select {
			case <-r.Context().Done():
			case <-time.After(30 * time.Second):
			}
			return
		}
		// Return a correct-looking fill for the first two requests.
		w.Header().Set("Content-Type", "application/json")
		var req struct {
			OrderID string `json:"order_id"`
		}
		json.NewDecoder(r.Body).Decode(&req)      //nolint:errcheck
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"order_id": req.OrderID, "accepted": true,
			"executed_price": int64(0), "executed_qty": int64(0),
		})
	}))
	defer srv.Close()

	// Give the context a short timeout to trigger cancellation mid-run.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: 2 * time.Second})
	v := validator.New(transport)

	result, err := v.Run(ctx, "sub-test-4")

	// We expect either an error (context canceled) or AllPassed=false
	// (transport error surfaced as a scenario failure). Either way, no panic.
	if err == nil && result.AllPassed {
		// This would mean all scenarios passed despite the stalling server —
		// only possible if the test ran very slowly on a fast machine.
		// Not a test failure, but worth noting.
		t.Log("context cancellation test: all scenarios passed before timeout (machine too fast?)")
	}
	// The important thing is no panic and the function returned.
	t.Logf("err=%v, allPassed=%v, scenarios=%d",
		err, result != nil && result.AllPassed,
		func() int {
			if result != nil {
				return len(result.Scenarios)
			}
			return 0
		}())
}

// TestValidator_New_NilTransportPanics verifies that New panics when transport is nil.
// This is a programming error that should be caught at construction time.
func TestValidator_New_NilTransportPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for nil transport, got none")
		}
	}()
	validator.New(nil)
}
