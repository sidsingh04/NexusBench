package correctness_test

// checker_test.go tests GoldenOrderbook and Checker in isolation.
// No external dependencies, no infrastructure.
//
// Tests:
//   TestGoldenOrderbook_LimitMatchesCrossing       — buy above ask → full fill
//   TestGoldenOrderbook_LimitRests                 — non-crossing limit rests in book
//   TestGoldenOrderbook_PriceTimePriority          — earlier order matched first at same price
//   TestGoldenOrderbook_MarketFill                 — market order drains book
//   TestGoldenOrderbook_MarketEmptyBook            — market on empty book → rejected
//   TestGoldenOrderbook_Cancel                     — cancel removes resting order
//   TestGoldenOrderbook_CancelUnknown              — cancel of unknown ID → rejected
//   TestGoldenOrderbook_PartialFill                — large market order partially fills
//   TestChecker_PerfectMatch                       — score 1.0 on identical fills
//   TestChecker_ZeroMatch                          — score 0.0 on all wrong
//   TestChecker_PartialMatch                       — score proportional to correct fills
//   TestChecker_BothEmpty                          — score 1.0 on empty slices
//   TestChecker_MissingContestantFill              — golden fills with no contestant → incorrect
//   TestChecker_ExtraContestantFill                — contestant fills with no golden → incorrect

import (
	"testing"

	"github.com/nexusbench/nexusbench/internal/correctness"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func limitBuy(id string, price, qty int64) correctness.GoldenOrder {
	return correctness.GoldenOrder{ID: id, Kind: correctness.KindLimit, Side: correctness.SideBuy, Price: price, Quantity: qty}
}

func limitSell(id string, price, qty int64) correctness.GoldenOrder {
	return correctness.GoldenOrder{ID: id, Kind: correctness.KindLimit, Side: correctness.SideSell, Price: price, Quantity: qty}
}

func marketBuy(id string, qty int64) correctness.GoldenOrder {
	return correctness.GoldenOrder{ID: id, Kind: correctness.KindMarket, Side: correctness.SideBuy, Quantity: qty}
}

func cancel(id string) correctness.GoldenOrder {
	return correctness.GoldenOrder{ID: id, Kind: correctness.KindCancel}
}

func apply(t *testing.T, book *correctness.GoldenOrderbook, o correctness.GoldenOrder) correctness.GoldenFill {
	t.Helper()
	fill, err := book.Apply(o)
	if err != nil {
		t.Fatalf("Apply(%s %s): %v", o.Kind, o.ID, err)
	}
	return fill
}

// ── GoldenOrderbook ───────────────────────────────────────────────────────────

func TestGoldenOrderbook_LimitMatchesCrossing(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	// Place a resting sell at 100.
	fill := apply(t, book, limitSell("sell-1", 10_000, 50))
	if fill.Accepted != true || fill.ExecutedQty != 0 {
		t.Errorf("resting sell: Accepted=%v ExecutedQty=%d, want Accepted=true, ExecutedQty=0", fill.Accepted, fill.ExecutedQty)
	}

	// Buy at 101 crosses the 100 ask → full fill.
	fill = apply(t, book, limitBuy("buy-1", 10_100, 30))
	if !fill.Accepted {
		t.Error("crossing buy should be accepted")
	}
	if fill.ExecutedQty != 30 {
		t.Errorf("ExecutedQty = %d, want 30", fill.ExecutedQty)
	}
	if fill.ExecutedPrice != 10_000 {
		t.Errorf("ExecutedPrice = %d, want 10000 (ask price)", fill.ExecutedPrice)
	}
}

func TestGoldenOrderbook_LimitRests(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	// Buy at 99, no resting sells → order rests.
	fill := apply(t, book, limitBuy("rest-buy", 9_900, 20))
	if !fill.Accepted {
		t.Error("limit buy should be accepted even when no match")
	}
	if fill.ExecutedQty != 0 {
		t.Errorf("ExecutedQty = %d, want 0 (resting)", fill.ExecutedQty)
	}
}

func TestGoldenOrderbook_PriceTimePriority(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	// Two resting sells at the same price — first in, first matched.
	apply(t, book, limitSell("first-sell", 10_000, 10))
	apply(t, book, limitSell("second-sell", 10_000, 10))

	// Buy matches 10 units — should hit first-sell.
	fill := apply(t, book, limitBuy("buy", 10_000, 10))
	if fill.ExecutedQty != 10 {
		t.Errorf("ExecutedQty = %d, want 10", fill.ExecutedQty)
	}
	// second-sell should still be in the book (place another buy to verify).
	fill2 := apply(t, book, limitBuy("buy2", 10_000, 10))
	if fill2.ExecutedQty != 10 {
		t.Errorf("second buy ExecutedQty = %d, want 10 (second-sell still resting)", fill2.ExecutedQty)
	}
}

func TestGoldenOrderbook_MarketFill(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	apply(t, book, limitSell("s1", 10_000, 100))

	fill := apply(t, book, marketBuy("m1", 50))
	if !fill.Accepted {
		t.Error("market buy against resting sell should be accepted")
	}
	if fill.ExecutedQty != 50 {
		t.Errorf("ExecutedQty = %d, want 50", fill.ExecutedQty)
	}
}

func TestGoldenOrderbook_MarketEmptyBook(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	// Empty book — market order cannot fill.
	fill := apply(t, book, marketBuy("m-empty", 10))
	if fill.Accepted {
		t.Error("market buy on empty book should be rejected")
	}
}

func TestGoldenOrderbook_Cancel(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	apply(t, book, limitBuy("to-cancel", 9_900, 50))

	fill := apply(t, book, cancel("to-cancel"))
	if !fill.Accepted {
		t.Error("cancel of existing order should be accepted")
	}

	// After cancel, a sell that would have matched should not match.
	fill2 := apply(t, book, limitSell("s1", 9_900, 50))
	if fill2.ExecutedQty != 0 {
		t.Errorf("after cancel, sell matched %d units, want 0", fill2.ExecutedQty)
	}
}

func TestGoldenOrderbook_CancelUnknown(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	fill := apply(t, book, cancel("nonexistent"))
	if fill.Accepted {
		t.Error("cancel of unknown order should be rejected")
	}
}

func TestGoldenOrderbook_PartialFill(t *testing.T) {
	t.Parallel()
	book := correctness.NewGoldenOrderbook()

	// 30 units resting.
	apply(t, book, limitSell("s1", 10_000, 30))

	// Market buy of 100 — only 30 available.
	fill := apply(t, book, marketBuy("m1", 100))
	if fill.ExecutedQty != 30 {
		t.Errorf("ExecutedQty = %d, want 30 (partial fill)", fill.ExecutedQty)
	}
}

// ── Checker ───────────────────────────────────────────────────────────────────

func goldenFill(id string, accepted bool, price, qty int64) correctness.GoldenFill {
	return correctness.GoldenFill{OrderID: id, Accepted: accepted, ExecutedPrice: price, ExecutedQty: qty}
}

func contestantFill(id string, accepted bool, price, qty int64) correctness.ContestantFill {
	return correctness.ContestantFill{OrderID: id, Accepted: accepted, ExecutedPrice: price, ExecutedQty: qty}
}

func TestChecker_PerfectMatch(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()

	golden := []correctness.GoldenFill{
		goldenFill("a", true, 10_000, 10),
		goldenFill("b", false, 0, 0),
	}
	contestant := []correctness.ContestantFill{
		contestantFill("a", true, 10_000, 10),
		contestantFill("b", false, 0, 0),
	}

	result := checker.Check(contestant, golden)
	if result.Score != 1.0 {
		t.Errorf("score = %f, want 1.0", result.Score)
	}
	if result.CorrectFills != 2 {
		t.Errorf("CorrectFills = %d, want 2", result.CorrectFills)
	}
}

func TestChecker_ZeroMatch(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()

	golden := []correctness.GoldenFill{goldenFill("x", true, 10_000, 5)}
	contestant := []correctness.ContestantFill{contestantFill("x", true, 9_000, 5)} // wrong price

	result := checker.Check(contestant, golden)
	if result.Score != 0.0 {
		t.Errorf("score = %f, want 0.0", result.Score)
	}
}

func TestChecker_PartialMatch(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()

	golden := []correctness.GoldenFill{
		goldenFill("a", true, 10_000, 10),
		goldenFill("b", true, 10_000, 10),
		goldenFill("c", true, 10_000, 10),
		goldenFill("d", true, 10_000, 10),
	}
	contestant := []correctness.ContestantFill{
		contestantFill("a", true, 10_000, 10), // correct
		contestantFill("b", true, 10_000, 10), // correct
		contestantFill("c", false, 0, 0),      // wrong
		contestantFill("d", true, 9_000, 10),  // wrong price
	}

	result := checker.Check(contestant, golden)
	// 2 correct out of 4 total
	wantScore := 0.5
	if result.Score != wantScore {
		t.Errorf("score = %f, want %f", result.Score, wantScore)
	}
	if result.CorrectFills != 2 {
		t.Errorf("CorrectFills = %d, want 2", result.CorrectFills)
	}
	if result.IncorrectFills != 2 {
		t.Errorf("IncorrectFills = %d, want 2", result.IncorrectFills)
	}
}

func TestChecker_BothEmpty(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()
	result := checker.Check([]correctness.ContestantFill{}, []correctness.GoldenFill{})
	if result.Score != 0.0 {
		t.Errorf("score = %f, want 0.0 for empty input", result.Score)
	}
}

func TestChecker_MissingContestantFill(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()

	golden := []correctness.GoldenFill{goldenFill("a", true, 10_000, 5)}
	// Contestant returned no fills for "a".
	result := checker.Check(nil, golden)
	if result.Score != 0.0 {
		t.Errorf("score = %f, want 0.0 (missing contestant fill)", result.Score)
	}
	if result.IncorrectFills != 1 {
		t.Errorf("IncorrectFills = %d, want 1", result.IncorrectFills)
	}
}

func TestChecker_ExtraContestantFill(t *testing.T) {
	t.Parallel()
	checker := correctness.NewChecker()

	// Golden has 1 fill; contestant returned 2 (one extra unknown ID).
	golden := []correctness.GoldenFill{goldenFill("a", true, 10_000, 5)}
	contestant := []correctness.ContestantFill{
		contestantFill("a", true, 10_000, 5), // correct
		contestantFill("z", true, 10_000, 5), // no golden reference → incorrect
	}

	result := checker.Check(contestant, golden)
	// 1 correct, 1 incorrect → score 0.5
	if result.Score != 0.5 {
		t.Errorf("score = %f, want 0.5", result.Score)
	}
}
