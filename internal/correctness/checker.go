package correctness

// CorrectnessResult is the output of a Checker run.
type CorrectnessResult struct {
	// Score is the fraction of contestant fills that matched the golden fills
	// exactly (0.0 = all wrong, 1.0 = all correct).
	Score float64

	// TotalFills is the number of fills compared.
	TotalFills int64

	// CorrectFills is the number that matched the golden reference exactly.
	CorrectFills int64

	// IncorrectFills is TotalFills - CorrectFills.
	IncorrectFills int64
}

// ContestantFill is the fill returned by the contestant's engine for a given
// order. It mirrors GoldenFill but is kept separate so the Checker does not
// conflate the two sources.
type ContestantFill struct {
	// OrderID identifies which order this fill corresponds to.
	OrderID string

	// ExecutedPrice is the price the contestant's engine reported.
	ExecutedPrice int64

	// ExecutedQty is the quantity the contestant's engine reported.
	ExecutedQty int64

	// Accepted is true when the contestant's engine accepted the order.
	Accepted bool
}

// Checker compares contestant fills against the reference fills produced by
// GoldenOrderbook, computing a correctness score.
//
// Matching rules:
//   - A contestant fill is "correct" if and only if:
//     (a) Accepted matches,
//     (b) ExecutedPrice matches (or both are 0), and
//     (c) ExecutedQty matches.
//   - Fills with no corresponding golden entry are counted as incorrect.
//   - Golden fills with no corresponding contestant fill are counted as incorrect.
//
// The zero value is valid and ready to use.
type Checker struct{}

// NewChecker returns a Checker. The zero value is equally valid.
func NewChecker() *Checker { return &Checker{} }

// Check compares contestantFills against goldenFills and returns the
// aggregated CorrectnessResult.
//
// Both slices are matched by OrderID. Order within each slice does not matter.
// If an OrderID appears multiple times in either slice, only the first
// occurrence is used; duplicates are counted as incorrect.
func (c *Checker) Check(contestantFills []ContestantFill, goldenFills []GoldenFill) CorrectnessResult {
	// Handle empty slices — no orders, no errors.
	if len(goldenFills) == 0 && len(contestantFills) == 0 {
		return CorrectnessResult{Score: 1.0}
	}

	// Build a lookup of golden fills by OrderID.
	goldenByID := make(map[string]GoldenFill, len(goldenFills))
	for _, gf := range goldenFills {
		if _, seen := goldenByID[gf.OrderID]; !seen {
			goldenByID[gf.OrderID] = gf
		}
	}

	// Track which golden IDs have been matched.
	matched := make(map[string]bool, len(goldenFills))

	var correct, incorrect int64

	for _, cf := range contestantFills {
		gf, ok := goldenByID[cf.OrderID]
		if !ok || matched[cf.OrderID] {
			// No golden reference or duplicate — incorrect.
			incorrect++
			continue
		}
		matched[cf.OrderID] = true

		if fillsMatch(cf, gf) {
			correct++
		} else {
			incorrect++
		}
	}

	// Golden fills with no contestant response are incorrect.
	for id := range goldenByID {
		if !matched[id] {
			incorrect++
		}
	}

	total := correct + incorrect
	score := 0.0
	if total > 0 {
		score = float64(correct) / float64(total)
	}

	return CorrectnessResult{
		Score:          score,
		TotalFills:     total,
		CorrectFills:   correct,
		IncorrectFills: incorrect,
	}
}

// fillsMatch returns true if the contestant fill exactly matches the golden fill.
func fillsMatch(cf ContestantFill, gf GoldenFill) bool {
	return cf.Accepted == gf.Accepted &&
		cf.ExecutedPrice == gf.ExecutedPrice &&
		cf.ExecutedQty == gf.ExecutedQty
}
