package botfleet

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// FleetConfig controls how a Fleet is constructed and run.
type FleetConfig struct {
	// BotCount is the number of concurrent virtual traders. Default: 100.
	BotCount int

	// RampUpDuration is the period over which bots are started, staggered
	// evenly. This avoids a thundering-herd spike at t=0.
	// Default: 5s.
	RampUpDuration time.Duration

	// TestDuration is how long the fleet runs at full capacity.
	// Default: 60s.
	TestDuration time.Duration

	// TargetURL is the base URL of the contestant's engine.
	// Example: "http://localhost:20001"
	TargetURL string

	// PerBotHTTPTimeout is the per-request HTTP timeout for each bot.
	// Default: 2s (tight enough to detect latency regressions).
	PerBotHTTPTimeout time.Duration

	// GeneratorConfig controls order generation for every bot.
	// Each bot gets an independent generator with a unique seed derived from
	// its index, so all bots generate independent order sequences.
	GeneratorConfig RandomGeneratorConfig
}

// DefaultFleetConfig returns production defaults.
func DefaultFleetConfig(targetURL string) FleetConfig {
	return FleetConfig{
		BotCount:          100,
		RampUpDuration:    5 * time.Second,
		TestDuration:      60 * time.Second,
		TargetURL:         targetURL,
		PerBotHTTPTimeout: 2 * time.Second,
		GeneratorConfig:   DefaultRandomGeneratorConfig(),
	}
}

// Validate returns a non-nil error if the config is invalid.
func (c *FleetConfig) Validate() error {
	if c.BotCount < 1 {
		return fmt.Errorf("fleet: BotCount must be >= 1, got %d", c.BotCount)
	}
	if c.TestDuration <= 0 {
		return fmt.Errorf("fleet: TestDuration must be > 0")
	}
	if c.TargetURL == "" {
		return fmt.Errorf("fleet: TargetURL must not be empty")
	}
	if err := c.GeneratorConfig.Ratios.Validate(); err != nil {
		return fmt.Errorf("fleet: %w", err)
	}
	return nil
}

// FleetResult is the raw output of a Fleet run, before stats aggregation.
type FleetResult struct {
	// Results contains every OrderResult from every bot.
	// The ordering is non-deterministic (results arrive as bots finish).
	Results []OrderResult

	// Stats is the aggregated performance summary computed from Results.
	Stats BotStats

	// StartedAt is when the first bot was launched.
	StartedAt time.Time

	// FinishedAt is when the last bot returned.
	FinishedAt time.Time
}

// Fleet manages a pool of Bots and runs them concurrently.
// The zero value is NOT valid. Use NewFleet.
type Fleet struct {
	cfg FleetConfig
}

// NewFleet constructs and validates a Fleet.
func NewFleet(cfg FleetConfig) (*Fleet, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Fleet{cfg: cfg}, nil
}

// Run launches all bots, waits for the test duration, then collects results.
//
// Ramp-up: bots are started in equal intervals over cfg.RampUpDuration so
// the first bot starts at t=0 and the last at t≈RampUpDuration.
// After RampUpDuration+TestDuration the root context is canceled, which
// causes every bot's Run loop to return.
//
// The outer ctx controls the maximum wall-clock time including ramp-up.
// If ctx is canceled before the test completes, Run returns immediately with
// whatever results have been collected so far.
func (f *Fleet) Run(ctx context.Context) (*FleetResult, error) {
	startedAt := time.Now()

	// Per-bot HTTP client with its own timeout.
	httpClient := &http.Client{Timeout: f.cfg.PerBotHTTPTimeout}

	// testCtx is canceled when the test duration (+ ramp-up) elapses.
	totalDuration := f.cfg.RampUpDuration + f.cfg.TestDuration
	testCtx, cancel := context.WithTimeout(ctx, totalDuration)
	defer cancel()

	// Each bot writes its results into resultsCh. Channel is buffered with
	// one slot per bot so goroutines never block on send after testCtx expires.
	resultsCh := make(chan []OrderResult, f.cfg.BotCount)

	var wg sync.WaitGroup

	rampInterval := time.Duration(0)
	if f.cfg.BotCount > 1 && f.cfg.RampUpDuration > 0 {
		rampInterval = f.cfg.RampUpDuration / time.Duration(f.cfg.BotCount)
	}

	for i := 0; i < f.cfg.BotCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			// Stagger bot start to spread the initial load.
			if rampInterval > 0 && idx > 0 {
				delay := time.Duration(idx) * rampInterval
				select {
				case <-testCtx.Done():
					resultsCh <- nil
					return
				case <-time.After(delay):
				}
			}

			bot, err := f.newBot(idx, httpClient)
			if err != nil {
				// Construction failure is a programming error, not a runtime one.
				// Log would be nice but we have no logger here — return nil results.
				resultsCh <- nil
				return
			}

			resultsCh <- bot.Run(testCtx)
		}(i)
	}

	// Close resultsCh once all bots finish so the collector loop can drain it.
	go func() {
		wg.Wait()
		close(resultsCh)
	}()

	// Collect all results.
	var allResults []OrderResult
	for botResults := range resultsCh {
		allResults = append(allResults, botResults...)
	}

	finishedAt := time.Now()
	stats := ComputeStats(allResults)

	return &FleetResult{
		Results:    allResults,
		Stats:      stats,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}, nil
}

// newBot constructs a single Bot for the given index.
// idx is used to derive a unique seed and bot ID.
func (f *Fleet) newBot(idx int, httpClient *http.Client) (*Bot, error) {
	botID := fmt.Sprintf("bot-%04d", idx)

	// Give each bot a unique seed derived from the base seed and its index,
	// so all bots generate independent order sequences even when the fleet
	// is reproduced with the same base seed.
	genCfg := f.cfg.GeneratorConfig
	if genCfg.Seed == 0 {
		genCfg.Seed = time.Now().UnixNano() + int64(idx)*1_000_003
	} else {
		genCfg.Seed = genCfg.Seed + int64(idx)*1_000_003
	}

	gen, err := NewRandomGenerator(botID, genCfg)
	if err != nil {
		return nil, fmt.Errorf("fleet: bot %d: generator: %w", idx, err)
	}

	transport := NewRESTTransport(f.cfg.TargetURL, httpClient)

	return NewBot(botID, gen, transport)
}
