package botfleet_test

// fleet_test.go tests the botfleet package in isolation — no Docker, no
// Redpanda, no real trading engine. A httptest.Server acts as the sandbox
// endpoint, and a fakeTransport drives Bot.Run directly for unit tests.
//
// Tests:
//   TestComputeStats_KnownLatencies           — exact percentile assertions
//   TestComputeStats_Empty                    — zero BotStats on empty input
//   TestComputeStats_AllErrors                — zero latency stats on all-error input
//   TestComputeStats_SingleSample             — p50=p90=p99 for a single result
//   TestComputeStats_MaxTPS_Window            — 100ms window picks correct peak
//   TestRandomGenerator_Ratios                — distribution within ±10%
//   TestRandomGenerator_UniqueIDs             — every generated ID is unique
//   TestRandomGenerator_InvalidConfig         — validate rejects bad ratios
//   TestBot_Run_ContextCancel                 — bot stops cleanly on ctx cancel
//   TestBot_Run_NoGoroutineLeak               — goroutine count stable after cancel
//   TestFleet_Run_AllBotsExecute              — N bots all send orders
//   TestFleet_Run_ContextCancel               — fleet respects ctx cancellation
//   TestFleet_Validate                        — invalid configs rejected

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nexusbench/nexusbench/internal/botfleet"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// okFill returns a JSON-encoded accepted fill response for the given orderID.
func okFill(orderID string) []byte {
	b, _ := json.Marshal(map[string]any{
		"order_id": orderID,
		"accepted": true,
		"executed_price": int64(10000),
		"executed_qty":   int64(10),
	})
	return b
}

// echoServer starts a test HTTP server that accepts all orders.
// It increments requestCount on every request.
func echoServer(t *testing.T, requestCount *atomic.Int64) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount.Add(1)
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		orderID, _ := req["order_id"].(string)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(okFill(orderID))
	}))
}

// makeResults builds a slice of OrderResults with the given latencies (ns).
// All results are successful (Err == nil).
func makeResults(latenciesNs ...int64) []botfleet.OrderResult {
	base := time.Now()
	results := make([]botfleet.OrderResult, len(latenciesNs))
	for i, ns := range latenciesNs {
		results[i] = botfleet.OrderResult{
			Order:     botfleet.Order{ID: "ord", Kind: botfleet.KindLimit},
			SentAt:    base.Add(time.Duration(i) * time.Millisecond * 10),
			AckedAt:   base.Add(time.Duration(i)*time.Millisecond*10 + time.Duration(ns)),
			LatencyNs: ns,
		}
	}
	return results
}

// ── ComputeStats ──────────────────────────────────────────────────────────────

func TestComputeStats_KnownLatencies(t *testing.T) {
	t.Parallel()
	// 10 samples: 1ms,2ms,...,10ms (100_000ns each step).
	latencies := []int64{
		1_000_000, 2_000_000, 3_000_000, 4_000_000, 5_000_000,
		6_000_000, 7_000_000, 8_000_000, 9_000_000, 10_000_000,
	}
	results := makeResults(latencies...)
	stats := botfleet.ComputeStats(results)

	// Nearest-rank method on 10 samples:
	// p50 → rank ceil(0.5*10)=5 → sorted[4]=5ms
	// p90 → rank ceil(0.9*10)=9 → sorted[8]=9ms
	// p99 → rank ceil(0.99*10)=10 → sorted[9]=10ms
	assertApprox(t, "P50Ns", stats.P50Ns, 5_000_000, 1)
	assertApprox(t, "P90Ns", stats.P90Ns, 9_000_000, 1)
	assertApprox(t, "P99Ns", stats.P99Ns, 10_000_000, 1)

	if stats.TotalOrders != 10 {
		t.Errorf("TotalOrders = %d, want 10", stats.TotalOrders)
	}
	if stats.SuccessfulOrders != 10 {
		t.Errorf("SuccessfulOrders = %d, want 10", stats.SuccessfulOrders)
	}
	if stats.ErrorOrders != 0 {
		t.Errorf("ErrorOrders = %d, want 0", stats.ErrorOrders)
	}
}

func TestComputeStats_Empty(t *testing.T) {
	t.Parallel()
	stats := botfleet.ComputeStats(nil)
	if stats.TotalOrders != 0 || stats.P99Ns != 0 || stats.MaxTPS != 0 {
		t.Errorf("empty input should produce zero BotStats, got %+v", stats)
	}
}

func TestComputeStats_AllErrors(t *testing.T) {
	t.Parallel()
	results := []botfleet.OrderResult{
		{Err: errTimeout, Order: botfleet.Order{ID: "a"}},
		{Err: errTimeout, Order: botfleet.Order{ID: "b"}},
	}
	stats := botfleet.ComputeStats(results)
	if stats.ErrorOrders != 2 {
		t.Errorf("ErrorOrders = %d, want 2", stats.ErrorOrders)
	}
	if stats.P99Ns != 0 {
		t.Errorf("P99Ns should be 0 when all results are errors, got %f", stats.P99Ns)
	}
	if stats.MaxTPS != 0 {
		t.Errorf("MaxTPS should be 0 when all results are errors, got %f", stats.MaxTPS)
	}
}

func TestComputeStats_SingleSample(t *testing.T) {
	t.Parallel()
	results := makeResults(3_000_000) // 3ms
	stats := botfleet.ComputeStats(results)

	assertApprox(t, "P50Ns", stats.P50Ns, 3_000_000, 1)
	assertApprox(t, "P90Ns", stats.P90Ns, 3_000_000, 1)
	assertApprox(t, "P99Ns", stats.P99Ns, 3_000_000, 1)
}

func TestComputeStats_MaxTPS_Window(t *testing.T) {
	t.Parallel()
	// Create 5 orders all within a 50ms window, then a 1s gap, then 2 more.
	// The 100ms window should capture the 5-order burst.
	base := time.Now()
	results := []botfleet.OrderResult{}
	for i := 0; i < 5; i++ {
		sent := base.Add(time.Duration(i) * 10 * time.Millisecond) // 0,10,20,30,40ms
		results = append(results, botfleet.OrderResult{
			SentAt:    sent,
			AckedAt:   sent.Add(time.Millisecond),
			LatencyNs: int64(time.Millisecond),
		})
	}
	// 2 more after a 1s gap
	for i := 0; i < 2; i++ {
		sent := base.Add(time.Second + time.Duration(i)*10*time.Millisecond)
		results = append(results, botfleet.OrderResult{
			SentAt:    sent,
			AckedAt:   sent.Add(time.Millisecond),
			LatencyNs: int64(time.Millisecond),
		})
	}

	stats := botfleet.ComputeStats(results)
	// The 100ms window captured 5 orders → MaxTPS = 5 * 10 = 50
	if stats.MaxTPS < 40 || stats.MaxTPS > 60 {
		t.Errorf("MaxTPS = %f, want ~50 (5 orders in 100ms window)", stats.MaxTPS)
	}
}

// ── RandomGenerator ───────────────────────────────────────────────────────────

func TestRandomGenerator_Ratios(t *testing.T) {
	t.Parallel()
	cfg := botfleet.DefaultRandomGeneratorConfig()
	cfg.Seed = 42

	gen, err := botfleet.NewRandomGenerator("test-bot", cfg)
	if err != nil {
		t.Fatalf("NewRandomGenerator: %v", err)
	}

	const n = 10_000
	counts := map[botfleet.OrderKind]int{}
	for i := 0; i < n; i++ {
		o := gen.Next()
		counts[o.Kind]++
	}

	assertRatio(t, "limit", float64(counts[botfleet.KindLimit])/n, cfg.Ratios.Limit, 0.10)
	assertRatio(t, "market", float64(counts[botfleet.KindMarket])/n, cfg.Ratios.Market, 0.10)
	assertRatio(t, "cancel", float64(counts[botfleet.KindCancel])/n, cfg.Ratios.Cancel, 0.10)
}

func TestRandomGenerator_UniqueIDs(t *testing.T) {
	t.Parallel()
	cfg := botfleet.DefaultRandomGeneratorConfig()
	cfg.Seed = 7
	// Only use limit+market (ratio 0.5/0.5, no cancel) so IDs are always fresh.
	cfg.Ratios = botfleet.OrderRatios{Limit: 0.5, Market: 0.5, Cancel: 0.0}

	gen, err := botfleet.NewRandomGenerator("uid-bot", cfg)
	if err != nil {
		t.Fatalf("NewRandomGenerator: %v", err)
	}

	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		o := gen.Next()
		if o.ID == "" {
			t.Fatalf("order %d has empty ID", i)
		}
		if _, dup := seen[o.ID]; dup {
			t.Fatalf("duplicate order ID %q at index %d", o.ID, i)
		}
		seen[o.ID] = struct{}{}
	}
}

func TestRandomGenerator_InvalidConfig(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  botfleet.RandomGeneratorConfig
	}{
		{
			name: "ratios sum to 0.5",
			cfg: botfleet.RandomGeneratorConfig{
				Ratios:   botfleet.OrderRatios{Limit: 0.5, Market: 0.0, Cancel: 0.0},
				Price:    botfleet.DefaultPriceConfig(),
				Quantity: botfleet.DefaultQuantityConfig(),
			},
		},
		{
			name: "negative ratio",
			cfg: botfleet.RandomGeneratorConfig{
				Ratios:   botfleet.OrderRatios{Limit: -0.1, Market: 0.6, Cancel: 0.5},
				Price:    botfleet.DefaultPriceConfig(),
				Quantity: botfleet.DefaultQuantityConfig(),
			},
		},
		{
			name: "quantity min > max",
			cfg: botfleet.RandomGeneratorConfig{
				Ratios:   botfleet.DefaultOrderRatios(),
				Price:    botfleet.DefaultPriceConfig(),
				Quantity: botfleet.QuantityConfig{Min: 100, Max: 50},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := botfleet.NewRandomGenerator("test", tc.cfg)
			if err == nil {
				t.Errorf("NewRandomGenerator should return error for %q", tc.name)
			}
		})
	}
}

// ── Bot ───────────────────────────────────────────────────────────────────────

func TestBot_Run_ContextCancel(t *testing.T) {
	t.Parallel()
	var reqCount atomic.Int64
	srv := echoServer(t, &reqCount)
	defer srv.Close()

	gen, _ := botfleet.NewRandomGenerator("cancel-bot", botfleet.DefaultRandomGeneratorConfig())
	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: time.Second})
	bot, _ := botfleet.NewBot("cancel-bot", gen, transport)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	results := bot.Run(ctx)

	// Bot must have sent at least one order and returned cleanly.
	if len(results) == 0 {
		t.Error("expected at least one result before context cancel")
	}
	// reqCount may be slightly higher than len(results) due to in-flight
	// requests when ctx was cancelled — that's fine.
	if reqCount.Load() == 0 {
		t.Error("server received zero requests")
	}
}

func TestBot_Run_NoGoroutineLeak(t *testing.T) {
	t.Parallel()
	var reqCount atomic.Int64
	srv := echoServer(t, &reqCount)
	defer srv.Close()

	goroutinesBefore := runtime.NumGoroutine()

	gen, _ := botfleet.NewRandomGenerator("leak-bot", botfleet.DefaultRandomGeneratorConfig())
	transport := botfleet.NewRESTTransport(srv.URL, &http.Client{Timeout: time.Second})
	bot, _ := botfleet.NewBot("leak-bot", gen, transport)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	bot.Run(ctx) // blocks until ctx expires

	// Allow any deferred goroutines to settle.
	time.Sleep(50 * time.Millisecond)
	goroutinesAfter := runtime.NumGoroutine()

	// Allow up to 2 extra goroutines for GC/finaliser variance.
	if goroutinesAfter > goroutinesBefore+2 {
		t.Errorf("goroutine leak: before=%d after=%d", goroutinesBefore, goroutinesAfter)
	}
}

// ── Fleet ─────────────────────────────────────────────────────────────────────

func TestFleet_Run_AllBotsExecute(t *testing.T) {
	t.Parallel()
	var reqCount atomic.Int64
	srv := echoServer(t, &reqCount)
	defer srv.Close()

	const botCount = 5
	cfg := botfleet.FleetConfig{
		BotCount:          botCount,
		RampUpDuration:    0, // no ramp-up for speed
		TestDuration:      80 * time.Millisecond,
		TargetURL:         srv.URL,
		PerBotHTTPTimeout: time.Second,
		GeneratorConfig:   botfleet.DefaultRandomGeneratorConfig(),
	}

	fleet, err := botfleet.NewFleet(cfg)
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	result, err := fleet.Run(context.Background())
	if err != nil {
		t.Fatalf("Fleet.Run: %v", err)
	}

	if len(result.Results) == 0 {
		t.Error("expected at least one result from the fleet")
	}
	if reqCount.Load() == 0 {
		t.Error("server received zero requests — bots did not execute")
	}
	// Each of 5 bots should have sent at least one order in 80ms.
	if reqCount.Load() < botCount {
		t.Errorf("expected at least %d requests (one per bot), got %d", botCount, reqCount.Load())
	}
	if result.Stats.TotalOrders != int64(len(result.Results)) {
		t.Errorf("Stats.TotalOrders=%d != len(Results)=%d", result.Stats.TotalOrders, len(result.Results))
	}
}

func TestFleet_Run_ContextCancel(t *testing.T) {
	t.Parallel()
	var reqCount atomic.Int64
	srv := echoServer(t, &reqCount)
	defer srv.Close()

	cfg := botfleet.FleetConfig{
		BotCount:          3,
		RampUpDuration:    0,
		TestDuration:      10 * time.Second, // long — we'll cancel externally
		TargetURL:         srv.URL,
		PerBotHTTPTimeout: time.Second,
		GeneratorConfig:   botfleet.DefaultRandomGeneratorConfig(),
	}

	fleet, err := botfleet.NewFleet(cfg)
	if err != nil {
		t.Fatalf("NewFleet: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err = fleet.Run(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Fleet.Run: %v", err)
	}
	// Fleet must return well before TestDuration (10s).
	if elapsed > 2*time.Second {
		t.Errorf("Fleet.Run took %s, expected < 2s with 80ms context", elapsed)
	}
}

func TestFleet_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  botfleet.FleetConfig
	}{
		{
			name: "zero bots",
			cfg:  botfleet.FleetConfig{BotCount: 0, TestDuration: time.Second, TargetURL: "http://x"},
		},
		{
			name: "zero test duration",
			cfg:  botfleet.FleetConfig{BotCount: 1, TestDuration: 0, TargetURL: "http://x"},
		},
		{
			name: "empty target URL",
			cfg:  botfleet.FleetConfig{BotCount: 1, TestDuration: time.Second, TargetURL: ""},
		},
		{
			name: "invalid ratios",
			cfg: botfleet.FleetConfig{
				BotCount:        1,
				TestDuration:    time.Second,
				TargetURL:       "http://x",
				GeneratorConfig: botfleet.RandomGeneratorConfig{Ratios: botfleet.OrderRatios{Limit: 0.5}},
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := botfleet.NewFleet(tc.cfg)
			if err == nil {
				t.Errorf("NewFleet should return error for %q", tc.name)
			}
		})
	}
}

// ── test helpers ──────────────────────────────────────────────────────────────

// errTimeout is a sentinel error used in tests that need a transport error.
var errTimeout = fmt.Errorf("test: simulated timeout")

// assertApprox fails if |got-want| > tolerance.
func assertApprox(t *testing.T, name string, got, want, tolerance float64) {
	t.Helper()
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		t.Errorf("%s: got %f, want %f (tolerance ±%f)", name, got, want, tolerance)
	}
}

// assertRatio fails if |got-want| > maxDelta.
func assertRatio(t *testing.T, name string, got, want, maxDelta float64) {
	t.Helper()
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	if diff > maxDelta {
		t.Errorf("ratio %q: got %.3f, want %.3f (max delta %.3f)", name, got, want, maxDelta)
	}
}

// Keep fmt import used by errTimeout.
var _ = fmt.Errorf
