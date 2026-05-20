package metrics_test

// Tests verify:
//  1. New() registers without panicking.
//  2. Every Record* method increments the correct metric.
//  3. Handler() returns a valid http.Handler that serves /metrics.
//  4. Isolated registries — two New() calls don't share state.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nexusbench/nexusbench/internal/metrics"
)

func TestNew_DoesNotPanic(t *testing.T) {
	t.Parallel()
	// MustRegister panics on duplicate registration.
	// Two calls must produce independent registries without panicking.
	reg1 := metrics.New()
	reg2 := metrics.New()
	if reg1 == nil || reg2 == nil {
		t.Fatal("New() returned nil")
	}
}

func TestHandler_ServesMetrics(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	// Record something so the output is non-trivial.
	reg.RecordHTTPRequest("GET", "/health", "200", 0.001)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	// Verify our custom metric appears in the output.
	if !strings.Contains(bodyStr, "nexusbench_http_requests_total") {
		t.Errorf("metrics output missing nexusbench_http_requests_total\n%s", bodyStr[:min(500, len(bodyStr))])
	}
}

func TestRecordHTTPRequest(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	reg.RecordHTTPRequest("GET", "/api/v1/submissions", "200", 0.005)
	reg.RecordHTTPRequest("POST", "/api/v1/submissions", "201", 0.1)
	reg.RecordHTTPRequest("GET", "/api/v1/submissions", "200", 0.003)

	body := scrape(t, reg)
	// Two GET /api/v1/submissions requests recorded
	assertContains(t, body, `nexusbench_http_requests_total{method="GET",path="/api/v1/submissions",status_code="200"} 2`)
	assertContains(t, body, `nexusbench_http_requests_total{method="POST",path="/api/v1/submissions",status_code="201"} 1`)
}

func TestRecordSubmission(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	reg.RecordSubmission("go", "accepted")
	reg.RecordSubmission("go", "accepted")
	reg.RecordSubmission("rust", "rejected")

	body := scrape(t, reg)
	assertContains(t, body, `nexusbench_submission_total{language="go",status="accepted"} 2`)
	assertContains(t, body, `nexusbench_submission_total{language="rust",status="rejected"} 1`)
}

func TestSandboxStartedAndStopped(t *testing.T) {
	t.Parallel()
	reg := metrics.New()

	// Start two Go sandboxes, stop one.
	reg.SandboxStarted("go", 0.5)
	reg.SandboxStarted("go", 0.8)
	reg.SandboxStopped("go", "stopped")

	body := scrape(t, reg)
	// Active = 2 started - 1 stopped = 1
	assertContains(t, body, `nexusbench_sandbox_active{language="go"} 1`)
	assertContains(t, body, `nexusbench_sandbox_exits_total{language="go",reason="stopped"} 1`)
}

func TestRecordTelemetryEvent(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	reg.RecordTelemetryEvent("order_ack")
	reg.RecordTelemetryEvent("order_ack")
	reg.RecordTelemetryEvent("fill")

	body := scrape(t, reg)
	assertContains(t, body, `nexusbench_telemetry_events_total{kind="order_ack"} 2`)
	assertContains(t, body, `nexusbench_telemetry_events_total{kind="fill"} 1`)
}

func TestRecordTelemetryEmitError(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	reg.RecordTelemetryEmitError()
	reg.RecordTelemetryEmitError()

	body := scrape(t, reg)
	assertContains(t, body, `nexusbench_telemetry_emit_errors_total 2`)
}

func TestRecordConsumerWindow(t *testing.T) {
	t.Parallel()
	reg := metrics.New()
	reg.RecordConsumerWindow("sub-abc", 0.1)
	reg.RecordConsumerWindow("sub-abc", 0.2)
	reg.RecordConsumerWindow("sub-xyz", 0.05)

	body := scrape(t, reg)
	assertContains(t, body, `nexusbench_consumer_windows_total{submission_id="sub-abc"} 2`)
	assertContains(t, body, `nexusbench_consumer_windows_total{submission_id="sub-xyz"} 1`)
}

func TestIsolatedRegistries(t *testing.T) {
	t.Parallel()
	reg1 := metrics.New()
	reg2 := metrics.New()

	// Record only in reg1.
	reg1.RecordSubmission("go", "accepted")

	body1 := scrape(t, reg1)
	body2 := scrape(t, reg2)

	if !strings.Contains(body1, `nexusbench_submission_total{language="go",status="accepted"} 1`) {
		t.Error("reg1 missing expected metric")
	}
	// reg2 must not see reg1's data — registries are isolated.
	if strings.Contains(body2, `nexusbench_submission_total{language="go",status="accepted"} 1`) {
		t.Error("reg2 contains reg1 data — registries are NOT isolated")
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

// scrape calls the /metrics handler and returns the response body as a string.
func scrape(t *testing.T, reg *metrics.Registry) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	reg.Handler().ServeHTTP(w, req)
	body, _ := io.ReadAll(w.Result().Body)
	return string(body)
}

// assertContains fails the test if substr is not found in s.
func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("metrics output missing:\n  %q\nfull output (first 1000 chars):\n%s",
			substr, s[:min(1000, len(s))])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
