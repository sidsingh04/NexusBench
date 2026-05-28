// Package metrics defines all Prometheus instrumentation for NexusBench.
//
// Design rules:
//  1. All metric descriptors live here — no package registers its own metrics.
//     This gives a single place to audit what we expose and prevents name collisions.
//  2. Metrics are registered against a custom Registry (not the global default).
//     This means tests can create isolated registries without cross-test pollution.
//  3. The package exposes a single *Registry value via New() that callers embed.
//     Individual Record* methods are the only write surface — callers never
//     touch prometheus types directly.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds all NexusBench Prometheus metrics and the underlying
// prometheus.Registry used to collect and expose them.
//
// The zero value is NOT valid. Use New().
type Registry struct {
	reg *prometheus.Registry

	// ── HTTP layer ────────────────────────────────────────────────────────────

	// HTTPRequestsTotal counts every HTTP request served by the control plane.
	// Labels: method, path, status_code
	HTTPRequestsTotal *prometheus.CounterVec

	// HTTPRequestDurationSeconds tracks request latency as a histogram.
	// Labels: method, path
	HTTPRequestDurationSeconds *prometheus.HistogramVec

	// ── Submission / sandbox layer ────────────────────────────────────────────

	// SubmissionsTotal counts submission ingestion attempts.
	// Labels: language, status ("accepted" | "rejected")
	SubmissionsTotal *prometheus.CounterVec

	// ActiveSandboxes is the current number of running sandbox containers.
	// Labels: language
	ActiveSandboxes *prometheus.GaugeVec

	// SandboxStartDurationSeconds tracks how long container startup takes.
	// Labels: language
	SandboxStartDurationSeconds *prometheus.HistogramVec

	// SandboxExitsTotal counts container exits by cause.
	// Labels: language, reason ("oom" | "timeout" | "crash" | "stopped")
	SandboxExitsTotal *prometheus.CounterVec

	// ── Telemetry pipeline layer ──────────────────────────────────────────────

	// TelemetryEventsTotal counts events emitted by submission engines.
	// Labels: kind ("order_ack" | "fill" | "cancel_ack" | "reject" | "heartbeat")
	TelemetryEventsTotal *prometheus.CounterVec

	// TelemetryEmitErrors counts failed Redpanda produce calls.
	TelemetryEmitErrors prometheus.Counter

	// ConsumerWindowsTotal counts latency windows written to TimescaleDB.
	// Labels: submission_id
	ConsumerWindowsTotal *prometheus.CounterVec

	// ConsumerLagSeconds tracks how far behind the consumer is relative to
	// the event timestamp. High values mean the consumer is falling behind.
	// Labels: submission_id
	ConsumerLagSeconds *prometheus.HistogramVec

	// ── Queue / autoscaling layer ─────────────────────────────────────────────

	// QueueDepth is the current number of pending (unconsumed) benchmark jobs
	// in the jobs.benchmark topic, measured as consumer-group lag for the
	// nexusbench-workers group.
	//
	// This gauge is updated every 15 seconds by a background goroutine in
	// the control plane (see cmd/server/main.go). It mirrors the metric KEDA
	// reads directly from the broker — exposing it in Prometheus lets Grafana
	// alert on queue buildup without needing a separate KEDA dashboard.
	//
	// A value of 0 means all submitted jobs have been dequeued (workers are
	// idle or processing their last job). A rising value means jobs are
	// accumulating faster than workers can process them — KEDA will scale up.
	QueueDepth prometheus.Gauge
}

// New creates a Registry with all metrics registered against a fresh
// prometheus.Registry. Also registers Go runtime and process collectors
// so Grafana can show GC pauses, goroutine counts, and file descriptors.
func New() *Registry {
	reg := prometheus.NewRegistry()

	// Standard Go runtime and process metrics — always useful.
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	r := &Registry{
		reg: reg,

		HTTPRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total HTTP requests served by the control plane.",
			},
			[]string{"method", "path", "status_code"},
		),

		HTTPRequestDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "nexusbench",
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "HTTP request latency in seconds.",
				// Buckets tuned for an internal API: sub-millisecond to 10 seconds.
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method", "path"},
		),

		SubmissionsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "submission",
				Name:      "total",
				Help:      "Total submission ingestion attempts.",
			},
			[]string{"language", "status"},
		),

		ActiveSandboxes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "nexusbench",
				Subsystem: "sandbox",
				Name:      "active",
				Help:      "Current number of running sandbox containers.",
			},
			[]string{"language"},
		),

		SandboxStartDurationSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "nexusbench",
				Subsystem: "sandbox",
				Name:      "start_duration_seconds",
				Help:      "Time taken to start a sandbox container.",
				Buckets:   []float64{.1, .25, .5, 1, 2, 5, 10, 30},
			},
			[]string{"language"},
		),

		SandboxExitsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "sandbox",
				Name:      "exits_total",
				Help:      "Total sandbox container exits, labeled by reason.",
			},
			[]string{"language", "reason"},
		),

		TelemetryEventsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "telemetry",
				Name:      "events_total",
				Help:      "Total telemetry events emitted by submission engines.",
			},
			[]string{"kind"},
		),

		TelemetryEmitErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "telemetry",
				Name:      "emit_errors_total",
				Help:      "Total failed Redpanda produce calls.",
			},
		),

		ConsumerWindowsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "nexusbench",
				Subsystem: "consumer",
				Name:      "windows_total",
				Help:      "Total latency windows written to TimescaleDB.",
			},
			[]string{"submission_id"},
		),

		ConsumerLagSeconds: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "nexusbench",
				Subsystem: "consumer",
				Name:      "lag_seconds",
				Help:      "Seconds between event timestamp and consumer processing time.",
				Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 5, 30},
			},
			[]string{"submission_id"},
		),

		QueueDepth: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "nexusbench",
				Subsystem: "queue",
				Name:      "depth",
				Help: "Current number of pending benchmark jobs in the jobs.benchmark topic " +
					"(consumer-group lag for nexusbench-workers). " +
					"Updated every 15 seconds. Mirrors the metric KEDA uses to drive HPA.",
			},
		),
	}

	reg.MustRegister(
		r.HTTPRequestsTotal,
		r.HTTPRequestDurationSeconds,
		r.SubmissionsTotal,
		r.ActiveSandboxes,
		r.SandboxStartDurationSeconds,
		r.SandboxExitsTotal,
		r.TelemetryEventsTotal,
		r.TelemetryEmitErrors,
		r.ConsumerWindowsTotal,
		r.ConsumerLagSeconds,
		r.QueueDepth,
	)

	return r
}

// Handler returns an http.Handler that serves the /metrics endpoint.
// Wire this into the router as: r.Handle("/metrics", reg.Handler())
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// ── Record helpers ────────────────────────────────────────────────────────────
// These are the ONLY write paths. Callers never touch prometheus types directly.

// RecordHTTPRequest records one completed HTTP request.
func (r *Registry) RecordHTTPRequest(method, path, statusCode string, durationSecs float64) {
	r.HTTPRequestsTotal.WithLabelValues(method, path, statusCode).Inc()
	r.HTTPRequestDurationSeconds.WithLabelValues(method, path).Observe(durationSecs)
}

// RecordSubmission records one submission attempt.
// status must be "accepted" or "rejected".
func (r *Registry) RecordSubmission(language, status string) {
	r.SubmissionsTotal.WithLabelValues(language, status).Inc()
}

// SandboxStarted increments the active sandbox gauge for the given language.
// Call immediately after a container is confirmed running.
func (r *Registry) SandboxStarted(language string, durationSecs float64) {
	r.ActiveSandboxes.WithLabelValues(language).Inc()
	r.SandboxStartDurationSeconds.WithLabelValues(language).Observe(durationSecs)
}

// SandboxStopped decrements the active sandbox gauge and increments the exit counter.
// reason: "oom" | "timeout" | "crash" | "stopped"
func (r *Registry) SandboxStopped(language, reason string) {
	r.ActiveSandboxes.WithLabelValues(language).Dec()
	r.SandboxExitsTotal.WithLabelValues(language, reason).Inc()
}

// RecordTelemetryEvent increments the event counter for the given kind.
func (r *Registry) RecordTelemetryEvent(kind string) {
	r.TelemetryEventsTotal.WithLabelValues(kind).Inc()
}

// RecordTelemetryEmitError increments the emit error counter.
func (r *Registry) RecordTelemetryEmitError() {
	r.TelemetryEmitErrors.Inc()
}

// RecordConsumerWindow increments the window counter and records consumer lag.
// lagSecs is wall_clock_now - event_timestamp (how far behind the consumer is).
func (r *Registry) RecordConsumerWindow(submissionID string, lagSecs float64) {
	r.ConsumerWindowsTotal.WithLabelValues(submissionID).Inc()
	r.ConsumerLagSeconds.WithLabelValues(submissionID).Observe(lagSecs)
}

// SetQueueDepth updates the queue depth gauge to the given value.
// Call from the background scraper goroutine every 15 seconds.
// depth must be non-negative; negative values are silently clamped to 0.
func (r *Registry) SetQueueDepth(depth int64) {
	if depth < 0 {
		depth = 0
	}
	r.QueueDepth.Set(float64(depth))
}
