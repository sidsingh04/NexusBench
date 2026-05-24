package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

const (
	// HeartbeatInterval is how often the worker pings the orchestrator.
	// Must be well under orchestrator.HeartbeatTTL (15s).
	HeartbeatInterval = 5 * time.Second

	// heartbeatTimeout is the per-request HTTP timeout.
	heartbeatTimeout = 3 * time.Second
)

// HeartbeatStatus is the worker's self-reported state, read by the Heartbeater
// on every tick via the statusFn callback. Exported so cmd/worker can construct
// it without importing the orchestrator package.
type HeartbeatStatus struct {
	// Busy is true when the worker is actively processing a job.
	Busy bool
	// CurrentJobID is the submission ID being processed. Empty when idle.
	CurrentJobID string
	// JobsCompleted is the total jobs finished since worker startup.
	JobsCompleted int
}

// heartbeatPayload is the JSON body sent to the orchestrator.
// Field names match orchestrator.HeartbeatUpdate exactly.
// Defined here (not imported from orchestrator) to avoid a cycle:
//   worker → orchestrator would mean orchestrator cannot import worker.
type heartbeatPayload struct {
	Status        string `json:"status"`
	CurrentJobID  string `json:"current_job_id,omitempty"`
	JobsCompleted int    `json:"jobs_completed"`
}

// Heartbeater sends periodic liveness pings to the orchestrator.
// Construct with NewHeartbeater; start with go hb.Run(ctx).
type Heartbeater struct {
	workerID        string
	orchestratorURL string
	client          *http.Client
	statusFn        func() HeartbeatStatus
}

// NewHeartbeater constructs a Heartbeater.
//
//   - workerID: the WORKER_ID env var value (must match what was registered).
//   - orchestratorURL: control plane base URL, e.g. "http://control-plane:8080".
//   - statusFn: called on every tick to get current status; must be goroutine-safe.
func NewHeartbeater(workerID, orchestratorURL string, statusFn func() HeartbeatStatus) *Heartbeater {
	return &Heartbeater{
		workerID:        workerID,
		orchestratorURL: orchestratorURL,
		client:          &http.Client{Timeout: heartbeatTimeout},
		statusFn:        statusFn,
	}
}

// Run registers the worker then sends heartbeats every HeartbeatInterval
// until ctx is cancelled. Designed to run as a goroutine:
//
//	go hb.Run(ctx)
//
// All errors are logged but never fatal — a missed heartbeat causes the
// orchestrator to mark the worker dead after TTL, but the worker continues
// processing its current job.
func (h *Heartbeater) Run(ctx context.Context) {
	// Register on startup; retry on next tick if this fails.
	if err := h.register(ctx); err != nil {
		slog.Warn("heartbeater: initial registration failed; will retry on next tick",
			"worker_id", h.workerID,
			"err", err,
		)
	}

	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("heartbeater: stopping", "worker_id", h.workerID)
			return
		case <-ticker.C:
			if err := h.beat(ctx); err != nil {
				slog.Warn("heartbeater: beat failed",
					"worker_id", h.workerID,
					"err", err,
				)
			}
		}
	}
}

func (h *Heartbeater) register(ctx context.Context) error {
	body, _ := json.Marshal(map[string]string{"worker_id": h.workerID})

	reqCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		h.orchestratorURL+"/internal/workers/register",
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("register request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register: HTTP %d", resp.StatusCode)
	}

	slog.Info("heartbeater: registered with orchestrator",
		"worker_id", h.workerID,
		"orchestrator", h.orchestratorURL,
	)
	return nil
}

func (h *Heartbeater) beat(ctx context.Context) error {
	status := h.statusFn()

	statusStr := "idle"
	if status.Busy {
		statusStr = "busy"
	}

	payload := heartbeatPayload{
		Status:        statusStr,
		CurrentJobID:  status.CurrentJobID,
		JobsCompleted: status.JobsCompleted,
	}
	body, _ := json.Marshal(payload)

	reqCtx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/internal/workers/%s/heartbeat", h.orchestratorURL, h.workerID)
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build beat request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return fmt.Errorf("beat request: %w", err)
	}
	defer resp.Body.Close()

	// 404 = orchestrator lost our registration (control plane restarted).
	// Re-register immediately so we don't stay invisible.
	if resp.StatusCode == http.StatusNotFound {
		slog.Warn("heartbeater: not registered; re-registering",
			"worker_id", h.workerID,
		)
		return h.register(ctx)
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("beat: HTTP %d", resp.StatusCode)
	}
	return nil
}
