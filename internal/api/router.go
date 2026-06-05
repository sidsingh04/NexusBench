// Package api provides the HTTP router and handlers for the NexusBench
// control plane.
//
// Dependency rules (enforced by go build):
//   - api may import models, config, metrics, submission, contest, orchestrator.
//   - api must NOT import worker, sandbox, botfleet, or correctness.
//   - api must NOT import validator directly — it depends on the ValidatorRunner
//     interface so the validator package can be tested independently of the HTTP
//     layer. The concrete *validator.Validator is wired in cmd/server/main.go.
//   - No business logic lives here: handlers translate HTTP ↔ domain calls only.
//     All invariants, status transitions, and scoring live in their respective
//     service packages.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/contest"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/orchestrator"
	"github.com/nexusbench/nexusbench/internal/submission"
)

// ── Handler structs ───────────────────────────────────────────────────────────

// handler holds the dependencies for all Phase 1–4 and Phase 5 public routes.
type handler struct {
	svc *submission.Service
	cfg *config.Config
	reg *metrics.Registry
}

// contestHandler handles all /api/v1/admin/contests/* routes.
// It contains no business logic — all lifecycle decisions live in ContestService.
type contestHandler struct {
	svc    *contest.ContestService
	subSvc *submission.Service
}

// ── Dry-run Validator (Stage 5.6) ─────────────────────────────────────────────

// ValidatorResult mirrors validator.ValidationResult so that the api package
// does not need to import internal/validator.
type ValidatorResult struct {
	SubmissionID string           `json:"submission_id"`
	Scenarios    []ScenarioResult `json:"scenarios"`
	AllPassed    bool             `json:"all_passed"`
	TestedAt     time.Time        `json:"tested_at"`
}

// ScenarioResult mirrors validator.ScenarioResult.
type ScenarioResult struct {
	Name   string `json:"name"`
	Passed bool   `json:"passed"`
	Reason string `json:"reason,omitempty"`
}

// ValidatorRunner is the narrow interface the validate endpoint depends on.
// It is satisfied by *validator.Validator (wired externally) and by test doubles.
//
// Defined here so the api package has no import on internal/validator.
type ValidatorRunner interface {
	Run(ctx context.Context, submissionID string) (*ValidatorResult, error)
}

// ValidatorFactory creates a ValidatorRunner targeting the given URL.
// Called once per validate request.
type ValidatorFactory func(targetURL string) ValidatorRunner

// validationHandler handles POST /api/v1/submissions/{id}/validate.
type validationHandler struct {
	svc         *submission.Service
	sandboxHost string
	factory     ValidatorFactory
	limiter     *validationRateLimiter
}

func newValidationHandler(svc *submission.Service, sandboxHost string, factory ValidatorFactory) *validationHandler {
	return &validationHandler{
		svc:         svc,
		sandboxHost: sandboxHost,
		factory:     factory,
		limiter:     newValidationRateLimiter(2 * time.Minute),
	}
}

// validate handles POST /api/v1/submissions/{id}/validate.
//
// Pre-conditions (in order): exists → not benchmarking → rate-limit → port known.
// No side effects on submission state, results, or leaderboard.
func (vh *validationHandler) validate(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	sub, err := vh.svc.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("submission %s not found", id))
		return
	}

	if !vh.limiter.allow(id) {
		retryAfter := vh.limiter.retryAfter(id)
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
		writeError(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS",
			fmt.Sprintf("validation rate limit: one request per 2 minutes per submission; retry after %.0f seconds",
				retryAfter.Seconds()))
		return
	}

	if sub.Status != models.StatusPending && sub.Status != models.StatusBenchmarking {
		// ALREADY completed or failed.
		vh.limiter.revoke(id)
		writeError(w, http.StatusConflict, "WRONG_STATUS", "submission is not in 'pending' or 'benchmarking' status")
		return
	}

	if sub.ExposedPort <= 0 {
		vh.limiter.revoke(id)
		writeError(w, http.StatusConflict, "CONTAINER_NOT_READY",
			"sandbox container is not yet running; submit the engine first and wait for status=running")
		return
	}

	targetURL := fmt.Sprintf("http://%s:%d", vh.sandboxHost, sub.ExposedPort)
	runner := vh.factory(targetURL)

	slog.Info("api: dry-run validation started",
		"submission_id", id, "target_url", targetURL, "team", sub.TeamName)

	result, runErr := runner.Run(r.Context(), id)
	if runErr != nil {
		if r.Context().Err() != nil {
			writeError(w, http.StatusRequestTimeout, "VALIDATION_CANCELED",
				"validation canceled by client")
			return
		}
		writeError(w, http.StatusInternalServerError, "VALIDATION_ERROR",
			fmt.Sprintf("validation run failed: %v", runErr))
		return
	}

	slog.Info("api: dry-run validation complete",
		"submission_id", id, "all_passed", result.AllPassed)

	writeJSON(w, http.StatusOK, result)
}

// ── Validation rate limiter ────────────────────────────────────────────────────

type validationRateLimiter struct {
	lastAllowed sync.Map
	window      time.Duration
}

func newValidationRateLimiter(window time.Duration) *validationRateLimiter {
	return &validationRateLimiter{window: window}
}

func (l *validationRateLimiter) allow(submissionID string) bool {
	now := time.Now()
	last, loaded := l.lastAllowed.LoadOrStore(submissionID, now)
	if !loaded {
		return true
	}
	lastTime, ok := last.(time.Time)
	if !ok {
		l.lastAllowed.Store(submissionID, now)
		return true
	}
	if now.Sub(lastTime) >= l.window {
		l.lastAllowed.Store(submissionID, now)
		return true
	}
	return false
}

func (l *validationRateLimiter) retryAfter(submissionID string) time.Duration {
	v, ok := l.lastAllowed.Load(submissionID)
	if !ok {
		return 0
	}
	last, ok := v.(time.Time)
	if !ok {
		return 0
	}
	if remaining := l.window - time.Since(last); remaining > 0 {
		return remaining
	}
	return 0
}

func (l *validationRateLimiter) revoke(submissionID string) {
	l.lastAllowed.Delete(submissionID)
}

// ── SSE leaderboard stream (Stage 5.7) ───────────────────────────────────────

// leaderboardStreamHandler serves GET /api/v1/leaderboard/stream.
// It holds the submission service (for the initial snapshot on connect) and
// the bus (for subsequent push events).
type leaderboardStreamHandler struct {
	svc        *submission.Service
	bus        *LeaderboardBus
	contestSvc *contest.ContestService
}

// stream is the SSE handler for GET /api/v1/leaderboard/stream.
//
// Protocol:
//  1. Set SSE response headers (text/event-stream, no-cache, keep-alive).
//  2. Subscribe to the bus before reading the current leaderboard snapshot so
//     no events can be missed in the window between snapshot and subscription.
//  3. Write the current leaderboard immediately as an "update" event so the
//     client has a complete picture without waiting for the next score change.
//  4. Forward bus events to the client as they arrive.
//  5. On a "frozen" event: write it, then close the connection — the contest
//     is over and the client should switch to the archived leaderboard.
//  6. On client disconnect (ctx.Done): unsubscribe and return.
//
// The existing GET /api/v1/leaderboard poll endpoint is not modified; it
// remains the backward-compatible polling path.
func (lsh *leaderboardStreamHandler) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "SSE_UNSUPPORTED",
			"streaming is not supported by this server")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Subscribe before reading the initial snapshot so we cannot miss an event
	// that fires between the snapshot read and the subscription.
	subID, ch := lsh.bus.subscribe()
	defer lsh.bus.unsubscribe(subID)

	slog.Info("api: SSE subscriber connected",
		"subscriber_id", subID, "remote_addr", r.RemoteAddr)

	// Send current leaderboard immediately on connect.
	if active, err := lsh.contestSvc.GetActive(r.Context()); err == nil {
		if subs, err := lsh.svc.List(); err == nil {
			var activeSubs []*models.Submission
			for _, sub := range subs {
				if sub.ContestID == active.ID {
					activeSubs = append(activeSubs, sub)
				}
			}
			entries := buildDedupedLeaderboard(activeSubs)
			ptrs := make([]*models.LeaderboardEntry, len(entries))
			for i := range entries {
				e := entries[i]
				ptrs[i] = &e
			}
			writeSSEEvent(w, LeaderboardEvent{Type: "update", ContestID: active.ID, ContestName: active.Name, Entries: ptrs})
			flusher.Flush()
		}
	} else {
		// No active contest, fetch the most recent closed contest
		past, err := lsh.contestSvc.ListPast(r.Context())
		if err == nil && len(past) > 0 {
			var recent *models.Contest
			for _, c := range past {
				if recent == nil || c.CreatedAt.After(recent.CreatedAt) {
					recent = c
				}
			}
			if snapshot, err := lsh.contestSvc.GetLeaderboardSnapshot(r.Context(), recent.ID); err == nil {
				writeSSEEvent(w, LeaderboardEvent{Type: "frozen", ContestID: recent.ID, ContestName: recent.Name, Entries: snapshot})
			} else {
				writeSSEEvent(w, LeaderboardEvent{Type: "frozen", ContestID: recent.ID, ContestName: recent.Name})
			}
		} else {
			writeSSEEvent(w, LeaderboardEvent{Type: "frozen"})
		}
		flusher.Flush()
		return
	}

	for {
		select {
		case <-r.Context().Done():
			slog.Info("api: SSE subscriber disconnected", "subscriber_id", subID)
			return

		case event, ok := <-ch:
			if !ok {
				// Channel was closed externally (e.g. server shutdown).
				return
			}
			writeSSEEvent(w, event)
			flusher.Flush()

			if event.Type == "frozen" {
				// Contest is over. Close the connection cleanly so clients
				// know to stop reconnecting and switch to the snapshot endpoint.
				slog.Info("api: SSE stream frozen, closing connection",
					"subscriber_id", subID)
				return
			}
		}
	}
}

// writeSSEEvent serializes event to the SSE wire format: "data: <json>\n\n".
// The double newline is required by the SSE spec to delimit events.
func writeSSEEvent(w http.ResponseWriter, event LeaderboardEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		slog.Error("api: SSE marshal error", "err", err)
		return
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", data); err != nil {
		// Broken pipe or similar — the context will cancel on next iteration.
		slog.Debug("api: SSE write error (client likely disconnected)", "err", err)
	}
}

// ── Router ────────────────────────────────────────────────────────────────────

// NewRouter wires up all routes and returns the root http.Handler.
//
// Parameters:
//   - svc              — submission service (required)
//   - cfg              — loaded config (required)
//   - reg              — Prometheus registry (required)
//   - orchHandler      — orchestrator handler; nil = local mode, routes not mounted
//   - contestSvc       — contest service; nil = admin + team-history routes not mounted
//   - validatorFactory — dry-run factory; nil = /validate not mounted
//   - bus              — leaderboard bus; nil = /leaderboard/stream not mounted
//
// Backward compatibility: all Phase 1–4 routes remain mounted and respond
// identically regardless of which optional parameters are nil.
func NewRouter(
	svc *submission.Service,
	cfg *config.Config,
	reg *metrics.Registry,
	orchHandler *orchestrator.Handler,
	contestSvc *contest.ContestService,
	validatorFactory ValidatorFactory,
	bus *LeaderboardBus,
) http.Handler {
	h := &handler{svc: svc, cfg: cfg, reg: reg}

	r := mux.NewRouter()
	r.Use(requestLogger)
	r.Use(corsMiddleware)
	r.Use(h.prometheusMiddleware)

	r.Handle("/metrics", reg.Handler()).Methods(http.MethodGet)
	r.HandleFunc("/health", h.health).Methods(http.MethodGet)

	v1 := r.PathPrefix("/api/v1").Subrouter()

	// ── Image registry (Phase 1) ──────────────────────────────────────────────
	v1.HandleFunc("/images", h.listImages).Methods(http.MethodGet)

	// ── Submissions (Phase 1) ─────────────────────────────────────────────────
	v1.HandleFunc("/submissions", h.listSubmissions).Methods(http.MethodGet)
	v1.HandleFunc("/submissions", h.createSubmission).Methods(http.MethodPost)
	v1.HandleFunc("/submissions/{id}", h.getSubmission).Methods(http.MethodGet)
	v1.HandleFunc("/submissions/{id}/stop", h.stopSubmission).Methods(http.MethodPost)

	// ── Dry-run Validator (Phase 5, Stage 5.6) ────────────────────────────────
	if validatorFactory != nil {
		sandboxHost := cfg.SandboxHost
		if sandboxHost == "" {
			sandboxHost = "localhost"
		}
		vh := newValidationHandler(svc, sandboxHost, validatorFactory)
		v1.HandleFunc("/submissions/{id}/validate", vh.validate).Methods(http.MethodPost)
	}

	// ── Leaderboard poll (Phase 1) + SSE stream (Phase 5, Stage 5.7) ──────────
	// The poll endpoint is always mounted — it is the Phase 1–4 interface and
	// must never be removed. The SSE stream is additive: only mounted when a
	// bus is wired in. Both routes co-exist without interference.
	v1.HandleFunc("/leaderboard", h.leaderboard).Methods(http.MethodGet)

	if bus != nil {
		lsh := &leaderboardStreamHandler{svc: svc, bus: bus, contestSvc: contestSvc}
		// NOTE: gorilla/mux matches routes in registration order. Register the
		// more-specific /leaderboard/stream BEFORE /leaderboard so mux does
		// not greedily match the stream path against the poll route.
		// In practice gorilla/mux uses exact matching, but this ordering makes
		// the intent explicit and avoids any future ambiguity.
		v1.HandleFunc("/leaderboard/stream", lsh.stream).Methods(http.MethodGet)
	}

	// ── Team history (Phase 5, AD-2) ──────────────────────────────────────────
	if contestSvc != nil {
		v1.HandleFunc("/teams/{name}/submissions", h.teamHistory).Methods(http.MethodGet)
	}

	// ── Admin routes (Phase 5) ────────────────────────────────────────────────
	if contestSvc != nil && cfg.AdminAPIKey != "" {
		ch := &contestHandler{svc: contestSvc, subSvc: svc}
		admin := v1.PathPrefix("/admin").Subrouter()
		admin.Use(adminAuthMiddleware(cfg.AdminAPIKey))

		admin.HandleFunc("/contests", ch.createContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests", ch.listContests).Methods(http.MethodGet)
		admin.HandleFunc("/contests/{id}/activate", ch.activateContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests/{id}/close", ch.closeContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests/{id}/leaderboard", ch.getContestLeaderboard).Methods(http.MethodGet)
	}

	// ── Orchestrator (Phase 3+ distributed mode only) ─────────────────────────
	if orchHandler != nil {
		internal := r.PathPrefix("/internal").Subrouter()
		internal.HandleFunc("/workers/register", orchHandler.HTTPRegister).Methods(http.MethodPost)
		internal.HandleFunc("/workers/{id}/heartbeat", orchHandler.HTTPHeartbeat).Methods(http.MethodPost)
		internal.HandleFunc("/workers", orchHandler.HTTPList).Methods(http.MethodGet)
		internal.HandleFunc("/workers/stats", orchHandler.HTTPStats).Methods(http.MethodGet)
	}

	return r
}

// ── Phase 1–4 handlers ────────────────────────────────────────────────────────

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "nexusbench-control-plane",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *handler) listImages(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"images": h.cfg.AllImages(),
	})
}

func (h *handler) createSubmission(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)
	//nolint:gosec // bounded by MaxBytesReader above
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FORM",
			"failed to parse multipart form: "+err.Error())
		return
	}

	req := models.SubmitRequest{
		TeamName: strings.TrimSpace(r.FormValue("team_name")),
		Language: models.Language(strings.ToLower(strings.TrimSpace(r.FormValue("language")))),
		Protocol: models.Protocol(strings.ToLower(strings.TrimSpace(r.FormValue("protocol")))),
	}

	_, fileHeader, err := r.FormFile("archive")
	if err != nil {
		writeError(w, http.StatusBadRequest, "MISSING_ARCHIVE", "archive file is required")
		return
	}

	sub, err := h.svc.Ingest(r.Context(), req, fileHeader)
	if err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

func (h *handler) getSubmission(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	sub, err := h.svc.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("submission %s not found", id))
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (h *handler) listSubmissions(w http.ResponseWriter, r *http.Request) {
	subs, err := h.svc.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LIST_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count":       len(subs),
		"submissions": subs,
	})
}

func (h *handler) stopSubmission(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := h.svc.StopContainer(r.Context(), id); err != nil {
		code := http.StatusInternalServerError
		if strings.Contains(err.Error(), "not found") {
			code = http.StatusNotFound
		}
		writeError(w, code, "STOP_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped", "id": id})
}

// ── Leaderboard (Phase 1 + Phase 5 deduplication, AD-1) ──────────────────────

// leaderboard handles GET /api/v1/leaderboard (poll endpoint).
func (h *handler) leaderboard(w http.ResponseWriter, r *http.Request) {
	subs, err := h.svc.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LEADERBOARD_ERROR", err.Error())
		return
	}
	entries := buildDedupedLeaderboard(subs)
	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(entries),
		"entries": entries,
	})
}

// buildDedupedLeaderboard converts a raw submission list into a ranked
// leaderboard where each team appears at most once (best score wins).
//
// Extracted as a pure function so it can be unit-tested and reused by:
//   - The poll handler (above)
//   - The SSE stream handler (sends initial snapshot on connect)
//   - The auto-close goroutine in cmd/server/main.go
func buildDedupedLeaderboard(subs []*models.Submission) []models.LeaderboardEntry {
	type candidate struct {
		entry models.LeaderboardEntry
		score float64
	}

	bestByTeam := make(map[string]candidate, len(subs))

	for _, sub := range subs {
		if sub.Status != models.StatusCompleted {
			continue
		}

		effectiveScore := sub.FinalScore
		if effectiveScore == 0 && sub.Results != nil {
			effectiveScore = sub.Results.CompositeScore
		}

		entry := buildLeaderboardEntry(sub, effectiveScore)

		existing, seen := bestByTeam[sub.TeamName]
		if !seen || effectiveScore > existing.score {
			bestByTeam[sub.TeamName] = candidate{entry: entry, score: effectiveScore}
		}
	}

	ranked := make([]models.LeaderboardEntry, 0, len(bestByTeam))
	for _, c := range bestByTeam {
		ranked = append(ranked, c.entry)
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].CompositeScore > ranked[j].CompositeScore
	})
	for i := range ranked {
		ranked[i].Rank = i + 1
	}
	return ranked
}

// buildLeaderboardEntry constructs a LeaderboardEntry from a completed submission.
// effectiveScore is the ranking key (FinalScore for Phase 5, CompositeScore for P1–4).
func buildLeaderboardEntry(sub *models.Submission, effectiveScore float64) models.LeaderboardEntry {
	entry := models.LeaderboardEntry{
		SubmissionID:   sub.ID,
		TeamName:       sub.TeamName,
		Language:       sub.Language,
		Protocol:       sub.Protocol,
		Status:         sub.Status,
		CompositeScore: effectiveScore,
		FinalScore:     sub.FinalScore,
		CompletedAt:    sub.CompletedAt,
	}

	if sub.Results != nil {
		entry.P99LatencyMs = sub.Results.P99LatencyMs
		entry.MaxTPS = sub.Results.MaxTPS
		entry.CorrectnessScore = sub.Results.CorrectnessScore
	}

	if len(sub.AllResults) > 0 {
		var sumCorrectness float64
		var countCorrectness int

		for _, r := range sub.AllResults {
			switch r.VolatilityLabel {
			case "low":
				entry.LowScore = r.RunScore
			case "medium":
				entry.MediumScore = r.RunScore
			case "high":
				entry.HighScore = r.RunScore
			}
			if r.P99LatencyMs > 0 && (entry.BestP99Ms == 0 || r.P99LatencyMs < entry.BestP99Ms) {
				entry.BestP99Ms = r.P99LatencyMs
			}
			if r.SustainedTPS > entry.PeakSustainedTPS {
				entry.PeakSustainedTPS = r.SustainedTPS
			}
			sumCorrectness += r.CorrectnessScore
			countCorrectness++
		}

		if countCorrectness > 0 {
			entry.AvgCorrectness = sumCorrectness / float64(countCorrectness)
		}
	}

	return entry
}

// ── Team history (Phase 5, AD-2) ──────────────────────────────────────────────

func (h *handler) teamHistory(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	if name == "" {
		writeError(w, http.StatusBadRequest, "MISSING_TEAM_NAME", "team name is required")
		return
	}

	all, err := h.svc.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LIST_ERROR", err.Error())
		return
	}

	team := make([]*models.Submission, 0)
	for _, s := range all {
		if s.TeamName == name {
			team = append(team, s)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"team_name":   name,
		"count":       len(team),
		"submissions": team,
	})
}

// ── Middleware ────────────────────────────────────────────────────────────────

func requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		slog.Info("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.status,
			"duration_ms", time.Since(start).Milliseconds(),
		)
	})
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h *handler) prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(wrapped, r)
		h.reg.RecordHTTPRequest(
			r.Method,
			normalisePath(r.URL.Path),
			strconv.Itoa(wrapped.status),
			time.Since(start).Seconds(),
		)
	})
}

func normalisePath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		if len(p) > 8 && (strings.Contains(p, "-") || containsDigit(p)) {
			parts[i] = "{id}"
		}
	}
	return strings.Join(parts, "/")
}

func containsDigit(s string) bool {
	for _, c := range s {
		if c >= '0' && c <= '9' {
			return true
		}
	}
	return false
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.status = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Flush() {
	if flusher, ok := rw.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("json encode error", "err", err)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, models.APIError{Code: code, Message: message})
}

// classifyError maps domain errors to HTTP status codes and API error codes.
func classifyError(err error) (int, models.APIError) {
	switch {
	case errors.Is(err, models.ErrSubmissionInProgress):
		return http.StatusConflict,
			models.APIError{Code: "SUBMISSION_IN_PROGRESS", Message: err.Error()}
	case errors.Is(err, models.ErrContestNotActive):
		return http.StatusConflict,
			models.APIError{Code: "CONTEST_NOT_ACTIVE", Message: err.Error()}
	case errors.Is(err, models.ErrNoActiveContest):
		return http.StatusConflict,
			models.APIError{Code: "NO_ACTIVE_CONTEST", Message: err.Error()}
	case errors.Is(err, contest.ErrAlreadyActive):
		return http.StatusConflict,
			models.APIError{Code: "CONTEST_ALREADY_ACTIVE", Message: err.Error()}
	case errors.Is(err, contest.ErrWrongStatus):
		return http.StatusConflict,
			models.APIError{Code: "CONTEST_WRONG_STATUS", Message: err.Error()}
	case errors.Is(err, contest.ErrNotFound):
		return http.StatusNotFound,
			models.APIError{Code: "NOT_FOUND", Message: err.Error()}
	}

	msg := err.Error()
	switch {
	case strings.Contains(msg, "required"),
		strings.Contains(msg, "unsupported"),
		strings.Contains(msg, "invalid"),
		strings.Contains(msg, "no sandbox image"):
		return http.StatusBadRequest,
			models.APIError{Code: "VALIDATION_ERROR", Message: msg}
	default:
		return http.StatusInternalServerError,
			models.APIError{Code: "INTERNAL_ERROR", Message: msg}
	}
}

// ── Admin auth middleware ──────────────────────────────────────────────────────

// adminAuthMiddleware enforces Authorization: Bearer <apiKey>.
// Returns 401 when the header is absent or the key is wrong.
func adminAuthMiddleware(apiKey string) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			const prefix = "Bearer "
			auth := r.Header.Get("Authorization")
			if !strings.HasPrefix(auth, prefix) {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED",
					"Authorization: Bearer <key> header is required")
				return
			}
			if strings.TrimPrefix(auth, prefix) != apiKey {
				writeError(w, http.StatusUnauthorized, "UNAUTHORIZED",
					"invalid admin API key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// ── contestHandler methods ────────────────────────────────────────────────────

func (h *contestHandler) createContest(w http.ResponseWriter, r *http.Request) {
	var req contest.CreateContestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_JSON",
			"failed to decode request body: "+err.Error())
		return
	}
	c, err := h.svc.Create(r.Context(), req)
	if err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusCreated, c)
}

func (h *contestHandler) listContests(w http.ResponseWriter, r *http.Request) {
	all, err := h.svc.ListAll(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LIST_ERROR", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"count":    len(all),
		"contests": all,
	})
}

func (h *contestHandler) activateContest(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	c, err := h.svc.Activate(r.Context(), id)
	if err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusOK, c)
}

// closeContest is the manual admin override. The drain-and-wait goroutine is
// the primary closing path and passes real leaderboard entries. This handler
// passes nil entries — the service handles the nil case gracefully.
func (h *contestHandler) closeContest(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	var ptrs []*models.LeaderboardEntry
	if subs, err := h.subSvc.List(); err == nil {
		var activeSubs []*models.Submission
		for _, sub := range subs {
			if sub.ContestID == id {
				activeSubs = append(activeSubs, sub)
			}
		}
		entries := buildDedupedLeaderboard(activeSubs)
		for i := range entries {
			ptrs = append(ptrs, &entries[i])
		}
	}

	if err := h.svc.Close(r.Context(), id, ptrs); err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed", "id": id})
}

func (h *contestHandler) getContestLeaderboard(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	entries, err := h.svc.GetLeaderboardSnapshot(r.Context(), id)
	if err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"contest_id": id,
		"count":      len(entries),
		"entries":    entries,
	})
}
