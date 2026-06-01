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
	svc *contest.ContestService
}

// ── Dry-run Validator (Stage 5.6) ─────────────────────────────────────────────

// ValidatorResult mirrors validator.ValidationResult so that the api package
// does not need to import internal/validator.
// The concrete validator.Validator satisfies ValidatorRunner below.
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
// It is satisfied by *validator.Validator (wired externally) and by test
// doubles in router_test.go.
//
// Defined here — not in internal/validator — so the api package has no
// import on the validator package. This keeps the dependency arrow pointing
// inward: api defines the contract; validator satisfies it.
//
// The ctx argument must be respected for graceful cancellation during request
// termination.
type ValidatorRunner interface {
	// Run executes the fixed smoke-test sequence against the engine reachable
	// via the transport the runner was constructed with, and returns
	// per-scenario pass/fail results.
	//
	// submissionID is embedded in the result for traceability. It must not
	// be empty.
	//
	// Implementations must be safe for concurrent use.
	Run(ctx context.Context, submissionID string) (*ValidatorResult, error)
}

// ValidatorFactory creates a ValidatorRunner that targets the given URL.
// It is called once per validate request, constructing a fresh runner
// pointed at the submission's live sandbox port.
//
// The factory abstraction keeps the api package free of all botfleet and
// validator imports while allowing cmd/server/main.go to wire the real
// validator.New + botfleet.NewRESTTransport implementation.
type ValidatorFactory func(targetURL string) ValidatorRunner

// validationHandler handles POST /api/v1/submissions/{id}/validate.
// It holds the factory and the per-submission rate limiter.
type validationHandler struct {
	svc         *submission.Service
	sandboxHost string // hostname to reach sandbox containers (e.g. "localhost")
	factory     ValidatorFactory
	limiter     *validationRateLimiter
}

// newValidationHandler constructs a validationHandler.
// sandboxHost is the host the control plane uses to reach sandbox containers;
// in distributed mode this is "host.docker.internal" or the worker node IP.
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
// Pre-conditions checked (in order):
//  1. Submission exists          → 404 NOT_FOUND
//  2. Not currently benchmarking → 409 VALIDATION_CONFLICT
//  3. Rate limit (1 per 2 min)   → 429 TOO_MANY_REQUESTS
//  4. Container port is known    → 409 CONTAINER_NOT_READY
//
// When all checks pass, a fresh ValidatorRunner is constructed pointing at
// http://<sandboxHost>:<ExposedPort> and Run is called. The result is
// returned as JSON with status 200.
//
// The validate call has no side effects on the submission: status, results,
// leaderboard, and metrics are all unchanged.
func (vh *validationHandler) validate(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]

	// 1. Submission must exist.
	sub, err := vh.svc.Get(id)
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND",
			fmt.Sprintf("submission %s not found", id))
		return
	}

	// 2. Cannot validate while the benchmark fleet is actively running —
	//    concurrent traffic would corrupt both the benchmark and the validation.
	if sub.Status == models.StatusBenchmarking {
		writeError(w, http.StatusConflict, "VALIDATION_CONFLICT",
			"submission is currently being benchmarked; please wait until it completes")
		return
	}

	// 3. Rate limit: one validation per submission per 2 minutes.
	//    Prevents abuse (re-validating every second to probe the scoreboard).
	if !vh.limiter.allow(id) {
		retryAfter := vh.limiter.retryAfter(id)
		w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())+1))
		writeError(w, http.StatusTooManyRequests, "TOO_MANY_REQUESTS",
			fmt.Sprintf("validation rate limit: one request per 2 minutes per submission; retry after %.0f seconds",
				retryAfter.Seconds()))
		return
	}

	// 4. Container must be running and have an exposed port.
	//    ExposedPort > 0 means the sandbox is live and accepting connections.
	if sub.ExposedPort <= 0 {
		vh.limiter.revoke(id) // don't burn the slot — the container isn't up yet
		writeError(w, http.StatusConflict, "CONTAINER_NOT_READY",
			"sandbox container is not yet running; submit the engine first and wait for status=running")
		return
	}

	// Construct a fresh runner pointing at the live sandbox container.
	targetURL := fmt.Sprintf("http://%s:%d", vh.sandboxHost, sub.ExposedPort)
	runner := vh.factory(targetURL)

	slog.Info("api: dry-run validation started",
		"submission_id", id,
		"target_url", targetURL,
		"team", sub.TeamName,
	)

	result, runErr := runner.Run(r.Context(), id)
	if runErr != nil {
		// Context cancellation (client disconnected) is not a server error.
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
		"submission_id", id,
		"all_passed", result.AllPassed,
	)

	writeJSON(w, http.StatusOK, result)
}

// ── Validation rate limiter ────────────────────────────────────────────────────

// validationRateLimiter tracks the last validation time per submission ID.
// It uses sync.Map for lock-free reads in the common (not-rate-limited) case.
//
// The limiter is intentionally simple: one request per submission per window.
// A sliding-window or token-bucket implementation is unnecessary at this scale.
type validationRateLimiter struct {
	// lastAllowed maps submissionID → time.Time of the last allowed request.
	lastAllowed sync.Map
	window      time.Duration
}

// newValidationRateLimiter creates a rate limiter with the given window.
func newValidationRateLimiter(window time.Duration) *validationRateLimiter {
	return &validationRateLimiter{window: window}
}

// allow returns true if the submission may be validated now, and records the
// current time as the last-allowed time. Returns false if a request was
// allowed within the window.
func (l *validationRateLimiter) allow(submissionID string) bool {
	now := time.Now()
	last, loaded := l.lastAllowed.LoadOrStore(submissionID, now)
	if !loaded {
		// First request for this submission — always allowed.
		return true
	}
	lastTime, ok := last.(time.Time)
	if !ok {
		// Malformed entry — allow and reset.
		l.lastAllowed.Store(submissionID, now)
		return true
	}
	if now.Sub(lastTime) >= l.window {
		// Window has elapsed — allow and update.
		l.lastAllowed.Store(submissionID, now)
		return true
	}
	// Within the window — deny.
	return false
}

// retryAfter returns how long the caller must wait before the next request
// is allowed. Returns 0 when no rate-limit record exists.
func (l *validationRateLimiter) retryAfter(submissionID string) time.Duration {
	v, ok := l.lastAllowed.Load(submissionID)
	if !ok {
		return 0
	}
	last, ok := v.(time.Time)
	if !ok {
		return 0
	}
	remaining := l.window - time.Since(last)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// revoke removes the rate-limit record for a submission so that the next
// request is treated as a first attempt. Called when a validation is rejected
// for a reason other than rate-limiting (e.g. container not ready) so the
// submission slot is not unnecessarily consumed.
func (l *validationRateLimiter) revoke(submissionID string) {
	l.lastAllowed.Delete(submissionID)
}

// ── Router ────────────────────────────────────────────────────────────────────

// NewRouter wires up all routes and returns the root http.Handler.
//
// Parameters:
//   - svc           — submission service (required)
//   - cfg           — loaded config (required)
//   - reg           — Prometheus registry (required)
//   - orchHandler   — orchestrator HTTP handler; nil = local mode, routes not mounted
//   - contestSvc    — contest service; nil = admin + team-history routes not mounted
//     (Phase 1–4 backward compat: passing nil keeps old behavior exactly)
//   - validatorFactory — factory for dry-run validators; nil = validate endpoint not
//     mounted. This avoids the api package importing internal/validator or
//     internal/botfleet.
//
// Backward compatibility guarantee: all Phase 1–4 routes (/health, /metrics,
// /api/v1/submissions/*, /api/v1/leaderboard, /api/v1/images,
// /internal/workers/*) remain mounted and respond identically regardless of
// whether contestSvc, orchHandler, or validatorFactory are nil.
func NewRouter(
	svc *submission.Service,
	cfg *config.Config,
	reg *metrics.Registry,
	orchHandler *orchestrator.Handler,
	contestSvc *contest.ContestService,
	validatorFactory ValidatorFactory,
) http.Handler {
	h := &handler{svc: svc, cfg: cfg, reg: reg}

	r := mux.NewRouter()
	r.Use(requestLogger)
	r.Use(corsMiddleware)
	r.Use(h.prometheusMiddleware)

	// /metrics — served directly by the Prometheus handler.
	// No auth, no JSON wrapper, not logged (would pollute access logs).
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
	// Only mounted when validatorFactory is non-nil. The factory is injected
	// from cmd/server/main.go so this package has no import on botfleet or
	// validator.
	//
	// POST /api/v1/submissions/{id}/validate
	//   → runs the fixed 20-order smoke test against the live sandbox
	//   → returns ValidationResult (per-scenario pass/fail, no score impact)
	//   → rate-limited: one call per submission per 2 minutes
	if validatorFactory != nil {
		sandboxHost := cfg.SandboxHost
		if sandboxHost == "" {
			sandboxHost = "localhost"
		}
		vh := newValidationHandler(svc, sandboxHost, validatorFactory)
		v1.HandleFunc("/submissions/{id}/validate", vh.validate).Methods(http.MethodPost)
	}

	// ── Leaderboard (Phase 1, extended in Phase 5 with deduplication) ─────────
	// GET /api/v1/leaderboard returns one row per team (best score wins).
	// Phase 1–4 behavior is fully preserved: the response shape is identical;
	// deduplication is an additive filter that only matters when a team has
	// multiple completed submissions.
	v1.HandleFunc("/leaderboard", h.leaderboard).Methods(http.MethodGet)

	// ── Team history (Phase 5, AD-2) ──────────────────────────────────────────
	// Only mounted when contestSvc is non-nil. No auth required — this is a
	// public read endpoint for contestants to see their own submission history.
	// We gate on contestSvc (not AdminAPIKey) because the route is meaningful
	// only when contest-scoped submissions exist.
	if contestSvc != nil {
		v1.HandleFunc("/teams/{name}/submissions", h.teamHistory).Methods(http.MethodGet)
	}

	// ── Admin routes (Phase 5) ────────────────────────────────────────────────
	// Only mounted when both contestSvc is non-nil AND AdminAPIKey is set.
	// All routes are protected by adminAuthMiddleware (Bearer token).
	if contestSvc != nil && cfg.AdminAPIKey != "" {
		ch := &contestHandler{svc: contestSvc}
		admin := v1.PathPrefix("/admin").Subrouter()
		admin.Use(adminAuthMiddleware(cfg.AdminAPIKey))

		admin.HandleFunc("/contests", ch.createContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests", ch.listContests).Methods(http.MethodGet)
		admin.HandleFunc("/contests/{id}/activate", ch.activateContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests/{id}/close", ch.closeContest).Methods(http.MethodPost)
		admin.HandleFunc("/contests/{id}/leaderboard", ch.getContestLeaderboard).Methods(http.MethodGet)
	}

	// ── Orchestrator (Phase 3+ distributed mode only) ─────────────────────────
	// Worker registration and heartbeat routes. Only mounted when orchHandler
	// is non-nil (DISTRIBUTED_MODE=true).
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

// createSubmission handles POST /api/v1/submissions.
//
// Multipart form fields:
//
//	team_name  string
//	language   string   (go | rust | cpp | python | binary)
//	protocol   string   (rest | websocket | fix)
//	archive    file     (.tar.gz or .zip)
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

// leaderboard handles GET /api/v1/leaderboard.
//
// Phase 5 change (AD-1): only the highest-scoring submission per team is
// shown. This prevents active teams from flooding the top-N spots with
// minor variants of the same engine.
//
// Backward compatibility: the response envelope shape is unchanged
// ({"count": N, "entries": [...]}). Each entry has identical fields to
// before. The only observable difference is that teams with multiple
// completed submissions now appear exactly once.
//
// Score source:
//   - Phase 5 submissions (FinalScore > 0): FinalScore used as the ranking key.
//   - Phase 1–4 submissions (FinalScore == 0, Results != nil):
//     Results.CompositeScore used — identical to the old leaderboard logic.
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
// It is extracted as a pure function so it can be unit-tested without an HTTP
// server and reused by the auto-close goroutine in cmd/server/main.go.
//
// Algorithm:
//  1. Iterate submissions; skip non-completed and those with no score.
//  2. For each team, keep only the submission with the higher effective score.
//  3. Sort the winners descending by score.
//  4. Assign 1-based ranks.
//
// The function allocates its own output slice; it does not mutate the input.
func buildDedupedLeaderboard(subs []*models.Submission) []models.LeaderboardEntry {
	type candidate struct {
		entry models.LeaderboardEntry
		score float64 // effective score for ranking (FinalScore or CompositeScore)
	}

	// bestByTeam maps TeamName → the highest-scoring completed submission.
	bestByTeam := make(map[string]candidate, len(subs))

	for _, sub := range subs {
		if sub.Status != models.StatusCompleted {
			continue
		}

		// Determine the effective score. Phase 5 submissions use FinalScore;
		// Phase 1–4 submissions use Results.CompositeScore.
		effectiveScore := sub.FinalScore
		if effectiveScore == 0 && sub.Results != nil {
			effectiveScore = sub.Results.CompositeScore
		}
		if effectiveScore == 0 {
			continue // no score yet (results not written)
		}

		entry := buildLeaderboardEntry(sub, effectiveScore)

		existing, seen := bestByTeam[sub.TeamName]
		if !seen || effectiveScore > existing.score {
			bestByTeam[sub.TeamName] = candidate{entry: entry, score: effectiveScore}
		}
	}

	// Collect and sort descending by effective score.
	ranked := make([]models.LeaderboardEntry, 0, len(bestByTeam))
	for _, c := range bestByTeam {
		ranked = append(ranked, c.entry)
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].CompositeScore > ranked[j].CompositeScore
	})

	// Assign 1-based ranks after sorting.
	for i := range ranked {
		ranked[i].Rank = i + 1
	}
	return ranked
}

// buildLeaderboardEntry constructs a LeaderboardEntry from a completed
// submission. effectiveScore is the value already computed by the caller
// (either FinalScore or CompositeScore) and is placed into CompositeScore
// for the Phase 1–4 backward-compatible field.
//
// Phase 5 fields (LowScore, MediumScore, HighScore, FinalScore, BestP99Ms,
// PeakSustainedTPS, AvgCorrectness) are populated from AllResults when present.
// For Phase 1–4 submissions these fields remain at their zero values and are
// omitted from JSON output via omitempty.
func buildLeaderboardEntry(sub *models.Submission, effectiveScore float64) models.LeaderboardEntry {
	entry := models.LeaderboardEntry{
		SubmissionID:   sub.ID,
		TeamName:       sub.TeamName,
		Language:       sub.Language,
		Protocol:       sub.Protocol,
		Status:         sub.Status,
		CompositeScore: effectiveScore, // backward-compat field; equals FinalScore for P5
		FinalScore:     sub.FinalScore,
		CompletedAt:    sub.CompletedAt,
	}

	// Populate Phase 1–4 diagnostic columns from the legacy Results field.
	if sub.Results != nil {
		entry.P99LatencyMs = sub.Results.P99LatencyMs
		entry.MaxTPS = sub.Results.MaxTPS
		entry.CorrectnessScore = sub.Results.CorrectnessScore
	}

	// Populate Phase 5 per-profile scores and aggregate diagnostics.
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

// teamHistory handles GET /api/v1/teams/{name}/submissions.
//
// Returns all submissions from the named team, sorted newest-first.
// No authentication required — this is a public read endpoint. Teams can
// see their own history; the frontend "Team Profile" page uses this to render
// a table of all historical runs (Go engine vs. Rust engine, etc.).
//
// Response:
//
//	{
//	  "team_name":   "alpha",
//	  "count":       3,
//	  "submissions": [ ...full Submission objects, newest first... ]
//	}
//
// Returns an empty submissions array (not null) when the team has no
// submissions. Returns 200 in all cases — a team name that does not exist
// simply returns count=0. This avoids leaking information about which
// team names are registered.
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

	// Filter to this team's submissions. List() already returns newest-first
	// (sorted by CreatedAt descending in DiskStore.List), so no re-sort needed.
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

// prometheusMiddleware records HTTP request counts and durations.
// Uses a normalised path to keep Prometheus label cardinality bounded.
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

// normalisePath replaces UUIDs and numeric IDs with {id} placeholders.
// e.g. /api/v1/submissions/abc-123 → /api/v1/submissions/{id}
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
//
// Design rule: always use errors.Is for sentinel errors. Never string-match
// a sentinel — the message may change. String matching is reserved for
// Phase 1–4 submission errors that pre-date the sentinel pattern.
func classifyError(err error) (int, models.APIError) {
	// Phase 5 contest-lifecycle sentinels.
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

	// Phase 1–4 submission validation errors (string-matched for compat).
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

// adminAuthMiddleware enforces Authorization: Bearer <apiKey> on all routes
// in the /api/v1/admin subrouter.
//
// Returns 401 when the header is absent or the token is wrong.
// 403 is reserved for future role-based access control (e.g. read-only admin).
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

// createContest handles POST /api/v1/admin/contests.
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

// listContests handles GET /api/v1/admin/contests.
// Returns all contests (draft, active, and closed).
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

// activateContest handles POST /api/v1/admin/contests/{id}/activate.
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

// closeContest handles POST /api/v1/admin/contests/{id}/close.
//
// Entries are nil here — the handler has no access to the submission store.
// The drain-and-wait goroutine (AD-3) is the primary closing path and passes
// real entries. This endpoint is the manual override (force-close by admin).
func (h *contestHandler) closeContest(w http.ResponseWriter, r *http.Request) {
	id := mux.Vars(r)["id"]
	if err := h.svc.Close(r.Context(), id, nil); err != nil {
		code, apiErr := classifyError(err)
		writeError(w, code, apiErr.Code, apiErr.Message)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "closed", "id": id})
}

// getContestLeaderboard handles GET /api/v1/admin/contests/{id}/leaderboard.
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
