// Package api provides the HTTP router and handlers for the NexusBench
// control plane.
//
// Dependency rules (enforced by go build):
//   - api may import models, config, metrics, submission, contest, orchestrator.
//   - api must NOT import worker, sandbox, botfleet, or correctness.
//   - No business logic lives here: handlers translate HTTP ↔ domain calls only.
//     All invariants, status transitions, and scoring live in their respective
//     service packages.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
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

// ── Router ────────────────────────────────────────────────────────────────────

// NewRouter wires up all routes and returns the root http.Handler.
//
// Parameters:
//   - svc         — submission service (required)
//   - cfg         — loaded config (required)
//   - reg         — Prometheus registry (required)
//   - orchHandler — orchestrator HTTP handler; nil = local mode, routes not mounted
//   - contestSvc  — contest service; nil = admin + team-history routes not mounted
//     (Phase 1–4 backward compat: passing nil keeps old behaviour exactly)
//
// Backward compatibility guarantee: all Phase 1–4 routes (/health, /metrics,
// /api/v1/submissions/*, /api/v1/leaderboard, /api/v1/images,
// /internal/workers/*) remain mounted and respond identically regardless of
// whether contestSvc or orchHandler are nil.
func NewRouter(
	svc *submission.Service,
	cfg *config.Config,
	reg *metrics.Registry,
	orchHandler *orchestrator.Handler,
	contestSvc *contest.ContestService,
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

	// ── Leaderboard (Phase 1, extended in Phase 5 with deduplication) ─────────
	// GET /api/v1/leaderboard returns one row per team (best score wins).
	// Phase 1–4 behaviour is fully preserved: the response shape is identical;
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
