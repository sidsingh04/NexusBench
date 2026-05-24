package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/metrics"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/orchestrator"
	"github.com/nexusbench/nexusbench/internal/submission"
)

type handler struct {
	svc *submission.Service
	cfg *config.Config
	reg *metrics.Registry
}

// NewRouter wires up all routes and returns the root http.Handler.
// orchHandler may be nil — if so, /internal/workers/* routes are not mounted
// (Phase 1/2 local mode where no worker fleet exists).
func NewRouter(svc *submission.Service, cfg *config.Config, reg *metrics.Registry, orchHandler *orchestrator.Handler) http.Handler {
	h := &handler{svc: svc, cfg: cfg, reg: reg}

	r := mux.NewRouter()
	r.Use(requestLogger)
	r.Use(corsMiddleware)
	r.Use(h.prometheusMiddleware)

	// /metrics is served directly by the Prometheus handler — no auth,
	// no JSON wrapper, no logging middleware (it would pollute access logs).
	r.Handle("/metrics", reg.Handler()).Methods(http.MethodGet)
	r.HandleFunc("/health", h.health).Methods(http.MethodGet)

	v1 := r.PathPrefix("/api/v1").Subrouter()

	// Sandbox image registry — lets callers see what languages are available
	// before uploading, without needing access to the Docker host.
	v1.HandleFunc("/images", h.listImages).Methods(http.MethodGet)

	// Submissions
	v1.HandleFunc("/submissions", h.listSubmissions).Methods(http.MethodGet)
	v1.HandleFunc("/submissions", h.createSubmission).Methods(http.MethodPost)
	v1.HandleFunc("/submissions/{id}", h.getSubmission).Methods(http.MethodGet)
	v1.HandleFunc("/submissions/{id}/stop", h.stopSubmission).Methods(http.MethodPost)

	// Leaderboard
	v1.HandleFunc("/leaderboard", h.leaderboard).Methods(http.MethodGet)

	// ── Orchestrator (Phase 3+ distributed mode only) ─────────────────────────
	// These internal routes let workers register themselves and send heartbeats.
	// Only mounted when orchHandler is non-nil (DISTRIBUTED_MODE=true).
	if orchHandler != nil {
		internal := r.PathPrefix("/internal").Subrouter()
		internal.HandleFunc("/workers/register", orchHandler.HTTPRegister).Methods(http.MethodPost)
		internal.HandleFunc("/workers/{id}/heartbeat", orchHandler.HTTPHeartbeat).Methods(http.MethodPost)
		internal.HandleFunc("/workers", orchHandler.HTTPList).Methods(http.MethodGet)
		internal.HandleFunc("/workers/stats", orchHandler.HTTPStats).Methods(http.MethodGet)
	}

	return r
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func (h *handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "nexusbench-control-plane",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// listImages handles GET /api/v1/images
// Returns the configured image tag for each supported language so callers
// know which languages are available before uploading.
//
// Example response:
//
//	{
//	  "images": {
//	    "go":     "nexusbench-sandbox-go:latest",
//	    "rust":   "nexusbench-sandbox-rust:latest",
//	    "cpp":    "nexusbench-sandbox-cpp:latest",
//	    "python": "nexusbench-sandbox-python:latest",
//	    "binary": "nexusbench-sandbox-binary:latest"
//	  }
//	}
func (h *handler) listImages(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"images": h.cfg.AllImages(),
	})
}

// createSubmission handles POST /api/v1/submissions
// Multipart form fields:
//
//	team_name  string
//	language   string   (go | rust | cpp | python | binary)
//	protocol   string   (rest | websocket | fix)
//	archive    file     (.tar.gz or .zip)
func (h *handler) createSubmission(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxUploadBytes)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_FORM", "failed to parse multipart form: "+err.Error())
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
		writeError(w, http.StatusNotFound, "NOT_FOUND", fmt.Sprintf("submission %s not found", id))
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

func (h *handler) leaderboard(w http.ResponseWriter, r *http.Request) {
	subs, err := h.svc.List()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "LEADERBOARD_ERROR", err.Error())
		return
	}

	var entries []models.LeaderboardEntry
	rank := 1
	for _, sub := range subs {
		if sub.Status != models.StatusCompleted || sub.Results == nil {
			continue
		}
		entries = append(entries, models.LeaderboardEntry{
			Rank:             rank,
			SubmissionID:     sub.ID,
			TeamName:         sub.TeamName,
			Language:         sub.Language,
			Protocol:         sub.Protocol,
			Status:           sub.Status,
			CompositeScore:   sub.Results.CompositeScore,
			P99LatencyMs:     sub.Results.P99LatencyMs,
			MaxTPS:           sub.Results.MaxTPS,
			CorrectnessScore: sub.Results.CorrectnessScore,
			CompletedAt:      sub.CompletedAt,
		})
		rank++
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"count":   len(entries),
		"entries": entries,
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
// It uses a normalised path (stripping dynamic IDs) to keep cardinality low.
func (h *handler) prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Skip /metrics itself — no need to instrument the instrumentation.
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

// normalisePath replaces UUIDs and numeric IDs with placeholders to keep
// Prometheus label cardinality bounded.
// e.g. /api/v1/submissions/abc-123 → /api/v1/submissions/{id}
func normalisePath(path string) string {
	parts := strings.Split(path, "/")
	for i, p := range parts {
		// Heuristic: segments longer than 8 chars that contain a hyphen or digit
		// are likely IDs (UUIDs, submission IDs).
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

func classifyError(err error) (int, models.APIError) {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "required"),
		strings.Contains(msg, "unsupported"),
		strings.Contains(msg, "invalid"),
		strings.Contains(msg, "no sandbox image"):
		return http.StatusBadRequest, models.APIError{Code: "VALIDATION_ERROR", Message: msg}
	default:
		return http.StatusInternalServerError, models.APIError{Code: "INTERNAL_ERROR", Message: msg}
	}
}
