package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexusbench/nexusbench/internal/config"
	"github.com/nexusbench/nexusbench/internal/models"
	"github.com/nexusbench/nexusbench/internal/submission"
)

type handler struct {
	svc *submission.Service
	cfg *config.Config
}

// NewRouter wires up all routes and returns the root http.Handler.
func NewRouter(svc *submission.Service, cfg *config.Config) http.Handler {
	h := &handler{svc: svc, cfg: cfg}

	r := mux.NewRouter()
	r.Use(requestLogger)
	r.Use(corsMiddleware)

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
