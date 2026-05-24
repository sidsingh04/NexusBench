package orchestrator

import (
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

// Handler exposes the orchestrator's worker registry over HTTP.
// All routes are mounted under /internal/workers by the main router.
//
// Routes (all prefixed by the caller's mount point):
//
//	POST   /register           — worker startup registration
//	POST   /{id}/heartbeat     — periodic liveness ping
//	GET    /                   — list all workers with their current status
//	GET    /stats              — aggregate fleet statistics
//
// These are internal routes — not exposed to contestants. The main router
// should mount them behind an internal prefix (e.g. /internal/workers).
type Handler struct {
	registry *WorkerRegistry
}

// NewHandler constructs a Handler backed by the given registry.
func NewHandler(registry *WorkerRegistry) *Handler {
	return &Handler{registry: registry}
}

// RegisterRoutes mounts all orchestrator routes onto mux under the given
// path prefix. The prefix must not have a trailing slash.
//
// Example:
//
//	h := orchestrator.NewHandler(registry)
//	h.RegisterRoutes(mux, "/internal/workers")
func (h *Handler) RegisterRoutes(mux *http.ServeMux, prefix string) {
	mux.HandleFunc("POST "+prefix+"/register", h.handleRegister)
	mux.HandleFunc("POST "+prefix+"/{id}/heartbeat", h.handleHeartbeat)
	mux.HandleFunc("GET "+prefix+"/", h.handleList)
	mux.HandleFunc("GET "+prefix+"/stats", h.handleStats)
}

// HTTPRegister processes POST /internal/workers/register.
func (h *Handler) HTTPRegister(w http.ResponseWriter, r *http.Request) {
	h.handleRegister(w, r)
}

// HTTPHeartbeat processes POST /internal/workers/{id}/heartbeat.
func (h *Handler) HTTPHeartbeat(w http.ResponseWriter, r *http.Request) {
	h.handleHeartbeat(w, r)
}

// HTTPList processes GET /internal/workers.
func (h *Handler) HTTPList(w http.ResponseWriter, r *http.Request) {
	h.handleList(w, r)
}

// HTTPStats processes GET /internal/workers/stats.
func (h *Handler) HTTPStats(w http.ResponseWriter, r *http.Request) {
	h.handleStats(w, r)
}

// handleRegister processes POST /internal/workers/register.
//
// Request body:
//
//	{ "worker_id": "worker-abc123" }
//
// Response 201:
//
//	{ "id": "...", "status": "idle", "registered_at": "..." }
func (h *Handler) handleRegister(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WorkerID string `json:"worker_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if req.WorkerID == "" {
		writeError(w, http.StatusBadRequest, "missing_field", "worker_id is required")
		return
	}

	rec, err := h.registry.Register(req.WorkerID)
	if err != nil {
		writeError(w, http.StatusBadRequest, "registration_failed", err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, rec)
}

// handleHeartbeat processes POST /internal/workers/{id}/heartbeat.
//
// Request body:
//
//	{
//	  "status":          "busy",
//	  "current_job_id":  "sub-uuid",
//	  "jobs_completed":  3
//	}
//
// Response 200: updated WorkerRecord.
// Response 404: worker not registered.
func (h *Handler) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	// Support both gorilla/mux (used by api.Router) and stdlib mux path values.
	id := mux.Vars(r)["id"]
	if id == "" {
		id = r.PathValue("id")
	}
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing_param", "worker id is required in path")
		return
	}

	var update HeartbeatUpdate
	if err := json.NewDecoder(r.Body).Decode(&update); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "request body must be valid JSON")
		return
	}
	if update.Status == "" {
		update.Status = WorkerStatusIdle
	}

	if err := h.registry.Heartbeat(id, update); err != nil {
		writeError(w, http.StatusNotFound, "not_registered", err.Error())
		return
	}

	rec, _ := h.registry.Get(id)
	writeJSON(w, http.StatusOK, rec)
}

// handleList processes GET /internal/workers/.
// Returns all registered workers with live/dead status recomputed.
func (h *Handler) handleList(w http.ResponseWriter, r *http.Request) {
	workers := h.registry.List()
	writeJSON(w, http.StatusOK, map[string]any{
		"workers": workers,
		"count":   len(workers),
	})
}

// handleStats processes GET /internal/workers/stats.
// Returns aggregate counts — useful for monitoring dashboards.
func (h *Handler) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.registry.Stats())
}

// ── JSON helpers ──────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]string{
		"code":    code,
		"message": msg,
	})
}
