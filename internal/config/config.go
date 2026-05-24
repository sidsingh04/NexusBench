package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration for the NexusBench control plane.
type Config struct {
	ListenAddr string

	// SubmissionDir is where uploaded archives are stored on the host.
	// On Windows with Docker Desktop this MUST be under C:\ (or another
	// shared drive) — Docker cannot bind-mount paths outside shared drives.
	// Default: C:\nexusbench\submissions  (Windows-safe, Docker-shareable)
	SubmissionDir string

	// Per-language sandbox images — pre-built, spun on demand.
	ImageGo     string
	ImageRust   string
	ImageCpp    string
	ImagePython string
	ImageBinary string

	// Resource limits per sandbox container
	SandboxCPUQuota    int64
	SandboxMemoryBytes int64
	SandboxTimeout     time.Duration
	SandboxNetworkMode string

	// Host port range allocated to sandbox containers
	SandboxPortMin int
	SandboxPortMax int

	// Maximum upload size (bytes)
	MaxUploadBytes int64

	// ── Phase 3: Distributed mode ─────────────────────────────────────────────

	// DistributedMode switches the control plane from local (Phase 1/2) mode
	// to distributed (Phase 3+) mode.
	//
	//   false (default) — Ingest deploys sandboxes directly in-process.
	//                     RedpandaBrokers is ignored. No queue dependency.
	//
	//   true            — Ingest enqueues a Job to jobs.benchmark.
	//                     A separate worker process picks it up and runs the
	//                     sandbox. RedpandaBrokers must be set.
	//
	// Set via environment variable: DISTRIBUTED_MODE=true
	DistributedMode bool

	// RedpandaBrokers is the comma-separated list of Redpanda broker addresses
	// used for the job queue in distributed mode.
	// Example: "redpanda:9092" or "localhost:19092,localhost:29092"
	// Set via environment variable: REDPANDA_BROKERS
	RedpandaBrokers []string

	// ── Worker-specific ───────────────────────────────────────────────────────

	// WorkerID uniquely identifies a worker instance in logs and metrics.
	// Defaults to the system hostname so each pod/container is distinct.
	// Set via environment variable: WORKER_ID
	WorkerID string

	// JobTimeout is the maximum wall-clock time a worker spends on a single
	// job (sandbox deploy + health wait + bot fleet + result write).
	// Set via environment variable: JOB_TIMEOUT
	JobTimeout time.Duration

	// OrchestratorURL is the base URL of the control plane that the worker
	// sends heartbeats to. Must be reachable from the worker container.
	// Example: "http://control-plane:8080"
	// Set via environment variable: ORCHESTRATOR_URL
	OrchestratorURL string
}

// Load reads configuration from environment variables with sane defaults.
func Load() *Config {
	return &Config{
		ListenAddr: getEnv("LISTEN_ADDR", ":8080"),

		// Windows default: C:\nexusbench\submissions
		// Linux/macOS default: /tmp/nexusbench/submissions
		// Override with SUBMISSION_DIR env var.
		SubmissionDir: getEnv("SUBMISSION_DIR", defaultSubmissionDir()),

		ImageGo:     getEnv("SANDBOX_IMAGE_GO", "nexusbench-sandbox-go:latest"),
		ImageRust:   getEnv("SANDBOX_IMAGE_RUST", "nexusbench-sandbox-rust:latest"),
		ImageCpp:    getEnv("SANDBOX_IMAGE_CPP", "nexusbench-sandbox-cpp:latest"),
		ImagePython: getEnv("SANDBOX_IMAGE_PYTHON", "nexusbench-sandbox-python:latest"),
		ImageBinary: getEnv("SANDBOX_IMAGE_BINARY", "nexusbench-sandbox-binary:latest"),

		SandboxCPUQuota:    getEnvInt64("SANDBOX_CPU_QUOTA", 100_000),
		SandboxMemoryBytes: getEnvInt64("SANDBOX_MEMORY_BYTES", 512*1024*1024),
		SandboxTimeout:     getEnvDuration("SANDBOX_TIMEOUT", 30*time.Minute),
		SandboxNetworkMode: getEnv("SANDBOX_NETWORK_MODE", "bridge"),
		SandboxPortMin:     getEnvInt("SANDBOX_PORT_MIN", 20000),
		SandboxPortMax:     getEnvInt("SANDBOX_PORT_MAX", 21000),
		MaxUploadBytes:     getEnvInt64("MAX_UPLOAD_BYTES", 256*1024*1024),

		DistributedMode: getEnvBool("DISTRIBUTED_MODE", false),
		RedpandaBrokers: getEnvStringSlice("REDPANDA_BROKERS", []string{"127.0.0.1:19092"}),

		WorkerID:        getEnv("WORKER_ID", hostname()),
		JobTimeout:      getEnvDuration("JOB_TIMEOUT", 35*time.Minute),
		OrchestratorURL: getEnv("ORCHESTRATOR_URL", "http://localhost:8080"),
	}
}

// ImageForLanguage returns the pre-built image tag for a given language.
func (c *Config) ImageForLanguage(lang string) (string, bool) {
	switch lang {
	case "go":
		return c.ImageGo, true
	case "rust":
		return c.ImageRust, true
	case "cpp":
		return c.ImageCpp, true
	case "python":
		return c.ImagePython, true
	case "binary":
		return c.ImageBinary, true
	default:
		return "", false
	}
}

// AllImages returns every configured image tag.
func (c *Config) AllImages() map[string]string {
	return map[string]string{
		"go":     c.ImageGo,
		"rust":   c.ImageRust,
		"cpp":    c.ImageCpp,
		"python": c.ImagePython,
		"binary": c.ImageBinary,
	}
}

// ── env helpers ───────────────────────────────────────────────────────────────

func getEnv(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func getEnvInt64(key string, defaultVal int64) int64 {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvInt(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return defaultVal
}

// getEnvBool parses a boolean environment variable.
// Accepts "true", "1", "yes" (case-insensitive) as true; everything else
// (including unset) returns the defaultVal.
func getEnvBool(key string, defaultVal bool) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(key)))
	switch v {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return defaultVal
	}
}

// getEnvStringSlice parses a comma-separated environment variable into a
// slice of trimmed, non-empty strings.
// Returns defaultVal if the variable is unset or empty.
func getEnvStringSlice(key string, defaultVal []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	if len(out) == 0 {
		return defaultVal
	}
	return out
}

// hostname returns the system hostname, falling back to "worker-unknown" if
// os.Hostname fails. Used as the default WorkerID so each pod is distinct
// without explicit configuration.
func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "worker-unknown"
	}
	return h
}
