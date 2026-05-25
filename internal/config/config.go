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

	// SandboxHost is the hostname the worker uses to reach published sandbox
	// ports after the Docker daemon starts a sandbox container.
	//
	// The Docker daemon runs on the HOST machine. When the worker (running
	// inside a container) asks the daemon to start a sandbox and publish port
	// N, that port is bound on the HOST's network interface — not inside the
	// worker's network namespace. So the worker must connect to the HOST, not
	// to localhost.
	//
	// Docker Desktop (Windows/Mac): use "host-gateway" — Docker resolves this
	// special name to the host's internal bridge IP automatically.
	// Linux Docker Engine: typically "172.17.0.1" (the docker0 bridge IP).
	// Local (non-containerised) dev: "localhost" (worker and daemon share the
	// same network namespace).
	//
	// Set via environment variable: SANDBOX_HOST
	// Default: "host-gateway" (correct for Docker Desktop; override for Linux).
	SandboxHost string

	// Maximum upload size (bytes)
	MaxUploadBytes int64

	// ── Phase 3: Distributed mode ─────────────────────────────────────────────

	// DistributedMode switches the control plane from local (Phase 1/2) mode
	// to distributed (Phase 3+) mode.
	DistributedMode bool

	// RedpandaBrokers is the comma-separated list of Redpanda broker addresses.
	RedpandaBrokers []string

	// ── Worker-specific ───────────────────────────────────────────────────────

	// WorkerID uniquely identifies a worker instance in logs and metrics.
	WorkerID string

	// JobTimeout is the maximum wall-clock time a worker spends on a single job.
	JobTimeout time.Duration

	// OrchestratorURL is the base URL of the control plane (for heartbeats).
	OrchestratorURL string

	// ── Bot fleet ─────────────────────────────────────────────────────────────

	// BotCount is the number of concurrent virtual traders per benchmark run.
	// Default: 100.
	BotCount int

	// BotTestDuration is the sustained load period after ramp-up completes.
	// Default: 60s.
	BotTestDuration time.Duration

	// BotRampUpDuration is the period over which bots are staggered at startup.
	// Default: 5s.
	BotRampUpDuration time.Duration

	// BotOrderRatioLimit / Market / Cancel are the fractions of each order type
	// generated per bot. Must sum to 1.0. Defaults: 0.60 / 0.30 / 0.10.
	BotOrderRatioLimit  float64
	BotOrderRatioMarket float64
	BotOrderRatioCancel float64

	// BotPerRequestTimeout is the HTTP timeout for a single bot order round-trip.
	// Default: 2s (tight enough to detect latency regressions).
	BotPerRequestTimeout time.Duration
}

// Load reads configuration from environment variables with sane defaults.
func Load() *Config {
	return &Config{
		ListenAddr: getEnv("LISTEN_ADDR", ":8080"),

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
		SandboxHost:        getEnv("SANDBOX_HOST", "host-gateway"),
		MaxUploadBytes:     getEnvInt64("MAX_UPLOAD_BYTES", 256*1024*1024),

		DistributedMode: getEnvBool("DISTRIBUTED_MODE", false),
		RedpandaBrokers: getEnvStringSlice("REDPANDA_BROKERS", []string{"127.0.0.1:19092"}),

		WorkerID:        getEnv("WORKER_ID", hostname()),
		JobTimeout:      getEnvDuration("JOB_TIMEOUT", 35*time.Minute),
		OrchestratorURL: getEnv("ORCHESTRATOR_URL", "http://localhost:8080"),

		// Bot fleet defaults
		BotCount:             getEnvInt("BOT_COUNT", 100),
		BotTestDuration:      getEnvDuration("BOT_TEST_DURATION", 60*time.Second),
		BotRampUpDuration:    getEnvDuration("BOT_RAMP_UP_DURATION", 5*time.Second),
		BotOrderRatioLimit:   getEnvFloat64("BOT_ORDER_RATIO_LIMIT", 0.60),
		BotOrderRatioMarket:  getEnvFloat64("BOT_ORDER_RATIO_MARKET", 0.30),
		BotOrderRatioCancel:  getEnvFloat64("BOT_ORDER_RATIO_CANCEL", 0.10),
		BotPerRequestTimeout: getEnvDuration("BOT_PER_REQUEST_TIMEOUT", 2*time.Second),
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

func getEnvFloat64(key string, defaultVal float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
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

func hostname() string {
	h, err := os.Hostname()
	if err != nil {
		return "worker-unknown"
	}
	return h
}
