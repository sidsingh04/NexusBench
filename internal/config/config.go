package config

import (
	"os"
	"strconv"
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
