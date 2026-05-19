// cmd/consumer is the entrypoint for the NexusBench metrics consumer.
//
// It reads from the metrics.latency Redpanda topic, computes rolling
// 5-second latency windows, and writes them to TimescaleDB.
//
// Configuration via environment variables:
//
//	REDPANDA_BROKERS   comma-separated broker list  (default: 127.0.0.1:19092)
//	CONSUMER_GROUP_ID  Kafka consumer group ID      (default: nexusbench-consumer)
//	TIMESCALE_DSN      PostgreSQL connection string  (required)
//
// Example:
//
//	TIMESCALE_DSN="postgres://nexus:nexus_dev@localhost:5432/nexusbench" \
//	  go run ./cmd/consumer
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/nexusbench/nexusbench/internal/consumer"
)

func main() {
	// ── structured logging ────────────────────────────────────────────────────
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	// ── configuration from environment ────────────────────────────────────────
	brokers := envOr("REDPANDA_BROKERS", "127.0.0.1:19092")
	groupID := envOr("CONSUMER_GROUP_ID", "nexusbench-consumer")
	dsn := mustEnv("TIMESCALE_DSN")

	brokerList := strings.Split(brokers, ",")
	for i := range brokerList {
		brokerList[i] = strings.TrimSpace(brokerList[i])
	}

	slog.Info("consumer: starting",
		"brokers", brokerList,
		"group_id", groupID,
	)

	// ── TimescaleDB store ─────────────────────────────────────────────────────
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	store, err := consumer.NewTimescaleStore(ctx, dsn)
	if err != nil {
		slog.Error("consumer: connect to TimescaleDB", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	// Bootstrap is idempotent — creates the hypertable if it doesn't exist.
	if err := store.Bootstrap(ctx); err != nil {
		slog.Error("consumer: bootstrap schema", "err", err)
		os.Exit(1)
	}

	// ── Consumer ──────────────────────────────────────────────────────────────
	cfg := consumer.Config{
		Brokers:        brokerList,
		GroupID:        groupID,
		CommitInterval: 5 * time.Second,
	}

	c, err := consumer.NewConsumer(cfg, store)
	if err != nil {
		slog.Error("consumer: create consumer", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	// ── Run ───────────────────────────────────────────────────────────────────
	// Run blocks until ctx is cancelled (SIGINT/SIGTERM).
	if err := c.Run(ctx); err != nil {
		slog.Error("consumer: run error", "err", err)
		os.Exit(1)
	}

	slog.Info("consumer: shutdown complete")
}

// envOr returns the value of the environment variable named by key,
// or fallback if the variable is unset or empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// mustEnv returns the value of the environment variable named by key.
// Exits with a clear error if the variable is not set.
func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		slog.Error("required environment variable not set", "var", key)
		os.Exit(1)
	}
	return v
}
