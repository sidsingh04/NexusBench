package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store is the write abstraction between the consumer pipeline and the database.
// Keeping it as an interface means:
//  1. Consumer never imports pgx — it depends only on this interface.
//  2. Tests use a MemoryStore (defined in store_test.go) without a real DB.
//  3. Swapping TimescaleDB for ClickHouse in Phase 3+ only requires a new
//     implementation of this interface, not changes to consumer.go.
type Store interface {
	// WriteWindow persists one LatencyWindow. Implementations must be
	// idempotent on (WindowStart, SubmissionID) — duplicate windows from
	// consumer restarts must not produce duplicate rows.
	WriteWindow(ctx context.Context, w LatencyWindow) error

	// Close releases the underlying connection pool.
	Close()
}

// ── TimescaleStore ────────────────────────────────────────────────────────────

// TimescaleStore implements Store using a TimescaleDB (PostgreSQL + TimescaleDB
// extension) connection pool via pgx/v5.
//
// Schema owned by this type (created by Bootstrap()):
//
//	CREATE TABLE IF NOT EXISTS latency_windows (
//	    time          TIMESTAMPTZ     NOT NULL,   -- window start, partition key
//	    submission_id TEXT            NOT NULL,
//	    p50_ns        BIGINT          NOT NULL,
//	    p90_ns        BIGINT          NOT NULL,
//	    p99_ns        BIGINT          NOT NULL,
//	    min_ns        BIGINT          NOT NULL,
//	    max_ns        BIGINT          NOT NULL,
//	    mean_ns       BIGINT          NOT NULL,
//	    tps           DOUBLE PRECISION NOT NULL,
//	    sample_n      INT             NOT NULL,
//	    PRIMARY KEY (time, submission_id)
//	);
//	-- converted to a TimescaleDB hypertable, partitioned by `time`
//
// Why store latencies as BIGINT nanoseconds?
//   - Lossless. Converting to µs or ms at ingestion discards sub-µs data.
//   - Grafana converts at query time: `p99_ns / 1e6` → milliseconds.
//   - TimescaleDB compression handles repetitive BIGINT columns efficiently.
type TimescaleStore struct {
	pool *pgxpool.Pool
}

// NewTimescaleStore opens a connection pool to the given PostgreSQL DSN and
// verifies connectivity. Does NOT create schema — call Bootstrap() first.
//
// dsn format: "postgres://user:password@host:port/dbname"
func NewTimescaleStore(ctx context.Context, dsn string) (*TimescaleStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("consumer: parse DSN: %w", err)
	}

	// Pool sizing: 5 connections is enough for one consumer writing 5-second
	// windows. Scale up if multiple consumers share the pool in Phase 3.
	cfg.MaxConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("consumer: open pool: %w", err)
	}

	// Ping to surface connection errors at startup, not on the first write.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("consumer: ping TimescaleDB: %w", err)
	}

	slog.Info("consumer: connected to TimescaleDB", "dsn_host", cfg.ConnConfig.Host)
	return &TimescaleStore{pool: pool}, nil
}

// Bootstrap creates the latency_windows table and hypertable if they don't
// exist. Safe to call on every startup — all statements are idempotent.
func (s *TimescaleStore) Bootstrap(ctx context.Context) error {
	slog.Info("consumer: bootstrapping TimescaleDB schema")

	// Single transaction: if any statement fails the whole migration rolls back.
	return s.withTx(ctx, func(tx pgx.Tx) error {
		// Step 1: create the base table.
		// ON CONFLICT DO NOTHING on the PRIMARY KEY makes WriteWindow idempotent
		// for duplicate windows (consumer restart replaying the same Redpanda offset).
		_, err := tx.Exec(ctx, `
			CREATE TABLE IF NOT EXISTS latency_windows (
				time           TIMESTAMPTZ      NOT NULL,
				submission_id  TEXT             NOT NULL,
				p50_ns         BIGINT           NOT NULL,
				p90_ns         BIGINT           NOT NULL,
				p99_ns         BIGINT           NOT NULL,
				min_ns         BIGINT           NOT NULL,
				max_ns         BIGINT           NOT NULL,
				mean_ns        BIGINT           NOT NULL,
				tps            DOUBLE PRECISION NOT NULL,
				sample_n       INT              NOT NULL,
				PRIMARY KEY    (time, submission_id)
			)
		`)
		if err != nil {
			return fmt.Errorf("create latency_windows: %w", err)
		}

		// Step 2: convert to a hypertable, partitioned by `time`.
		// create_hypertable errors if the table is already a hypertable, so we
		// catch and ignore that specific error using IF NOT EXISTS syntax
		// (available in TimescaleDB ≥ 2.0).
		_, err = tx.Exec(ctx, `
			SELECT create_hypertable(
				'latency_windows',
				'time',
				if_not_exists => TRUE,
				migrate_data  => TRUE
			)
		`)
		if err != nil {
			return fmt.Errorf("create_hypertable: %w", err)
		}

		// Step 3: index on submission_id for per-submission dashboard queries.
		// The PRIMARY KEY already covers (time, submission_id), but queries
		// that filter only by submission_id (e.g. "show me sub-123's history")
		// benefit from this secondary index.
		_, err = tx.Exec(ctx, `
			CREATE INDEX IF NOT EXISTS idx_latency_windows_submission
			ON latency_windows (submission_id, time DESC)
		`)
		if err != nil {
			return fmt.Errorf("create submission index: %w", err)
		}

		slog.Info("consumer: schema bootstrap complete")
		return nil
	})
}

// WriteWindow persists one LatencyWindow to the latency_windows table.
// ON CONFLICT DO NOTHING makes it idempotent: replaying a Redpanda offset
// after a consumer restart will not produce duplicate rows.
func (s *TimescaleStore) WriteWindow(ctx context.Context, w LatencyWindow) error {
	if w.SampleN == 0 {
		// Don't write empty windows — they carry no useful information and
		// would pollute percentile queries with zero rows.
		slog.Debug("consumer: skipping empty window", "submission_id", w.SubmissionID)
		return nil
	}

	const q = `
		INSERT INTO latency_windows
			(time, submission_id, p50_ns, p90_ns, p99_ns, min_ns, max_ns, mean_ns, tps, sample_n)
		VALUES
			($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (time, submission_id) DO NOTHING
	`

	_, err := s.pool.Exec(ctx, q,
		w.WindowStart,
		w.SubmissionID,
		w.P50Ns,
		w.P90Ns,
		w.P99Ns,
		w.MinNs,
		w.MaxNs,
		w.MeanNs,
		w.TPS,
		w.SampleN,
	)
	if err != nil {
		return fmt.Errorf("consumer: insert latency_window: %w", err)
	}

	slog.Debug("consumer: wrote window",
		"submission_id", w.SubmissionID,
		"window_start", w.WindowStart,
		"p99_ns", w.P99Ns,
		"tps", w.TPS,
		"sample_n", w.SampleN,
	)
	return nil
}

// Close releases all connections in the pool.
func (s *TimescaleStore) Close() {
	s.pool.Close()
	slog.Info("consumer: TimescaleStore closed")
}

// ── helpers ───────────────────────────────────────────────────────────────────

// withTx runs fn inside a serialisable transaction, rolling back on any error.
func (s *TimescaleStore) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("consumer: begin tx: %w", err)
	}
	defer func() {
		// Rollback is a no-op if the transaction was already committed.
		if rbErr := tx.Rollback(ctx); rbErr != nil && rbErr != pgx.ErrTxClosed {
			slog.Warn("consumer: rollback error", "err", rbErr)
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("consumer: commit tx: %w", err)
	}
	return nil
}
