package contest

// postgres.go — PostgreSQL-backed ContestStore for production deployments.
//
// # Overview
//
// PostgresContestStore satisfies the ContestStore interface using a pgxpool
// connection pool. It is the production implementation; MemoryContestStore
// remains the implementation used in all unit tests (no database required).
//
// # Schema
//
// Two tables, both created idempotently by migrate() on startup:
//
//	contests                        — one row per contest
//	contest_leaderboard_snapshots   — one row per closed contest (JSONB entries)
//
// VolatilityProfile structs are stored as JSONB in three columns of the
// contests table (low_profile, medium_profile, high_profile). This avoids
// a third normalised table and keeps the schema flat and easy to inspect with
// psql. The Go encoding/json package handles serialization via pgx's JSONB
// support.
//
// # Concurrency
//
// pgxpool is fully goroutine-safe and manages a pool of connections internally.
// No additional locking is needed in this file.
//
// # Migration strategy
//
// migrate() runs CREATE TABLE IF NOT EXISTS on startup. This is sufficient for
// the hackathon lifecycle — there is at most one schema version and no
// rollback requirement. A proper migration tool (goose, golang-migrate) can be
// introduced post-hackathon.
//
// # Dependency note
//
// pgx/v5 is already in go.mod (indirect dependency via TimescaleDB consumer).
// No new module is introduced by this file.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nexusbench/nexusbench/internal/models"
)

// PostgresContestStore implements ContestStore backed by a PostgreSQL database.
//
// The zero value is NOT valid. Use NewPostgresContestStore.
type PostgresContestStore struct {
	pool *pgxpool.Pool
}

// NewPostgresContestStore connects to the database at dsn, runs the schema
// migration, and returns a ready-to-use store.
//
// dsn must be a valid libpq connection string, e.g.:
//
//	"postgres://nexusbench:secret@localhost:5432/nexusbench"
//
// The migration is idempotent — calling NewPostgresContestStore against a
// database that already has the correct schema is safe.
//
// ctx is used for the initial connection and migration queries only. The
// returned store uses its own context per-operation.
func NewPostgresContestStore(ctx context.Context, dsn string) (*PostgresContestStore, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("contest/postgres: parse DSN: %w", err)
	}

	// Connection pool tuning: sized for the control-plane workload.
	// Contest operations are infrequent (admin actions + auto-close ticks)
	// so a small pool is sufficient. The connection limit keeps us well under
	// the PostgreSQL default of 100.
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.HealthCheckPeriod = time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("contest/postgres: create pool: %w", err)
	}

	// Verify connectivity before returning.
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("contest/postgres: ping: %w", err)
	}

	s := &PostgresContestStore{pool: pool}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("contest/postgres: migrate: %w", err)
	}

	return s, nil
}

// Close releases all connections in the pool. Call when the server shuts down.
func (s *PostgresContestStore) Close() {
	s.pool.Close()
}

// migrate creates the two tables if they do not already exist.
// Safe to call on every startup — uses CREATE TABLE IF NOT EXISTS.
func (s *PostgresContestStore) migrate(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS contests (
    id                    TEXT        PRIMARY KEY,
    name                  TEXT        NOT NULL,
    status                TEXT        NOT NULL,
    low_profile           JSONB       NOT NULL,
    medium_profile        JSONB       NOT NULL,
    high_profile          JSONB       NOT NULL,
    low_weight            FLOAT8      NOT NULL DEFAULT 0.20,
    medium_weight         FLOAT8      NOT NULL DEFAULT 0.35,
    high_weight           FLOAT8      NOT NULL DEFAULT 0.45,
    submissions_closed_at TIMESTAMPTZ,
    contest_closed_at     TIMESTAMPTZ,
    ends_at               TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS contest_leaderboard_snapshots (
    contest_id     TEXT        PRIMARY KEY REFERENCES contests(id),
    entries        JSONB       NOT NULL,
    snapshotted_at TIMESTAMPTZ NOT NULL DEFAULT now()
);`

	_, err := s.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("DDL: %w", err)
	}
	return nil
}

// ── ContestStore implementation ────────────────────────────────────────────────

// Save persists a new contest. Returns ErrDuplicateContest on primary-key conflict.
func (s *PostgresContestStore) Save(c *models.Contest) error {
	low, err := marshalProfile(c.LowProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal low_profile: %w", err)
	}
	med, err := marshalProfile(c.MediumProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal medium_profile: %w", err)
	}
	high, err := marshalProfile(c.HighProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal high_profile: %w", err)
	}

	const q = `
INSERT INTO contests
    (id, name, status,
     low_profile, medium_profile, high_profile,
     low_weight, medium_weight, high_weight,
     submissions_closed_at, contest_closed_at, ends_at,
     created_at, updated_at)
VALUES
    ($1, $2, $3,
     $4, $5, $6,
     $7, $8, $9,
     $10, $11, $12,
     $13, $14)`

	ctx, cancel := operationCtx()
	defer cancel()

	_, execErr := s.pool.Exec(ctx, q,
		c.ID, c.Name, string(c.Status),
		low, med, high,
		c.LowWeight, c.MediumWeight, c.HighWeight,
		nilableTime(c.SubmissionsClosedAt),
		nilableTime(c.ContestClosedAt),
		nilableTime(c.EndsAt),
		c.CreatedAt, c.UpdatedAt,
	)
	if execErr != nil {
		if isDuplicateKey(execErr) {
			return ErrDuplicateContest
		}
		return fmt.Errorf("contest/postgres: save %s: %w", c.ID, execErr)
	}
	return nil
}

// Get returns the contest with the given ID. Returns ErrNotFound if absent.
func (s *PostgresContestStore) Get(id string) (*models.Contest, error) {
	const q = `
SELECT id, name, status,
       low_profile, medium_profile, high_profile,
       low_weight, medium_weight, high_weight,
       submissions_closed_at, contest_closed_at, ends_at,
       created_at, updated_at
FROM contests
WHERE id = $1`

	ctx, cancel := operationCtx()
	defer cancel()

	row := s.pool.QueryRow(ctx, q, id)
	c, err := scanContest(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("contest/postgres: get %s: %w", id, err)
	}
	return c, nil
}

// GetActive returns the single contest whose status is "active".
// Returns models.ErrNoActiveContest if none is found.
func (s *PostgresContestStore) GetActive() (*models.Contest, error) {
	const q = `
SELECT id, name, status,
       low_profile, medium_profile, high_profile,
       low_weight, medium_weight, high_weight,
       submissions_closed_at, contest_closed_at, ends_at,
       created_at, updated_at
FROM contests
WHERE status = 'active'
LIMIT 1`

	ctx, cancel := operationCtx()
	defer cancel()

	row := s.pool.QueryRow(ctx, q)
	c, err := scanContest(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, models.ErrNoActiveContest
		}
		return nil, fmt.Errorf("contest/postgres: get active: %w", err)
	}
	return c, nil
}

// List returns all contests in ascending creation order.
func (s *PostgresContestStore) List() ([]*models.Contest, error) {
	const q = `
SELECT id, name, status,
       low_profile, medium_profile, high_profile,
       low_weight, medium_weight, high_weight,
       submissions_closed_at, contest_closed_at, ends_at,
       created_at, updated_at
FROM contests
ORDER BY created_at ASC`

	ctx, cancel := operationCtx()
	defer cancel()

	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("contest/postgres: list: %w", err)
	}
	defer rows.Close()

	var out []*models.Contest
	for rows.Next() {
		c, err := scanContest(rows)
		if err != nil {
			return nil, fmt.Errorf("contest/postgres: list scan: %w", err)
		}
		out = append(out, c)
	}
	if rows.Err() != nil {
		return nil, fmt.Errorf("contest/postgres: list rows: %w", rows.Err())
	}
	if out == nil {
		out = []*models.Contest{} // never return nil slice
	}
	return out, nil
}

// Update overwrites all mutable fields of an existing contest.
// Returns ErrNotFound if no contest with c.ID exists.
func (s *PostgresContestStore) Update(c *models.Contest) error {
	low, err := marshalProfile(c.LowProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal low_profile: %w", err)
	}
	med, err := marshalProfile(c.MediumProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal medium_profile: %w", err)
	}
	high, err := marshalProfile(c.HighProfile)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal high_profile: %w", err)
	}

	const q = `
UPDATE contests SET
    name                  = $2,
    status                = $3,
    low_profile           = $4,
    medium_profile        = $5,
    high_profile          = $6,
    low_weight            = $7,
    medium_weight         = $8,
    high_weight           = $9,
    submissions_closed_at = $10,
    contest_closed_at     = $11,
    ends_at               = $12,
    updated_at            = $13
WHERE id = $1`

	ctx, cancel := operationCtx()
	defer cancel()

	tag, execErr := s.pool.Exec(ctx, q,
		c.ID, c.Name, string(c.Status),
		low, med, high,
		c.LowWeight, c.MediumWeight, c.HighWeight,
		nilableTime(c.SubmissionsClosedAt),
		nilableTime(c.ContestClosedAt),
		nilableTime(c.EndsAt),
		c.UpdatedAt,
	)
	if execErr != nil {
		return fmt.Errorf("contest/postgres: update %s: %w", c.ID, execErr)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SnapshotLeaderboard archives the final ranked leaderboard for a closed contest.
// Uses INSERT … ON CONFLICT DO UPDATE so the operation is idempotent.
func (s *PostgresContestStore) SnapshotLeaderboard(contestID string, entries []*models.LeaderboardEntry) error {
	data, err := json.Marshal(entries)
	if err != nil {
		return fmt.Errorf("contest/postgres: marshal snapshot: %w", err)
	}

	const q = `
INSERT INTO contest_leaderboard_snapshots (contest_id, entries, snapshotted_at)
VALUES ($1, $2, now())
ON CONFLICT (contest_id) DO UPDATE
    SET entries        = EXCLUDED.entries,
        snapshotted_at = now()`

	ctx, cancel := operationCtx()
	defer cancel()

	if _, err := s.pool.Exec(ctx, q, contestID, data); err != nil {
		return fmt.Errorf("contest/postgres: snapshot %s: %w", contestID, err)
	}
	return nil
}

// GetLeaderboardSnapshot returns the archived snapshot for a closed contest.
// Returns ErrNotFound if no snapshot has been written yet.
func (s *PostgresContestStore) GetLeaderboardSnapshot(contestID string) ([]*models.LeaderboardEntry, error) {
	const q = `
SELECT entries
FROM contest_leaderboard_snapshots
WHERE contest_id = $1`

	ctx, cancel := operationCtx()
	defer cancel()

	var raw []byte
	if err := s.pool.QueryRow(ctx, q, contestID).Scan(&raw); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("contest/postgres: get snapshot %s: %w", contestID, err)
	}

	var entries []*models.LeaderboardEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("contest/postgres: unmarshal snapshot: %w", err)
	}
	return entries, nil
}

// ── pgx row scanner ───────────────────────────────────────────────────────────

// scanner is satisfied by both pgx.Row and pgx.Rows so scanContest works
// for both QueryRow and Query loops.
type scanner interface {
	Scan(dest ...any) error
}

// scanContest reads one row from the contests table into a *models.Contest.
// The JSONB profile columns are decoded from raw bytes via json.Unmarshal.
func scanContest(row scanner) (*models.Contest, error) {
	var (
		c               models.Contest
		status          string
		lowRaw          []byte
		medRaw          []byte
		highRaw         []byte
		subClosedAt     *time.Time
		contestClosedAt *time.Time
		endsAt          *time.Time
	)

	if err := row.Scan(
		&c.ID, &c.Name, &status,
		&lowRaw, &medRaw, &highRaw,
		&c.LowWeight, &c.MediumWeight, &c.HighWeight,
		&subClosedAt, &contestClosedAt, &endsAt,
		&c.CreatedAt, &c.UpdatedAt,
	); err != nil {
		return nil, err
	}

	c.Status = models.ContestStatus(status)
	c.SubmissionsClosedAt = subClosedAt
	c.ContestClosedAt = contestClosedAt
	c.EndsAt = endsAt

	if err := json.Unmarshal(lowRaw, &c.LowProfile); err != nil {
		return nil, fmt.Errorf("unmarshal low_profile: %w", err)
	}
	if err := json.Unmarshal(medRaw, &c.MediumProfile); err != nil {
		return nil, fmt.Errorf("unmarshal medium_profile: %w", err)
	}
	if err := json.Unmarshal(highRaw, &c.HighProfile); err != nil {
		return nil, fmt.Errorf("unmarshal high_profile: %w", err)
	}

	return &c, nil
}

// ── helper functions ──────────────────────────────────────────────────────────

// operationCtx returns a context with a 10-second timeout for individual
// database operations. Contest store calls are low-frequency admin/lifecycle
// operations — 10 seconds is generous; a tight deadline surfaces misbehaving
// queries quickly.
func operationCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Second)
}

// marshalProfile converts a VolatilityProfile to JSON bytes for JSONB storage.
func marshalProfile(p models.VolatilityProfile) ([]byte, error) {
	return json.Marshal(p)
}

// nilableTime converts a *time.Time to an any that pgx stores as NULL when nil
// and as a TIMESTAMPTZ otherwise.
func nilableTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return *t
}

// isDuplicateKey returns true when err is a PostgreSQL unique-violation error
// (SQLSTATE 23505). We avoid importing pgconn to keep the dependency surface
// minimal — inspecting the error string is equivalent and pgx wraps it consistently.
//
// Alternative: use pgconn.PgError and check Code == "23505". Both approaches
// are equally reliable; string detection is marginally simpler here.
func isDuplicateKey(err error) bool {
	if err == nil {
		return false
	}
	// pgx wraps pgconn.PgError which implements error with a message that
	// includes the SQLSTATE code. Check both the type assertion and the string.
	type pgErr interface {
		SQLState() string
	}
	var pe pgErr
	if errors.As(err, &pe) {
		return pe.SQLState() == "23505"
	}
	return false
}
