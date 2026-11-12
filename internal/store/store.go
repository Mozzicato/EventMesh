// Package store is the PostgreSQL persistence layer. Postgres is the source of
// truth for every job: its status, attempts, lease and errors. Redis only
// carries job ids to workers; if Redis loses an id, the sweeper finds the job
// here and enqueues it again.
//
// Every time comparison uses a time passed in from Go (the injected clock),
// never the database's now(), so tests can control time precisely.
package store

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job statuses. See schema.sql for the CHECK constraint.
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusRetrying  = "retrying"
	StatusSucceeded = "succeeded"
	StatusDead      = "dead"
)

var (
	// ErrNotFound is returned when no job has the requested id.
	ErrNotFound = errors.New("job not found")
	// ErrKeyConflict means the idempotency key was already used for a
	// different request (different queue, type or payload).
	ErrKeyConflict = errors.New("idempotency key reused with a different request")
)

//go:embed schema.sql
var schemaSQL string

// Job is one row of the jobs table.
type Job struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	LastError      *string         `json:"last_error,omitempty"`
	ReceivedAt     time.Time       `json:"received_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	RunAt          time.Time       `json:"run_at"`
	LeaseUntil     *time.Time      `json:"lease_until,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

// NewJob is what a client asks us to create.
type NewJob struct {
	IdempotencyKey string
	Queue          string
	Type           string
	Payload        json.RawMessage
	MaxAttempts    int
	ReceivedAt     time.Time
}

const jobColumns = `id, idempotency_key, queue, type, payload, status, attempts, max_attempts,
	last_error, received_at, updated_at, run_at, lease_until, finished_at`

// scanJob reads jobColumns, plus any extra destinations appended at the end.
func scanJob(row pgx.Row, extra ...any) (Job, error) {
	var j Job
	dest := []any{&j.ID, &j.IdempotencyKey, &j.Queue, &j.Type, &j.Payload, &j.Status,
		&j.Attempts, &j.MaxAttempts, &j.LastError, &j.ReceivedAt, &j.UpdatedAt, &j.RunAt,
		&j.LeaseUntil, &j.FinishedAt}
	err := row.Scan(append(dest, extra...)...)
	return j, err
}

// Store wraps a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// Open connects to Postgres and applies the schema.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := applySchema(ctx, pool); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

// schemaLockID is an arbitrary constant naming the advisory lock below.
const schemaLockID = 727274

// applySchema runs schema.sql while holding a transaction-scoped advisory lock.
// "CREATE TABLE IF NOT EXISTS" is not safe when two processes run it at the
// same moment (both see no table, both create, one fails), which happens when
// several servers or test packages start together. The lock serialises them.
func applySchema(ctx context.Context, pool *pgxpool.Pool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) // no-op after Commit
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", schemaLockID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, schemaSQL); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Close releases all connections.
func (s *Store) Close() { s.pool.Close() }

// Ping checks the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// CreateJob inserts a job, or returns the existing job if the idempotency key
// was already used. created reports whether a new row was inserted.
//
// The race between two concurrent requests with the same key is handled by the
// UNIQUE constraint, not by a "check then insert" in Go: ON CONFLICT DO NOTHING
// makes the loser's INSERT wait for the winner to commit and then return no
// row, after which the SELECT sees the winner's committed row.
func (s *Store) CreateJob(ctx context.Context, nj NewJob) (job Job, created bool, err error) {
	row := s.pool.QueryRow(ctx, `
		INSERT INTO jobs (idempotency_key, queue, type, payload, status, max_attempts,
		                  received_at, updated_at, run_at)
		VALUES ($1, $2, $3, $4, 'pending', $5, $6, $6, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		nj.IdempotencyKey, nj.Queue, nj.Type, nj.Payload, nj.MaxAttempts, nj.ReceivedAt)
	job, err = scanJob(row)
	if err == nil {
		return job, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	// The key already exists. Return the original job, but only if this is the
	// same request; reusing a key for a different request is a client bug.
	var same bool
	job, err = scanJob(s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`, (queue = $2 AND type = $3 AND payload = $4::jsonb)
		FROM jobs WHERE idempotency_key = $1`,
		nj.IdempotencyKey, nj.Queue, nj.Type, nj.Payload), &same)
	if err != nil {
		return Job{}, false, fmt.Errorf("load existing job: %w", err)
	}
	if !same {
		return job, false, ErrKeyConflict
	}
	return job, false, nil
}

// GetJob returns one job by id.
func (s *Store) GetJob(ctx context.Context, id int64) (Job, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

// ListDead returns dead-lettered jobs, newest first.
func (s *Store) ListDead(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+jobColumns+` FROM jobs WHERE status = 'dead' ORDER BY id DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := []Job{}
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}

// Claim moves a job to running and starts its lease, if the job is runnable.
// ok=false means the job is not runnable right now (already running under a
// live lease, finished, out of attempts, or backing off) and the caller must
// skip it. Because this is one conditional UPDATE, a duplicate delivery of the
// same id can never be claimed twice: the row lock makes the second UPDATE see
// the first one's result.
//
// A running job whose lease has expired is claimable again: its worker is
// presumed dead (the visibility timeout). The crashed attempt still counts.
func (s *Store) Claim(ctx context.Context, id int64, now, leaseUntil time.Time) (Job, bool, error) {
	job, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'running', attempts = attempts + 1, lease_until = $3, updated_at = $2
		WHERE id = $1
		  AND attempts < max_attempts
		  AND (status = 'pending'
		       OR (status = 'retrying' AND run_at <= $2)
		       OR (status = 'running' AND lease_until < $2))
		RETURNING `+jobColumns, id, now, leaseUntil))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// The finishing updates below only apply if the job is still running under the
// same attempt the caller claimed. If the lease expired and another worker
// re-claimed the job, attempts has moved on and the stale worker's write
// affects no rows. The attempt number acts as a fencing token.

// Succeed marks a claimed job as succeeded. ok=false means the caller's claim
// was stale and nothing was written.
func (s *Store) Succeed(ctx context.Context, job Job, now time.Time) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', finished_at = $3, updated_at = $3, lease_until = NULL
		WHERE id = $1 AND status = 'running' AND attempts = $2`,
		job.ID, job.Attempts, now)
	return tag.RowsAffected() == 1, err
}

// MarkDead moves a claimed job to the dead-letter state.
func (s *Store) MarkDead(ctx context.Context, job Job, now time.Time, reason string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead', last_error = $3, finished_at = $4, updated_at = $4, lease_until = NULL
		WHERE id = $1 AND status = 'running' AND attempts = $2`,
		job.ID, job.Attempts, reason, now)
	return tag.RowsAffected() == 1, err
}

// KillExhaustedLeases dead-letters running jobs whose lease expired on their
// final attempt (their worker died and no attempts are left). Without this,
// Claim would refuse them forever and they would be stuck in running.
func (s *Store) KillExhaustedLeases(ctx context.Context, queue string, now time.Time) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'dead', last_error = 'lease expired on final attempt',
		    finished_at = $2, updated_at = $2, lease_until = NULL
		WHERE queue = $1 AND status = 'running' AND lease_until < $2
		  AND attempts >= max_attempts`, queue, now)
	return tag.RowsAffected(), err
}

// StrandedJobIDs returns unfinished jobs that a worker should have picked up
// by now but may not be in Redis: expired leases (worker died), and pending or
// retrying jobs that have waited longer than staleAfter (Redis lost the id, or
// the server crashed between the INSERT and the Redis push). Re-enqueueing a
// job that *is* still in Redis is harmless: Claim lets only one delivery win.
func (s *Store) StrandedJobIDs(ctx context.Context, queue string, now time.Time,
	staleAfter time.Duration, limit int) ([]int64, error) {
	staleBefore := now.Add(-staleAfter)
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM jobs
		WHERE queue = $1
		  AND status IN ('pending', 'running', 'retrying')
		  AND ((status = 'running' AND lease_until < $2)
		       OR (status = 'pending' AND updated_at < $3)
		       OR (status = 'retrying' AND run_at < $3))
		ORDER BY id
		LIMIT $4`, queue, now, staleBefore, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[int64])
}
