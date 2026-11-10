// Package store is the PostgreSQL persistence layer. Postgres is the source of
// truth for every job: its status, attempts, lease and errors.
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
	// different request (different type or payload).
	ErrKeyConflict = errors.New("idempotency key reused with a different request")
)

//go:embed schema.sql
var schemaSQL string

// Job is one row of the jobs table.
type Job struct {
	ID             int64           `json:"id"`
	IdempotencyKey string          `json:"idempotency_key"`
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
	Type           string
	Payload        json.RawMessage
	MaxAttempts    int
	ReceivedAt     time.Time
}

const jobColumns = `id, idempotency_key, type, payload, status, attempts, max_attempts,
	last_error, received_at, updated_at, run_at, lease_until, finished_at`

func scanJob(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.IdempotencyKey, &j.Type, &j.Payload, &j.Status, &j.Attempts,
		&j.MaxAttempts, &j.LastError, &j.ReceivedAt, &j.UpdatedAt, &j.RunAt, &j.LeaseUntil,
		&j.FinishedAt)
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
	if _, err := pool.Exec(ctx, schemaSQL); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{pool: pool}, nil
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
		INSERT INTO jobs (idempotency_key, type, payload, status, max_attempts,
		                  received_at, updated_at, run_at)
		VALUES ($1, $2, $3, 'pending', $4, $5, $5, $5)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+jobColumns,
		nj.IdempotencyKey, nj.Type, nj.Payload, nj.MaxAttempts, nj.ReceivedAt)
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
	row = s.pool.QueryRow(ctx, `
		SELECT `+jobColumns+`, (type = $2 AND payload = $3::jsonb)
		FROM jobs WHERE idempotency_key = $1`,
		nj.IdempotencyKey, nj.Type, nj.Payload)
	err = row.Scan(&job.ID, &job.IdempotencyKey, &job.Type, &job.Payload, &job.Status,
		&job.Attempts, &job.MaxAttempts, &job.LastError, &job.ReceivedAt, &job.UpdatedAt,
		&job.RunAt, &job.LeaseUntil, &job.FinishedAt, &same)
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
