// Package worker runs jobs with a bounded pool of goroutines.
//
// Shape of the pool:
//
//	Redis ──BRPOP──▶ fetcher ──▶ chan int64 (capacity Buffer) ──▶ N workers
//
// One fetcher goroutine pulls job ids from Redis and sends them into a bounded
// channel. A fixed number of worker goroutines receive from the channel and
// run jobs. When all workers are busy and the channel is full, the fetcher's
// send blocks, so it stops pulling from Redis. That is the backpressure: work
// waits in Redis (cheap, observable) instead of piling up in this process's
// memory, and at most Workers jobs ever run at once.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/Mozzicato/EventMesh/internal/queue"
	"github.com/Mozzicato/EventMesh/internal/retry"
	"github.com/Mozzicato/EventMesh/internal/store"
)

// Handler runs one job. It must return when ctx is done: Go cannot kill a
// goroutine, so a handler that ignores ctx cannot be timed out.
type Handler func(ctx context.Context, payload json.RawMessage) error

// Config tunes the pool. Zero values get the defaults in withDefaults.
type Config struct {
	Queue          string        // queue name to consume
	Workers        int           // number of worker goroutines (the concurrency bound)
	Buffer         int           // channel capacity between fetcher and workers
	Lease          time.Duration // visibility timeout; must be longer than JobTimeout
	JobTimeout     time.Duration // per-attempt deadline passed to the handler
	DequeueTimeout time.Duration // how long one BRPOP blocks; bounds shutdown delay
	SweepInterval  time.Duration // how often the sweeper scans Postgres
	StaleAfter     time.Duration // pending/retrying jobs older than this are re-enqueued
	Retry          retry.Policy  // backoff between attempts
	PromoteEvery   time.Duration // how often due retries are moved to the ready list
}

func (c Config) withDefaults() Config {
	if c.Queue == "" {
		c.Queue = "default"
	}
	if c.Workers <= 0 {
		c.Workers = 8
	}
	if c.Buffer <= 0 {
		c.Buffer = c.Workers
	}
	if c.Lease <= 0 {
		c.Lease = 30 * time.Second
	}
	if c.JobTimeout <= 0 {
		c.JobTimeout = 10 * time.Second
	}
	if c.DequeueTimeout <= 0 {
		c.DequeueTimeout = time.Second
	}
	if c.SweepInterval <= 0 {
		c.SweepInterval = 5 * time.Second
	}
	if c.StaleAfter <= 0 {
		c.StaleAfter = time.Minute
	}
	if c.Retry.Base <= 0 && c.Retry.Cap <= 0 {
		c.Retry = retry.Policy{Base: time.Second, Cap: 5 * time.Minute}
	}
	if c.PromoteEvery <= 0 {
		c.PromoteEvery = 100 * time.Millisecond
	}
	return c
}

// dbTimeout bounds each database call made by the pool.
const dbTimeout = 5 * time.Second

// Pool consumes one queue.
type Pool struct {
	Store    *store.Store
	Queue    *queue.Queue
	Handlers map[string]Handler
	Config   Config
	Now      func() time.Time
	Log      *slog.Logger
	// OnSucceeded, if set, is called after a job's success has been committed.
	// The load test uses it to record end-to-end latency.
	OnSucceeded func(job store.Job, committedAt time.Time)

	cfg Config // Config with defaults applied; set once at the start of Run
}

// setup applies defaults and validates the config.
func (p *Pool) setup() error {
	p.cfg = p.Config.withDefaults()
	if p.cfg.Lease <= p.cfg.JobTimeout {
		return errors.New("worker: Lease must be longer than JobTimeout, " +
			"or healthy jobs would be reclaimed while still running")
	}
	if p.cfg.Retry.Cap < p.cfg.Retry.Base {
		return errors.New("worker: Retry.Cap must be at least Retry.Base")
	}
	return nil
}

// Run processes jobs until ctx is cancelled, then shuts down: it stops taking
// work from Redis, waits for in-flight jobs to finish, and pushes ids that
// were fetched but never started back to the front of the Redis queue.
func (p *Pool) Run(ctx context.Context) error {
	if err := p.setup(); err != nil {
		return err
	}

	jobs := make(chan int64, p.cfg.Buffer)

	var workers sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p.work(ctx, jobs)
		}()
	}

	var background sync.WaitGroup
	background.Add(2)
	go func() {
		defer background.Done()
		p.sweepLoop(ctx)
	}()
	go func() {
		defer background.Done()
		p.promoteLoop(ctx)
	}()

	p.fetch(ctx, jobs) // returns once ctx is cancelled
	workers.Wait()
	background.Wait()

	// Only this goroutine sends on jobs, and the fetcher has returned, so it
	// is safe to close and drain whatever is still buffered.
	close(jobs)
	for id := range jobs {
		p.handBack(id)
	}
	return nil
}

// fetch moves ids from Redis into the jobs channel until ctx is cancelled.
func (p *Pool) fetch(ctx context.Context, jobs chan<- int64) {
	for ctx.Err() == nil {
		// BRPOP gets a background context on purpose: if a cancelled context
		// interrupted it after Redis had already popped an id, that id would
		// be lost. Instead each BRPOP finishes within DequeueTimeout and the
		// loop condition notices the cancellation.
		id, ok, err := p.Queue.Dequeue(context.Background(), p.cfg.Queue, p.cfg.DequeueTimeout)
		if err != nil {
			p.Log.Error("dequeue failed", "err", err)
			sleep(ctx, 100*time.Millisecond) // avoid a hot loop while Redis is down
			continue
		}
		if !ok {
			continue // queue was empty for DequeueTimeout
		}
		select {
		case jobs <- id: // blocks while all workers are busy and the buffer is full
		case <-ctx.Done():
			p.handBack(id)
		}
	}
}

// work runs jobs from the channel until ctx is cancelled.
func (p *Pool) work(ctx context.Context, jobs <-chan int64) {
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-jobs:
			// select picks randomly when both cases are ready, so re-check:
			// once shutdown has started, don't begin new jobs.
			if ctx.Err() != nil {
				p.handBack(id)
				return
			}
			p.process(id)
		}
	}
}

// process claims, runs and finishes one job. It deliberately does not take
// the pool's context: a job that has started is allowed to finish during
// shutdown.
func (p *Pool) process(id int64) {
	now := p.Now()
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	job, ok, err := p.Store.Claim(ctx, id, now, now.Add(p.cfg.Lease))
	cancel()
	if err != nil {
		// The job stays in Postgres; the sweeper will re-enqueue it.
		p.Log.Error("claim failed", "job_id", id, "err", err)
		return
	}
	if !ok {
		// Duplicate or early delivery: someone else holds it, or it is done.
		return
	}

	runErr := p.runHandler(job)

	ctx, cancel = context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	if runErr == nil {
		ok, err := p.Store.Succeed(ctx, job, p.Now())
		if err != nil {
			// Not recorded: the lease will expire and the job will run again.
			// That repeat run is the "at least once" in at-least-once.
			p.Log.Error("record success failed", "job_id", id, "err", err)
			return
		}
		if ok && p.OnSucceeded != nil {
			p.OnSucceeded(job, p.Now())
		}
		return
	}
	p.fail(ctx, job, runErr)
}

// PermanentError marks a failure that retrying cannot fix (a malformed
// payload, an unknown job type). The job goes straight to the dead-letter
// queue instead of burning its remaining attempts.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err so the job is not retried.
func Permanent(err error) error { return &PermanentError{Err: err} }

// fail records a failed attempt: either schedule a retry with backoff, or,
// when attempts are used up or the error is permanent, dead-letter the job.
func (p *Pool) fail(ctx context.Context, job store.Job, runErr error) {
	now := p.Now()
	var permanent *PermanentError
	if job.Attempts >= job.MaxAttempts || errors.As(runErr, &permanent) {
		if _, err := p.Store.MarkDead(ctx, job, now, runErr.Error()); err != nil {
			p.Log.Error("dead-letter failed", "job_id", job.ID, "err", err)
		}
		return
	}

	runAt := now.Add(p.cfg.Retry.Delay(job.Attempts))
	// Postgres first: if this write is refused (our claim is stale) we must
	// not schedule anything. If we crash after it but before Schedule, the
	// sweeper finds the retrying job once it is overdue.
	ok, err := p.Store.MarkRetrying(ctx, job, now, runAt, runErr.Error())
	if err != nil || !ok {
		if err != nil {
			p.Log.Error("record retry failed", "job_id", job.ID, "err", err)
		}
		return
	}
	// Fresh timeout: a slow Postgres write above must not leave this call
	// with an already-expired context.
	rctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	if err := p.Queue.Schedule(rctx, p.cfg.Queue, job.ID, runAt); err != nil {
		p.Log.Error("schedule retry failed; sweeper will recover it", "job_id", job.ID, "err", err)
	}
}

// runHandler runs the job's handler with a timeout and turns a panic into an
// ordinary error, so one bad job cannot crash the whole process.
func (p *Pool) runHandler(job store.Job) (err error) {
	h, ok := p.Handlers[job.Type]
	if !ok {
		return Permanent(fmt.Errorf("no handler for job type %q", job.Type))
	}
	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.JobTimeout)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h(ctx, job.Payload)
}

// handBack returns an id that was taken from Redis but never started.
func (p *Pool) handBack(id int64) {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	if err := p.Queue.PushFront(ctx, p.cfg.Queue, id); err != nil {
		// Still safe: the job is pending in Postgres and the sweeper finds it.
		p.Log.Error("hand back failed; sweeper will recover it", "job_id", id, "err", err)
	}
}

// promoteLoop moves retries whose backoff has elapsed onto the ready list.
func (p *Pool) promoteLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.PromoteEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := p.Queue.PromoteDue(ctx, p.cfg.Queue, p.Now(), 1000); err != nil &&
				ctx.Err() == nil {
				p.Log.Error("promote due retries", "err", err)
			}
		}
	}
}

func (p *Pool) sweepLoop(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.sweepOnce()
		}
	}
}

// sweepOnce repairs Redis from Postgres: it dead-letters jobs whose worker
// died on their last attempt, and re-enqueues stranded jobs.
func (p *Pool) sweepOnce() {
	ctx, cancel := context.WithTimeout(context.Background(), dbTimeout)
	defer cancel()
	now := p.Now()
	if n, err := p.Store.KillExhaustedLeases(ctx, p.cfg.Queue, now); err != nil {
		p.Log.Error("sweep: kill exhausted", "err", err)
	} else if n > 0 {
		p.Log.Warn("sweep: dead-lettered jobs whose final attempt was lost", "count", n)
	}
	ids, err := p.Store.StrandedJobIDs(ctx, p.cfg.Queue, now, p.cfg.StaleAfter, 1000)
	if err != nil {
		p.Log.Error("sweep: find stranded", "err", err)
		return
	}
	for _, id := range ids {
		if err := p.Queue.Enqueue(ctx, p.cfg.Queue, id); err != nil {
			p.Log.Error("sweep: re-enqueue", "job_id", id, "err", err)
			return
		}
	}
	if len(ids) > 0 {
		p.Log.Info("sweep: re-enqueued stranded jobs", "count", len(ids))
	}
}

// sleep waits for d or until ctx is done.
func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
