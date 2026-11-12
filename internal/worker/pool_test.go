package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/queue"
	"github.com/Mozzicato/EventMesh/internal/retry"
	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
	"github.com/Mozzicato/EventMesh/internal/worker"
)

// env is one test's pool plus its dependencies, on a queue no other test uses.
type env struct {
	t     *testing.T
	store *store.Store
	queue *queue.Queue
	name  string
	pool  *worker.Pool
}

func newEnv(t *testing.T, handlers map[string]worker.Handler, cfg worker.Config) *env {
	t.Helper()
	e := &env{
		t:     t,
		store: testutil.Store(t),
		queue: queue.New(testutil.Redis(t)),
		name:  "q-" + testutil.RandomID(),
	}
	cfg.Queue = e.name
	if cfg.DequeueTimeout == 0 {
		cfg.DequeueTimeout = 50 * time.Millisecond
	}
	if cfg.SweepInterval == 0 {
		cfg.SweepInterval = 50 * time.Millisecond
	}
	e.pool = &worker.Pool{
		Store:    e.store,
		Queue:    e.queue,
		Handlers: handlers,
		Config:   cfg,
		Now:      time.Now,
		Log:      testLogger(),
	}
	return e
}

// start runs the pool until the test ends.
func (e *env) start() {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.pool.Run(ctx) }()
	e.t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				e.t.Errorf("Run returned %v", err)
			}
		case <-time.After(15 * time.Second):
			e.t.Error("pool did not stop within 15s")
		}
	})
}

// submit stores a job and pushes it to Redis, like the API does.
func (e *env) submit(jobType string, maxAttempts int) int64 {
	e.t.Helper()
	ctx := context.Background()
	job, _, err := e.store.CreateJob(ctx, store.NewJob{
		IdempotencyKey: "k-" + testutil.RandomID(),
		Queue:          e.name,
		Type:           jobType,
		Payload:        json.RawMessage(`{}`),
		MaxAttempts:    maxAttempts,
		ReceivedAt:     time.Now(),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.queue.Enqueue(ctx, e.name, job.ID); err != nil {
		e.t.Fatal(err)
	}
	return job.ID
}

func (e *env) job(id int64) store.Job {
	e.t.Helper()
	j, err := e.store.GetJob(context.Background(), id)
	if err != nil {
		e.t.Fatal(err)
	}
	return j
}

// waitStatus waits until every listed job reaches status.
func (e *env) waitStatus(status string, ids ...int64) {
	e.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for _, id := range ids {
		for e.job(id).Status != status {
			if time.Now().After(deadline) {
				e.t.Fatalf("job %d: status %s, want %s", id, e.job(id).Status, status)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// counter counts how many times its handler has run.
type counter struct {
	mu   sync.Mutex
	runs int
}

func (c *counter) handler(context.Context, json.RawMessage) error {
	c.mu.Lock()
	c.runs++
	c.mu.Unlock()
	return nil
}

func (c *counter) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

func TestPool_RunsEveryJobExactlyOnce(t *testing.T) {
	c := &counter{}
	e := newEnv(t, map[string]worker.Handler{"count": c.handler}, worker.Config{Workers: 8})
	var succeeded sync.Map
	var callbacks atomic.Int32
	e.pool.OnSucceeded = func(job store.Job, _ time.Time) {
		callbacks.Add(1)
		if _, dup := succeeded.LoadOrStore(job.ID, true); dup {
			t.Errorf("job %d reported succeeded twice", job.ID)
		}
	}
	e.start()

	const n = 200
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = e.submit("count", 3)
	}
	e.waitStatus(store.StatusSucceeded, ids...)
	if c.count() != n {
		t.Fatalf("handler ran %d times for %d jobs", c.count(), n)
	}
	if callbacks.Load() != n {
		t.Fatalf("OnSucceeded called %d times, want %d", callbacks.Load(), n)
	}
	for _, id := range ids {
		if j := e.job(id); j.Attempts != 1 || j.FinishedAt == nil {
			t.Fatalf("job %d: attempts %d finished_at %v", id, j.Attempts, j.FinishedAt)
		}
	}
}

// The pool is bounded: with far more jobs than workers, exactly Workers
// handlers run at once and no more start until one finishes. Handlers block
// on a channel instead of sleeping, so the test does not depend on timing.
func TestPool_ConcurrencyIsBoundedByWorkers(t *testing.T) {
	const workers = 4
	var running atomic.Int32
	release := make(chan struct{})
	blocked := func(context.Context, json.RawMessage) error {
		running.Add(1)
		<-release
		running.Add(-1)
		return nil
	}
	e := newEnv(t, map[string]worker.Handler{"block": blocked}, worker.Config{Workers: workers})
	e.start()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock) // if the test fails early, let the pool stop
	ids := make([]int64, 40)
	for i := range ids {
		ids[i] = e.submit("block", 1)
	}
	waitUntil(t, func() bool { return running.Load() == workers })
	time.Sleep(200 * time.Millisecond) // give a 5th handler every chance to start
	if n := running.Load(); n != workers {
		t.Fatalf("%d handlers running at once, want %d", n, workers)
	}
	unblock()
	e.waitStatus(store.StatusSucceeded, ids...)
}

// Backpressure: with every worker blocked and the channel full, the fetcher
// must stop taking ids from Redis instead of buffering without limit.
func TestPool_BackpressureLeavesExcessWorkInRedis(t *testing.T) {
	release := make(chan struct{})
	var started atomic.Int32
	blocked := func(ctx context.Context, _ json.RawMessage) error {
		started.Add(1)
		<-release
		return nil
	}
	e := newEnv(t, map[string]worker.Handler{"block": blocked},
		worker.Config{Workers: 2, Buffer: 3})
	e.start()
	t.Cleanup(func() { close(release) }) // runs before the pool is stopped

	const n = 20
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = e.submit("block", 1)
	}
	waitUntil(t, func() bool { return started.Load() == 2 })
	// Expected in Redis: n - 2 running - 3 buffered - 1 held by the fetcher,
	// which has dequeued one more id and is blocked trying to send it.
	const want = n - 2 - 3 - 1
	waitUntil(t, func() bool { l, _ := e.queue.Len(context.Background(), e.name); return l == want })
	time.Sleep(200 * time.Millisecond) // and it stays there
	if l, _ := e.queue.Len(context.Background(), e.name); l != want {
		t.Fatalf("redis length %d, want %d: fetcher kept pulling while workers were busy", l, want)
	}
	if started.Load() != 2 {
		t.Fatalf("%d handlers started, want 2", started.Load())
	}
}

// At-least-once delivery means the same id can arrive more than once.
// The conditional claim in Postgres makes the handler run only once.
func TestPool_DuplicateDeliveryRunsHandlerOnce(t *testing.T) {
	c := &counter{}
	e := newEnv(t, map[string]worker.Handler{"count": c.handler}, worker.Config{Workers: 4})
	id := e.submit("count", 3)
	for i := 0; i < 4; i++ { // 5 deliveries of the same id in total
		_ = e.queue.Enqueue(context.Background(), e.name, id)
	}
	e.start()
	e.waitStatus(store.StatusSucceeded, id)
	waitUntil(t, func() bool { l, _ := e.queue.Len(context.Background(), e.name); return l == 0 })
	time.Sleep(100 * time.Millisecond) // let any duplicate finish its (refused) claim
	if c.count() != 1 {
		t.Fatalf("handler ran %d times, want 1", c.count())
	}
}

func TestPool_HandlerErrorOnLastAttemptDeadLetters(t *testing.T) {
	failing := func(context.Context, json.RawMessage) error { return errors.New("upstream said no") }
	e := newEnv(t, map[string]worker.Handler{"fail": failing}, worker.Config{Workers: 2})
	e.start()
	id := e.submit("fail", 1)
	e.waitStatus(store.StatusDead, id)
	if j := e.job(id); j.LastError == nil || *j.LastError != "upstream said no" {
		t.Fatalf("last_error = %v", j.LastError)
	}
}

func TestPool_PanicIsRecoveredAndPoolKeepsWorking(t *testing.T) {
	c := &counter{}
	handlers := map[string]worker.Handler{
		"panic": func(context.Context, json.RawMessage) error { panic("bad job") },
		"count": c.handler,
	}
	e := newEnv(t, handlers, worker.Config{Workers: 1}) // one worker: it must survive
	e.start()
	bad := e.submit("panic", 1)
	good := e.submit("count", 1)
	e.waitStatus(store.StatusDead, bad)
	e.waitStatus(store.StatusSucceeded, good)
	if j := e.job(bad); j.LastError == nil || !strings.Contains(*j.LastError, "panicked: bad job") {
		t.Fatalf("last_error = %v", j.LastError)
	}
}

func TestPool_JobTimeoutCancelsHandler(t *testing.T) {
	waitForCancel := func(ctx context.Context, _ json.RawMessage) error {
		<-ctx.Done()
		return ctx.Err()
	}
	e := newEnv(t, map[string]worker.Handler{"hang": waitForCancel},
		worker.Config{Workers: 1, JobTimeout: 50 * time.Millisecond})
	e.start()
	id := e.submit("hang", 1)
	e.waitStatus(store.StatusDead, id)
	if j := e.job(id); j.LastError == nil || !strings.Contains(*j.LastError, "deadline exceeded") {
		t.Fatalf("last_error = %v", j.LastError)
	}
}

func TestPool_UnknownJobTypeDeadLetters(t *testing.T) {
	e := newEnv(t, map[string]worker.Handler{}, worker.Config{Workers: 1})
	e.start()
	id := e.submit("nobody-handles-this", 3)
	waitUntil(t, func() bool { return e.job(id).LastError != nil })
	if j := e.job(id); !strings.Contains(*j.LastError, "no handler") {
		t.Fatalf("last_error = %v", *j.LastError)
	}
}

// A worker claimed the job and then died (simulated: claimed with a lease
// that has already expired, and the id is gone from Redis). The sweeper must
// notice the expired lease and get the job run again.
func TestPool_RecoversJobFromCrashedWorker(t *testing.T) {
	c := &counter{}
	e := newEnv(t, map[string]worker.Handler{"count": c.handler}, worker.Config{Workers: 2})
	id := e.submit("count", 3)
	ctx := context.Background()
	if _, _, err := e.queue.Dequeue(ctx, e.name, time.Second); err != nil { // the dead worker took it
		t.Fatal(err)
	}
	past := time.Now().Add(-time.Minute)
	if _, ok, err := e.store.Claim(ctx, id, past, past.Add(time.Second)); err != nil || !ok {
		t.Fatalf("simulated claim: ok=%v err=%v", ok, err)
	}

	e.start()
	e.waitStatus(store.StatusSucceeded, id)
	if j := e.job(id); j.Attempts != 2 {
		t.Fatalf("attempts %d, want 2 (crashed attempt + successful one)", j.Attempts)
	}
	if c.count() != 1 {
		t.Fatalf("handler ran %d times in this pool, want 1", c.count())
	}
}

// The job is in Postgres but its id never reached Redis (e.g. Redis was down
// when the API tried to enqueue). The sweeper must find and enqueue it.
func TestPool_RecoversJobMissingFromRedis(t *testing.T) {
	c := &counter{}
	e := newEnv(t, map[string]worker.Handler{"count": c.handler},
		worker.Config{Workers: 2, StaleAfter: time.Minute})
	job, _, err := e.store.CreateJob(context.Background(), store.NewJob{
		IdempotencyKey: "k-" + testutil.RandomID(),
		Queue:          e.name,
		Type:           "count",
		Payload:        json.RawMessage(`{}`),
		MaxAttempts:    3,
		ReceivedAt:     time.Now().Add(-2 * time.Minute), // waiting longer than StaleAfter
	})
	if err != nil {
		t.Fatal(err)
	}
	e.start() // note: never enqueued
	e.waitStatus(store.StatusSucceeded, job.ID)
}

func TestPool_RejectsLeaseNotLongerThanJobTimeout(t *testing.T) {
	e := newEnv(t, nil, worker.Config{Lease: time.Second, JobTimeout: time.Second})
	if err := e.pool.Run(context.Background()); err == nil {
		t.Fatal("Run should refuse a lease that is not longer than the job timeout")
	}
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached within 10s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// End to end with the real clock and background goroutines: a job that fails
// twice is retried through the delayed set and then succeeds.
func TestPool_RetriesWithBackoffThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	flaky := func(context.Context, json.RawMessage) error {
		if calls.Add(1) <= 2 {
			return errors.New("not yet")
		}
		return nil
	}
	e := newEnv(t, map[string]worker.Handler{"flaky": flaky}, worker.Config{
		Workers:      2,
		Retry:        retry.Policy{Base: 20 * time.Millisecond, Cap: 100 * time.Millisecond},
		PromoteEvery: 10 * time.Millisecond,
	})
	e.start()
	id := e.submit("flaky", 5)
	e.waitStatus(store.StatusSucceeded, id)
	if j := e.job(id); j.Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("attempts %d, handler calls %d; want 3 and 3", j.Attempts, calls.Load())
	}
}

func TestPool_PermanentErrorSkipsRemainingAttempts(t *testing.T) {
	var calls atomic.Int32
	bad := func(context.Context, json.RawMessage) error {
		calls.Add(1)
		return worker.Permanent(errors.New("payload missing field"))
	}
	e := newEnv(t, map[string]worker.Handler{"bad": bad}, worker.Config{Workers: 1})
	e.start()
	id := e.submit("bad", 5)
	e.waitStatus(store.StatusDead, id)
	if j := e.job(id); j.Attempts != 1 || calls.Load() != 1 {
		t.Fatalf("attempts %d, calls %d; want 1 and 1 (no retries)", j.Attempts, calls.Load())
	}
}

func TestPool_RejectsRetryCapBelowBase(t *testing.T) {
	e := newEnv(t, nil, worker.Config{Retry: retry.Policy{Base: time.Second, Cap: time.Millisecond}})
	if err := e.pool.Run(context.Background()); err == nil {
		t.Fatal("Run should refuse Retry.Cap < Retry.Base")
	}
}

// testLogger discards pool logs unless EVENTMESH_TEST_LOG is set, which is
// handy when debugging a failing test.
func testLogger() *slog.Logger {
	if os.Getenv("EVENTMESH_TEST_LOG") != "" {
		return slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
