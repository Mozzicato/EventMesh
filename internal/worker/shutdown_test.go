package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
	"github.com/Mozzicato/EventMesh/internal/worker"
)

// runManually starts the pool and returns a function that triggers shutdown
// and a channel that is closed when Run returns.
func (e *env) runManually() (shutdown func(), stopped <-chan struct{}) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := e.pool.Run(ctx); err != nil {
			e.t.Errorf("Run: %v", err)
		}
	}()
	e.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			e.t.Error("pool did not stop")
		}
	})
	return cancel, done
}

// submitPayload is submit with a caller-chosen payload.
func (e *env) submitPayload(jobType string, payload string) int64 {
	e.t.Helper()
	ctx := context.Background()
	job, _, err := e.store.CreateJob(ctx, store.NewJob{
		IdempotencyKey: "k-" + testutil.RandomID(), Queue: e.name, Type: jobType,
		Payload: json.RawMessage(payload), MaxAttempts: 3, ReceivedAt: time.Now(),
	})
	if err != nil {
		e.t.Fatal(err)
	}
	if err := e.queue.Enqueue(ctx, e.name, job.ID); err != nil {
		e.t.Fatal(err)
	}
	return job.ID
}

// newPoolSameQueue replaces the env's pool with a fresh one on the same queue,
// as if the process had restarted.
func (e *env) newPoolSameQueue(handlers map[string]worker.Handler) {
	e.pool = &worker.Pool{
		Store: e.store, Queue: e.queue, Handlers: handlers, Config: e.pool.Config,
		Now: time.Now, Log: e.pool.Log,
	}
}

func waitClosed(t *testing.T, ch <-chan struct{}, within time.Duration, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(within):
		t.Fatalf("%s did not happen within %v", what, within)
	}
}

// Graceful shutdown, the normal case: jobs already running are allowed to
// finish; jobs fetched but not started, and jobs still in Redis, are not
// started and remain available. Nothing is lost and nothing runs twice.
func TestShutdown_InFlightJobsFinishAndUnstartedJobsGoBackToRedis(t *testing.T) {
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var running, calls atomic.Int32
	blocked := func(context.Context, json.RawMessage) error {
		calls.Add(1)
		running.Add(1)
		<-release
		return nil
	}
	e := newEnv(t, map[string]worker.Handler{"block": blocked},
		worker.Config{Workers: 2, Buffer: 3, ShutdownGrace: 30 * time.Second})
	shutdown, stopped := e.runManually()

	const n = 10
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = e.submit("block", 3)
	}
	waitUntil(t, func() bool { return running.Load() == 2 })

	shutdown()
	time.Sleep(300 * time.Millisecond)
	select {
	case <-stopped:
		t.Fatal("Run returned while jobs were still in flight")
	default:
	}
	unblock() // the two in-flight jobs may now finish
	waitClosed(t, stopped, 10*time.Second, "pool stop")

	succeeded, pending := 0, 0
	for _, id := range ids {
		switch j := e.job(id); j.Status {
		case store.StatusSucceeded:
			succeeded++
		case store.StatusPending:
			pending++
			if j.Attempts != 0 {
				t.Fatalf("job %d never started but has attempts=%d", id, j.Attempts)
			}
		default:
			t.Fatalf("job %d in status %s after shutdown", id, j.Status)
		}
	}
	if succeeded != 2 || pending != n-2 || calls.Load() != 2 {
		t.Fatalf("succeeded=%d pending=%d handler calls=%d; want 2, %d, 2",
			succeeded, pending, calls.Load(), n-2)
	}
	if l, _ := e.queue.Len(context.Background(), e.name); l != n-2 {
		t.Fatalf("redis holds %d ids, want %d (every unstarted job handed back)", l, n-2)
	}

	// A fresh pool finishes the rest; each job's handler ran exactly once overall.
	e.newPoolSameQueue(map[string]worker.Handler{"block": func(context.Context, json.RawMessage) error {
		calls.Add(1)
		return nil
	}})
	e.start()
	e.waitStatus(store.StatusSucceeded, ids...)
	if calls.Load() != n {
		t.Fatalf("handler ran %d times for %d jobs", calls.Load(), n)
	}
}

// A job that does not finish within ShutdownGrace is interrupted (its context
// is cancelled) and released: pending again, attempt not counted, back in Redis.
func TestShutdown_GraceExpiryReleasesStuckJob(t *testing.T) {
	var started atomic.Int32
	stuck := func(ctx context.Context, _ json.RawMessage) error {
		started.Add(1)
		<-ctx.Done() // only returns when cancelled
		return ctx.Err()
	}
	e := newEnv(t, map[string]worker.Handler{"stuck": stuck},
		worker.Config{Workers: 1, ShutdownGrace: 100 * time.Millisecond})
	shutdown, stopped := e.runManually()
	id := e.submit("stuck", 3)
	waitUntil(t, func() bool { return started.Load() == 1 })

	start := time.Now()
	shutdown()
	waitClosed(t, stopped, 5*time.Second, "pool stop after grace expiry")
	if took := time.Since(start); took < 100*time.Millisecond {
		t.Fatalf("stopped after %v: grace period not honoured", took)
	}

	j := e.job(id)
	if j.Status != store.StatusPending || j.Attempts != 0 || j.LeaseUntil != nil {
		t.Fatalf("released job: status=%s attempts=%d lease=%v; want pending, 0, nil",
			j.Status, j.Attempts, j.LeaseUntil)
	}
	if l, _ := e.queue.Len(context.Background(), e.name); l != 1 {
		t.Fatalf("redis holds %d ids, want 1", l)
	}

	c := &counter{}
	e.newPoolSameQueue(map[string]worker.Handler{"stuck": c.handler})
	e.start()
	e.waitStatus(store.StatusSucceeded, id)
	if j := e.job(id); j.Attempts != 1 {
		t.Fatalf("attempts %d, want 1: the interrupted attempt must not count", j.Attempts)
	}
}

// Once shutdown starts, no new job may begin, and an idle pool stops promptly.
func TestShutdown_IdlePoolStopsPromptlyAndStartsNothingNew(t *testing.T) {
	c := &counter{}
	e := newEnv(t, map[string]worker.Handler{"count": c.handler}, worker.Config{Workers: 4})
	shutdown, stopped := e.runManually()
	time.Sleep(100 * time.Millisecond) // let the fetcher block in BRPOP

	shutdown()
	waitClosed(t, stopped, 3*time.Second, "idle pool stop")

	id := e.submit("count", 3) // arrives after shutdown
	time.Sleep(200 * time.Millisecond)
	if c.count() != 0 || e.job(id).Status != store.StatusPending {
		t.Fatalf("a job ran after shutdown: calls=%d status=%s", c.count(), e.job(id).Status)
	}
}

// Shut the pool down three times while 300 jobs are being processed, then let
// a final pool finish. Every job must end succeeded and, because the grace
// period is long enough for in-flight jobs, every handler must have run
// exactly once: no job lost, none run twice.
func TestShutdown_RepeatedShutdownsUnderLoadLoseNothingAndRunNothingTwice(t *testing.T) {
	const n = 300
	var mu sync.Mutex
	runs := map[string]int{}
	handler := func(_ context.Context, payload json.RawMessage) error {
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		runs[string(payload)]++
		mu.Unlock()
		return nil
	}
	total := func() int {
		mu.Lock()
		defer mu.Unlock()
		s := 0
		for _, v := range runs {
			s += v
		}
		return s
	}
	handlers := map[string]worker.Handler{"work": handler}
	e := newEnv(t, handlers, worker.Config{Workers: 8, ShutdownGrace: 30 * time.Second})
	ids := make([]int64, n)
	for i := range ids {
		ids[i] = e.submitPayload("work", fmt.Sprintf(`{"i":%d}`, i))
	}

	for round, target := range []int{50, 120, 200} {
		shutdown, stopped := e.runManually()
		waitUntil(t, func() bool { return total() >= target })
		shutdown()
		waitClosed(t, stopped, 10*time.Second, fmt.Sprintf("stop in round %d", round))
		e.newPoolSameQueue(handlers)
	}
	e.start() // the final pool runs to completion
	e.waitStatus(store.StatusSucceeded, ids...)

	mu.Lock()
	defer mu.Unlock()
	if len(runs) != n {
		t.Fatalf("%d distinct jobs ran, want %d", len(runs), n)
	}
	for payload, count := range runs {
		if count != 1 {
			t.Fatalf("job %s ran %d times, want exactly 1", payload, count)
		}
	}
	for _, id := range ids {
		if j := e.job(id); j.Attempts != 1 {
			t.Fatalf("job %d: attempts %d, want 1", id, j.Attempts)
		}
	}
}

// A job that respects its context but needs a little longer than the moment of
// shutdown must be allowed to finish, as long as it fits in ShutdownGrace.
func TestShutdown_JobFinishingWithinGraceIsNotInterrupted(t *testing.T) {
	var started atomic.Int32
	slow := func(ctx context.Context, _ json.RawMessage) error {
		started.Add(1)
		select {
		// Longer than one BRPOP (1 s): the grace timer starts only after the
		// fetcher notices shutdown, so a shorter job would pass even with no grace.
		case <-time.After(2500 * time.Millisecond): // finishes on its own
			return nil
		case <-ctx.Done(): // ...unless interrupted
			return ctx.Err()
		}
	}
	e := newEnv(t, map[string]worker.Handler{"slow": slow},
		worker.Config{Workers: 1, ShutdownGrace: 5 * time.Second})
	shutdown, stopped := e.runManually()
	id := e.submit("slow", 3)
	waitUntil(t, func() bool { return started.Load() == 1 })
	shutdown()
	waitClosed(t, stopped, 10*time.Second, "pool stop")
	if j := e.job(id); j.Status != store.StatusSucceeded || j.Attempts != 1 {
		t.Fatalf("status=%s attempts=%d; want succeeded, 1 (job fit in the grace period)",
			j.Status, j.Attempts)
	}
}
