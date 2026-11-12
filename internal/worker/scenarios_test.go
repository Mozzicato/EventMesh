package worker

// The failure-scenario suite. Scenarios are generated, not hand-written:
//
//	failure kind  x  failures before success (0..6)  x  max_attempts (1..5)
//	   3          x            7                     x        5          = 105
//	+ permanent errors x max_attempts (1..5)                            =   5
//	                                                                     -----
//	                                                                      110
//
// Each scenario drives one job through the real pool step (claim, run, record
// outcome), real Postgres and real Redis, but with a fake clock, so no test
// sleeps through a backoff. After every failed attempt it checks that the
// retry was scheduled inside the backoff window, then jumps the clock to the
// retry time, promotes it from the delayed set and delivers it again.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/queue"
	"github.com/Mozzicato/EventMesh/internal/retry"
	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = t
}

// failureKind is one way a handler attempt can fail.
type failureKind struct {
	name    string
	fail    func(ctx context.Context) error
	wantErr string // must appear in last_error when the job dies
}

var failureKinds = []failureKind{
	{"error", func(context.Context) error { return errors.New("transient failure") }, "transient failure"},
	{"panic", func(context.Context) error { panic("handler blew up") }, "panicked: handler blew up"},
	{"timeout", func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }, "deadline exceeded"},
}

type scenario struct {
	kind        failureKind
	failures    int  // attempts that fail before one succeeds
	permanent   bool // first attempt returns a permanent error
	maxAttempts int
}

func (s scenario) name() string {
	if s.permanent {
		return fmt.Sprintf("permanent/max_%d", s.maxAttempts)
	}
	return fmt.Sprintf("%s/fails_%d/max_%d", s.kind.name, s.failures, s.maxAttempts)
}

func generateScenarios() []scenario {
	var out []scenario
	for _, kind := range failureKinds {
		for failures := 0; failures <= 6; failures++ {
			for max := 1; max <= 5; max++ {
				out = append(out, scenario{kind: kind, failures: failures, maxAttempts: max})
			}
		}
	}
	for max := 1; max <= 5; max++ {
		out = append(out, scenario{permanent: true, maxAttempts: max})
	}
	return out
}

func TestFailureScenarios(t *testing.T) {
	scenarios := generateScenarios()
	if len(scenarios) < 100 {
		t.Fatalf("only %d scenarios generated", len(scenarios))
	}
	st := testutil.Store(t)
	q := queue.New(testutil.Redis(t))
	for _, sc := range scenarios {
		t.Run(sc.name(), func(t *testing.T) {
			t.Parallel()
			runScenario(t, st, q, sc)
		})
	}
}

func runScenario(t *testing.T, st *store.Store, q *queue.Queue, sc scenario) {
	ctx := context.Background()
	policy := retry.Policy{Base: 100 * time.Millisecond, Cap: time.Second} // real jitter
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	name := "scn-" + testutil.RandomID()

	calls := 0 // only touched by p.process, which this goroutine calls synchronously
	handler := func(ctx context.Context, _ json.RawMessage) error {
		calls++
		if sc.permanent {
			return Permanent(errors.New("payload is malformed"))
		}
		if calls <= sc.failures {
			return sc.kind.fail(ctx)
		}
		return nil
	}
	p := &Pool{
		Store:    st,
		Queue:    q,
		Handlers: map[string]Handler{"scenario": handler},
		Config:   Config{Queue: name, JobTimeout: 20 * time.Millisecond, Retry: policy},
		Now:      clock.Now,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := p.setup(); err != nil {
		t.Fatal(err)
	}

	job, _, err := st.CreateJob(ctx, store.NewJob{
		IdempotencyKey: name, Queue: name, Type: "scenario",
		Payload: json.RawMessage(`{}`), MaxAttempts: sc.maxAttempts, ReceivedAt: clock.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}

	retries := 0
	for step := 0; ; step++ {
		if step > 10 {
			t.Fatal("job did not reach a final state after 10 attempts")
		}
		failedAt := clock.Now()
		p.process(job.ID)

		j, err := st.GetJob(ctx, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == store.StatusSucceeded || j.Status == store.StatusDead {
			job = j
			break
		}
		if j.Status != store.StatusRetrying {
			t.Fatalf("after attempt %d: status %s, want retrying", j.Attempts, j.Status)
		}
		retries++

		// The retry must be inside the backoff window for this attempt...
		delay := j.RunAt.Sub(failedAt)
		if ceiling := policy.Ceiling(j.Attempts); delay < 0 || delay > ceiling {
			t.Fatalf("attempt %d: retry delay %v outside [0, %v]", j.Attempts, delay, ceiling)
		}
		// ...and scheduled in Redis for the same time (Redis stores milliseconds).
		due, ok, err := q.DueAt(ctx, name, job.ID)
		if err != nil || !ok || due.UnixMilli() != j.RunAt.UnixMilli() {
			t.Fatalf("redis due=%v ok=%v err=%v; postgres run_at=%v", due, ok, err, j.RunAt)
		}
		// Not deliverable a moment before it is due.
		if n, _ := q.PromoteDue(ctx, name, due.Add(-time.Millisecond), 10); n != 0 {
			t.Fatal("retry was promoted before its run_at")
		}

		// Jump to the retry time, promote it, and deliver it like the fetcher would.
		clock.Set(j.RunAt)
		if n, err := q.PromoteDue(ctx, name, clock.Now(), 10); err != nil || n != 1 {
			t.Fatalf("promote: moved %d err %v, want 1", n, err)
		}
		id, ok, err := q.Dequeue(ctx, name, time.Second)
		if err != nil || !ok || id != job.ID {
			t.Fatalf("dequeue: id %d ok %v err %v", id, ok, err)
		}
	}

	// Final state.
	switch {
	case sc.permanent:
		expectFinal(t, job, calls, retries, store.StatusDead, 1, 0)
		if !strings.Contains(*job.LastError, "payload is malformed") {
			t.Fatalf("last_error %q", *job.LastError)
		}
	case sc.failures < sc.maxAttempts:
		expectFinal(t, job, calls, retries, store.StatusSucceeded, sc.failures+1, sc.failures)
	default:
		expectFinal(t, job, calls, retries, store.StatusDead, sc.maxAttempts, sc.maxAttempts-1)
		if job.LastError == nil || !strings.Contains(*job.LastError, sc.kind.wantErr) {
			t.Fatalf("last_error %v, want it to contain %q", job.LastError, sc.kind.wantErr)
		}
	}
	// Nothing left behind in Redis.
	if n, _ := q.DelayedLen(ctx, name); n != 0 {
		t.Fatalf("%d ids left in the delayed set", n)
	}
	if n, _ := q.Len(ctx, name); n != 0 {
		t.Fatalf("%d ids left in the ready list", n)
	}
}

func expectFinal(t *testing.T, job store.Job, calls, retries int, status string, attempts, wantRetries int) {
	t.Helper()
	if job.Status != status || job.Attempts != attempts || calls != attempts || retries != wantRetries {
		t.Fatalf("got status=%s attempts=%d handler_calls=%d retries=%d; "+
			"want status=%s attempts=%d handler_calls=%d retries=%d",
			job.Status, job.Attempts, calls, retries, status, attempts, attempts, wantRetries)
	}
	if job.FinishedAt == nil {
		t.Fatal("finished_at not set on a final job")
	}
}
