package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
)

func newJob(key string) store.NewJob {
	return store.NewJob{
		IdempotencyKey: key,
		Type:           "noop",
		Payload:        json.RawMessage(`{"n":1}`),
		MaxAttempts:    3,
		ReceivedAt:     time.Now(),
	}
}

func TestCreateJob_NewKeyCreatesPendingJob(t *testing.T) {
	s := testutil.Store(t)
	job, created, err := s.CreateJob(context.Background(), newJob("k-"+testutil.RandomID()))
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("expected created=true for a new key")
	}
	if job.Status != store.StatusPending || job.Attempts != 0 || job.MaxAttempts != 3 {
		t.Fatalf("unexpected new job: %+v", job)
	}
}

func TestCreateJob_SameKeyReturnsOriginal(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	key := "k-" + testutil.RandomID()

	first, created, err := s.CreateJob(ctx, newJob(key))
	if err != nil || !created {
		t.Fatalf("first create: created=%v err=%v", created, err)
	}
	second, created, err := s.CreateJob(ctx, newJob(key))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("second create with same key must not create a row")
	}
	if second.ID != first.ID {
		t.Fatalf("got id %d, want original %d", second.ID, first.ID)
	}
}

func TestCreateJob_SameKeyDifferentPayloadConflicts(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	key := "k-" + testutil.RandomID()
	if _, _, err := s.CreateJob(ctx, newJob(key)); err != nil {
		t.Fatal(err)
	}
	other := newJob(key)
	other.Payload = json.RawMessage(`{"n":2}`)
	if _, _, err := s.CreateJob(ctx, other); !errors.Is(err, store.ErrKeyConflict) {
		t.Fatalf("got err %v, want ErrKeyConflict", err)
	}
}

// JSONB compares parsed values, so key order and whitespace don't matter.
func TestCreateJob_SameKeyEquivalentJSONIsNotAConflict(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	key := "k-" + testutil.RandomID()
	a := newJob(key)
	a.Payload = json.RawMessage(`{"a":1,"b":2}`)
	b := newJob(key)
	b.Payload = json.RawMessage(`{ "b": 2, "a": 1 }`)
	if _, _, err := s.CreateJob(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, created, err := s.CreateJob(ctx, b); err != nil || created {
		t.Fatalf("created=%v err=%v; want existing job, no error", created, err)
	}
}

// The core idempotency proof: many goroutines insert the same key at the same
// instant. Exactly one must create the row and all must see the same job id.
func TestCreateJob_ConcurrentSameKey_ExactlyOneCreated(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	const rounds, goroutines = 40, 50 // 2,000 duplicate inserts in total

	for r := 0; r < rounds; r++ {
		key := "race-" + testutil.RandomID()
		var (
			wg      sync.WaitGroup
			start   = make(chan struct{})
			mu      sync.Mutex
			created int
			ids     = map[int64]bool{}
		)
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start // release all goroutines together to maximise the race
				job, c, err := s.CreateJob(ctx, newJob(key))
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if c {
					created++
				}
				ids[job.ID] = true
			}()
		}
		close(start)
		wg.Wait()
		if created != 1 || len(ids) != 1 {
			t.Fatalf("round %d: created=%d distinct ids=%d; want 1 and 1", r, created, len(ids))
		}
	}
}

func TestGetJob_NotFound(t *testing.T) {
	s := testutil.Store(t)
	if _, err := s.GetJob(context.Background(), -1); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestGetJob_ReturnsStoredFields(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	nj := newJob("k-" + testutil.RandomID())
	created, _, err := s.CreateJob(ctx, nj)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.GetJob(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.IdempotencyKey != nj.IdempotencyKey || got.Type != nj.Type ||
		string(got.Payload) != `{"n": 1}` {
		t.Fatalf("unexpected job: %+v payload=%s", got, got.Payload)
	}
	// Postgres stores microseconds; compare at that precision.
	if !got.ReceivedAt.Equal(nj.ReceivedAt.Truncate(time.Microsecond)) {
		t.Fatalf("received_at %v, want %v", got.ReceivedAt, nj.ReceivedAt)
	}
}
