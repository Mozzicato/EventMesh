package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/queue"
	"github.com/Mozzicato/EventMesh/internal/testutil"
)

func TestQueue_FIFOOrder(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	name := "q-" + testutil.RandomID()
	for id := int64(1); id <= 5; id++ {
		if err := q.Enqueue(ctx, name, id); err != nil {
			t.Fatal(err)
		}
	}
	for want := int64(1); want <= 5; want++ {
		got, ok, err := q.Dequeue(ctx, name, time.Second)
		if err != nil || !ok || got != want {
			t.Fatalf("dequeue: got %d ok=%v err=%v, want %d", got, ok, err, want)
		}
	}
}

func TestQueue_DequeueEmptyTimesOut(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	start := time.Now()
	_, ok, err := q.Dequeue(context.Background(), "q-"+testutil.RandomID(), 100*time.Millisecond)
	if err != nil || ok {
		t.Fatalf("ok=%v err=%v; want empty, no error", ok, err)
	}
	if time.Since(start) < 90*time.Millisecond {
		t.Fatal("Dequeue returned before its timeout")
	}
}

func TestQueue_PushFrontIsDequeuedNext(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	name := "q-" + testutil.RandomID()
	_ = q.Enqueue(ctx, name, 1)
	_ = q.Enqueue(ctx, name, 2)
	_ = q.PushFront(ctx, name, 99)
	got, _, _ := q.Dequeue(ctx, name, time.Second)
	if got != 99 {
		t.Fatalf("got %d, want 99 (pushed to front)", got)
	}
}

func TestQueue_QueuesAreIsolated(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	a, b := "q-"+testutil.RandomID(), "q-"+testutil.RandomID()
	_ = q.Enqueue(ctx, a, 7)
	if n, _ := q.Len(ctx, b); n != 0 {
		t.Fatalf("queue b has %d items, want 0", n)
	}
	if n, _ := q.Len(ctx, a); n != 1 {
		t.Fatalf("queue a has %d items, want 1", n)
	}
}

func TestQueue_ScheduledIDIsInvisibleUntilPromoted(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	name := "q-" + testutil.RandomID()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := q.Schedule(ctx, name, 42, t0.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if n, _ := q.Len(ctx, name); n != 0 {
		t.Fatalf("ready length %d, want 0 before promotion", n)
	}
	due, ok, err := q.DueAt(ctx, name, 42)
	if err != nil || !ok || !due.Equal(t0.Add(time.Second)) {
		t.Fatalf("DueAt = %v ok=%v err=%v", due, ok, err)
	}
}

func TestQueue_PromoteDueMovesOnlyDueIDs(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	name := "q-" + testutil.RandomID()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	_ = q.Schedule(ctx, name, 1, t0.Add(1*time.Second))
	_ = q.Schedule(ctx, name, 2, t0.Add(2*time.Second))
	_ = q.Schedule(ctx, name, 3, t0.Add(3*time.Second))

	n, err := q.PromoteDue(ctx, name, t0.Add(2*time.Second), 100) // due exactly at 2s counts
	if err != nil || n != 2 {
		t.Fatalf("promoted %d (err %v), want 2", n, err)
	}
	if l, _ := q.Len(ctx, name); l != 2 {
		t.Fatalf("ready length %d, want 2", l)
	}
	if l, _ := q.DelayedLen(ctx, name); l != 1 {
		t.Fatalf("delayed length %d, want 1 (job 3 not due yet)", l)
	}
	if _, ok, _ := q.DueAt(ctx, name, 3); !ok {
		t.Fatal("job 3 should still be scheduled")
	}
}

func TestQueue_PromoteDueRespectsLimit(t *testing.T) {
	q := queue.New(testutil.Redis(t))
	ctx := context.Background()
	name := "q-" + testutil.RandomID()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for id := int64(1); id <= 5; id++ {
		_ = q.Schedule(ctx, name, id, t0)
	}
	if n, _ := q.PromoteDue(ctx, name, t0, 3); n != 3 {
		t.Fatalf("promoted %d, want 3", n)
	}
	if n, _ := q.PromoteDue(ctx, name, t0, 3); n != 2 {
		t.Fatalf("second promote moved %d, want the remaining 2", n)
	}
}
