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
