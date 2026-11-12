// Package queue is the Redis-backed transport that carries job ids to workers.
//
// Each named queue is a Redis list, "eventmesh:<queue>:ready". Producers LPUSH
// onto the left; workers BRPOP from the right, so the list is FIFO.
//
// Redis holds ids only, never job state. Postgres is the source of truth, so
// losing or duplicating an id here is recoverable: the sweeper re-enqueues
// stranded jobs from Postgres, and Store.Claim ignores duplicate deliveries.
// That is why Redis persistence can stay off.
package queue

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue talks to Redis.
type Queue struct {
	rdb *redis.Client
}

// New wraps a Redis client.
func New(rdb *redis.Client) *Queue { return &Queue{rdb: rdb} }

func readyKey(queue string) string { return "eventmesh:" + queue + ":ready" }

// Enqueue appends a job id to the back of the queue.
func (q *Queue) Enqueue(ctx context.Context, queue string, id int64) error {
	return q.rdb.LPush(ctx, readyKey(queue), id).Err()
}

// PushFront puts a job id at the front of the queue, so it is dequeued next.
// Used to hand back jobs that were taken from Redis but never started.
func (q *Queue) PushFront(ctx context.Context, queue string, id int64) error {
	return q.rdb.RPush(ctx, readyKey(queue), id).Err()
}

// Dequeue waits up to timeout for a job id. ok=false means the queue stayed
// empty for the whole timeout. A short timeout keeps shutdown responsive,
// because a blocked BRPOP is not interrupted by context cancellation.
func (q *Queue) Dequeue(ctx context.Context, queue string, timeout time.Duration) (id int64, ok bool, err error) {
	res, err := q.rdb.BRPop(ctx, timeout, readyKey(queue)).Result()
	if errors.Is(err, redis.Nil) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	// res is [key, value].
	id, err = strconv.ParseInt(res[1], 10, 64)
	if err != nil {
		return 0, false, err
	}
	return id, true, nil
}

// Len returns how many ids are waiting in the queue.
func (q *Queue) Len(ctx context.Context, queue string) (int64, error) {
	return q.rdb.LLen(ctx, readyKey(queue)).Result()
}
