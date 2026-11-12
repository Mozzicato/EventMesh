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

// delayedKey is a sorted set of job ids waiting to be retried, scored by the
// time (Unix milliseconds) they become due.
func delayedKey(queue string) string { return "eventmesh:" + queue + ":delayed" }

// Schedule makes a job id due at runAt. Until then it is invisible to workers.
func (q *Queue) Schedule(ctx context.Context, queue string, id int64, runAt time.Time) error {
	return q.rdb.ZAdd(ctx, delayedKey(queue), redis.Z{Score: float64(runAt.UnixMilli()), Member: id}).Err()
}

// promoteScript moves due ids from the delayed set to the ready list in one
// atomic step, so an id can never be in both or in neither, even with several
// promoters running. KEYS[1]=delayed, KEYS[2]=ready, ARGV[1]=now ms, ARGV[2]=limit.
var promoteScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, id in ipairs(ids) do
	redis.call('ZREM', KEYS[1], id)
	redis.call('LPUSH', KEYS[2], id)
end
return #ids
`)

// PromoteDue moves up to limit ids that are due at now onto the ready list
// and returns how many it moved.
func (q *Queue) PromoteDue(ctx context.Context, queue string, now time.Time, limit int) (int, error) {
	return promoteScript.Run(ctx, q.rdb, []string{delayedKey(queue), readyKey(queue)},
		now.UnixMilli(), limit).Int()
}

// DelayedLen returns how many ids are waiting for their retry time.
func (q *Queue) DelayedLen(ctx context.Context, queue string) (int64, error) {
	return q.rdb.ZCard(ctx, delayedKey(queue)).Result()
}

// DueAt returns when a delayed id becomes due, and whether it is scheduled.
func (q *Queue) DueAt(ctx context.Context, queue string, id int64) (time.Time, bool, error) {
	score, err := q.rdb.ZScore(ctx, delayedKey(queue), strconv.FormatInt(id, 10)).Result()
	if errors.Is(err, redis.Nil) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, err
	}
	return time.UnixMilli(int64(score)), true, nil
}

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
