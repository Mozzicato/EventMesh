package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
)

// t0 is a fixed instant; tests move "now" around it explicitly.
var t0 = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const lease = 30 * time.Second

// createIn creates a pending job in its own queue, received at t0.
func createIn(t *testing.T, s *store.Store, queue string, maxAttempts int) store.Job {
	t.Helper()
	nj := newJob("k-" + testutil.RandomID())
	nj.Queue = queue
	nj.MaxAttempts = maxAttempts
	nj.ReceivedAt = t0
	job, _, err := s.CreateJob(context.Background(), nj)
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func claim(t *testing.T, s *store.Store, id int64, now time.Time) (store.Job, bool) {
	t.Helper()
	job, ok, err := s.Claim(context.Background(), id, now, now.Add(lease))
	if err != nil {
		t.Fatal(err)
	}
	return job, ok
}

func TestClaim_PendingJobBecomesRunningWithLease(t *testing.T) {
	s := testutil.Store(t)
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	got, ok := claim(t, s, job.ID, t0)
	if !ok {
		t.Fatal("pending job should be claimable")
	}
	if got.Status != store.StatusRunning || got.Attempts != 1 {
		t.Fatalf("status %s attempts %d; want running, 1", got.Status, got.Attempts)
	}
	if got.LeaseUntil == nil || !got.LeaseUntil.Equal(t0.Add(lease)) {
		t.Fatalf("lease_until %v, want %v", got.LeaseUntil, t0.Add(lease))
	}
}

func TestClaim_SecondClaimUnderLiveLeaseIsRefused(t *testing.T) {
	s := testutil.Store(t)
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	claim(t, s, job.ID, t0)
	if _, ok := claim(t, s, job.ID, t0.Add(lease-time.Second)); ok {
		t.Fatal("a job under a live lease must not be claimed again")
	}
}

// Duplicate delivery: many workers receive the same id at the same moment.
// Exactly one may claim it.
func TestClaim_ConcurrentClaimsOfSameJob_ExactlyOneWins(t *testing.T) {
	s := testutil.Store(t)
	queue := "q-" + testutil.RandomID()
	for round := 0; round < 20; round++ {
		job := createIn(t, s, queue, 3)
		var (
			wg    sync.WaitGroup
			start = make(chan struct{})
			wins  atomic.Int32
		)
		for g := 0; g < 20; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_, ok, err := s.Claim(context.Background(), job.ID, t0, t0.Add(lease))
				if err != nil {
					t.Error(err)
				}
				if ok {
					wins.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		if wins.Load() != 1 {
			t.Fatalf("round %d: %d claims succeeded, want exactly 1", round, wins.Load())
		}
	}
}

// Visibility timeout: if the worker dies, the lease runs out and another
// worker may claim the job. The lost attempt still counts.
func TestClaim_ExpiredLeaseIsReclaimable(t *testing.T) {
	s := testutil.Store(t)
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	claim(t, s, job.ID, t0)
	got, ok := claim(t, s, job.ID, t0.Add(lease+time.Second))
	if !ok {
		t.Fatal("job with an expired lease should be reclaimable")
	}
	if got.Attempts != 2 {
		t.Fatalf("attempts %d, want 2 (the crashed attempt counts)", got.Attempts)
	}
}

func TestClaim_OutOfAttemptsIsRefused(t *testing.T) {
	s := testutil.Store(t)
	job := createIn(t, s, "q-"+testutil.RandomID(), 1)
	claim(t, s, job.ID, t0)
	if _, ok := claim(t, s, job.ID, t0.Add(lease+time.Second)); ok {
		t.Fatal("job that used its only attempt must not be claimed again")
	}
}

func TestClaim_FinishedJobIsRefused(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	claimed, _ := claim(t, s, job.ID, t0)
	if ok, err := s.Succeed(ctx, claimed, t0); err != nil || !ok {
		t.Fatalf("succeed: ok=%v err=%v", ok, err)
	}
	if _, ok := claim(t, s, job.ID, t0.Add(time.Hour)); ok {
		t.Fatal("a succeeded job must never be claimed again")
	}
}

// Fencing: a worker whose lease expired (and whose job was re-claimed by
// someone else) must not be able to overwrite the newer attempt's result.
func TestSucceed_StaleAttemptCannotFinishJob(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	stale, _ := claim(t, s, job.ID, t0)                        // attempt 1, then "hangs"
	fresh, _ := claim(t, s, job.ID, t0.Add(lease+time.Second)) // attempt 2

	if ok, _ := s.Succeed(ctx, stale, t0.Add(time.Minute)); ok {
		t.Fatal("stale attempt 1 must not be able to mark the job succeeded")
	}
	if ok, _ := s.MarkDead(ctx, stale, t0.Add(time.Minute), "late failure"); ok {
		t.Fatal("stale attempt 1 must not be able to dead-letter the job")
	}
	if ok, err := s.Succeed(ctx, fresh, t0.Add(time.Minute)); err != nil || !ok {
		t.Fatalf("current attempt should succeed: ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, job.ID)
	if got.Status != store.StatusSucceeded || got.FinishedAt == nil || got.LeaseUntil != nil {
		t.Fatalf("unexpected final job: %+v", got)
	}
}

func TestMarkDead_RecordsReason(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	claimed, _ := claim(t, s, job.ID, t0)
	if ok, err := s.MarkDead(ctx, claimed, t0, "boom"); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, job.ID)
	if got.Status != store.StatusDead || got.LastError == nil || *got.LastError != "boom" {
		t.Fatalf("unexpected dead job: %+v", got)
	}
}

func TestStrandedJobIDs_FindsOnlyJobsThatNeedRescue(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	queue := "q-" + testutil.RandomID()
	const stale = time.Minute

	oldPending := createIn(t, s, queue, 3) // updated_at = t0
	expired := createIn(t, s, queue, 3)
	claim(t, s, expired.ID, t0) // lease ends t0+30s
	live := createIn(t, s, queue, 3)
	claim(t, s, live.ID, t0.Add(50*time.Second)) // lease ends t0+80s

	now := t0.Add(61 * time.Second)
	ids, err := s.StrandedJobIDs(ctx, queue, now, stale, 100)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]bool{oldPending.ID: true, expired.ID: true}
	if len(ids) != len(want) {
		t.Fatalf("got ids %v, want %v", ids, want)
	}
	for _, id := range ids {
		if !want[id] {
			t.Fatalf("unexpected stranded id %d (live lease must not be rescued)", id)
		}
	}

	// A pending job that has not been waiting long is not stranded yet.
	fresh := createIn(t, s, "q-"+testutil.RandomID(), 3)
	ids, _ = s.StrandedJobIDs(ctx, fresh.Queue, t0.Add(stale-time.Second), stale, 100)
	if len(ids) != 0 {
		t.Fatalf("fresh pending job reported as stranded: %v", ids)
	}
}

func TestKillExhaustedLeases_DeadLettersLostFinalAttempt(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	queue := "q-" + testutil.RandomID()
	last := createIn(t, s, queue, 1) // only one attempt
	claim(t, s, last.ID, t0)
	spare := createIn(t, s, queue, 3) // has attempts left: must be left alone
	claim(t, s, spare.ID, t0)

	n, err := s.KillExhaustedLeases(ctx, queue, t0.Add(lease+time.Second))
	if err != nil || n != 1 {
		t.Fatalf("killed %d (err %v), want 1", n, err)
	}
	got, _ := s.GetJob(ctx, last.ID)
	if got.Status != store.StatusDead {
		t.Fatalf("status %s, want dead", got.Status)
	}
	got, _ = s.GetJob(ctx, spare.ID)
	if got.Status != store.StatusRunning {
		t.Fatalf("job with attempts left: status %s, want running", got.Status)
	}
}

func TestMarkRetrying_JobIsNotClaimableBeforeRunAt(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	job := createIn(t, s, "q-"+testutil.RandomID(), 3)
	claimed, _ := claim(t, s, job.ID, t0)
	runAt := t0.Add(10 * time.Second)
	if ok, err := s.MarkRetrying(ctx, claimed, t0, runAt, "flaky"); err != nil || !ok {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	got, _ := s.GetJob(ctx, job.ID)
	if got.Status != store.StatusRetrying || !got.RunAt.Equal(runAt) || got.LeaseUntil != nil ||
		got.LastError == nil || *got.LastError != "flaky" {
		t.Fatalf("unexpected retrying job: %+v", got)
	}
	if _, ok := claim(t, s, job.ID, runAt.Add(-time.Millisecond)); ok {
		t.Fatal("a retrying job must not be claimable before run_at (backoff ignored)")
	}
	again, ok := claim(t, s, job.ID, runAt)
	if !ok || again.Attempts != 2 {
		t.Fatalf("claim at run_at: ok=%v attempts=%d; want true, 2", ok, again.Attempts)
	}
}

func TestStrandedJobIDs_FindsRetryLostFromRedis(t *testing.T) {
	s := testutil.Store(t)
	ctx := context.Background()
	queue := "q-" + testutil.RandomID()
	job := createIn(t, s, queue, 3)
	claimed, _ := claim(t, s, job.ID, t0)
	runAt := t0.Add(time.Second)
	_, _ = s.MarkRetrying(ctx, claimed, t0, runAt, "x")

	// Shortly after run_at, the promoter should have handled it: not stranded.
	if ids, _ := s.StrandedJobIDs(ctx, queue, runAt.Add(time.Second), time.Minute, 10); len(ids) != 0 {
		t.Fatalf("retry reported stranded too early: %v", ids)
	}
	// Long past run_at and still retrying: Redis must have lost it.
	ids, _ := s.StrandedJobIDs(ctx, queue, runAt.Add(2*time.Minute), time.Minute, 10)
	if len(ids) != 1 || ids[0] != job.ID {
		t.Fatalf("got %v, want [%d]", ids, job.ID)
	}
}
