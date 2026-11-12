package retry_test

import (
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/retry"
)

func TestCeiling_DoublesThenCaps(t *testing.T) {
	p := retry.Policy{Base: 100 * time.Millisecond, Cap: 2 * time.Second}
	want := []time.Duration{ // attempts 1..8
		100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond,
		800 * time.Millisecond, 1600 * time.Millisecond, 2 * time.Second,
		2 * time.Second, 2 * time.Second,
	}
	for i, w := range want {
		if got := p.Ceiling(i + 1); got != w {
			t.Errorf("Ceiling(%d) = %v, want %v", i+1, got, w)
		}
	}
}

func TestCeiling_HugeAttemptDoesNotOverflow(t *testing.T) {
	p := retry.Policy{Base: time.Second, Cap: time.Hour}
	for _, attempt := range []int{63, 64, 100, 1 << 20} {
		if got := p.Ceiling(attempt); got != time.Hour {
			t.Fatalf("Ceiling(%d) = %v, want cap %v", attempt, got, time.Hour)
		}
	}
}

func TestCeiling_AttemptBelowOneTreatedAsFirst(t *testing.T) {
	p := retry.Policy{Base: time.Second, Cap: time.Minute}
	if got := p.Ceiling(0); got != time.Second {
		t.Fatalf("Ceiling(0) = %v, want %v", got, time.Second)
	}
}

// The random source's extremes map exactly to the ends of [0, ceiling].
func TestDelay_FullJitterRangeIsZeroToCeiling(t *testing.T) {
	low := retry.Policy{Base: time.Second, Cap: time.Minute, Rand: func(int64) int64 { return 0 }}
	high := retry.Policy{Base: time.Second, Cap: time.Minute, Rand: func(n int64) int64 { return n - 1 }}
	for attempt := 1; attempt <= 8; attempt++ {
		if d := low.Delay(attempt); d != 0 {
			t.Errorf("attempt %d: lowest delay %v, want 0", attempt, d)
		}
		if d, c := high.Delay(attempt), high.Ceiling(attempt); d != c {
			t.Errorf("attempt %d: highest delay %v, want ceiling %v", attempt, d, c)
		}
	}
}

func TestDelay_RealRandomStaysInRange(t *testing.T) {
	p := retry.Policy{Base: 10 * time.Millisecond, Cap: time.Second}
	for attempt := 1; attempt <= 10; attempt++ {
		c := p.Ceiling(attempt)
		for i := 0; i < 1000; i++ {
			if d := p.Delay(attempt); d < 0 || d > c {
				t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, c)
			}
		}
	}
}

// Why jitter matters: 1,000 jobs failing at the same moment must not all be
// scheduled for the same instant. With full jitter they spread over the
// whole window; check every tenth of the window gets a share.
func TestDelay_SpreadsSimultaneousRetries(t *testing.T) {
	p := retry.Policy{Base: time.Second, Cap: time.Second}
	var buckets [10]int
	for i := 0; i < 1000; i++ {
		d := p.Delay(1)
		b := int(d * 10 / (time.Second + 1))
		buckets[b]++
	}
	for i, n := range buckets {
		// Expected 100 per bucket; 40 is far outside normal random variation.
		if n < 40 {
			t.Fatalf("bucket %d got %d of 1000 retries; distribution %v", i, n, buckets)
		}
	}
}

func TestDelay_ZeroPolicyMeansNoWait(t *testing.T) {
	if d := (retry.Policy{}).Delay(3); d != 0 {
		t.Fatalf("zero policy delay %v, want 0", d)
	}
}
