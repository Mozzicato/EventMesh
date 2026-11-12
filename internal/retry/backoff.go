// Package retry computes how long to wait before retrying a failed job.
//
// It uses exponential backoff with "full jitter":
//
//	ceiling(n) = min(Cap, Base * 2^(n-1))   // n = the attempt that just failed
//	delay(n)   = random value in [0, ceiling(n)]
//
// Exponential growth gives a struggling dependency more and more room.
// Jitter spreads retries out: without it, 1,000 jobs that failed together
// (say, a database blip) would all retry at the same instant and could cause
// the same failure again (a "retry storm" / thundering herd).
package retry

import (
	"math/rand/v2"
	"time"
)

// Policy is a backoff policy.
type Policy struct {
	Base time.Duration // ceiling for the first retry
	Cap  time.Duration // the ceiling never grows past this
	// Rand returns a random int64 in [0, n). Nil means math/rand/v2.
	// Tests inject a fixed function to make delays deterministic.
	Rand func(n int64) int64
}

// Ceiling returns the largest possible delay after the given failed attempt
// (1-based): min(Cap, Base * 2^(attempt-1)).
func (p Policy) Ceiling(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.Base
	for i := 1; i < attempt; i++ {
		if d >= p.Cap/2 { // doubling would reach the cap; also prevents overflow
			return p.Cap
		}
		d *= 2
	}
	return min(d, p.Cap)
}

// Delay returns a random delay in [0, Ceiling(attempt)].
func (p Policy) Delay(attempt int) time.Duration {
	ceiling := p.Ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	randN := p.Rand
	if randN == nil {
		randN = rand.Int64N // safe for concurrent use
	}
	return time.Duration(randN(int64(ceiling) + 1))
}
