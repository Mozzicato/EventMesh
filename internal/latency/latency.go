// Package latency records end-to-end job latency for load tests.
//
// End-to-end latency of a job is defined as:
//
//	start: the moment the API handler received the submit request (jobs.received_at)
//	end:   the moment the worker's "succeeded" UPDATE committed (Pool.OnSucceeded)
//
// Both timestamps come from the server process's clock. Every job is recorded
// (no sampling), so percentiles are computed from the full sorted population.
package latency

import (
	"encoding/json"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
)

// Sample is one finished job's timings.
type Sample struct {
	JobID       int64     `json:"id"`
	ReceivedAt  time.Time `json:"received_at"`
	CommittedAt time.Time `json:"committed_at"`
}

// Latency is the job's end-to-end latency.
func (s Sample) Latency() time.Duration { return s.CommittedAt.Sub(s.ReceivedAt) }

// Recorder collects samples. It is safe for concurrent use.
type Recorder struct {
	mu      sync.Mutex
	samples []Sample
}

// Record matches worker.Pool.OnSucceeded.
func (r *Recorder) Record(job store.Job, committedAt time.Time) {
	r.mu.Lock()
	r.samples = append(r.samples, Sample{JobID: job.ID, ReceivedAt: job.ReceivedAt, CommittedAt: committedAt})
	r.mu.Unlock()
}

// Count returns how many samples are held.
func (r *Recorder) Count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.samples)
}

// Take returns all samples and clears the recorder.
func (r *Recorder) Take() []Sample {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.samples
	r.samples = nil
	return out
}

// CountHandler serves {"count": n}.
func (r *Recorder) CountHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]int{"count": r.Count()})
}

// TakeHandler serves all samples as JSON and clears them.
func (r *Recorder) TakeHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(r.Take())
}

// Percentile returns the p-th percentile (0 < p <= 100) of sorted, using the
// nearest-rank method: the smallest value such that at least p% of values are
// less than or equal to it. sorted must be in ascending order.
func Percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(p / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

// Summary describes a set of durations.
type Summary struct {
	Count              int
	Min, P50, P95, P99 time.Duration
	Max, Mean          time.Duration
}

// Summarize sorts a copy of ds and computes its summary.
func Summarize(ds []time.Duration) Summary {
	if len(ds) == 0 {
		return Summary{}
	}
	sorted := append([]time.Duration(nil), ds...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	return Summary{
		Count: len(sorted),
		Min:   sorted[0],
		P50:   Percentile(sorted, 50),
		P95:   Percentile(sorted, 95),
		P99:   Percentile(sorted, 99),
		Max:   sorted[len(sorted)-1],
		Mean:  total / time.Duration(len(sorted)),
	}
}
