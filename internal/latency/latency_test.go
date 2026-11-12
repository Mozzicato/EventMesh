package latency_test

import (
	"sync"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/latency"
	"github.com/Mozzicato/EventMesh/internal/store"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

// 1..100 ms: the nearest-rank percentiles are exact and easy to check by hand.
func TestPercentile_NearestRank(t *testing.T) {
	sorted := make([]time.Duration, 100)
	for i := range sorted {
		sorted[i] = ms(i + 1)
	}
	cases := map[float64]time.Duration{50: ms(50), 95: ms(95), 99: ms(99), 100: ms(100), 1: ms(1), 0.1: ms(1)}
	for p, want := range cases {
		if got := latency.Percentile(sorted, p); got != want {
			t.Errorf("p%v = %v, want %v", p, got, want)
		}
	}
}

func TestPercentile_SmallAndEmptyInputs(t *testing.T) {
	if got := latency.Percentile(nil, 50); got != 0 {
		t.Fatalf("empty: %v", got)
	}
	one := []time.Duration{ms(7)}
	for _, p := range []float64{1, 50, 95, 100} {
		if got := latency.Percentile(one, p); got != ms(7) {
			t.Fatalf("single value p%v = %v", p, got)
		}
	}
	// With 10 values, p95 is the 10th (ceil(9.5) = 10): the tail is not hidden.
	ten := []time.Duration{ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(1), ms(500)}
	if got := latency.Percentile(ten, 95); got != ms(500) {
		t.Fatalf("p95 of ten = %v, want 500ms", got)
	}
}

// Why percentiles: nine fast jobs and one slow one. The mean describes no job.
func TestSummarize_MeanMisleadsPercentilesDoNot(t *testing.T) {
	ds := []time.Duration{ms(500), ms(5), ms(5), ms(5), ms(5), ms(5), ms(5), ms(5), ms(5), ms(5)}
	s := latency.Summarize(ds)
	if s.Count != 10 || s.Min != ms(5) || s.Max != ms(500) || s.P50 != ms(5) {
		t.Fatalf("summary %+v", s)
	}
	if s.Mean != 54500*time.Microsecond {
		t.Fatalf("mean %v, want 54.5ms", s.Mean)
	}
	if ds[0] != ms(500) {
		t.Fatal("Summarize must not reorder the caller's slice")
	}
}

func TestRecorder_ConcurrentRecordAndTake(t *testing.T) {
	var r latency.Recorder
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				r.Record(store.Job{ID: int64(g*1000 + i), ReceivedAt: base}, base.Add(ms(3)))
			}
		}(g)
	}
	wg.Wait()
	if r.Count() != 4000 {
		t.Fatalf("count %d, want 4000", r.Count())
	}
	samples := r.Take()
	if len(samples) != 4000 || r.Count() != 0 {
		t.Fatalf("took %d, left %d", len(samples), r.Count())
	}
	if samples[0].Latency() != ms(3) {
		t.Fatalf("latency %v, want 3ms", samples[0].Latency())
	}
}
