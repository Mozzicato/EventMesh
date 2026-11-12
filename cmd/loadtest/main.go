// Command loadtest submits jobs to an EventMesh server from many concurrent
// HTTP clients and measures throughput and end-to-end latency.
//
// The server must run with -bench, so it records, for every job, when the API
// received it and when the worker's "succeeded" UPDATE committed. Latency is
// computed from those per-job timestamps (no sampling, no averaging).
//
// Two modes:
//
//	-rate 0   burst: all jobs are released at once; clients submit as fast as
//	          they can. Measures maximum throughput. Latency includes time
//	          spent waiting in the queue backlog.
//	-rate N   open loop at N jobs/sec: job i is scheduled at start + i/N,
//	          regardless of how fast earlier requests returned. Measures
//	          latency at a sustained rate. "sched_e2e" is measured from the
//	          scheduled send time, so a stalled client cannot hide a stall
//	          (coordinated omission).
package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Mozzicato/EventMesh/internal/latency"
)

type config struct {
	url          string
	jobs         int
	clients      int
	rate         float64
	payloadBytes int
	jobType      string
	outDir       string
	label        string
	notes        string
	timeout      time.Duration
}

// result is what one client saw for one job.
type result struct {
	index     int
	jobID     int64
	status    int
	err       string
	scheduled time.Time // when the job was meant to be sent
	sent      time.Time
	responded time.Time
}

func main() {
	var cfg config
	flag.StringVar(&cfg.url, "url", "http://localhost:8080", "EventMesh server URL")
	flag.IntVar(&cfg.jobs, "jobs", 10000, "number of jobs to submit")
	flag.IntVar(&cfg.clients, "clients", 100, "number of concurrent HTTP clients")
	flag.Float64Var(&cfg.rate, "rate", 0, "target submit rate in jobs/sec (0 = burst, as fast as possible)")
	flag.IntVar(&cfg.payloadBytes, "payload-bytes", 1024, "approximate size of each job payload")
	flag.StringVar(&cfg.jobType, "type", "hash", "job type to submit")
	flag.StringVar(&cfg.outDir, "out", "benchmarks/results", "directory for CSV results")
	flag.StringVar(&cfg.label, "label", "", "label for this run (used in file names)")
	flag.StringVar(&cfg.notes, "notes", "", "free-text machine conditions recorded in the CSV")
	flag.DurationVar(&cfg.timeout, "timeout", 10*time.Minute, "give up waiting for jobs after this long")
	flag.Parse()
	if cfg.label == "" {
		cfg.label = "burst"
		if cfg.rate > 0 {
			cfg.label = fmt.Sprintf("rate%.0f", cfg.rate)
		}
	}
	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg config) error {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        cfg.clients,
			MaxIdleConnsPerHost: cfg.clients,
			MaxConnsPerHost:     cfg.clients,
		},
	}

	// Discard samples from anything that ran before this test.
	if _, err := takeSamples(client, cfg.url); err != nil {
		return fmt.Errorf("server not reachable or not in -bench mode: %w", err)
	}

	runID := randomHex(6)
	filler := strings.Repeat("x", max(0, cfg.payloadBytes-40))
	type task struct {
		index     int
		scheduled time.Time
	}
	tasks := make(chan task, cfg.jobs) // buffered: the scheduler never waits for clients
	results := make([]result, cfg.jobs)

	// Clients.
	var wg sync.WaitGroup
	for c := 0; c < cfg.clients; c++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				results[t.index] = submit(client, cfg, runID, filler, t.index, t.scheduled)
			}
		}()
	}

	// Scheduler.
	start := time.Now().Add(50 * time.Millisecond)
	for i := 0; i < cfg.jobs; i++ {
		at := start
		if cfg.rate > 0 {
			at = start.Add(time.Duration(float64(i) * float64(time.Second) / cfg.rate))
		}
		if d := time.Until(at); d > 0 {
			time.Sleep(d)
		}
		tasks <- task{index: i, scheduled: at}
	}
	close(tasks)
	wg.Wait()
	submitDone := time.Now()

	created, statusCounts, errorCount := 0, map[int]int{}, 0
	for _, r := range results {
		if r.err != "" {
			errorCount++
			continue
		}
		statusCounts[r.status]++
		if r.status == http.StatusCreated {
			created++
		}
	}
	log.Printf("submitted %d jobs in %v: statuses %v, transport errors %d",
		cfg.jobs, submitDone.Sub(start).Round(time.Millisecond), statusCounts, errorCount)

	// Wait until the server has finished every created job.
	deadline := time.Now().Add(cfg.timeout)
	for {
		n, err := countSamples(client, cfg.url)
		if err != nil {
			return err
		}
		if n >= created {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out: %d of %d jobs finished", n, created)
		}
		time.Sleep(100 * time.Millisecond)
	}
	samples, err := takeSamples(client, cfg.url)
	if err != nil {
		return err
	}

	return report(cfg, results, samples, created, statusCounts, errorCount, start, submitDone)
}

func submit(client *http.Client, cfg config, runID, filler string, i int, scheduled time.Time) result {
	r := result{index: i, scheduled: scheduled}
	body := fmt.Sprintf(`{"type":%q,"payload":{"i":%d,"fill":%q}}`, cfg.jobType, i, filler)
	req, err := http.NewRequest(http.MethodPost, cfg.url+"/jobs", strings.NewReader(body))
	if err != nil {
		r.err = err.Error()
		return r
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", fmt.Sprintf("lt-%s-%d", runID, i))
	r.sent = time.Now()
	resp, err := client.Do(req)
	r.responded = time.Now()
	if err != nil {
		r.err = err.Error()
		return r
	}
	defer resp.Body.Close()
	r.status = resp.StatusCode
	var job struct {
		ID int64 `json:"id"`
	}
	data, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(data, &job)
	r.jobID = job.ID
	return r
}

func countSamples(client *http.Client, url string) (int, error) {
	resp, err := client.Get(url + "/debug/latencies/count")
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("count: HTTP %d", resp.StatusCode)
	}
	var v struct {
		Count int `json:"count"`
	}
	return v.Count, json.NewDecoder(resp.Body).Decode(&v)
}

func takeSamples(client *http.Client, url string) ([]latency.Sample, error) {
	resp, err := client.Post(url+"/debug/latencies/take", "application/json", bytes.NewReader(nil))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("take: HTTP %d", resp.StatusCode)
	}
	var s []latency.Sample
	return s, json.NewDecoder(resp.Body).Decode(&s)
}

func report(cfg config, results []result, samples []latency.Sample, created int,
	statusCounts map[int]int, errorCount int, start, submitDone time.Time) error {

	byJob := map[int64]result{}
	for _, r := range results {
		if r.status == http.StatusCreated {
			byJob[r.jobID] = r
		}
	}

	var serverE2E, schedE2E, submitRTT []time.Duration
	seen := map[int64]bool{}
	duplicates, unknown := 0, 0
	first, last := samples[0].ReceivedAt, samples[0].CommittedAt
	for _, s := range samples {
		if seen[s.JobID] {
			duplicates++ // the same job reported succeeded twice: must never happen
			continue
		}
		seen[s.JobID] = true
		r, ok := byJob[s.JobID]
		if !ok {
			unknown++ // a job this run did not create
			continue
		}
		serverE2E = append(serverE2E, s.Latency())
		schedE2E = append(schedE2E, s.CommittedAt.Sub(r.scheduled))
		submitRTT = append(submitRTT, r.responded.Sub(r.sent))
		if s.ReceivedAt.Before(first) {
			first = s.ReceivedAt
		}
		if s.CommittedAt.After(last) {
			last = s.CommittedAt
		}
	}
	missing := created - len(serverE2E)

	window := last.Sub(first)
	throughput := float64(len(serverE2E)) / window.Seconds()
	submitRate := float64(created) / submitDone.Sub(start).Seconds()
	e2e := latency.Summarize(serverE2E)
	sched := latency.Summarize(schedE2E)
	rtt := latency.Summarize(submitRTT)

	fmt.Printf("\n=== EventMesh load test: %s ===\n", cfg.label)
	fmt.Printf("jobs %d, clients %d, mode %s, job type %s, payload ~%d B\n",
		cfg.jobs, cfg.clients, modeName(cfg.rate), cfg.jobType, cfg.payloadBytes)
	fmt.Printf("created %d, statuses %v, transport errors %d, finished %d, missing %d, duplicates %d, unknown %d\n",
		created, statusCounts, errorCount, len(serverE2E), missing, duplicates, unknown)
	fmt.Printf("throughput: %.0f jobs/sec completed (%d jobs, first receive -> last commit = %v)\n",
		throughput, len(serverE2E), window.Round(time.Millisecond))
	fmt.Printf("submit rate achieved: %.0f jobs/sec\n", submitRate)
	printSummary("end-to-end (API receive -> success committed)", e2e)
	printSummary("end-to-end from scheduled send time        ", sched)
	printSummary("submit round trip (client)                  ", rtt)

	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		return err
	}
	stamp := time.Now().UTC().Format("20060102T150405Z")
	base := filepath.Join(cfg.outDir, stamp+"_"+cfg.label)

	summary := [][2]string{
		{"timestamp_utc", stamp}, {"label", cfg.label}, {"mode", modeName(cfg.rate)},
		{"target_rate_jobs_per_s", fmt.Sprintf("%.0f", cfg.rate)},
		{"jobs", fmt.Sprint(cfg.jobs)}, {"clients", fmt.Sprint(cfg.clients)},
		{"job_type", cfg.jobType}, {"payload_bytes", fmt.Sprint(cfg.payloadBytes)},
		{"created", fmt.Sprint(created)}, {"finished", fmt.Sprint(len(serverE2E))},
		{"missing", fmt.Sprint(missing)}, {"duplicates", fmt.Sprint(duplicates)},
		{"transport_errors", fmt.Sprint(errorCount)}, {"non_201", fmt.Sprint(cfg.jobs - created - errorCount)},
		{"window_s", fmt.Sprintf("%.3f", window.Seconds())},
		{"throughput_jobs_per_s", fmt.Sprintf("%.1f", throughput)},
		{"submit_rate_jobs_per_s", fmt.Sprintf("%.1f", submitRate)},
		{"e2e_p50_ms", msStr(e2e.P50)}, {"e2e_p95_ms", msStr(e2e.P95)},
		{"e2e_p99_ms", msStr(e2e.P99)}, {"e2e_max_ms", msStr(e2e.Max)}, {"e2e_mean_ms", msStr(e2e.Mean)},
		{"sched_e2e_p50_ms", msStr(sched.P50)}, {"sched_e2e_p95_ms", msStr(sched.P95)},
		{"sched_e2e_p99_ms", msStr(sched.P99)},
		{"submit_rtt_p50_ms", msStr(rtt.P50)}, {"submit_rtt_p95_ms", msStr(rtt.P95)},
		{"client_gomaxprocs", fmt.Sprint(runtime.GOMAXPROCS(0))},
		{"notes", cfg.notes},
	}
	if err := writeCSV(base+".csv", summary); err != nil {
		return err
	}

	// Per-job timings (sorted by job id) so the percentiles can be recomputed.
	sort.Slice(samples, func(i, j int) bool { return samples[i].JobID < samples[j].JobID })
	var b strings.Builder
	b.WriteString("job_id,e2e_us,sched_e2e_us,submit_rtt_us\n")
	written := map[int64]bool{}
	for _, s := range samples {
		r, ok := byJob[s.JobID]
		if !ok || written[s.JobID] {
			continue
		}
		written[s.JobID] = true
		fmt.Fprintf(&b, "%d,%d,%d,%d\n", s.JobID, s.Latency().Microseconds(),
			s.CommittedAt.Sub(r.scheduled).Microseconds(), r.responded.Sub(r.sent).Microseconds())
	}
	if err := os.WriteFile(base+"_samples.csv", []byte(b.String()), 0o644); err != nil {
		return err
	}
	fmt.Printf("results: %s.csv, %s_samples.csv\n", base, base)
	if missing != 0 || duplicates != 0 {
		return fmt.Errorf("integrity check failed: missing=%d duplicates=%d", missing, duplicates)
	}
	return nil
}

func writeCSV(path string, kv [][2]string) error {
	var head, row []string
	for _, p := range kv {
		head = append(head, p[0])
		row = append(row, csvField(p[1]))
	}
	return os.WriteFile(path, []byte(strings.Join(head, ",")+"\n"+strings.Join(row, ",")+"\n"), 0o644)
}

func csvField(s string) string {
	if strings.ContainsAny(s, ",\"\n") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func printSummary(name string, s latency.Summary) {
	fmt.Printf("%s: p50 %s  p95 %s  p99 %s  max %s  mean %s ms (n=%d)\n",
		name, msStr(s.P50), msStr(s.P95), msStr(s.P99), msStr(s.Max), msStr(s.Mean), s.Count)
}

func msStr(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d.Microseconds())/1000) }

func modeName(rate float64) string {
	if rate > 0 {
		return "open-loop"
	}
	return "burst"
}

func randomHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
