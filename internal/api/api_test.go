package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/api"
	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/testutil"
)

// fakeQueue records enqueued ids so tests can assert "enqueued exactly once".
type fakeQueue struct {
	mu  sync.Mutex
	ids []int64
	err error
}

func (q *fakeQueue) Enqueue(_ context.Context, id int64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return q.err
	}
	q.ids = append(q.ids, id)
	return nil
}

func (q *fakeQueue) count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.ids)
}

func newServer(t *testing.T, q *fakeQueue) *httptest.Server {
	t.Helper()
	srv := &api.Server{
		Store:    testutil.Store(t),
		Queue:    q,
		JobTypes: map[string]bool{"noop": true},
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:      time.Now,
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, client *http.Client, url, key, body string) (int, store.Job) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url+"/jobs", strings.NewReader(body))
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Error(err)
		return 0, store.Job{}
	}
	defer resp.Body.Close()
	var job store.Job
	_ = json.NewDecoder(resp.Body).Decode(&job)
	return resp.StatusCode, job
}

const body = `{"type":"noop","payload":{"x":1}}`

func TestSubmit_NewJobReturns201AndEnqueuesOnce(t *testing.T) {
	q := &fakeQueue{}
	ts := newServer(t, q)
	code, job := post(t, ts.Client(), ts.URL, "k-"+testutil.RandomID(), body)
	if code != http.StatusCreated {
		t.Fatalf("status %d, want 201", code)
	}
	if job.ID == 0 || job.Status != store.StatusPending {
		t.Fatalf("unexpected job %+v", job)
	}
	if q.count() != 1 {
		t.Fatalf("enqueued %d times, want 1", q.count())
	}
}

func TestSubmit_DefaultMaxAttemptsIsFive(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	_, job := post(t, ts.Client(), ts.URL, "", body)
	if job.MaxAttempts != 5 {
		t.Fatalf("max_attempts %d, want 5", job.MaxAttempts)
	}
}

func TestSubmit_DuplicateKeyReturns200WithOriginalAndDoesNotEnqueue(t *testing.T) {
	q := &fakeQueue{}
	ts := newServer(t, q)
	key := "k-" + testutil.RandomID()
	_, first := post(t, ts.Client(), ts.URL, key, body)
	code, second := post(t, ts.Client(), ts.URL, key, body)
	if code != http.StatusOK {
		t.Fatalf("duplicate status %d, want 200", code)
	}
	if second.ID != first.ID {
		t.Fatalf("duplicate returned id %d, want %d", second.ID, first.ID)
	}
	if q.count() != 1 {
		t.Fatalf("enqueued %d times, want 1", q.count())
	}
}

func TestSubmit_KeyReusedWithDifferentPayloadReturns409(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	key := "k-" + testutil.RandomID()
	post(t, ts.Client(), ts.URL, key, body)
	code, _ := post(t, ts.Client(), ts.URL, key, `{"type":"noop","payload":{"x":2}}`)
	if code != http.StatusConflict {
		t.Fatalf("status %d, want 409", code)
	}
}

func TestSubmit_WithoutKeyCreatesDistinctJobs(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	_, a := post(t, ts.Client(), ts.URL, "", body)
	_, b := post(t, ts.Client(), ts.URL, "", body)
	if a.ID == b.ID {
		t.Fatal("requests without a key must create separate jobs")
	}
}

// 1,000 HTTP requests with one idempotency key, sent by 100 concurrent clients
// (10 each): exactly one 201, 999 x 200, one job id, and one enqueue.
// (100 clients rather than 1,000 sockets at once: Windows' listen backlog
// refuses ~1,000 simultaneous dials before the server ever sees them.)
func TestSubmit_1000ConcurrentDuplicates_ExactlyOneJob(t *testing.T) {
	q := &fakeQueue{}
	ts := newServer(t, q)
	client := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 100}}
	key := "dup-" + testutil.RandomID()
	const clients, perClient = 100, 10
	const n = clients * perClient

	var (
		wg     sync.WaitGroup
		start  = make(chan struct{})
		mu     sync.Mutex
		codes  = map[int]int{}
		jobIDs = map[int64]bool{}
	)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perClient; j++ {
				code, job := post(t, client, ts.URL, key, body)
				mu.Lock()
				codes[code]++
				jobIDs[job.ID] = true
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if codes[http.StatusCreated] != 1 || codes[http.StatusOK] != n-1 {
		t.Fatalf("status counts %v; want 1x201 and %dx200", codes, n-1)
	}
	if len(jobIDs) != 1 {
		t.Fatalf("got %d distinct job ids, want 1", len(jobIDs))
	}
	if q.count() != 1 {
		t.Fatalf("enqueued %d times, want 1", q.count())
	}
}

func TestSubmit_EnqueueFailureStillReturns201(t *testing.T) {
	q := &fakeQueue{err: errors.New("redis down")}
	ts := newServer(t, q)
	code, job := post(t, ts.Client(), ts.URL, "", body)
	if code != http.StatusCreated || job.Status != store.StatusPending {
		t.Fatalf("status %d job %+v; want 201 pending (sweeper enqueues later)", code, job)
	}
}

func TestSubmit_RejectsInvalidInput(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	cases := []struct {
		name, key, body string
	}{
		{"malformed json", "", `{"type":`},
		{"unknown type", "", `{"type":"nope"}`},
		{"missing type", "", `{"payload":{}}`},
		{"unknown field", "", `{"type":"noop","extra":1}`},
		{"max_attempts negative", "", `{"type":"noop","max_attempts":-1}`},
		{"max_attempts too big", "", `{"type":"noop","max_attempts":26}`},
		{"payload not json", "", `{"type":"noop","payload":nope}`},
		{"key too long", strings.Repeat("k", 201), body},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _ := post(t, ts.Client(), ts.URL, tc.key, tc.body)
			if code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", code)
			}
		})
	}
}

func TestSubmit_BodyTooLargeReturns413(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	big := `{"type":"noop","payload":"` + strings.Repeat("a", 2<<20) + `"}`
	resp, err := ts.Client().Post(ts.URL+"/jobs", "application/json", bytes.NewBufferString(big))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status %d, want 413", resp.StatusCode)
	}
}

func TestGetJob_ReturnsStatus(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	_, created := post(t, ts.Client(), ts.URL, "", body)
	resp, err := ts.Client().Get(ts.URL + "/jobs/" + itoa(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got store.Job
	_ = json.NewDecoder(resp.Body).Decode(&got)
	if resp.StatusCode != http.StatusOK || got.ID != created.ID || got.Status != store.StatusPending {
		t.Fatalf("status %d job %+v", resp.StatusCode, got)
	}
}

func TestGetJob_NotFoundAndBadID(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	for path, want := range map[string]int{"/jobs/999999999999": 404, "/jobs/abc": 400} {
		resp, err := ts.Client().Get(ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("%s: status %d, want %d", path, resp.StatusCode, want)
		}
	}
}

func TestHealthz(t *testing.T) {
	ts := newServer(t, &fakeQueue{})
	resp, err := ts.Client().Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }
