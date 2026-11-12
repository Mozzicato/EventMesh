// Package api is the HTTP interface: submit a job, read its status, list the
// dead-letter queue.
package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
)

const (
	maxBodyBytes       = 1 << 20 // 1 MiB
	maxKeyLength       = 200
	defaultMaxAttempts = 5
	maxMaxAttempts     = 25
)

// Enqueuer pushes a newly created job id onto the work queue (Redis).
type Enqueuer interface {
	Enqueue(ctx context.Context, queue string, id int64) error
}

// Server holds the API's dependencies.
type Server struct {
	Store    *store.Store
	Queue    Enqueuer        // may be nil: jobs are still stored, the sweeper enqueues them later
	Queues   map[string]bool // queue names that have a worker pool
	JobTypes map[string]bool // job types that have a registered handler
	Log      *slog.Logger
	Now      func() time.Time
}

// Handler returns the HTTP routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", s.submitJob)
	mux.HandleFunc("GET /jobs/{id}", s.getJob)
	mux.HandleFunc("GET /dlq", s.listDead)
	mux.HandleFunc("GET /healthz", s.health)
	return mux
}

type submitRequest struct {
	Queue       string          `json:"queue"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	MaxAttempts int             `json:"max_attempts"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request) {
	// Start of end-to-end latency: the moment the request reaches the handler.
	receivedAt := s.Now()

	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	var req submitRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Queue == "" {
		req.Queue = "default"
	}
	if !s.Queues[req.Queue] {
		writeError(w, http.StatusBadRequest, "unknown queue: "+strconv.Quote(req.Queue))
		return
	}
	if !s.JobTypes[req.Type] {
		writeError(w, http.StatusBadRequest, "unknown job type: "+strconv.Quote(req.Type))
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}
	if req.MaxAttempts == 0 {
		req.MaxAttempts = defaultMaxAttempts
	}
	if req.MaxAttempts < 1 || req.MaxAttempts > maxMaxAttempts {
		writeError(w, http.StatusBadRequest, "max_attempts must be between 1 and 25")
		return
	}

	key := r.Header.Get("Idempotency-Key")
	if len(key) > maxKeyLength {
		writeError(w, http.StatusBadRequest, "Idempotency-Key too long")
		return
	}
	if key == "" {
		// No key from the client: mint one so the column can stay NOT NULL.
		// Such a request is not protected against client retries.
		key = newKey()
	}

	job, created, err := s.Store.CreateJob(r.Context(), store.NewJob{
		IdempotencyKey: key,
		Queue:          req.Queue,
		Type:           req.Type,
		Payload:        req.Payload,
		MaxAttempts:    req.MaxAttempts,
		ReceivedAt:     receivedAt,
	})
	switch {
	case errors.Is(err, store.ErrKeyConflict):
		writeError(w, http.StatusConflict, err.Error())
		return
	case err != nil:
		s.Log.Error("create job", "err", err)
		writeError(w, http.StatusInternalServerError, "could not store job")
		return
	}

	if !created {
		// Duplicate request: return the original job and do not enqueue again.
		writeJSON(w, http.StatusOK, job)
		return
	}
	if s.Queue != nil {
		// The job is already durable in Postgres. If Redis is down, the sweeper
		// will find this pending job and enqueue it later, so we still say 201.
		if err := s.Queue.Enqueue(r.Context(), job.Queue, job.ID); err != nil {
			s.Log.Warn("enqueue failed; sweeper will retry", "job_id", job.ID, "err", err)
		}
	}
	writeJSON(w, http.StatusCreated, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be an integer")
		return
	}
	job, err := s.Store.GetJob(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		s.Log.Error("get job", "err", err)
		writeError(w, http.StatusInternalServerError, "could not load job")
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listDead(w http.ResponseWriter, r *http.Request) {
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
			return
		}
		limit = n
	}
	jobs, err := s.Store.ListDead(r.Context(), limit)
	if err != nil {
		s.Log.Error("list dead", "err", err)
		writeError(w, http.StatusInternalServerError, "could not list dead jobs")
		return
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func newKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return "auto-" + hex.EncodeToString(b)
}
