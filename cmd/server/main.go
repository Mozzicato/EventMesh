// Command server runs the EventMesh HTTP API and a worker pool in one process.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Mozzicato/EventMesh/internal/api"
	"github.com/Mozzicato/EventMesh/internal/latency"
	"github.com/Mozzicato/EventMesh/internal/queue"
	"github.com/Mozzicato/EventMesh/internal/store"
	"github.com/Mozzicato/EventMesh/internal/worker"
)

type config struct {
	addr      string
	pgDSN     string
	redisAddr string
	queue     string
	workers   int
	bench     bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.addr, "addr", ":8080", "HTTP listen address")
	flag.StringVar(&cfg.pgDSN, "pg", envOr("EVENTMESH_PG",
		"postgres://eventmesh:eventmesh@localhost:5432/eventmesh?sslmode=disable&pool_max_conns=20"),
		"PostgreSQL DSN")
	flag.StringVar(&cfg.redisAddr, "redis", envOr("EVENTMESH_REDIS", "localhost:6379"), "Redis address")
	flag.StringVar(&cfg.queue, "queue", "default", "queue this server's worker pool consumes")
	flag.IntVar(&cfg.workers, "workers", 16, "number of worker goroutines")
	flag.BoolVar(&cfg.bench, "bench", false,
		"record per-job end-to-end latency and serve it at /debug/latencies/* (load tests only)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(cfg, log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

// handlers are the job types this server can run.
var handlers = map[string]worker.Handler{
	// noop does nothing.
	"noop": func(context.Context, json.RawMessage) error { return nil },
	// hash computes SHA-256 of the payload: a small, real piece of CPU work.
	// The load test uses it.
	"hash": func(_ context.Context, payload json.RawMessage) error {
		_ = sha256.Sum256(payload)
		return nil
	},
}

func run(cfg config, log *slog.Logger) error {
	// ctx is cancelled on Ctrl+C / SIGTERM; that starts shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.pgDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.redisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return err
	}
	q := queue.New(rdb)

	jobTypes := map[string]bool{}
	for name := range handlers {
		jobTypes[name] = true
	}
	srv := &api.Server{
		Store:    st,
		Queue:    q,
		Queues:   map[string]bool{cfg.queue: true},
		JobTypes: jobTypes,
		Log:      log,
		Now:      time.Now,
	}
	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	httpSrv := &http.Server{
		Addr:              cfg.addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	pool := &worker.Pool{
		Store:    st,
		Queue:    q,
		Handlers: handlers,
		Config:   worker.Config{Queue: cfg.queue, Workers: cfg.workers},
		Now:      time.Now,
		Log:      log,
	}
	if cfg.bench {
		rec := &latency.Recorder{}
		pool.OnSucceeded = rec.Record
		mux.HandleFunc("GET /debug/latencies/count", rec.CountHandler)
		mux.HandleFunc("POST /debug/latencies/take", rec.TakeHandler)
		log.Info("bench mode: recording per-job latency")
	}

	httpErr := make(chan error, 1)
	go func() { httpErr <- httpSrv.ListenAndServe() }()
	poolDone := make(chan error, 1)
	go func() { poolDone <- pool.Run(ctx) }()
	log.Info("listening", "addr", cfg.addr, "queue", cfg.queue, "workers", cfg.workers)

	select {
	case err := <-httpErr:
		return err
	case err := <-poolDone:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	// 1. Stop accepting HTTP requests (in-flight requests finish).
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http shutdown", "err", err)
	}
	// 2. The pool saw ctx cancelled too: wait for it to drain.
	return <-poolDone
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
