// Command server runs the EventMesh HTTP API.
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mozzicato/EventMesh/internal/api"
	"github.com/Mozzicato/EventMesh/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	pgDSN := flag.String("pg", envOr("EVENTMESH_PG",
		"postgres://eventmesh:eventmesh@localhost:5432/eventmesh?sslmode=disable&pool_max_conns=20"),
		"PostgreSQL DSN")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(*addr, *pgDSN, log); err != nil {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr, pgDSN string, log *slog.Logger) error {
	// ctx is cancelled on Ctrl+C / SIGTERM; that starts graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, pgDSN)
	if err != nil {
		return err
	}
	defer st.Close()

	srv := &api.Server{
		Store:    st,
		JobTypes: map[string]bool{"noop": true},
		Log:      log,
		Now:      time.Now,
	}
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Info("listening", "addr", addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
