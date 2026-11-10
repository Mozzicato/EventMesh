// Package testutil connects tests to the real Postgres and Redis started by
// docker compose (locally) or by CI service containers. Tests fail, rather than
// skip, when the databases are missing, so a broken setup can't look green.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/Mozzicato/EventMesh/internal/store"
)

// PostgresDSN returns the test database DSN.
func PostgresDSN() string {
	if dsn := os.Getenv("EVENTMESH_TEST_PG"); dsn != "" {
		return dsn
	}
	return "postgres://eventmesh:eventmesh@localhost:5432/eventmesh?sslmode=disable&pool_max_conns=20"
}

// RedisAddr returns the test Redis address.
func RedisAddr() string {
	if addr := os.Getenv("EVENTMESH_TEST_REDIS"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

// Store opens a store against the test database and closes it after the test.
func Store(t testing.TB) *store.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.Open(ctx, PostgresDSN())
	if err != nil {
		t.Fatalf("open store (is `docker compose up -d` running?): %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

// RandomID returns a random hex string, used to keep tests from colliding on
// idempotency keys or Redis key prefixes.
func RandomID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
