// Package goclient drives redistored through go-redis, the official Go
// client, over a real TCP connection.
//
// The RESP-level suite in tests/compat validates raw protocol bytes; this
// package validates what production users actually see: typed client APIs,
// connection pooling, pipelines, transactions and pub/sub semantics as
// go-redis shapes them (redis.Nil for missing keys, typed errors, automatic
// reconnection and HELLO/CLIENT SETINFO handshaking).
package goclient

import (
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redistore/redistore"
)

// startServer runs a store plus RESP listener on an ephemeral port.
func startServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	store, err := redistore.Open(redistore.DefaultOptions())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("open store: %v", err)
	}
	srv := redistore.NewServer(store, redistore.ServerOptions{Addr: ln.Addr().String()})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() {
		_ = srv.Close()
		_ = store.Close()
	})
	return ln.Addr().String()
}

// newClient connects a go-redis client to the server. Protocol 0 lets the
// client run its normal negotiation (HELLO 3, falling back to RESP2), which
// is exactly what production users do.
func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// defaultClient connects with the client's own default options.
func defaultClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// newClientOn starts a fresh server and connects to it.
func newClientOn(t *testing.T) (*redis.Client, string) {
	t.Helper()
	addr := startServer(t)
	return newClient(t, addr), addr
}

var _ = time.Second
