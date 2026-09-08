// Package compat holds the protocol-level compatibility suite.
//
// The cases are ported from SugarDB's command tests, which drive the server
// over real TCP with a RESP client - exactly the level a compatibility suite
// should target. Where SugarDB encoded its own behaviour rather than Redis'
// (it errors on GETRANGE of a missing key, for instance, where Redis returns an
// empty string), the expectation has been changed to the Redis behaviour and
// marked with a NOTE comment, because matching Redis is the whole point here.
package compat

import (
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pebbis/pebbis"
	"github.com/pebbis/pebbis/config"
	"github.com/tidwall/resp"
)

// Error substrings, taken from real Redis replies.
const (
	errWrongArgs  = "wrong number of arguments"
	errNotInt     = "value is not an integer or out of range"
	errWrongType  = "WRONGTYPE"
	errSyntax     = "syntax error"
	errInvalidExp = "invalid expire time"
	errOutOfRange = "out of range"
	errUnknownCmd = "unknown command"
	errDBIndex    = "DB index is out of range"
	errNoSuchKey  = "no such key"
	errExceedsMax = "exceeds maximum allowed size"
)

// setup runs setupWith using the default options.
func setup(t *testing.T) *resp.Conn { return setupWith(t, nil) }

// setupWith starts a store plus its RESP listener on an ephemeral port and
// returns a connected RESP client. mutate, when set, adjusts the options before
// opening. Everything is torn down with the test.
func setupWith(t *testing.T, mutate func(*config.Config)) *resp.Conn {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg := pebbis.DefaultOptions()
	if mutate != nil {
		mutate(cfg)
	}
	store, err := pebbis.Open(cfg)
	if err != nil {
		_ = ln.Close()
		t.Fatalf("open store: %v", err)
	}
	srv := pebbis.NewServer(store, pebbis.ServerOptions{Addr: ln.Addr().String()})

	go func() { _ = srv.Serve(ln) }()

	t.Cleanup(func() {
		_ = srv.Close()
		_ = store.Close()
	})

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))

	rc := resp.NewConn(conn)
	addrs.Store(rc, ln.Addr().String())
	t.Cleanup(func() { addrs.Delete(rc) })
	return rc
}

// addrs maps a client back to the address it is connected to, so a test can
// open a second connection to the same server (needed for pub/sub).
var addrs sync.Map

// setupResult bundles a client with its own teardown, for tests that restart
// a store on a fixed directory.
type setupResult struct {
	c        *resp.Conn
	teardown func()
}

// setupPersistent opens a store on the given directory and serves it, handing
// back a client plus the teardown that closes everything. Two calls with the
// same directory model a stop/restart cycle.
func setupPersistent(t *testing.T, dir string) *setupResult {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	cfg := pebbis.DefaultOptions()
	cfg.Dir = dir
	cfg.SyncPolicy = config.SyncAlways
	store, err := pebbis.Open(cfg)
	if err != nil {
		_ = ln.Close()
		t.Fatalf("open store: %v", err)
	}
	srv := pebbis.NewServer(store, pebbis.ServerOptions{Addr: ln.Addr().String()})
	go func() { _ = srv.Serve(ln) }()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = srv.Close()
		_ = store.Close()
		_ = ln.Close()
		t.Fatalf("dial: %v", err)
	}
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	rc := resp.NewConn(conn)

	return &setupResult{c: rc, teardown: func() {
		_ = srv.Close()
		_ = store.Close()
		_ = conn.Close()
		_ = ln.Close()
		addrs.Delete(rc)
	}}
}

// serverAddrOf returns the address a client is connected to.
func serverAddrOf(t *testing.T, c *resp.Conn) string {
	t.Helper()
	if v, ok := addrs.Load(c); ok {
		return v.(string)
	}
	t.Fatal("client was not created by setup()")
	return ""
}

// do writes a command and reads one reply.
func do(t *testing.T, c *resp.Conn, args ...string) resp.Value {
	t.Helper()
	vals := make([]resp.Value, len(args))
	for i, a := range args {
		vals[i] = resp.StringValue(a)
	}
	if err := c.WriteArray(vals); err != nil {
		t.Fatalf("write %v: %v", args, err)
	}
	v, _, err := c.ReadValue()
	if err != nil {
		t.Fatalf("read reply to %v: %v", args, err)
	}
	return v
}

// preset seeds a key and asserts the write succeeded.
func preset(t *testing.T, c *resp.Conn, key, val string) {
	t.Helper()
	if got := do(t, c, "SET", key, val).String(); !strings.EqualFold(got, "OK") {
		t.Fatalf("preset SET %s: got %q, want OK", key, got)
	}
}

// assertErr checks that v is an error containing want.
func assertErr(t *testing.T, v resp.Value, want string) bool {
	t.Helper()
	if v.Type() != resp.Error {
		t.Errorf("expected an error containing %q, got %s reply %q", want, v.Type(), v.String())
		return false
	}
	if !strings.Contains(v.String(), want) {
		t.Errorf("expected error containing %q, got %q", want, v.String())
		return false
	}
	return true
}

// assertInt checks an integer reply.
func assertInt(t *testing.T, v resp.Value, want int) {
	t.Helper()
	if v.Integer() != want {
		t.Errorf("expected %d, got %d (%s)", want, v.Integer(), v.String())
	}
}

// assertStr checks a bulk/simple string reply.
func assertStr(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.String() != want {
		t.Errorf("expected %q, got %q", want, v.String())
	}
}

// nowPlus returns a deadline helper for pushes from goroutines.
func nowPlus(seconds int) time.Time {
	return time.Now().Add(time.Duration(seconds) * time.Second)
}
