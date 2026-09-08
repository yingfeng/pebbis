// Package tcl hosts the mechanical port of frogdb's redis-regression suite
// (Redis 8.6.0 unit/*.tcl scenarios) to go-redis against redistored.
//
// Each generated test keeps its upstream Rust function name, so failures
// trace straight back to the corresponding redis-regression source file.
package tcl

import (
	"context"
	"fmt"
	"math"
	"net"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redistore/redistore"
)

var (
	ctx  = context.Background()
	ctxo sync.Once
)

func contextCtx() context.Context { ctxo.Do(func() {}); return ctx }

// --- shared server singleton -------------------------------------------------
// Opening an in-memory Pebble store + RESP server per test (the original
// per-test startServer) costs ~100-300ms of setup/teardown each. With ~90
// ported tests in a batch that is 10-30s of pure overhead before any command
// runs. All tcl scenarios are independent functional checks, so we boot ONE
// server for the whole package and isolate tests with an automatic FLUSHDB.
// This cuts the startup cost to a single open/close around TestMain.

var (
	sharedStore *redistore.Store
	sharedSrv   *redistore.Server
	sharedAddr  string
	sharedOnce  sync.Once
)

func bootShared() {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("listen: %v", err))
	}
	store, err := redistore.Open(redistore.DefaultOptions())
	if err != nil {
		_ = ln.Close()
		panic(fmt.Sprintf("open store: %v", err))
	}
	srv := redistore.NewServer(store, redistore.ServerOptions{Addr: ln.Addr().String()})
	go func() { _ = srv.Serve(ln) }()
	sharedStore, sharedSrv, sharedAddr = store, srv, ln.Addr().String()
}

// startServer returns the address of the shared server, flushing the keyspace
// first so each test begins from a clean state.
func startServer(t testing.TB) string {
	t.Helper()
	sharedOnce.Do(bootShared)
	flush := redis.NewClient(&redis.Options{Addr: sharedAddr, Protocol: 2})
	defer func() { _ = flush.Close() }()
	// A prior test may have failed mid-way with a low maxmemory configured;
	// reset it first so FLUSHDB (a write) cannot be rejected with OOM.
	_ = flush.Do(ctx, "CONFIG", "SET", "maxmemory", "0")
	if err := flush.FlushDB(ctx).Err(); err != nil {
		t.Fatalf("flush before test: %v", err)
	}
	return sharedAddr
}

func TestMain(m *testing.M) {
	code := m.Run()
	if sharedSrv != nil {
		_ = sharedSrv.Close()
	}
	if sharedStore != nil {
		_ = sharedStore.Close()
	}
	os.Exit(code)
}

// connect opens a client to the server, mirroring server.connect().
func connect(t testing.TB, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// connectConn opens a dedicated single connection to the server. MULTI/EXEC,
// WATCH, SUBSCRIBE and XREADGROUP(BLOCK) all require every command to travel on
// one underlying TCP connection, so the pooled *redis.Client (whose Do() borrows
// a different connection each call) cannot be used for them. frogdb's test
// client is itself a single connection, so this mirrors that semantics.
func connectConn(t testing.TB, addr string) *redis.Conn {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2})
	conn := c.Conn()
	t.Cleanup(func() {
		_ = conn.Close()
		_ = c.Close()
	})
	return conn
}

// reply is one command reply, shaped like frogdb's Response enum.
type reply struct {
	val interface{}
	err error
}

// cDo sends one command and captures the reply. Args follow Go-redis' flat
// string form: cDo(t, client, "SET", "k", "v").
func cDo(t testing.TB, c *redis.Client, args ...interface{}) reply {
	t.Helper()
	cmd := c.Do(ctx, args...)
	v, err := cmd.Result()
	if err == redis.Nil {
		// RESP null bulk is a legitimate nil reply, not a transport error.
		// go-redis surfaces it as redis.Nil, but our assertions treat nil
		// replies (key missing, NX/XX rejected, etc.) as success values.
		return reply{val: nil, err: nil}
	}
	return reply{val: v, err: err}
}

// --- assertion helpers mirroring frogdb_test_harness::response ---

func assertOK(t testing.TB, r reply) {
	t.Helper()
	if r.err != nil || r.val != "OK" {
		t.Fatalf("expected OK, got %v (err %v)", r.val, r.err)
	}
}

func assertQueued(t testing.TB, r reply) {
	t.Helper()
	if r.err != nil || r.val != "QUEUED" {
		t.Fatalf("expected QUEUED, got %v (err %v)", r.val, r.err)
	}
}

func assertIntegerEq(t testing.TB, r reply, want int64) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("expected integer %d, got err %v", want, r.err)
	}
	n, ok := r.val.(int64)
	if !ok || n != want {
		t.Fatalf("expected integer %d, got %v", want, r.val)
	}
}

func assertBulkEq(t testing.TB, r reply, want string) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("expected bulk %q, got err %v", want, r.err)
	}
	s, ok := r.val.(string)
	if !ok || s != want {
		t.Fatalf("expected bulk %q, got %v", want, r.val)
	}
}

func assertNil(t testing.TB, r reply) {
	t.Helper()
	if r.err != nil || r.val != nil {
		t.Fatalf("expected nil, got %v (err %v)", r.val, r.err)
	}
}

func assertErrorPrefix(t testing.TB, r reply, prefix string) {
	t.Helper()
	if r.err == nil {
		t.Fatalf("expected error starting with %q, got %v", prefix, r.val)
	}
	if !startsWith(r.err.Error(), prefix) {
		t.Fatalf("expected error starting with %q, got %q", prefix, r.err.Error())
	}
}

func startsWith(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func assertArrayLen(t testing.TB, r interface{}, want int) {
	t.Helper()
	var n int
	switch v := r.(type) {
	case reply:
		if v.err != nil {
			t.Fatalf("expected array of %d, got err %v", want, v.err)
		}
		a, ok := v.val.([]interface{})
		if !ok {
			t.Fatalf("expected array of %d, got %v", want, v.val)
		}
		n = len(a)
	case []reply:
		n = len(v)
	default:
		t.Fatalf("assertArrayLen: unsupported type %T", r)
	}
	if n != want {
		t.Fatalf("expected array of %d, got %d", want, n)
	}
}

func assertFloatEq(t testing.TB, r reply, want float64) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("assertFloatEq: err %v", r.err)
	}
	s, ok := r.val.(string)
	if !ok {
		t.Fatalf("assertFloatEq: expected bulk string, got %v (%T)", r.val, r.val)
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		t.Fatalf("assertFloatEq: parse %q: %v", s, err)
	}
	if math.Abs(f-want) > 1e-10 {
		t.Fatalf("assertFloatEq: expected %v, got %v", want, f)
	}
}

// cDoConn sends one command on a dedicated connection (redis.Conn), needed for
// MULTI/EXEC transactions and (blocking) SUBSCRIBE, where all commands must
// share a single underlying TCP connection rather than the client pool.
func cDoConn(t testing.TB, c *redis.Conn, args ...interface{}) reply {
	t.Helper()
	cmd := c.Do(ctx, args...)
	v, err := cmd.Result()
	if err == redis.Nil {
		return reply{val: nil, err: nil}
	}
	return reply{val: v, err: err}
}

// unwrapArray returns the elements of an array reply.
func unwrapArray(t testing.TB, r reply) []reply {
	t.Helper()
	a, ok := r.val.([]interface{})
	if !ok {
		t.Fatalf("expected array, got %v (err %v)", r.val, r.err)
	}
	out := make([]reply, len(a))
	for i, v := range a {
		// Error replies nested inside an EXEC array arrive as go-redis'
		// RedisError (an error-implementing string); surface them as errors
		// so the assert helpers see them like frogdb's Response::Error.
		if err, isErr := v.(error); isErr {
			out[i] = reply{err: err}
			continue
		}
		out[i] = reply{val: v}
	}
	return out
}

// extractBulkStrings flattens an array reply to its string elements, skipping
// non-strings (same contract as frogdb's extract_bulk_strings).
func extractBulkStrings(t testing.TB, r reply) []string {
	t.Helper()
	a, ok := r.val.([]interface{})
	if !ok {
		t.Fatalf("expected array, got %v (err %v)", r.val, r.err)
	}
	out := []string{}
	for _, v := range a {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}
	return out
}

// parseBulkString / unwrapBulk return the single bulk-string payload of a
// reply, mirroring frogdb's parse_bulk_string / unwrap_bulk.
func parseBulkString(t testing.TB, r reply) string {
	t.Helper()
	if r.err != nil {
		t.Fatalf("parseBulkString: err %v", r.err)
	}
	s, ok := r.val.(string)
	if !ok {
		t.Fatalf("parseBulkString: expected bulk string, got %v", r.val)
	}
	return s
}

func unwrapBulk(t testing.TB, r reply) string {
	t.Helper()
	return parseBulkString(t, r)
}

// unwrapBpopResponse returns the (key, value) pair of a blocking-pop reply
// (a 2-element array [key, value]), mirroring frogdb's unwrap_bpop_response.
func unwrapBpopResponse(t testing.TB, r reply) (string, string) {
	t.Helper()
	if r.err != nil {
		t.Fatalf("unwrapBpopResponse: err %v", r.err)
	}
	a, ok := r.val.([]interface{})
	if !ok || len(a) != 2 {
		t.Fatalf("unwrapBpopResponse: expected 2-element array, got %v", r.val)
	}
	k, ok := a[0].(string)
	if !ok {
		t.Fatalf("unwrapBpopResponse: key not a string: %v", a[0])
	}
	v, ok := a[1].(string)
	if !ok {
		t.Fatalf("unwrapBpopResponse: value not a string: %v", a[1])
	}
	return k, v
}

func assertStringsEq(t testing.TB, got []string, want ...string) {
	t.Helper()
	// Normalize empty slices: extractBulkStrings returns []string{} (non-nil)
	// for an empty array, while a variadic with no args is nil. Redis treats
	// both as "no elements", so compare by length/content, not pointer identity.
	if len(got) == 0 {
		got = nil
	}
	w := []string(want)
	if len(w) == 0 {
		w = nil
	}
	if !reflect.DeepEqual(got, w) {
		t.Fatalf("got %v, want %v", got, w)
	}
}

// unwrapInteger returns the integer payload of a reply, mirroring frogdb's
// unwrap_integer.
func unwrapInteger(t testing.TB, r reply) int64 {
	t.Helper()
	if r.err != nil {
		t.Fatalf("unwrapInteger: err %v", r.err)
	}
	n, ok := r.val.(int64)
	if !ok {
		t.Fatalf("unwrapInteger: expected integer, got %v", r.val)
	}
	return n
}

// assertBulkEqAny handles assert_bulk_eq(&items[N], b'...') where items is
// either a []string (from extractBulkStrings) or a []reply (from unwrapArray).
func assertBulkEqAny(t testing.TB, got interface{}, want string) {
	t.Helper()
	switch v := got.(type) {
	case string:
		if v != want {
			t.Fatalf("expected %q, got %q", want, v)
		}
	case reply:
		assertBulkEq(t, v, want)
	default:
		t.Fatalf("assertBulkEqAny: unsupported type %T", got)
	}
}

var _ = time.Second
