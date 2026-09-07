package compat

import (
	"strconv"
	"testing"
	"time"

	"github.com/tidwall/resp"
)

// Generic key-space coverage. The string suite above is a direct port of
// SugarDB's cases; these follow the same black-box style and pin the Redis
// semantics for the commands SugarDB's generic suite covers (SET options, DEL,
// TTL, EXPIRE, PERSIST, KEYS, SCAN, RENAME).

func TestSetOptions(t *testing.T) {
	c := setup(t)

	// EX sets a TTL in seconds.
	assertStr(t, do(t, c, "SET", "ex", "v", "EX", "100"), "OK")
	assertInt(t, do(t, c, "TTL", "ex"), 100)

	// PX sets a TTL in milliseconds.
	assertStr(t, do(t, c, "SET", "px", "v", "PX", "5000"), "OK")
	if ttl := do(t, c, "PTTL", "px").Integer(); ttl <= 0 || ttl > 5000 {
		t.Errorf("PTTL after PX 5000 = %d, want (0,5000]", ttl)
	}

	// NX only writes when absent. A successful SET replies +OK; a rejected one
	// replies nil. (SETNX, the dedicated command, replies with an integer.)
	assertStr(t, do(t, c, "SET", "nx", "first", "NX"), "OK")
	if v := do(t, c, "SET", "nx", "second", "NX"); !v.IsNull() {
		t.Errorf("SET NX on existing key should return nil, got %q", v.String())
	}
	assertStr(t, do(t, c, "GET", "nx"), "first")

	// XX only writes when present.
	if v := do(t, c, "SET", "xx-absent", "v", "XX"); !v.IsNull() {
		t.Errorf("SET XX on absent key should return nil, got %q", v.String())
	}
	preset(t, c, "xx", "old")
	assertStr(t, do(t, c, "SET", "xx", "new", "XX"), "OK")
	assertStr(t, do(t, c, "GET", "xx"), "new")

	// GET returns the previous value; on a fresh key there is none.
	if v := do(t, c, "SET", "get", "second", "GET"); !v.IsNull() {
		t.Errorf("SET ... GET on a fresh key should reply nil, got %q", v.String())
	}
	assertStr(t, do(t, c, "SET", "get", "third", "GET"), "second")

	// KEEPTTL preserves the TTL on overwrite.
	assertStr(t, do(t, c, "SET", "kt", "v", "EX", "100"), "OK")
	assertStr(t, do(t, c, "SET", "kt", "v2", "KEEPTTL"), "OK")
	if ttl := do(t, c, "TTL", "kt").Integer(); ttl <= 0 || ttl > 100 {
		t.Errorf("KEEPTTL did not preserve the TTL: got %d", ttl)
	}

	// NX and XX together is a syntax error.
	assertErr(t, do(t, c, "SET", "k", "v", "NX", "XX"), errSyntax)
	// A non-positive expiry is rejected.
	assertErr(t, do(t, c, "SET", "k", "v", "EX", "0"), errInvalidExp)
	// An unknown option is a syntax error.
	assertErr(t, do(t, c, "SET", "k", "v", "BOGUS"), errSyntax)
	// Too few arguments.
	assertErr(t, do(t, c, "SET", "k"), errWrongArgs)
}

func TestSetExAndPSetEx(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "SETEX", "a", "100", "v"), "OK")
	assertInt(t, do(t, c, "TTL", "a"), 100)
	assertStr(t, do(t, c, "PSETEX", "b", "5000", "v"), "OK")
	if ttl := do(t, c, "PTTL", "b").Integer(); ttl <= 0 || ttl > 5000 {
		t.Errorf("PTTL after PSETEX = %d", ttl)
	}
	assertErr(t, do(t, c, "SETEX", "a", "0", "v"), errInvalidExp)
	assertErr(t, do(t, c, "SETEX", "a", "1"), errWrongArgs)
}

func TestExpiry(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "v")

	// No TTL: -1. Missing key: -2.
	assertInt(t, do(t, c, "TTL", "k"), -1)
	assertInt(t, do(t, c, "TTL", "missing"), -2)
	assertInt(t, do(t, c, "PTTL", "missing"), -2)

	assertInt(t, do(t, c, "EXPIRE", "k", "60"), 1)
	if ttl := do(t, c, "TTL", "k").Integer(); ttl != 60 {
		t.Errorf("TTL after EXPIRE 60 = %d", ttl)
	}

	// NX is a no-op when a TTL is already set.
	assertInt(t, do(t, c, "EXPIRE", "k", "120", "NX"), 0)
	// XX applies when a TTL exists.
	assertInt(t, do(t, c, "EXPIRE", "k", "120", "XX"), 1)
	// GT only extends, LT only shortens.
	assertInt(t, do(t, c, "EXPIRE", "k", "30", "GT"), 0)
	assertInt(t, do(t, c, "EXPIRE", "k", "30", "LT"), 1)
	if ttl := do(t, c, "TTL", "k").Integer(); ttl != 30 {
		t.Errorf("TTL after LT = %d, want 30", ttl)
	}

	assertInt(t, do(t, c, "PERSIST", "k"), 1)
	assertInt(t, do(t, c, "TTL", "k"), -1)
	// PERSIST on a key without a TTL returns 0.
	assertInt(t, do(t, c, "PERSIST", "k"), 0)

	// An expiry in the past deletes the key.
	assertInt(t, do(t, c, "EXPIRE", "k", "-1"), 1)
	assertInt(t, do(t, c, "EXISTS", "k"), 0)

	// EXPIREAT takes an absolute unix timestamp.
	preset(t, c, "abs", "v")
	assertInt(t, do(t, c, "EXPIREAT", "abs", strconvFuture()), 1)
	if ttl := do(t, c, "TTL", "abs").Integer(); ttl <= 0 {
		t.Errorf("TTL after EXPIREAT = %d", ttl)
	}
}

func strconvFuture() string {
	return strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
}

func TestExpiryActuallyExpires(t *testing.T) {
	c := setup(t)
	preset(t, c, "short", "v")
	assertInt(t, do(t, c, "PEXPIRE", "short", "150"), 1)

	time.Sleep(300 * time.Millisecond)
	if v := do(t, c, "GET", "short"); !v.IsNull() {
		t.Errorf("key should have expired, got %q", v.String())
	}
	assertInt(t, do(t, c, "EXISTS", "short"), 0)
}

func TestDelUnlinkExists(t *testing.T) {
	c := setup(t)
	preset(t, c, "a", "1")
	preset(t, c, "b", "2")

	assertInt(t, do(t, c, "DEL", "a"), 1)
	assertInt(t, do(t, c, "DEL", "a"), 0) // already gone
	assertInt(t, do(t, c, "UNLINK", "b"), 1)
	assertInt(t, do(t, c, "EXISTS", "a", "b"), 0)
	assertErr(t, do(t, c, "DEL"), errWrongArgs)
}

func TestRenameProtocol(t *testing.T) {
	c := setup(t)
	preset(t, c, "src", "value")

	assertStr(t, do(t, c, "RENAME", "src", "dst"), "OK")
	assertInt(t, do(t, c, "EXISTS", "src"), 0)
	assertStr(t, do(t, c, "GET", "dst"), "value")

	// RENAMENX refuses to overwrite and reports 0.
	preset(t, c, "other", "x")
	assertInt(t, do(t, c, "RENAMENX", "dst", "other"), 0)
	assertInt(t, do(t, c, "RENAMENX", "dst", "fresh"), 1)
	assertStr(t, do(t, c, "GET", "fresh"), "value")

	assertErr(t, do(t, c, "RENAME", "nope", "x"), errNoSuchKey)
}

func TestKeysPattern(t *testing.T) {
	c := setup(t)
	for _, k := range []string{"user:1", "user:2", "post:1"} {
		preset(t, c, k, "v")
	}

	all := do(t, c, "KEYS", "*").Array()
	if len(all) != 3 {
		t.Errorf("KEYS * returned %d, want 3", len(all))
	}
	users := do(t, c, "KEYS", "user:*").Array()
	if len(users) != 2 {
		t.Errorf("KEYS user:* returned %d, want 2", len(users))
	}
	if n := len(do(t, c, "KEYS", "nope*").Array()); n != 0 {
		t.Errorf("KEYS nope* returned %d, want 0", n)
	}
	assertErr(t, do(t, c, "KEYS"), errWrongArgs)
}

func TestScanIteratesAllKeys(t *testing.T) {
	c := setup(t)
	const n = 50
	for i := range n {
		preset(t, c, "k"+itoa(i), "v")
	}

	seen := map[string]bool{}
	cursor := "0"
	for {
		res := do(t, c, "SCAN", cursor, "COUNT", "10")
		if res.Type() != resp.Array {
			t.Fatalf("SCAN should return an array, got %s", res.Type())
		}
		parts := res.Array()
		cursor = parts[0].String()
		for _, k := range parts[1].Array() {
			seen[k.String()] = true
		}
		if cursor == "0" {
			break
		}
	}
	if len(seen) != n {
		t.Errorf("SCAN visited %d keys, want %d", len(seen), n)
	}
}

func TestIncrDecr(t *testing.T) {
	c := setup(t)

	assertInt(t, do(t, c, "INCR", "n"), 1)
	assertInt(t, do(t, c, "INCR", "n"), 2)
	assertInt(t, do(t, c, "INCRBY", "n", "10"), 12)
	assertInt(t, do(t, c, "DECR", "n"), 11)
	assertInt(t, do(t, c, "DECRBY", "n", "5"), 6)

	// A non-numeric value is an error.
	preset(t, c, "s", "abc")
	assertErr(t, do(t, c, "INCR", "s"), errNotInt)

	// Overflowing the 64-bit range is an error.
	preset(t, c, "big", "9223372036854775807")
	assertErr(t, do(t, c, "INCR", "big"), "increment or decrement would overflow")
}

func TestFlushDBAndAll(t *testing.T) {
	c := setup(t)
	preset(t, c, "a", "1")
	preset(t, c, "b", "2")
	assertInt(t, do(t, c, "DBSIZE"), 2)

	assertStr(t, do(t, c, "FLUSHDB"), "OK")
	assertInt(t, do(t, c, "DBSIZE"), 0)

	preset(t, c, "c", "3")
	assertStr(t, do(t, c, "FLUSHALL"), "OK")
	assertInt(t, do(t, c, "DBSIZE"), 0)
}

func TestDatabaseIsolationOverProtocol(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "db0")

	assertStr(t, do(t, c, "SELECT", "1"), "OK")
	assertStr(t, do(t, c, "SET", "k", "db1"), "OK")
	assertStr(t, do(t, c, "GET", "k"), "db1")

	assertStr(t, do(t, c, "SELECT", "0"), "OK")
	assertStr(t, do(t, c, "GET", "k"), "db0")
}

func TestPipelineStaysInSync(t *testing.T) {
	c := setup(t)

	// Write a whole pipeline with no reads in between, then read the replies
	// in order. This is what exercises redcon's batched parse/flush path.
	for _, cmd := range [][]string{
		{"SET", "p1", "a"},
		{"SET", "p2", "b"},
		{"INCR", "p3"},
		{"GET", "p1"},
		{"GET", "p2"},
		{"GET", "missing"},
	} {
		vals := make([]resp.Value, len(cmd))
		for i, a := range cmd {
			vals[i] = resp.StringValue(a)
		}
		if err := c.WriteArray(vals); err != nil {
			t.Fatalf("write %v: %v", cmd, err)
		}
	}
	for range 6 {
		if _, _, err := c.ReadValue(); err != nil {
			t.Fatalf("read pipelined reply: %v", err)
		}
	}

	// The connection must still be usable and correctly aligned.
	assertStr(t, do(t, c, "GET", "p2"), "b")
	assertStr(t, do(t, c, "PING"), "PONG")
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	b := make([]byte, 0, 8)
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}
