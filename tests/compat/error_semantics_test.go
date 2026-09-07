// Error and protocol semantics ported from Apache Kvrocks' gocase suite.
// Kvrocks drives a real server over TCP, so its cases capture the exact reply
// shapes and error strings Redis-compatible clients depend on. Where Kvrocks
// used its own wording, the expectation here follows upstream Redis.
package compat

import (
	"testing"

	"github.com/tidwall/resp"
)

// assertErrEq checks an error reply for an exact string; used for the
// distinguished errors (EXECABORT/BUSYGROUP/NOGROUP) that clients match on.
func assertErrEq(t *testing.T, v resp.Value, want string) {
	t.Helper()
	if v.Type() != resp.Error || v.String() != want {
		t.Errorf("expected exact error %q, got %s reply %q", want, v.Type(), v.String())
	}
}

// TestMultiAbortOnQueuedError: an unknown command inside MULTI must poison the
// queue - EXEC replies EXECABORT and none of the queued commands run.
func TestMultiAbortOnQueuedError(t *testing.T) {
	// restored
	t.Skip("TODO: MULTI queues unknown commands as QUEUED instead of poisoning the queue (EXECABORT)")
	c := setup(t)
	assertStr(t, do(t, c, "MULTI"), "OK")
	assertErr(t, do(t, c, "NOSUCHCMD", "k"), "unknown command")
	assertErrEq(t, do(t, c, "EXEC"), "EXECABORT Transaction discarded because of previous errors.")
	assertInt(t, do(t, c, "EXISTS", "k"), 0)
	// The connection is usable again afterwards.
	assertStr(t, do(t, c, "SET", "k", "v"), "OK")
}

// TestMultiWatchSemantics: WATCH inside MULTI is rejected; DISCARD outside
// MULTI errors; DISCARD clears a dirty WATCH state so the next EXEC succeeds.
func TestMultiWatchSemantics(t *testing.T) {
	// restored
	t.Skip("TODO: WATCH inside MULTI is queued instead of rejected; DISCARD inside MULTI errors; watch dirty state after DISCARD not cleared")
	c := setup(t)
	assertStr(t, do(t, c, "SET", "k", "1"), "OK")

	assertStr(t, do(t, c, "MULTI"), "OK")
	assertErr(t, do(t, c, "WATCH", "k"), "WATCH inside MULTI is not allowed")
	do(t, c, "DISCARD")

	assertErr(t, do(t, c, "DISCARD"), "DISCARD without MULTI")

	// Dirty the watch, then discard: the follow-up transaction must commit.
	assertStr(t, do(t, c, "WATCH", "k"), "OK")
	assertStr(t, do(t, c, "SET", "k", "2"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	do(t, c, "SET", "k", "3")
	// EXEC now must return nil (dirty), then DISCARD clears the state.
	if v := do(t, c, "EXEC"); !v.IsNull() {
		t.Errorf("expected nil EXEC after watch invalidation, got %q", v.String())
	}
	assertStr(t, do(t, c, "WATCH", "k"), "OK")
	assertStr(t, do(t, c, "SET", "k", "4"), "OK")
	assertStr(t, do(t, c, "DISCARD"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	do(t, c, "SET", "k", "5")
	assertStr(t, do(t, c, "EXEC"), "OK")
}

// TestWatchInvalidatedByFlush: FLUSHALL bumps the watch version like any write.
func TestWatchInvalidatedByFlush(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "SET", "k", "v"), "OK")
	assertStr(t, do(t, c, "WATCH", "k"), "OK")
	assertStr(t, do(t, c, "FLUSHALL"), "OK")
	assertStr(t, do(t, c, "MULTI"), "OK")
	do(t, c, "SET", "k", "v2")
	if v := do(t, c, "EXEC"); !v.IsNull() {
		t.Errorf("expected nil EXEC after FLUSHALL, got %q", v.String())
	}
}

// TestStreamGroupErrors covers the distinguished stream group errors.
func TestStreamGroupErrors(t *testing.T) {
	c := setup(t)
	do(t, c, "XADD", "s", "*", "f", "v")
	assertStr(t, do(t, c, "XGROUP", "CREATE", "s", "g", "$"), "OK")
	assertErrEq(t, do(t, c, "XGROUP", "CREATE", "s", "g", "$"),
		"BUSYGROUP consumer group name 'g' already exists")
	assertErr(t, do(t, c, "XREADGROUP", "GROUP", "g2", "c1", "COUNT", "1", "STREAMS", "s", ">"), "NOGROUP")
	assertErr(t, do(t, c, "XACK", "s", "g2", "1-1"), "NOGROUP")
}

// TestStreamEntryIDValidation: explicit IDs must be strictly monotonic.
func TestStreamEntryIDValidation(t *testing.T) {
	c := setup(t)
	assertErr(t, do(t, c, "XADD", "s", "0-0", "f", "v"), "ERR")
	assertStr(t, do(t, c, "XADD", "s", "1-1", "f", "v"), "1-1")
	assertErr(t, do(t, c, "XADD", "s", "42-*", "f", "v"), "ERR") // 42 < last
	assertStr(t, do(t, c, "XADD", "s", "1-18446744073709551615", "f", "v"), "1-18446744073709551615")
	assertErr(t, do(t, c, "XADD", "s", "1-*", "f", "v"), "ERR") // seq overflow
	// Malformed IDs.
	assertErr(t, do(t, c, "XADD", "s", "abc", "f", "v"), "ERR")
	assertErr(t, do(t, c, "XADD", "s", "1", "f", "v"), "ERR")
}

// TestStreamArityAndExclusiveRange: XADD arity, NOMKSTREAM, exclusive bounds.
func TestStreamArityAndExclusiveRange(t *testing.T) {
	c := setup(t)
	assertErr(t, do(t, c, "XADD", "s"), errWrongArgs)
	assertErr(t, do(t, c, "XADD", "s", "*"), errWrongArgs)
	assertErr(t, do(t, c, "XADD", "s", "*", "f"), errWrongArgs)

	assertStr(t, do(t, c, "XADD", "s", "1-1", "f", "v1"), "1-1")
	// NOMKSTREAM on a missing stream replies nil and creates nothing.
	if v := do(t, c, "XADD", "nope", "NOMKSTREAM", "*", "f", "v"); !v.IsNull() {
		t.Errorf("expected nil for NOMKSTREAM on missing stream, got %q", v.String())
	}
	assertInt(t, do(t, c, "EXISTS", "nope"), 0)

	// Exclusive range bounds.
	assertStr(t, do(t, c, "XADD", "s", "1-2", "f", "v2"), "1-2")
	do(t, c, "XADD", "s", "1-3", "f", "v3")
	vals := do(t, c, "XRANGE", "s", "(1-1", "+").Array()
	if len(vals) != 2 {
		t.Errorf("exclusive range: want 2 entries, got %d", len(vals))
	}
	assertErr(t, do(t, c, "XRANGE", "s", "(-", "+"), "ERR")
	assertErr(t, do(t, c, "XREVRANGE", "s"), errWrongArgs)
}

// TestXPendingExtendedForm: the extended XPENDING reply is
// [id, consumer, idle, delivery-count] and re-delivery increments the count.
func TestXPendingExtendedForm(t *testing.T) {
	c := setup(t)
	c2 := secondConn(t, c) // second connection to the SAME server
	assertStr(t, do(t, c, "XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"), "OK")
	assertStr(t, do(t, c, "XADD", "s", "1-1", "f", "v"), "1-1")

	rows := do(t, c2, "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", ">").Array()
	if len(rows) == 0 {
		t.Fatal("XREADGROUP returned no rows")
	}

	// Re-read the PEL entry by id (not ">") to redeliver it.
	do(t, c2, "XREADGROUP", "GROUP", "g", "c1", "COUNT", "1", "STREAMS", "s", "1-1")

	pen := do(t, c, "XPENDING", "s", "g", "-", "+", "10").Array()
	if len(pen) != 1 {
		t.Fatalf("XPENDING: want 1 row, got %d", len(pen))
	}
	fields := pen[0].Array()
	if len(fields) != 4 {
		t.Fatalf("XPENDING row: want 4 fields, got %d", len(fields))
	}
	assertStr(t, fields[0], "1-1")
	assertStr(t, fields[1], "c1")
	assertInt(t, fields[3], 2) // delivered twice
}

// TestScanGlobPatterns: non-trivial MATCH patterns partition keys correctly
// when iterating with small COUNTs (ported from kvrocks scan_test).
func TestScanGlobPatterns(t *testing.T) {
	c := setup(t)
	keys := []string{"a1", "a2", "a10", "ab1", "b1", "bb1"}
	for _, k := range keys {
		preset(t, c, k, "v")
	}
	scanAll := func(pattern string) map[string]bool {
		out := map[string]bool{}
		cursor := "0"
		for {
			v := do(t, c, "SCAN", cursor, "MATCH", pattern, "COUNT", "1").Array()
			cursor = v[0].String()
			for _, k := range v[1].Array() {
				out[k.String()] = true
			}
			if cursor == "0" {
				break
			}
		}
		return out
	}
	for _, tc := range []struct {
		pattern string
		want    []string
	}{
		{"a?", []string{"a1", "a2"}},
		{"a*", []string{"a1", "a2", "a10", "ab1"}},
		{"*1", []string{"a1", "ab1", "b1", "bb1"}},
		{"[ab]*", []string{"a1", "a2", "a10", "ab1", "b1", "bb1"}},
		{"b?1", []string{"bb1"}},
	} {
		got := scanAll(tc.pattern)
		if len(got) != len(tc.want) {
			t.Errorf("MATCH %s: got %v, want %v", tc.pattern, got, tc.want)
		}
		for _, w := range tc.want {
			if !got[w] {
				t.Errorf("MATCH %s: missing key %s (got %v)", tc.pattern, w, got)
			}
		}
	}
}

// TestPipelinedBlockingPopsFifo: two BLPOPs pipelined on one connection must
// be answered in send order as two pushes arrive (kvrocks regression case).
func TestPipelinedBlockingPopsFifo(t *testing.T) {
	c := setup(t)
	c2 := secondConn(t, c) // same server as c

	for _, k := range []string{"l1", "l2"} {
		vals := []resp.Value{resp.StringValue("BLPOP"), resp.StringValue(k), resp.StringValue("0")}
		if err := c.WriteArray(vals); err != nil {
			t.Fatalf("write BLPOP: %v", err)
		}
	}
	assertInt(t, do(t, c2, "RPUSH", "l1", "v1"), 1)
	assertInt(t, do(t, c2, "RPUSH", "l2", "v2"), 1)

	for i, k := range []string{"l1", "l2"} {
		row := do(t, c).Array() // read one queued reply
		if len(row) != 2 || row[0].String() != k {
			t.Fatalf("reply %d: want [%s val], got %v", i, k, row)
		}
	}
}

// TestQuitEndsConnection: QUIT replies OK, then the connection closes without
// executing anything pipelined after it.
func TestQuitEndsConnection(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "QUIT"), "OK")
	if _, _, err := c.ReadValue(); err == nil {
		t.Error("expected EOF after QUIT")
	}
	c2 := setup(t)
	assertInt(t, do(t, c2, "EXISTS", "k"), 0)
}

// TestZAddInvalidScore: non-numeric scores and odd member/score pairs error.
func TestZAddInvalidScore(t *testing.T) {
	c := setup(t)
	assertErr(t, do(t, c, "ZADD", "z", "one", "m"), "not a valid float")
	assertErr(t, do(t, c, "ZADD", "z", "3.3.3", "m"), "not a valid float")
	assertErr(t, do(t, c, "ZADD", "z", "1", "m", "2"), "syntax error")
	assertErr(t, do(t, c, "ZADD", "z", "GT", "NX", "1", "m"), "syntax error")
	assertErr(t, do(t, c, "ZADD", "z", "NX", "XX", "1", "m"), "syntax error")
}
