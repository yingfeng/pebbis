package compat

import (
	"strconv"
	"testing"
)

// Stream reliable-queue semantics: the reason this type exists at all.
// Each scenario follows the same shape a real worker pool would.

func TestStreamAddReadRange(t *testing.T) {
	c := setup(t)

	// Auto IDs must come back monotonically distinct.
	id1 := do(t, c, "XADD", "s", "*", "job", "a", "prio", "1").String()
	id2 := do(t, c, "XADD", "s", "*", "job", "b").String()
	if id1 == "" || id1 == id2 {
		t.Fatalf("XADD IDs not distinct: %q vs %q", id1, id2)
	}
	assertInt(t, do(t, c, "XLEN", "s"), 2)

	all := do(t, c, "XRANGE", "s", "-", "+").Array()
	if len(all) != 2 {
		t.Fatalf("XRANGE returned %d, want 2", len(all))
	}
	first := all[0].Array()
	if len(first) != 2 {
		t.Fatalf("entry shape wrong: %d elements", len(first))
	}
	if first[1].Array()[1].String() != "a" {
		t.Errorf("first entry payload = %v", first[1].Array())
	}

	rev := do(t, c, "XREVRANGE", "s", "+", "-", "COUNT", "1").Array()
	if len(rev) != 1 || rev[0].Array()[1].Array()[1].String() != "b" {
		t.Errorf("XREVRANGE = %v", rev)
	}

	// An explicit ID below the top item must be rejected.
	assertErr(t, do(t, c, "XADD", "s", "0-1", "job", "z"), "smaller than")
}

func TestConsumerGroupAckCycle(t *testing.T) {
	c := setup(t)

	assertStr(t, do(t, c, "XGROUP", "CREATE", "q", "workers", "$", "MKSTREAM"), "OK")

	for i := range 5 {
		_ = do(t, c, "XADD", "q", "*", "job", strconv.Itoa(i))
	}

	// A consumer reads 3; all three land in its PEL.
	batch := do(t, c, "XREADGROUP", "GROUP", "workers", "w1", "COUNT", "3", "STREAMS", "q", ">").Array()
	if len(batch) != 1 {
		t.Fatalf("XREADGROUP returned %d streams, want 1", len(batch))
	}
	entries := batch[0].Array()[1].Array()
	if len(entries) != 3 {
		t.Fatalf("delivered %d entries, want 3", len(entries))
	}
	ids := make([]string, 3)
	for i, e := range entries {
		ids[i] = e.Array()[0].String()
	}

	// Ack two; one stays pending.
	assertInt(t, do(t, c, "XACK", "q", "workers", ids[0], ids[1]), 2)

	// History read (0) returns only the un-acked entry.
	pending := do(t, c, "XREADGROUP", "GROUP", "workers", "w1", "COUNT", "10", "STREAMS", "q", "0").Array()
	if len(pending) != 1 {
		t.Fatalf("pending read returned %d streams, want 1", len(pending))
	}
	pendEntries := pending[0].Array()[1].Array()
	if len(pendEntries) != 1 {
		t.Fatalf("pending entries = %d, want 1", len(pendEntries))
	}
	if pendEntries[0].Array()[0].String() != ids[2] {
		t.Errorf("pending entry is %q, want the un-acked %q",
			pendEntries[0].Array()[0].String(), ids[2])
	}

	// The group cursor moved past what was delivered: ">" yields the remaining 2.
	fresh := do(t, c, "XREADGROUP", "GROUP", "workers", "w1", "COUNT", "10", "STREAMS", "q", ">").Array()
	if len(fresh) != 1 || len(fresh[0].Array()[1].Array()) != 2 {
		t.Errorf("fresh read = %v, want the 2 remaining entries", fresh)
	}
}

func TestStreamClaimMovesOwnership(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "XGROUP", "CREATE", "q", "g", "$", "MKSTREAM"), "OK")
	id := do(t, c, "XADD", "q", "*", "job", "x").String()

	delivered := do(t, c, "XREADGROUP", "GROUP", "g", "w1", "STREAMS", "q", ">").Array()
	if got := delivered[0].Array()[1].Array()[0].Array()[1].Array()[1].String(); got != "x" {
		t.Fatalf("delivered payload = %q, want x", got)
	}

	// w2 claims the entry from w1.
	claimed := do(t, c, "XCLAIM", "q", "g", "w2", "0", id).Array()
	if len(claimed) != 1 {
		t.Fatalf("XCLAIM returned %d entries, want 1", len(claimed))
	}
	// w2 now sees it in its own history.
	own2 := do(t, c, "XREADGROUP", "GROUP", "g", "w2", "STREAMS", "q", "0").Array()
	if len(own2) != 1 || len(own2[0].Array()[1].Array()) != 1 {
		t.Errorf("w2 history = %v", own2)
	}
}

func TestStreamXPendingAndGroupDelete(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "XGROUP", "CREATE", "q", "g", "$", "MKSTREAM"), "OK")
	_ = do(t, c, "XADD", "q", "*", "job", "x")
	_ = do(t, c, "XREADGROUP", "GROUP", "g", "w1", "STREAMS", "q", ">")

	// Summary form reports one pending entry.
	assertInt(t, do(t, c, "XPENDING", "q", "g").Array()[0], 1)

	// Destroying the group clears its PEL.
	assertInt(t, do(t, c, "XGROUP", "DESTROY", "q", "g"), 1)
	assertInt(t, do(t, c, "XPENDING", "q", "g").Array()[0], 0)

	// The stream itself is untouched.
	assertInt(t, do(t, c, "XLEN", "q"), 1)
}

func TestStreamTrimAndDelete(t *testing.T) {
	c := setup(t)
	for i := range 10 {
		_ = do(t, c, "XADD", "s", "*", "n", strconv.Itoa(i))
	}
	assertInt(t, do(t, c, "XTRIM", "s", "MAXLEN", "5"), 5)
	assertInt(t, do(t, c, "XLEN", "s"), 5)

	first := do(t, c, "XRANGE", "s", "-", "+", "COUNT", "1").Array()[0].Array()[0].String()
	assertInt(t, do(t, c, "XDEL", "s", first), 1)
	assertInt(t, do(t, c, "XLEN", "s"), 4)
}

// TestStreamReliableAcrossRestart is the promise that makes this a reliable
// queue: un-acked deliveries survive a full process restart, and the group
// cursor never rewinds.
func TestStreamReliableAcrossRestart(t *testing.T) {
	dir := t.TempDir()

	r1 := setupPersistent(t, dir)
	assertStr(t, do(t, r1.c, "XGROUP", "CREATE", "q", "g", "$", "MKSTREAM"), "OK")
	for i := range 3 {
		_ = do(t, r1.c, "XADD", "q", "*", "job", strconv.Itoa(i))
	}
	batch := do(t, r1.c, "XREADGROUP", "GROUP", "g", "w1", "COUNT", "2", "STREAMS", "q", ">").Array()
	if len(batch[0].Array()[1].Array()) != 2 {
		t.Fatal("expected 2 delivered entries")
	}
	acked := batch[0].Array()[1].Array()[0].Array()[0].String()
	assertInt(t, do(t, r1.c, "XACK", "q", "g", acked), 1)
	r1.teardown()

	r2 := setupPersistent(t, dir)
	defer r2.teardown()

	// The pending entry survived, still addressed to w1.
	pending := do(t, r2.c, "XREADGROUP", "GROUP", "g", "w1", "STREAMS", "q", "0").Array()
	if len(pending) != 1 || len(pending[0].Array()[1].Array()) != 1 {
		t.Fatalf("pending after restart = %v, want 1 entry", pending)
	}
	// The cursor did not rewind: only the never-delivered entry is new.
	fresh := do(t, r2.c, "XREADGROUP", "GROUP", "g", "w1", "COUNT", "10", "STREAMS", "q", ">").Array()
	if len(fresh) != 1 || len(fresh[0].Array()[1].Array()) != 1 {
		t.Fatalf("fresh after restart = %v, want the 1 never-delivered entry", fresh)
	}
}
