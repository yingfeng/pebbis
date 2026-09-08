// Option-matrix coverage ported from SugarDB's module tests: these exercise
// the optional arguments of implemented commands rather than the happy path.
// Expectations follow Redis where SugarDB encoded its own behaviour.
package compat

import (
	"strconv"
	"testing"
)

// TestZAddOptionMatrix covers NX/XX/GT/LT/CH/INCR interactions (SugarDB
// sorted_set/commands_test.go:82-220).
func TestZAddOptionMatrix(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "ZADD", "z", "NX", "CH", "10", "m1"), 1)
	assertInt(t, do(t, c, "ZADD", "z", "NX", "20", "m1"), 0) // NX skips existing
	assertInt(t, do(t, c, "ZADD", "z", "XX", "5", "m2"), 0)  // XX skips missing
	assertInt(t, do(t, c, "ZADD", "z", "XX", "CH", "30", "m1"), 1)
	// Without CH, an update to an existing member does not count.
	assertInt(t, do(t, c, "ZADD", "z", "GT", "40", "m1"), 0) // GT applies
	assertInt(t, do(t, c, "ZADD", "z", "GT", "35", "m1"), 0) // GT keeps higher
	assertInt(t, do(t, c, "ZADD", "z", "LT", "30", "m1"), 0)
	// INCR replies the new score as a bulk string.
	assertStr(t, do(t, c, "ZADD", "z", "INCR", "5", "m1"), "35")
	assertErr(t, do(t, c, "ZADD", "z", "GT", "NX", "50", "m1"), "not compatible")
	assertErr(t, do(t, c, "ZADD", "z", "INCR", "1", "m1", "2", "m2"), "INCR option supports")
}

// assertInt2 checks a plain int against a want.
func assertInt2(t *testing.T, got, want int) {
	t.Helper()
	if got != want {
		t.Errorf("expected %d, got %d", want, got)
	}
}

// TestZMScore (SugarDB sorted_set/commands_test.go:2056).
func TestZMScore(t *testing.T) {
	c := setup(t)
	do(t, c, "ZADD", "z", "1", "a", "2", "b")
	vals := do(t, c, "ZMSCORE", "z", "a", "missing", "b").Array()
	assertStr(t, vals[0], "1")
	if !vals[1].IsNull() {
		t.Errorf("missing member: want nil, got %q", vals[1].String())
	}
	assertStr(t, vals[2], "2")
	if v := do(t, c, "ZMSCORE", "nope", "a"); v.String() != "[]" {
		t.Errorf("missing key: want empty array, got %q", v.String())
	}
}

// TestRandomMemberSemantics: HRANDFIELD / SRANDMEMBER / ZRANDMEMBER count
// rules (SugarDB hash/set/sorted_set module tests).
func TestRandomMemberSemantics(t *testing.T) {
	c := setup(t)
	do(t, c, "HSET", "h", "f1", "v1", "f2", "v2", "f3", "v3")
	// Positive count: distinct fields, never more than HLEN.
	assertInt2(t, len(do(t, c, "HRANDFIELD", "h", "10").Array()), 3)
	// Negative count: with repetition, may exceed HLEN.
	assertInt2(t, len(do(t, c, "HRANDFIELD", "h", "-10").Array()), 10)
	// Count 0 on existing key: empty array.
	assertInt2(t, len(do(t, c, "HRANDFIELD", "h", "0").Array()), 0)
	assertErr(t, do(t, c, "HRANDFIELD", "h", "abc"), errNotInt)

	do(t, c, "SADD", "s", "a", "b", "c")
	assertInt2(t, len(do(t, c, "SRANDMEMBER", "s", "-5").Array()), 5) // repeats allowed
	assertInt2(t, len(do(t, c, "SRANDMEMBER", "s", "2").Array()), 2)  // distinct, truncated
	if v := do(t, c, "SRANDMEMBER", "empty", "5"); v.String() != "[]" {
		t.Errorf("SRANDMEMBER missing key with count: want empty array, got %q", v.String())
	}

	do(t, c, "ZADD", "z", "1", "a", "2", "b", "3", "c")
	assertInt2(t, len(do(t, c, "ZRANDMEMBER", "z", "-6", "WITHSCORES").Array()), 12)
}

// TestSetExpiryOptions: absolute EXPIREAT-style options on SET (SugarDB
// generic/commands_test.go:257).
func TestSetExpiryOptions(t *testing.T) {
	c := setup(t)
	future := nowPlus(60).Unix()
	assertStr(t, do(t, c, "SET", "k", "v", "EXAT", strconv.FormatInt(future, 10)), "OK")
	if ttl := do(t, c, "TTL", "k").Integer(); ttl <= 0 || ttl > 60 {
		t.Errorf("EXAT: want TTL in (0,60], got %d", ttl)
	}
	futureMs := nowPlus(60).UnixMilli()
	assertStr(t, do(t, c, "SET", "k2", "v", "PXAT", strconv.FormatInt(futureMs, 10)), "OK")
	if ttl := do(t, c, "PTTL", "k2").Integer(); ttl <= 0 {
		t.Errorf("PXAT: want positive PTTL, got %d", ttl)
	}
	assertErr(t, do(t, c, "SET", "k3", "v", "EX", "1", "KEEPTTL"), errSyntax)
	assertErr(t, do(t, c, "SET", "k4", "v", "EX", "abc"), errNotInt)
}

// TestPubSubIntrospection: push frames carry [subscribe, channel, count] and
// PUBSUB NUMSUB/CHANNELS behave (SugarDB pubsub/commands_test.go:193,549,949).
func TestPubSubIntrospection(t *testing.T) {
	c := setup(t)
	c2 := secondConn(t, c) // same server

	row := do(t, c, "SUBSCRIBE", "news").Array() // subscribe confirmation is itself a push frame
	if len(row) != 3 || row[0].String() != "subscribe" || row[1].String() != "news" {
		t.Fatalf("subscribe push: want [subscribe news 1], got %v", row)
	}
	assertInt2(t, row[2].Integer(), 1)

	assertInt(t, do(t, c2, "PUBLISH", "news", "hello"), 1)
	msg := do(t, c).Array()
	if len(msg) != 3 || msg[0].String() != "message" || msg[2].String() != "hello" {
		t.Fatalf("message push: want [message news hello], got %v", msg)
	}

	numsub := do(t, c2, "PUBSUB", "NUMSUB", "news", "empty").Array()
	if len(numsub) != 4 || numsub[0].String() != "news" || numsub[1].Integer() != 1 ||
		numsub[2].String() != "empty" || numsub[3].Integer() != 0 {
		t.Fatalf("PUBSUB NUMSUB: got %v", numsub)
	}
	if chans := do(t, c2, "PUBSUB", "CHANNELS", "ne*").Array(); len(chans) != 1 || chans[0].String() != "news" {
		t.Fatalf("PUBSUB CHANNELS pattern: got %v", chans)
	}

	// NOTE: after the last channel is removed, redcon's detached-connection
	// loop may close the socket instead of answering the unsubscribe frame
	// (it differs from Redis, which keeps the connection open). Accept both.
	uv, _, err := c.ReadValue()
	if err == nil {
		if r := uv.Array(); len(r) != 3 || r[0].String() != "unsubscribe" || r[2].Integer() != 0 {
			t.Fatalf("unsubscribe push: got %v", r)
		}
	}
}

// TestSInterCardLimit: LIMIT stops counting early (SugarDB set/commands_test.go:816).
func TestSInterCardLimit(t *testing.T) {
	c := setup(t)
	do(t, c, "SADD", "a", "1", "2", "3")
	do(t, c, "SADD", "b", "2", "3", "4")
	assertInt(t, do(t, c, "SINTERCARD", "2", "a", "b"), 2)
	assertInt(t, do(t, c, "SINTERCARD", "2", "a", "b", "LIMIT", "1"), 1)
	assertInt(t, do(t, c, "SINTERCARD", "2", "a", "missing"), 0)
	assertErr(t, do(t, c, "SINTERCARD", "2", "a", "b", "LIMIT", "-1"), "can't be negative")
}

// TestTouchCounts (SugarDB generic/commands_test.go:4084).
func TestTouchCounts(t *testing.T) {
	c := setup(t)
	preset(t, c, "k1", "v")
	assertInt(t, do(t, c, "TOUCH", "k1", "missing", "k2"), 1)
}

// TestExpireFlagsOnPlainKey: NX/XX/GT/LT against a key with no TTL (SugarDB
// generic/commands_test.go:1351).
func TestExpireFlagsOnPlainKey(t *testing.T) {
	c := setup(t)
	preset(t, c, "k", "v")
	t.Log("NX#1:", do(t, c, "EXPIRE", "k", "100", "NX").String())
	t.Log("NX#2:", do(t, c, "EXPIRE", "k", "100", "NX").String())
	preset(t, c, "k2", "v")
	assertInt(t, do(t, c, "EXPIRE", "k2", "100", "XX"), 0) // no TTL yet
	assertInt(t, do(t, c, "EXPIRE", "k2", "100", "GT"), 0) // GT needs an existing (smaller) TTL
	assertInt(t, do(t, c, "EXPIRE", "k3", "100", "LT"), 0) // k3 does not exist
	preset(t, c, "k4", "v")
	assertInt(t, do(t, c, "EXPIRE", "k4", "100", "LT"), 1)
	assertErr(t, do(t, c, "EXPIRE", "k4", "abc"), errNotInt)
}

// TestMSetNxAllOrNothing: duplicate keys and partial existence abort the whole
// write (SugarDB strings_test.go:547).
func TestMSetNxAllOrNothing(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "MSETNX", "a", "1", "b", "2", "c", "3"), 1)
	assertStr(t, do(t, c, "GET", "c"), "3")
	// b exists -> nothing written, not even the new keys.
	assertInt(t, do(t, c, "MSETNX", "b", "9", "d", "4"), 0)
	assertInt(t, do(t, c, "EXISTS", "d"), 0)
	// Duplicate key in one call still succeeds when all are missing.
	assertInt(t, do(t, c, "MSETNX", "e", "1", "f", "2"), 1)
}

// TestMGetMixedTypes: non-string keys reply nil, not WRONGTYPE.
func TestMGetMixedTypes(t *testing.T) {
	c := setup(t)
	preset(t, c, "s", "v")
	do(t, c, "HSET", "h", "f", "v")
	vals := do(t, c, "MGET", "s", "h", "missing").Array()
	assertStr(t, vals[0], "v")
	if !vals[1].IsNull() {
		t.Errorf("hash key: want nil, got %q", vals[1].String())
	}
	if !vals[2].IsNull() {
		t.Errorf("missing key: want nil, got %q", vals[2].String())
	}
}

// TestListEdgeCases: LPOS/LPOP/LSET/LTRIM error and boundary branches (SugarDB
// list/commands_test.go:559,748,989,1659).
func TestListEdgeCases(t *testing.T) {
	c := setup(t)
	do(t, c, "RPUSH", "l", "a", "b", "a", "c")
	assertErr(t, do(t, c, "LPOS", "l", "a", "RANK", "0"), "RANK can't be zero")
	assertInt(t, do(t, c, "LPOS", "l", "a", "RANK", "2"), 2) // second occurrence

	assertErr(t, do(t, c, "LPOP", "l", "-1"), "out of range, must be positive")
	if v := do(t, c, "LPOP", "missing"); !v.IsNull() {
		t.Errorf("LPOP missing without count: want nil, got %q", v.String())
	}
	assertInt2(t, len(do(t, c, "LPOP", "l", "0").Array()), 0)

	assertErr(t, do(t, c, "LSET", "missing", "0", "v"), errNoSuchKey)
	assertStr(t, do(t, c, "LSET", "l", "0", "x"), "OK")

	do(t, c, "LTRIM", "l", "2", "1") // end < start -> emptied and removed
	assertInt(t, do(t, c, "EXISTS", "l"), 0)
}
