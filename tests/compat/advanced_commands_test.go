// Tests for the second batch of commands: zset lex/set ops/pops, list
// LMOVE family, generic COPY/MOVE/EXPIRETIME, string INCRBYFLOAT/LCS.
package compat

import (
	"strconv"
	"testing"
	"time"
)

func TestZSetLexRange(t *testing.T) {
	c := setup(t)
	for _, m := range []string{"alpha", "beta", "delta", "gamma"} {
		_ = do(t, c, "ZADD", "z", "0", m)
	}
	// All scores are 0, so lex order rules.
	rg := do(t, c, "ZRANGEBYLEX", "z", "-", "+").Array()
	if len(rg) != 4 || rg[0].String() != "alpha" || rg[3].String() != "gamma" {
		t.Errorf("ZRANGEBYLEX = %v", rg)
	}
	// Exclusive lower bound.
	rg = do(t, c, "ZRANGEBYLEX", "z", "(alpha", "+").Array()
	if len(rg) != 3 || rg[0].String() != "beta" {
		t.Errorf("ZRANGEBYLEX (alpha = %v", rg)
	}
	// Inclusive upper bound.
	rg = do(t, c, "ZRANGEBYLEX", "z", "-", "[beta").Array()
	if len(rg) != 2 {
		t.Errorf("ZRANGEBYLEX - [beta = %v", rg)
	}
	assertInt(t, do(t, c, "ZLEXCOUNT", "z", "[beta", "+"), 3)

	assertInt(t, do(t, c, "ZREMRANGEBYLEX", "z", "(alpha", "(delta"), 1)
	assertInt(t, do(t, c, "ZLEXCOUNT", "z", "-", "+"), 3)
	assertErr(t, do(t, c, "ZRANGEBYLEX", "z", "alpha", "+"), errSyntax)
}

func TestZSetSetOperations(t *testing.T) {
	c := setup(t)
	seed := func(key string, pairs ...string) {
		for i := 0; i < len(pairs); i += 2 {
			_ = do(t, c, "ZADD", key, pairs[i], pairs[i+1])
		}
	}
	seed("a", "1", "one", "2", "two")
	seed("b", "2", "two", "3", "three")

	// Union with weights: one=1*2=2, two=2*2+2*3=10, three=3*3=9.
	u := do(t, c, "ZUNION", "2", "a", "b", "WEIGHTS", "2", "3", "WITHSCORES").Array()
	if len(u) != 6 {
		t.Fatalf("ZUNION returned %d elements, want 6", len(u))
	}
	scores := map[string]string{}
	for i := 0; i+1 < len(u); i += 2 {
		scores[u[i].String()] = u[i+1].String()
	}
	if scores["one"] != "2" || scores["two"] != "10" || scores["three"] != "9" {
		t.Errorf("ZUNION WEIGHTS = %v", scores)
	}

	// Inter with MAX aggregation: two = max(2,2) = 2.
	inter := do(t, c, "ZINTER", "2", "a", "b", "AGGREGATE", "MAX", "WITHSCORES").Array()
	if len(inter) != 2 || inter[0].String() != "two" || inter[1].String() != "2" {
		t.Errorf("ZINTER AGGREGATE MAX = %v", inter)
	}

	// Diff keeps only members of the first set.
	diff := do(t, c, "ZDIFF", "2", "a", "b").Array()
	if len(diff) != 1 || diff[0].String() != "one" {
		t.Errorf("ZDIFF = %v", diff)
	}

	// STORE variants.
	assertInt(t, do(t, c, "ZUNIONSTORE", "dst", "2", "a", "b"), 3)
	assertInt(t, do(t, c, "ZCARD", "dst"), 3)
	assertInt(t, do(t, c, "ZINTERSTORE", "dst", "2", "a", "b"), 1)
	assertStr(t, do(t, c, "ZSCORE", "dst", "two"), "4")
}

func TestZSetRangeStoreRandPop(t *testing.T) {
	c := setup(t)
	for i := range 5 {
		_ = do(t, c, "ZADD", "src", strconv.Itoa(i), "m"+strconv.Itoa(i))
	}
	assertInt(t, do(t, c, "ZRANGESTORE", "dst", "src", "0", "1"), 2)
	assertInt(t, do(t, c, "ZCARD", "dst"), 2)
	if top := do(t, c, "ZRANGE", "dst", "0", "-1", "REV").Array()[0].String(); top != "m1" {
		t.Errorf("ZCARD dst top member = %q, want m1", top)
	}

	rm := do(t, c, "ZRANDMEMBER", "src", "3").Array()
	if len(rm) != 3 {
		t.Errorf("ZRANDMEMBER 3 = %v", rm)
	}
	if n := len(do(t, c, "ZRANDMEMBER", "src", "100").Array()); n != 5 {
		t.Errorf("ZRANDMEMBER with count > card returned %d, want 5", n)
	}

	// ZMPOP pops from the first non-empty set.
	if zmp := do(t, c, "ZMPOP", "2", "nosuch", "src", "MIN").Array(); len(zmp) != 2 || zmp[0].String() != "src" {
		t.Errorf("ZMPOP = %v, want [src ...]", zmp)
	}
	assertInt(t, do(t, c, "ZCARD", "src"), 4)
}

func TestBZPopBlocksAndServes(t *testing.T) {
	c := setup(t)
	pusher := secondConn(t, c)
	time.Sleep(100 * time.Millisecond)

	done := make(chan string, 1)
	go func() {
		time.Sleep(150 * time.Millisecond) // let the block register first
		v, _, err := sendAndRead(t, c, "BZPOPMIN", "zs", "5")
		if err != nil {
			t.Errorf("BZPOPMIN: %v", err)
			done <- ""
			return
		}
		// Reply: [key, member, score].
		if len(v.Array()) < 3 {
			t.Errorf("BZPOPMIN reply = %v, want [key member score]", v)
			done <- ""
			return
		}
		done <- v.Array()[1].String()
	}()

	_ = do(t, pusher, "ZADD", "zs", "7", "seven")

	got := <-done
	if got != "seven" {
		t.Fatalf("BZPOPMIN woke with %q, want seven", got)
	}
}

func TestListMoveFamily(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "RPUSH", "src", "a", "b", "c"), 3)

	// RPOPLPUSH equivalent: right pop, left push.
	assertStr(t, do(t, c, "LMOVE", "src", "dst", "RIGHT", "LEFT"), "c")
	assertStr(t, do(t, c, "LMOVE", "src", "dst", "RIGHT", "LEFT"), "b")
	assertInt(t, do(t, c, "LLEN", "src"), 1)
	assertInt(t, do(t, c, "LLEN", "dst"), 2)
	assertStr(t, do(t, c, "LINDEX", "dst", "0"), "b")

	assertErr(t, do(t, c, "LMOVE", "src", "dst", "UP", "LEFT"), errSyntax)

	// LPOS: rank and count.
	assertInt(t, do(t, c, "RPUSH", "l", "x", "y", "x", "z", "x"), 5)
	assertInt(t, do(t, c, "LPOS", "l", "x"), 0)
	assertInt(t, do(t, c, "LPOS", "l", "x", "RANK", "-1"), 4)
	pos := do(t, c, "LPOS", "l", "x", "COUNT", "0").Array()
	if len(pos) != 3 {
		t.Errorf("LPOS COUNT 0 = %v, want 3 matches", pos)
	}
	if v := do(t, c, "LPOS", "l", "nope"); !v.IsNull() {
		t.Errorf("LPOS of a missing element should be nil, got %q", v.String())
	}

	// LMPOP pops from the first non-empty list.
	assertInt(t, do(t, c, "RPUSH", "q1", "1", "2"), 2)
	out := do(t, c, "LMPOP", "2", "zz", "q1", "LEFT", "COUNT", "2").Array()
	if len(out) != 2 || out[1].Array()[1].String() != "2" {
		t.Errorf("LMPOP = %v", out)
	}
}

func TestGenericCopyMoveExpireTime(t *testing.T) {
	c := setup(t)
	preset(t, c, "s", "v")

	// COPY without REPLACE refuses to overwrite.
	assertInt(t, do(t, c, "COPY", "s", "s"), 0)
	assertStr(t, do(t, c, "COPY", "s", "d"), "1")
	assertStr(t, do(t, c, "GET", "d"), "v")
	assertInt(t, do(t, c, "COPY", "s", "d", "REPLACE"), 1)

	// COPY across databases.
	assertStr(t, do(t, c, "COPY", "s", "s", "DB", "1"), "1")
	assertStr(t, do(t, c, "SELECT", "1"), "OK")
	assertStr(t, do(t, c, "GET", "s"), "v")
	assertStr(t, do(t, c, "SELECT", "0"), "OK")

	// MOVE relocates.
	assertInt(t, do(t, c, "MOVE", "d", "1"), 1)
	assertInt(t, do(t, c, "EXISTS", "d"), 0)
	assertStr(t, do(t, c, "SELECT", "1"), "OK")
	assertStr(t, do(t, c, "GET", "d"), "v")
	assertStr(t, do(t, c, "SELECT", "0"), "OK")

	// EXPIRETIME reports the absolute deadline.
	assertInt(t, do(t, c, "EXPIRETIME", "nope"), -2)
	preset(t, c, "t", "v")
	assertInt(t, do(t, c, "EXPIRETIME", "t"), -1)
	assertInt(t, do(t, c, "EXPIREAT", "t", strconvFuture()), 1)
	if ts := do(t, c, "EXPIRETIME", "t").Integer(); ts <= 0 {
		t.Errorf("EXPIRETIME = %d", ts)
	}
	if ms := do(t, c, "PEXPIRETIME", "t").Integer(); ms <= 0 {
		t.Errorf("PEXPIRETIME = %d", ms)
	}
}

func TestRandomKey(t *testing.T) {
	c := setup(t)
	if v := do(t, c, "RANDOMKEY"); !v.IsNull() {
		t.Errorf("RANDOMKEY on an empty db should be nil, got %q", v.String())
	}
	preset(t, c, "k", "v")
	if v := do(t, c, "RANDOMKEY"); v.String() != "k" {
		t.Errorf("RANDOMKEY = %q, want k", v.String())
	}
}

func TestIncrByFloatAndLCS(t *testing.T) {
	c := setup(t)
	preset(t, c, "f", "10.5")
	assertStr(t, do(t, c, "INCRBYFLOAT", "f", "0.1"), "10.6")
	assertStr(t, do(t, c, "INCRBYFLOAT", "f", "-10.6"), "0")
	assertErr(t, do(t, c, "INCRBYFLOAT", "f", "abc"), "ERR value is not a valid float")

	// INCRBYFLOAT preserves the TTL.
	assertStr(t, do(t, c, "SET", "ft", "1", "EX", "100"), "OK")
	_ = do(t, c, "INCRBYFLOAT", "ft", "1")
	if ttl := do(t, c, "TTL", "ft").Integer(); ttl <= 0 {
		t.Errorf("INCRBYFLOAT lost the TTL: %d", ttl)
	}

	preset(t, c, "a", "abcd")
	preset(t, c, "b", "xbcy")
	assertStr(t, do(t, c, "LCS", "a", "b"), "bc")
	assertInt(t, do(t, c, "LCS", "a", "b", "LEN"), 2)
}
