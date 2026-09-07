package compat

import (
	"testing"

	"github.com/tidwall/resp"
)

// Coverage for the aggregate types. The interesting cases are the ones a
// single-representation implementation would get wrong: crossing the
// inline->sparse threshold mid-life, and emptying a collection.

func TestHashBasics(t *testing.T) {
	c := setup(t)

	assertInt(t, do(t, c, "HSET", "h", "f1", "v1", "f2", "v2"), 2)
	assertInt(t, do(t, c, "HSET", "h", "f2", "v2b"), 0) // update, not insert
	assertStr(t, do(t, c, "HGET", "h", "f1"), "v1")
	assertStr(t, do(t, c, "HGET", "h", "f2"), "v2b")
	if v := do(t, c, "HGET", "h", "nope"); !v.IsNull() {
		t.Errorf("HGET of a missing field should be nil, got %q", v.String())
	}

	assertInt(t, do(t, c, "HLEN", "h"), 2)
	assertInt(t, do(t, c, "HEXISTS", "h", "f1"), 1)
	assertInt(t, do(t, c, "HEXISTS", "h", "zz"), 0)

	assertInt(t, do(t, c, "HSETNX", "h", "f3", "v3"), 1)
	assertInt(t, do(t, c, "HSETNX", "h", "f3", "other"), 0)

	assertInt(t, do(t, c, "HDEL", "h", "f1", "zz"), 1)
	assertInt(t, do(t, c, "HLEN", "h"), 2)

	assertInt(t, do(t, c, "HSTRLEN", "h", "f2"), len("v2b"))
	assertInt(t, do(t, c, "HSTRLEN", "h", "nope"), 0)

	assertInt(t, do(t, c, "HINCRBY", "h", "counter", "5"), 5)
	assertInt(t, do(t, c, "HINCRBY", "h", "counter", "-2"), 3)

	assertErr(t, do(t, c, "HSET", "h"), errWrongArgs)
	assertErr(t, do(t, c, "HSET", "h", "f1"), errWrongArgs)
}

func TestHashEnumerate(t *testing.T) {
	c := setup(t)
	assertStr(t, do(t, c, "HMSET", "h", "a", "1", "b", "2"), "OK")

	keys := do(t, c, "HKEYS", "h").Array()
	if len(keys) != 2 {
		t.Fatalf("HKEYS returned %d, want 2", len(keys))
	}
	vals := do(t, c, "HVALS", "h").Array()
	for _, v := range vals {
		if v.String() != "1" && v.String() != "2" {
			t.Errorf("unexpected HVALS entry %q", v.String())
		}
	}

	all := do(t, c, "HGETALL", "h").Array()
	if len(all) != 4 {
		t.Fatalf("HGETALL returned %d, want 4", len(all))
	}
	got := map[string]string{}
	for i := 0; i+1 < len(all); i += 2 {
		got[all[i].String()] = all[i+1].String()
	}
	if got["a"] != "1" || got["b"] != "2" {
		t.Errorf("HGETALL = %v", got)
	}

	mget := do(t, c, "HMGET", "h", "a", "zz", "b").Array()
	if len(mget) != 3 {
		t.Fatalf("HMGET returned %d, want 3", len(mget))
	}
	assertStr(t, mget[0], "1")
	if !mget[1].IsNull() {
		t.Errorf("HMGET of missing field should be nil, got %q", mget[1].String())
	}
	assertStr(t, mget[2], "2")
}

func TestHashEmptiesAndRemovesKey(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "HSET", "h", "f", "v"), 1)
	assertInt(t, do(t, c, "HDEL", "h", "f"), 1)
	assertInt(t, do(t, c, "EXISTS", "h"), 0)
	assertStr(t, do(t, c, "TYPE", "h"), "none")
}

func TestListBasics(t *testing.T) {
	c := setup(t)

	// RPUSH appends, LPUSH prepends in reverse argument order.
	assertInt(t, do(t, c, "RPUSH", "l", "a", "b", "c"), 3)
	assertInt(t, do(t, c, "LPUSH", "l", "z"), 4)
	// Redis: LPUSH applies each element in turn, so the list is z,a,b,c.
	assertInt(t, do(t, c, "LLEN", "l"), 4)

	all := do(t, c, "LRANGE", "l", "0", "-1").Array()
	want := []string{"z", "a", "b", "c"}
	if len(all) != len(want) {
		t.Fatalf("LRANGE returned %d, want %d", len(all), len(want))
	}
	for i, w := range want {
		if all[i].String() != w {
			t.Errorf("LRANGE[%d] = %q, want %q", i, all[i].String(), w)
		}
	}

	assertStr(t, do(t, c, "LPOP", "l"), "z")
	assertStr(t, do(t, c, "RPOP", "l"), "c")
	assertInt(t, do(t, c, "LLEN", "l"), 2)

	assertStr(t, do(t, c, "LINDEX", "l", "0"), "a")
	assertStr(t, do(t, c, "LINDEX", "l", "-1"), "b")
	if v := do(t, c, "LINDEX", "l", "99"); !v.IsNull() {
		t.Errorf("LINDEX out of range should be nil, got %q", v.String())
	}

	assertStr(t, do(t, c, "LSET", "l", "0", "A"), "OK")
	assertStr(t, do(t, c, "LINDEX", "l", "0"), "A")
	assertErr(t, do(t, c, "LSET", "l", "99", "x"), "index out of range")
}

func TestListPushXAndPopCounts(t *testing.T) {
	c := setup(t)

	// LPUSHX on a missing key is a no-op.
	assertInt(t, do(t, c, "LPUSHX", "l", "a"), 0)
	assertInt(t, do(t, c, "EXISTS", "l"), 0)

	assertInt(t, do(t, c, "RPUSH", "l", "1", "2", "3", "4"), 4)
	assertInt(t, do(t, c, "LPUSHX", "l", "0"), 5)
	assertInt(t, do(t, c, "LLEN", "l"), 5)

	// LPOP with a count returns an array.
	out := do(t, c, "LPOP", "l", "2").Array()
	if len(out) != 2 || out[0].String() != "0" || out[1].String() != "1" {
		t.Errorf("LPOP 2 = %v", out)
	}
	assertInt(t, do(t, c, "LLEN", "l"), 3)
}

func TestListRemAndTrim(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "RPUSH", "l", "a", "b", "a", "c", "a"), 5)

	assertInt(t, do(t, c, "LREM", "l", "2", "a"), 2)
	rest := do(t, c, "LRANGE", "l", "0", "-1").Array()
	if len(rest) != 3 || rest[0].String() != "b" || rest[1].String() != "c" || rest[2].String() != "a" {
		t.Errorf("after LREM, list = %v", rest)
	}

	assertStr(t, do(t, c, "LTRIM", "l", "0", "0"), "OK")
	assertInt(t, do(t, c, "LLEN", "l"), 1)
	assertStr(t, do(t, c, "LINDEX", "l", "0"), "b")
}

func TestListEmptiesAndRemovesKey(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "RPUSH", "l", "a", "b"), 2)
	assertStr(t, do(t, c, "LPOP", "l"), "a")
	assertStr(t, do(t, c, "LPOP", "l"), "b")
	if v := do(t, c, "LPOP", "l"); !v.IsNull() {
		t.Errorf("LPOP on an empty list should be nil, got %q", v.String())
	}
	assertInt(t, do(t, c, "EXISTS", "l"), 0)
}

func TestSetBasics(t *testing.T) {
	c := setup(t)

	assertInt(t, do(t, c, "SADD", "s", "a", "b", "c"), 3)
	assertInt(t, do(t, c, "SADD", "s", "a"), 0) // already a member
	assertInt(t, do(t, c, "SCARD", "s"), 3)
	assertInt(t, do(t, c, "SISMEMBER", "s", "a"), 1)
	assertInt(t, do(t, c, "SISMEMBER", "s", "zz"), 0)

	members := do(t, c, "SMEMBERS", "s").Array()
	if len(members) != 3 {
		t.Fatalf("SMEMBERS returned %d, want 3", len(members))
	}

	assertInt(t, do(t, c, "SREM", "s", "a", "zz"), 1)
	assertInt(t, do(t, c, "SCARD", "s"), 2)

	// SMISMEMBER reports per member.
	flags := do(t, c, "SMISMEMBER", "s", "a", "b").Array()
	if len(flags) != 2 || flags[0].Integer() != 0 || flags[1].Integer() != 1 {
		t.Errorf("SMISMEMBER = %v", flags)
	}
}

func TestSetMoveAndOperations(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "SADD", "a", "1", "2", "3"), 3)
	assertInt(t, do(t, c, "SADD", "b", "2", "3", "4"), 3)

	inter := setOf(t, do(t, c, "SINTER", "a", "b"))
	if len(inter) != 2 || !inter["2"] || !inter["3"] {
		t.Errorf("SINTER = %v, want {2,3}", inter)
	}
	union := setOf(t, do(t, c, "SUNION", "a", "b"))
	if len(union) != 4 {
		t.Errorf("SUNION = %v, want 4 members", union)
	}
	diff := setOf(t, do(t, c, "SDIFF", "a", "b"))
	if len(diff) != 1 || !diff["1"] {
		t.Errorf("SDIFF = %v, want {1}", diff)
	}

	assertInt(t, do(t, c, "SMOVE", "a", "b", "1"), 1)
	assertInt(t, do(t, c, "SISMEMBER", "a", "1"), 0)
	assertInt(t, do(t, c, "SISMEMBER", "b", "1"), 1)

	assertInt(t, do(t, c, "SINTERCARD", "2", "a", "b"), 2)
}

func TestSetStoreAndPop(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "SADD", "a", "1", "2", "3"), 3)
	assertInt(t, do(t, c, "SADD", "b", "2", "3", "4"), 3)

	assertInt(t, do(t, c, "SINTERSTORE", "dst", "a", "b"), 2)
	assertInt(t, do(t, c, "SCARD", "dst"), 2)
	assertInt(t, do(t, c, "SUNIONSTORE", "dst2", "a", "b"), 4)
	assertInt(t, do(t, c, "SDIFFSTORE", "dst3", "a", "b"), 1)

	assertInt(t, do(t, c, "SADD", "p", "x", "y", "z"), 3)
	popped := setOf(t, do(t, c, "SPOP", "p", "2"))
	if len(popped) != 2 {
		t.Errorf("SPOP 2 returned %v", popped)
	}
	assertInt(t, do(t, c, "SCARD", "p"), 1)
}

func TestSetEmptiesAndRemovesKey(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "SADD", "s", "a"), 1)
	assertInt(t, do(t, c, "SREM", "s", "a"), 1)
	assertInt(t, do(t, c, "EXISTS", "s"), 0)
}

func TestZSetBasics(t *testing.T) {
	c := setup(t)

	assertInt(t, do(t, c, "ZADD", "z", "1", "one", "2", "two", "3", "three"), 3)
	assertInt(t, do(t, c, "ZADD", "z", "2.5", "two"), 0) // update
	assertInt(t, do(t, c, "ZCARD", "z"), 3)

	assertStr(t, do(t, c, "ZSCORE", "z", "one"), "1")
	assertStr(t, do(t, c, "ZSCORE", "z", "two"), "2.5")
	if v := do(t, c, "ZSCORE", "z", "nope"); !v.IsNull() {
		t.Errorf("ZSCORE of a missing member should be nil, got %q", v.String())
	}

	rng := do(t, c, "ZRANGE", "z", "0", "-1").Array()
	if len(rng) != 3 || rng[0].String() != "one" || rng[2].String() != "three" {
		t.Errorf("ZRANGE = %v", rng)
	}

	withScores := do(t, c, "ZRANGE", "z", "0", "-1", "WITHSCORES").Array()
	if len(withScores) != 6 {
		t.Fatalf("ZRANGE WITHSCORES returned %d, want 6", len(withScores))
	}
	if withScores[1].String() != "1" || withScores[3].String() != "2.5" {
		t.Errorf("ZRANGE WITHSCORES = %v", withScores)
	}

	assertInt(t, do(t, c, "ZRANK", "z", "one"), 0)
	assertInt(t, do(t, c, "ZRANK", "z", "three"), 2)
	assertInt(t, do(t, c, "ZREVRANK", "z", "one"), 2)

	assertStr(t, do(t, c, "ZINCRBY", "z", "2", "one"), "3")
	assertInt(t, do(t, c, "ZCOUNT", "z", "2", "3"), 3)

	rev := do(t, c, "ZRANGE", "z", "0", "-1", "REV").Array()
	if len(rev) != 3 || rev[0].String() != "three" {
		t.Errorf("ZRANGE REV = %v", rev)
	}
}

func TestZSetRangeByScoreAndPop(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"), 4)

	byScore := do(t, c, "ZRANGEBYSCORE", "z", "2", "3").Array()
	if len(byScore) != 2 || byScore[0].String() != "b" || byScore[1].String() != "c" {
		t.Errorf("ZRANGEBYSCORE 2 3 = %v", byScore)
	}
	excl := do(t, c, "ZRANGEBYSCORE", "z", "(2", "4").Array()
	if len(excl) != 2 || excl[0].String() != "c" {
		t.Errorf("ZRANGEBYSCORE (2 4 = %v", excl)
	}
	if n := len(do(t, c, "ZRANGEBYSCORE", "z", "-inf", "+inf").Array()); n != 4 {
		t.Errorf("ZRANGEBYSCORE -inf +inf = %d, want 4", n)
	}
	if n := len(do(t, c, "ZRANGEBYSCORE", "z", "2", "3", "LIMIT", "0", "1").Array()); n != 1 {
		t.Errorf("LIMIT should cap the result at 1, got %d", n)
	}

	assertInt(t, do(t, c, "ZREM", "z", "a", "zz"), 1)
	assertInt(t, do(t, c, "ZCARD", "z"), 3)

	pop := do(t, c, "ZPOPMIN", "z").Array()
	if len(pop) != 2 || pop[0].String() != "b" {
		t.Errorf("ZPOPMIN = %v", pop)
	}
	popMax := do(t, c, "ZPOPMAX", "z").Array()
	if len(popMax) != 2 || popMax[0].String() != "d" {
		t.Errorf("ZPOPMAX = %v", popMax)
	}
	assertInt(t, do(t, c, "ZCARD", "z"), 1)
}

func TestZSetRemRange(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d"), 4)

	assertInt(t, do(t, c, "ZREMRANGEBYRANK", "z", "0", "1"), 2)
	assertInt(t, do(t, c, "ZCARD", "z"), 2)
	assertInt(t, do(t, c, "ZREMRANGEBYSCORE", "z", "3", "10"), 2)
	assertInt(t, do(t, c, "EXISTS", "z"), 0)
}

// TestAggregateCrossesInlineThreshold is the case the two-representation
// design exists for: a collection that starts inline and outgrows it must be
// promoted without losing anything, and must demote again when it shrinks.
func TestAggregateCrossesInlineThreshold(t *testing.T) {
	c := setup(t)
	const n = 400 // well past storage.InlineMaxEntries

	for i := range n {
		assertInt(t, do(t, c, "HSET", "h", "f"+itoa(i), "v"+itoa(i)), 1)
	}
	assertInt(t, do(t, c, "HLEN", "h"), n)
	assertStr(t, do(t, c, "HGET", "h", "f0"), "v0")
	assertStr(t, do(t, c, "HGET", "h", "f"+itoa(n-1)), "v"+itoa(n-1))

	// Same for a set.
	for i := range n {
		assertInt(t, do(t, c, "SADD", "s", "m"+itoa(i)), 1)
	}
	assertInt(t, do(t, c, "SCARD", "s"), n)
	assertInt(t, do(t, c, "SISMEMBER", "s", "m"+itoa(n-1)), 1)

	// And a list: order must survive promotion.
	for i := range n {
		assertInt(t, do(t, c, "RPUSH", "l", itoa(i)), i+1)
	}
	assertInt(t, do(t, c, "LLEN", "l"), n)
	assertStr(t, do(t, c, "LINDEX", "l", "0"), "0")
	assertStr(t, do(t, c, "LINDEX", "l", "-1"), itoa(n-1))
	assertStr(t, do(t, c, "LPOP", "l"), "0")

	// And a sorted set.
	for i := range n {
		assertInt(t, do(t, c, "ZADD", "z", itoa(i), "m"+itoa(i)), 1)
	}
	assertInt(t, do(t, c, "ZCARD", "z"), n)
	assertStr(t, do(t, c, "ZSCORE", "z", "m"+itoa(n-1)), itoa(n-1))
	assertInt(t, do(t, c, "ZRANK", "z", "m0"), 0)
}

func TestAggregateShrinksBackToInline(t *testing.T) {
	c := setup(t)
	const n = 300
	for i := range n {
		assertInt(t, do(t, c, "SADD", "s", "m"+itoa(i)), 1)
	}
	assertInt(t, do(t, c, "SCARD", "s"), n)

	args := []string{"SREM", "s"}
	for i := range n - 3 {
		args = append(args, "m"+itoa(i))
	}
	assertInt(t, do(t, c, args...), n-3)
	assertInt(t, do(t, c, "SCARD", "s"), 3)
	// The survivors must still be reachable after demotion.
	assertInt(t, do(t, c, "SISMEMBER", "s", "m"+itoa(n-1)), 1)
}

func TestWrongTypeAcrossAggregates(t *testing.T) {
	c := setup(t)
	preset(t, c, "str", "v")

	assertErr(t, do(t, c, "HGET", "str", "f"), errWrongType)
	assertErr(t, do(t, c, "LPUSH", "str", "x"), errWrongType)
	assertErr(t, do(t, c, "SADD", "str", "x"), errWrongType)
	assertErr(t, do(t, c, "ZADD", "str", "1", "x"), errWrongType)
	// Reading a string with an aggregate command is an error, but writing to a
	// string with a string command still works: the type check is symmetric.
	assertInt(t, do(t, c, "APPEND", "str", "!"), len("v!"))
}

func TestTypeReportsAggregates(t *testing.T) {
	c := setup(t)
	assertInt(t, do(t, c, "HSET", "h", "f", "v"), 1)
	assertInt(t, do(t, c, "RPUSH", "l", "a"), 1)
	assertInt(t, do(t, c, "SADD", "s", "a"), 1)
	assertInt(t, do(t, c, "ZADD", "z", "1", "a"), 1)

	assertStr(t, do(t, c, "TYPE", "h"), "hash")
	assertStr(t, do(t, c, "TYPE", "l"), "list")
	assertStr(t, do(t, c, "TYPE", "s"), "set")
	assertStr(t, do(t, c, "TYPE", "z"), "zset")
}

// TestDeleteAggregateRemovesElements guards a real leak: deleting a key must
// also drop its per-element keys, otherwise they linger forever.
func TestDeleteAggregateRemovesElements(t *testing.T) {
	c := setup(t)
	const n = 300
	for i := range n {
		assertInt(t, do(t, c, "HSET", "h", "f"+itoa(i), "v"+itoa(i)), 1)
	}
	assertInt(t, do(t, c, "DEL", "h"), 1)
	assertInt(t, do(t, c, "EXISTS", "h"), 0)
	// Recreating the key must not see any residue from the deleted one.
	assertInt(t, do(t, c, "HSET", "h", "only", "1"), 1)
	assertInt(t, do(t, c, "HLEN", "h"), 1)
}

// TestFlushDBClearsAggregates is the same concern at database scope.
func TestFlushDBClearsAggregates(t *testing.T) {
	c := setup(t)
	const n = 300
	for i := range n {
		assertInt(t, do(t, c, "SADD", "s", "m"+itoa(i)), 1)
	}
	assertStr(t, do(t, c, "FLUSHDB"), "OK")
	assertInt(t, do(t, c, "SADD", "s", "only"), 1)
	assertInt(t, do(t, c, "SCARD", "s"), 1)
}

func setOf(t *testing.T, v resp.Value) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, m := range v.Array() {
		out[m.String()] = true
	}
	return out
}
