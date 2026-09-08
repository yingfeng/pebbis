package tcl

// Hand-ported tests from frogdb crates/redis-regression/tests/zset_tcl.rs
// (Redis 8.6.0 unit/type/zset.tcl). Previously skip-only stubs emitted by the
// mechanical converter; written by hand against the real Rust source.

import (
	"testing"
)

func Test_TCL_tcl_zadd_basic_and_score_update(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "ztmp")
	cDo(t, client, "ZADD", "ztmp", "10", "x")
	cDo(t, client, "ZADD", "ztmp", "20", "y")
	cDo(t, client, "ZADD", "ztmp", "30", "z")
	r := extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "-1"))
	assertStringsEq(t, r, "x", "y", "z")

	cDo(t, client, "ZADD", "ztmp", "1", "y")
	r = extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "-1"))
	assertStringsEq(t, r, "y", "x", "z")
}

func Test_TCL_tcl_zincrby_nan_rejected(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)
	assertErrorPrefix(t, cDo(t, client, "ZINCRBY", "myzset", "nan", "abc"), "ERR")
}

func Test_TCL_tcl_zadd_variadic(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "myzset")
	assertIntegerEq(t, cDo(t, client, "ZADD", "myzset", "10", "a", "20", "b", "30", "c"), 3)
	r := extractBulkStrings(t, cDo(t, client, "ZRANGE", "myzset", "0", "-1", "WITHSCORES"))
	assertStringsEq(t, r, "a", "10", "b", "20", "c", "30")
}

func Test_TCL_tcl_zrange_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "ztmp")
	cDo(t, client, "ZADD", "ztmp", "1", "a", "2", "b", "3", "c", "4", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "-1")), "a", "b", "c", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "-2")), "a", "b", "c")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "1", "-1")), "b", "c", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "-2", "-1")), "c", "d")
	// out of range: ZRANGE 5 -1 yields empty
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "5", "-1")))
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "5")), "a", "b", "c", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "ztmp", "0", "-1", "WITHSCORES")), "a", "1", "b", "2", "c", "3", "d", "4")
}

func Test_TCL_tcl_zrevrange_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "ztmp")
	cDo(t, client, "ZADD", "ztmp", "1", "a", "2", "b", "3", "c", "4", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGE", "ztmp", "0", "-1")), "d", "c", "b", "a")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGE", "ztmp", "0", "-2")), "d", "c", "b")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGE", "ztmp", "1", "-1")), "c", "b", "a")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGE", "ztmp", "0", "-1", "WITHSCORES")), "d", "4", "c", "3", "b", "2", "a", "1")
}

func Test_TCL_tcl_zincrby_create_new_sorted_set(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZINCRBY", "zset", "1", "foo")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "foo")
	assertBulkEq(t, cDo(t, client, "ZSCORE", "zset", "foo"), "1")
}

func Test_TCL_tcl_zincrby_increment_and_decrement(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZINCRBY", "zset", "1", "foo")
	cDo(t, client, "ZINCRBY", "zset", "2", "foo")
	cDo(t, client, "ZINCRBY", "zset", "1", "bar")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "bar", "foo")

	cDo(t, client, "ZINCRBY", "zset", "10", "bar")
	cDo(t, client, "ZINCRBY", "zset", "-5", "foo")
	cDo(t, client, "ZINCRBY", "zset", "-5", "bar")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "foo", "bar")
	assertBulkEq(t, cDo(t, client, "ZSCORE", "zset", "foo"), "-2")
	assertBulkEq(t, cDo(t, client, "ZSCORE", "zset", "bar"), "6")
}

func Test_TCL_tcl_zrangebyscore_zcount_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "-inf", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "f", "+inf", "g")

	// inclusive
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "-inf", "2")), "a", "b", "c")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "0", "3")), "b", "c", "d")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "3", "6")), "d", "e", "f")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "4", "+inf")), "e", "f", "g")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGEBYSCORE", "zset", "2", "-inf")), "c", "b", "a")
	assertIntegerEq(t, cDo(t, client, "ZCOUNT", "zset", "0", "3"), 3)

	// exclusive
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "(-inf", "(2")), "b")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "(0", "(3")), "b", "c")
	assertIntegerEq(t, cDo(t, client, "ZCOUNT", "zset", "(0", "(3"), 2)
}

func Test_TCL_tcl_zrangebyscore_with_withscores(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "-inf", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "f", "+inf", "g")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "0", "3", "WITHSCORES")), "b", "1", "c", "2", "d", "3")
}

func Test_TCL_tcl_zrangebyscore_with_limit(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "-inf", "a", "1", "b", "2", "c", "3", "d", "4", "e", "5", "f", "+inf", "g")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "0", "2")), "b", "c")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "2", "3")), "d", "e", "f")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYSCORE", "zset", "0", "10", "LIMIT", "20", "10")))
}

var zsetLexMembers = []string{
	"alpha", "bar", "cool", "down", "elephant", "foo", "great", "hill", "omega",
}

func Test_TCL_tcl_zrangebylex_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	for _, m := range zsetLexMembers {
		cDo(t, client, "ZADD", "zset", "0", m)
	}
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYLEX", "zset", "-", "[cool")), "alpha", "bar", "cool")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYLEX", "zset", "[bar", "[down")), "bar", "cool", "down")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYLEX", "zset", "[g", "+")), "great", "hill", "omega")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZREVRANGEBYLEX", "zset", "[cool", "-")), "cool", "bar", "alpha")
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "[ele", "[h"), 3)

	// exclusive
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYLEX", "zset", "-", "(cool")), "alpha", "bar")
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGEBYLEX", "zset", "(bar", "(down")), "cool")
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "(ele", "(great"), 2)
}

func Test_TCL_tcl_zlexcount_advanced(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	for _, m := range zsetLexMembers {
		cDo(t, client, "ZADD", "zset", "0", m)
	}
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "-", "+"), 9)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "+", "-"), 0)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "[bar", "+"), 8)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "[bar", "[foo"), 5)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "[bar", "(foo"), 4)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "(bar", "[foo"), 4)
	assertIntegerEq(t, cDo(t, client, "ZLEXCOUNT", "zset", "(bar", "(foo"), 3)
}

func Test_TCL_tcl_zremrangebyscore_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYSCORE", "zset", "2", "4"), 3)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "a", "e")

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYSCORE", "zset", "-inf", "+inf"), 5)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")))

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYSCORE", "zset", "(1", "(5"), 3)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "a", "e")
}

func Test_TCL_tcl_zremrangebyrank_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYRANK", "zset", "1", "3"), 3)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "a", "e")

	// destroy when empty
	cDo(t, client, "DEL", "zset")
	cDo(t, client, "ZADD", "zset", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYRANK", "zset", "0", "4"), 5)
	assertIntegerEq(t, cDo(t, client, "EXISTS", "zset"), 0)
}

func Test_TCL_tcl_zremrangebylex_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zset")
	for _, m := range zsetLexMembers {
		cDo(t, client, "ZADD", "zset", "0", m)
	}
	assertIntegerEq(t, cDo(t, client, "ZREMRANGEBYLEX", "zset", "-", "[cool"), 3)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zset", "0", "-1")), "down", "elephant", "foo", "great", "hill", "omega")
}

func Test_TCL_tcl_zunionstore_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zseta{t}", "zsetb{t}", "zsetc{t}")
	cDo(t, client, "ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c")
	cDo(t, client, "ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d")
	assertIntegerEq(t, cDo(t, client, "ZUNIONSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"), 4)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES")), "a", "1", "b", "3", "d", "3", "c", "5")
}

func Test_TCL_tcl_zinterstore_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zseta{t}", "zsetb{t}", "zsetc{t}")
	cDo(t, client, "ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c")
	cDo(t, client, "ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d")
	assertIntegerEq(t, cDo(t, client, "ZINTERSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"), 2)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES")), "b", "3", "c", "5")
}

func Test_TCL_tcl_zdiffstore_basics(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zseta{t}", "zsetb{t}", "zsetc{t}")
	cDo(t, client, "ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c")
	cDo(t, client, "ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d")
	assertIntegerEq(t, cDo(t, client, "ZDIFFSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}"), 1)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES")), "a", "1")
}

func Test_TCL_tcl_zunionstore_with_weights(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	cDo(t, client, "DEL", "zseta{t}", "zsetb{t}", "zsetc{t}")
	cDo(t, client, "ZADD", "zseta{t}", "1", "a", "2", "b", "3", "c")
	cDo(t, client, "ZADD", "zsetb{t}", "1", "b", "2", "c", "3", "d")
	assertIntegerEq(t, cDo(t, client, "ZUNIONSTORE", "zsetc{t}", "2", "zseta{t}", "zsetb{t}", "WEIGHTS", "2", "3"), 4)
	assertStringsEq(t, extractBulkStrings(t, cDo(t, client, "ZRANGE", "zsetc{t}", "0", "-1", "WITHSCORES")), "a", "2", "b", "7", "d", "9", "c", "12")
}

func Test_TCL_tcl_zunionstore_zinterstore_with_inf_scores(t *testing.T) {
	addr := startServer(t)
	client := connect(t, addr)

	for _, cmd := range []string{"ZUNIONSTORE", "ZINTERSTORE"} {
		cDo(t, client, "DEL", "zsetinf1{t}", "zsetinf2{t}")

		cDo(t, client, "ZADD", "zsetinf1{t}", "+inf", "key")
		cDo(t, client, "ZADD", "zsetinf2{t}", "+inf", "key")
		cDo(t, client, cmd, "zsetinf3{t}", "2", "zsetinf1{t}", "zsetinf2{t}")
		assertBulkEq(t, cDo(t, client, "ZSCORE", "zsetinf3{t}", "key"), "inf")

		cDo(t, client, "ZADD", "zsetinf1{t}", "-inf", "key")
		cDo(t, client, "ZADD", "zsetinf2{t}", "+inf", "key")
		cDo(t, client, cmd, "zsetinf3{t}", "2", "zsetinf1{t}", "zsetinf2{t}")
		assertBulkEq(t, cDo(t, client, "ZSCORE", "zsetinf3{t}", "key"), "0")

		cDo(t, client, "ZADD", "zsetinf1{t}", "+inf", "key")
		cDo(t, client, "ZADD", "zsetinf2{t}", "-inf", "key")
		cDo(t, client, cmd, "zsetinf3{t}", "2", "zsetinf1{t}", "zsetinf2{t}")
		assertBulkEq(t, cDo(t, client, "ZSCORE", "zsetinf3{t}", "key"), "0")

		cDo(t, client, "ZADD", "zsetinf1{t}", "-inf", "key")
		cDo(t, client, "ZADD", "zsetinf2{t}", "-inf", "key")
		cDo(t, client, cmd, "zsetinf3{t}", "2", "zsetinf1{t}", "zsetinf2{t}")
		assertBulkEq(t, cDo(t, client, "ZSCORE", "zsetinf3{t}", "key"), "-inf")
	}
}
