package tcl

// Mechanical port of frogdb crates/redis-regression/tests/zset_tcl.rs
// (Redis 8.6.0 unit/zset.tcl scenarios).

import (
	"testing"
)



func Test_TCL_tcl_zadd_nan_rejected(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "ZADD", "myzset", "nan", "abc")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}



func Test_TCL_tcl_zadd_xx_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	r := cDo(t, client1, "ZADD", "ztmp", "XX", "10", "x")
	_ = r
	assertIntegerEq(t, r, 0)
	cDo(t, client1, "ZADD", "ztmp", "10", "x")
	r = cDo(t, client1, "ZADD", "ztmp", "XX", "20", "y")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZCARD", "ztmp")
	assertIntegerEq(t, r, 1)
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
	cDo(t, client1, "ZADD", "ztmp", "XX", "5", "foo", "11", "x", "21", "y", "40", "zap")
	r = cDo(t, client1, "ZCARD", "ztmp")
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "ZSCORE", "ztmp", "x")
	assertBulkEq(t, r, "11")
	r = cDo(t, client1, "ZSCORE", "ztmp", "y")
	assertBulkEq(t, r, "21")
}

func Test_TCL_tcl_zadd_nx_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "NX", "10", "x", "20", "y", "30", "z")
	r := cDo(t, client1, "ZCARD", "ztmp")
	_ = r
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "ZADD", "ztmp", "NX", "11", "x", "21", "y", "100", "a", "200", "b")
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "ZSCORE", "ztmp", "x")
	assertBulkEq(t, r, "10")
	r = cDo(t, client1, "ZSCORE", "ztmp", "y")
	assertBulkEq(t, r, "20")
	r = cDo(t, client1, "ZSCORE", "ztmp", "a")
	assertBulkEq(t, r, "100")
}

func Test_TCL_tcl_zadd_xx_nx_not_compatible(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "ZADD", "ztmp", "XX", "NX", "10", "x")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_zadd_gt_lt_nx_not_compatible(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "ZADD", "ztmp", "GT", "NX", "10", "x")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZADD", "ztmp", "LT", "NX", "10", "x")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZADD", "ztmp", "LT", "GT", "10", "x")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_zadd_gt_updates_when_new_scores_greater(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
	r := cDo(t, client1, "ZADD", "ztmp", "GT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
	_ = r
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "ZCARD", "ztmp")
	assertIntegerEq(t, r, 4)
	r = cDo(t, client1, "ZSCORE", "ztmp", "x")
	assertBulkEq(t, r, "11")
	r = cDo(t, client1, "ZSCORE", "ztmp", "y")
	assertBulkEq(t, r, "21")
	r = cDo(t, client1, "ZSCORE", "ztmp", "z")
	assertBulkEq(t, r, "30")
}

func Test_TCL_tcl_zadd_lt_updates_when_new_scores_lower(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
	r := cDo(t, client1, "ZADD", "ztmp", "LT", "CH", "5", "foo", "11", "x", "21", "y", "29", "z")
	_ = r
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "ZCARD", "ztmp")
	assertIntegerEq(t, r, 4)
	r = cDo(t, client1, "ZSCORE", "ztmp", "x")
	assertBulkEq(t, r, "10")
	r = cDo(t, client1, "ZSCORE", "ztmp", "y")
	assertBulkEq(t, r, "20")
	r = cDo(t, client1, "ZSCORE", "ztmp", "z")
	assertBulkEq(t, r, "29")
}

func Test_TCL_tcl_zadd_ch_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
	r := cDo(t, client1, "ZADD", "ztmp", "11", "x", "21", "y", "30", "z")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZADD", "ztmp", "CH", "12", "x", "22", "y", "30", "z")
	assertIntegerEq(t, r, 2)
}

func Test_TCL_tcl_zadd_incr_works_like_zincrby(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y", "30", "z")
	cDo(t, client1, "ZADD", "ztmp", "INCR", "15", "x")
	r := cDo(t, client1, "ZSCORE", "ztmp", "x")
	_ = r
	assertBulkEq(t, r, "25")
}



func Test_TCL_tcl_zadd_variadic_return_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myzset")
	cDo(t, client1, "ZADD", "myzset", "10", "a", "20", "b", "30", "c")
	r := cDo(t, client1, "ZADD", "myzset", "5", "x", "20", "b", "30", "c")
	_ = r
	assertIntegerEq(t, r, 1)
}

func Test_TCL_tcl_zcard_basics(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "a", "20", "b", "30", "c")
	r := cDo(t, client1, "ZCARD", "ztmp")
	_ = r
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "ZCARD", "zdoesntexist")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_zrem_removes_key_after_last_element(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "x", "20", "y")
	r := cDo(t, client1, "EXISTS", "ztmp")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "ZREM", "ztmp", "z")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZREM", "ztmp", "y")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "ZREM", "ztmp", "x")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "EXISTS", "ztmp")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_zrem_variadic(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "ztmp")
	cDo(t, client1, "ZADD", "ztmp", "10", "a", "20", "b", "30", "c")
	r := cDo(t, client1, "ZREM", "ztmp", "x", "y", "a", "b", "k")
	_ = r
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "ZREM", "ztmp", "foo", "bar")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZREM", "ztmp", "c")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "EXISTS", "ztmp")
	assertIntegerEq(t, r, 0)
}





func Test_TCL_tcl_zrank_zrevrank_basics(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zranktmp")
	cDo(t, client1, "ZADD", "zranktmp", "10", "x", "20", "y", "30", "z")
	r := cDo(t, client1, "ZRANK", "zranktmp", "x")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZRANK", "zranktmp", "y")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "ZRANK", "zranktmp", "z")
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "ZREVRANK", "zranktmp", "x")
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "ZREVRANK", "zranktmp", "y")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "ZREVRANK", "zranktmp", "z")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZRANK", "zranktmp", "foo")
	assertNil(t, r)
	r = cDo(t, client1, "ZREVRANK", "zranktmp", "foo")
	assertNil(t, r)
}

func Test_TCL_tcl_zrank_after_deletion(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zranktmp")
	cDo(t, client1, "ZADD", "zranktmp", "10", "x", "20", "y", "30", "z")
	cDo(t, client1, "ZREM", "zranktmp", "y")
	r := cDo(t, client1, "ZRANK", "zranktmp", "x")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "ZRANK", "zranktmp", "z")
	assertIntegerEq(t, r, 1)
}







func Test_TCL_tcl_zadd_incr_leading_to_nan_is_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myzset")
	cDo(t, client1, "ZADD", "myzset", "INCR", "+inf", "abc")
	r := cDo(t, client1, "ZADD", "myzset", "INCR", "-inf", "abc")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}


































func Test_TCL_tcl_zpopmin_zpopmax_count_zero(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zset")
	cDo(t, client1, "ZADD", "zset", "1", "a", "2", "b", "3", "c")
	assertArrayLen(t, unwrapArray(t, cDo(t, client1, "ZPOPMIN", "zset", "0")), 0)
	assertArrayLen(t, unwrapArray(t, cDo(t, client1, "ZPOPMAX", "zset", "0")), 0)
	r := cDo(t, client1, "ZCARD", "zset")
	_ = r
	assertIntegerEq(t, r, 3)
}

func Test_TCL_tcl_zpopmin_zpopmax_negative_count(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zset")
	cDo(t, client1, "ZADD", "zset", "1", "a", "2", "b", "3", "c")
	r := cDo(t, client1, "ZPOPMIN", "zset", "-1")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZPOPMAX", "zset", "-3")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_zpop_zmpop_against_wrong_type(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo{t}", "bar")
	r := cDo(t, client1, "ZPOPMIN", "foo{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "ZPOPMAX", "foo{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "ZMPOP", "1", "foo{t}", "MIN")
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "ZMPOP", "1", "foo{t}", "MAX")
	assertErrorPrefix(t, r, "WRONGTYPE")
}









func Test_TCL_tcl_zmscore_requires_one_or_more_members(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zmscoretest")
	cDo(t, client1, "ZADD", "zmscoretest", "10", "x")
	r := cDo(t, client1, "ZMSCORE", "zmscoretest")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}











func Test_TCL_tcl_zrangestore_src_key_missing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "z2{t}")
	r := cDo(t, client1, "ZRANGESTORE", "z2{t}", "missing{t}", "0", "-1")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "z2{t}")
	assertIntegerEq(t, r, 0)
}



func Test_TCL_tcl_zrangestore_empty_range(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "z1{t}", "z2{t}")
	cDo(t, client1, "ZADD", "z1{t}", "1", "a", "2", "b", "3", "c", "4", "d")
	r := cDo(t, client1, "ZRANGESTORE", "z2{t}", "z1{t}", "5", "6")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "z2{t}")
	assertIntegerEq(t, r, 0)
}





func Test_TCL_tcl_zrandmember_count_of_0(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myzset")
	cDo(t, client1, "ZADD", "myzset", "1", "a", "2", "b")
	assertArrayLen(t, unwrapArray(t, cDo(t, client1, "ZRANDMEMBER", "myzset", "0")), 0)
}

func Test_TCL_tcl_zrandmember_with_count_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	assertArrayLen(t, unwrapArray(t, cDo(t, client1, "ZRANDMEMBER", "nonexisting_key", "100")), 0)
}

func Test_TCL_tcl_zrandmember_count_overflow(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myzset")
	cDo(t, client1, "ZADD", "myzset", "0", "a")
	r := cDo(t, client1, "ZRANDMEMBER", "myzset", "-9223372036854770000", "WITHSCORES")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZRANDMEMBER", "myzset", "-9223372036854775808")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_zset_commands_dont_accept_empty_strings_as_valid_score(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "ZADD", "myzset", "", "abc")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}



func Test_TCL_tcl_zstore_error_if_using_withscores(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "zsetd{t}", "zsetf{t}")
	cDo(t, client1, "ZADD", "zsetd{t}", "1", "a")
	cDo(t, client1, "ZADD", "zsetf{t}", "1", "a")
	r := cDo(t, client1, "ZUNIONSTORE", "foo{t}", "2", "zsetd{t}", "zsetf{t}", "WITHSCORES")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZINTERSTORE", "foo{t}", "2", "zsetd{t}", "zsetf{t}", "WITHSCORES")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZDIFFSTORE", "foo{t}", "2", "zsetd{t}", "zsetf{t}", "WITHSCORES")
	assertErrorPrefix(t, r, "ERR")
}







func Test_TCL_tcl_zrangebylex_with_invalid_lex_range_specifiers(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "ZRANGEBYLEX", "fooz", "foo", "bar")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZRANGEBYLEX", "fooz", "[foo", "bar")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZRANGEBYLEX", "fooz", "foo", "[bar")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZRANGEBYLEX", "fooz", "+x", "[bar")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "ZRANGEBYLEX", "fooz", "-x", "[bar")
	assertErrorPrefix(t, r, "ERR")
}























func Test_TCL_tcl_bzmpop_illegal_argument_negative_numkeys(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "BZMPOP", "1", "-1", "myzset{t}", "MAX")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}








































