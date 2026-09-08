package tcl

// Mechanical port of frogdb crates/redis-regression/tests/hash_tcl.rs
// (Redis 8.6.0 unit/hash.tcl scenarios).

import (
	"testing"
)





func Test_TCL_tcl_hget_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "f1", "v1")
	r := cDo(t, client1, "HGET", "myhash", "__123123123__")
	_ = r
	assertNil(t, r)
}

func Test_TCL_tcl_hset_in_update_and_insert_mode(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "existing", "old")
	r := cDo(t, client1, "HSET", "myhash", "existing", "newval")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "HGET", "myhash", "existing")
	assertBulkEq(t, r, "newval")
	r = cDo(t, client1, "HSET", "myhash", "newfield", "newval")
	assertIntegerEq(t, r, 1)
}

func Test_TCL_tcl_hsetnx_target_key_missing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "HSETNX", "myhash", "f1", "foo")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "HGET", "myhash", "f1")
	assertBulkEq(t, r, "foo")
}

func Test_TCL_tcl_hsetnx_target_key_exists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "f1", "foo")
	r := cDo(t, client1, "HSETNX", "myhash", "f1", "bar")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "HGET", "myhash", "f1")
	assertBulkEq(t, r, "foo")
}

func Test_TCL_tcl_hset_hmset_wrong_number_of_args(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "HSET", "myhash", "key1", "val1", "key2")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HMSET", "myhash", "key1", "val1", "key2")
	assertErrorPrefix(t, r, "ERR")
}





func Test_TCL_tcl_hgetall_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "htest")
	resp := cDo(t, client1, "HGETALL", "htest")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_hdel_more_than_a_single_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HMSET", "myhash", "a", "1", "b", "2", "c", "3")
	r := cDo(t, client1, "HDEL", "myhash", "x", "y")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "HDEL", "myhash", "a", "c", "f")
	assertIntegerEq(t, r, 2)
	resp := cDo(t, client1, "HGETALL", "myhash")
	_ = resp
	items := extractBulkStrings(t, resp)
	_ = items
	assertStringsEq(t, items, "b", "2")
}

func Test_TCL_tcl_hdel_hash_becomes_empty_before_deleting_all_specified_fields(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HMSET", "myhash", "a", "1", "b", "2", "c", "3")
	r := cDo(t, client1, "HDEL", "myhash", "a", "b", "c", "d", "e")
	_ = r
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "EXISTS", "myhash")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_hexists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "f1", "v1")
	r := cDo(t, client1, "HEXISTS", "myhash", "f1")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "HEXISTS", "myhash", "nokey")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_hincrby_against_non_existing_database_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "htest")
	r := cDo(t, client1, "HINCRBY", "htest", "foo", "2")
	_ = r
	assertIntegerEq(t, r, 2)
}



func Test_TCL_tcl_hincrby_against_non_existing_hash_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "existing", "1")
	cDo(t, client1, "HDEL", "myhash", "tmp")
	r := cDo(t, client1, "HINCRBY", "myhash", "tmp", "2")
	_ = r
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "HGET", "myhash", "tmp")
	assertBulkEq(t, r, "2")
}

func Test_TCL_tcl_hincrby_against_hash_key_created_by_hincrby_itself(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HINCRBY", "myhash", "tmp", "2")
	r := cDo(t, client1, "HINCRBY", "myhash", "tmp", "3")
	_ = r
	assertIntegerEq(t, r, 5)
	r = cDo(t, client1, "HGET", "myhash", "tmp")
	assertBulkEq(t, r, "5")
}

func Test_TCL_tcl_hincrby_against_hash_key_originally_set_with_hset(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "tmp", "100")
	r := cDo(t, client1, "HINCRBY", "myhash", "tmp", "2")
	_ = r
	assertIntegerEq(t, r, 102)
}

func Test_TCL_tcl_hincrby_over_32bit_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "tmp", "17179869184")
	r := cDo(t, client1, "HINCRBY", "myhash", "tmp", "1")
	_ = r
	assertIntegerEq(t, r, 17179869185)
}

func Test_TCL_tcl_hincrby_over_32bit_value_with_over_32bit_increment(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "tmp", "17179869184")
	r := cDo(t, client1, "HINCRBY", "myhash", "tmp", "17179869184")
	_ = r
	assertIntegerEq(t, r, 34359738368)
}

func Test_TCL_tcl_hincrby_fails_against_hash_value_with_spaces_left(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "str", " 11")
	r := cDo(t, client1, "HINCRBY", "myhash", "str", "1")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_hincrby_fails_against_hash_value_with_spaces_right(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "str", "11 ")
	r := cDo(t, client1, "HINCRBY", "myhash", "str", "1")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_hincrby_can_detect_overflows(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "hash")
	cDo(t, client1, "HSET", "hash", "n", "-9223372036854775484")
	r := cDo(t, client1, "HINCRBY", "hash", "n", "-1")
	_ = r
	assertIntegerEq(t, r, -9223372036854775485)
	r = cDo(t, client1, "HINCRBY", "hash", "n", "-10000")
	assertErrorPrefix(t, r, "ERR")
}











func Test_TCL_tcl_hstrlen_against_non_existing_field(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "f1", "v1")
	r := cDo(t, client1, "HSTRLEN", "myhash", "__123123123__")
	_ = r
	assertIntegerEq(t, r, 0)
}



func Test_TCL_tcl_hrandfield_count_of_0_is_handled_correctly(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "a", "1", "b", "2")
	resp := cDo(t, client1, "HRANDFIELD", "myhash", "0")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_hrandfield_count_overflow(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HMSET", "myhash", "a", "1")
	r := cDo(t, client1, "HRANDFIELD", "myhash", "-9223372036854770000", "WITHVALUES")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HRANDFIELD", "myhash", "-9223372036854775808", "WITHVALUES")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HRANDFIELD", "myhash", "-9223372036854775808")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_hrandfield_with_count_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "HRANDFIELD", "nonexisting_key", "100")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_hrandfield_negative_count_allows_duplicates(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "HSET", "myhash", "a", "1", "b", "2", "c", "3")
	resp := cDo(t, client1, "HRANDFIELD", "myhash", "-20")
	_ = resp
	assertArrayLen(t, resp, 20)
	resp = cDo(t, client1, "HRANDFIELD", "myhash", "-20", "WITHVALUES")
	assertArrayLen(t, resp, 40)
}



func Test_TCL_tcl_hgetdel_input_validation(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "key1")
	r := cDo(t, client1, "HGETDEL")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS", "0")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "XFIELDX", "1", "a")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS", "2", "a")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS", "2", "a", "b", "c")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS", "0", "a")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "HGETDEL", "key1", "FIELDS", "-1", "a")
	assertErrorPrefix(t, r, "ERR")
}











func Test_TCL_tcl_hmset_hmget_roundtrip(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "HMSET", "myhash", "a", "1", "b", "2", "c", "3")
	_ = r
	assertOK(t, r)
	resp := cDo(t, client1, "HMGET", "myhash", "a", "b", "c")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	assertBulkEqAny(t, items[0], "1")
	assertBulkEqAny(t, items[1], "2")
	assertBulkEqAny(t, items[2], "3")
}

func Test_TCL_tcl_hdel_and_return_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myhash")
	cDo(t, client1, "HSET", "myhash", "f1", "v1", "f2", "v2")
	r := cDo(t, client1, "HDEL", "myhash", "nokey")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "HDEL", "myhash", "f1")
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "HDEL", "myhash", "f1")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "HGET", "myhash", "f1")
	assertNil(t, r)
}










