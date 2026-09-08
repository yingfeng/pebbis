package tcl

// Mechanical port of frogdb crates/redis-regression/tests/list_tcl.rs
// (Redis 8.6.0 unit/list.tcl scenarios).

import (
	"fmt"
	"testing"
)

func Test_TCL_tcl_lpos_basic_usage(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c")
	r := cDo(t, client1, "LPOS", "mylist", "a")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "LPOS", "mylist", "c")
	assertIntegerEq(t, r, 2)
}

func Test_TCL_tcl_lpos_rank_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a", "b", "c", "d", "2", "3", "c", "c")
	r := cDo(t, client1, "LPOS", "mylist", "c", "RANK", "1")
	_ = r
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "LPOS", "mylist", "c", "RANK", "2")
	assertIntegerEq(t, r, 6)
	r = cDo(t, client1, "LPOS", "mylist", "c", "RANK", "4")
	assertNil(t, r)
	r = cDo(t, client1, "LPOS", "mylist", "c", "RANK", "-1")
	assertIntegerEq(t, r, 7)
	r = cDo(t, client1, "LPOS", "mylist", "c", "RANK", "-2")
	assertIntegerEq(t, r, 6)
	r = cDo(t, client1, "LPOS", "mylist", "c", "RANK", "0")
	assertErrorPrefix(t, r, "ERR")
}





func Test_TCL_tcl_lpos_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "LPOS", "mylistxxx", "c", "COUNT", "0", "RANK", "2")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_lpos_no_match(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a", "b", "c")
	resp := cDo(t, client1, "LPOS", "mylist", "x", "COUNT", "2", "RANK", "-1")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
	r := cDo(t, client1, "LPOS", "mylist", "x", "RANK", "-1")
	_ = r
	assertNil(t, r)
}



func Test_TCL_tcl_lpush_rpush_llength_lindex_lpop(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist1")
	r := cDo(t, client1, "LPUSH", "mylist1", "a")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "RPUSH", "mylist1", "b")
	assertIntegerEq(t, r, 2)
	r = cDo(t, client1, "RPUSH", "mylist1", "c")
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "LLEN", "mylist1")
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "LINDEX", "mylist1", "0")
	assertBulkEq(t, r, "a")
	r = cDo(t, client1, "LINDEX", "mylist1", "1")
	assertBulkEq(t, r, "b")
	r = cDo(t, client1, "LINDEX", "mylist1", "2")
	assertBulkEq(t, r, "c")
	r = cDo(t, client1, "LINDEX", "mylist1", "3")
	assertNil(t, r)
	r = cDo(t, client1, "RPOP", "mylist1")
	assertBulkEq(t, r, "c")
	r = cDo(t, client1, "LPOP", "mylist1")
	assertBulkEq(t, r, "a")
}

func Test_TCL_tcl_lpop_rpop_wrong_number_of_arguments(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "LPOP", "key", "1", "1")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "RPOP", "key", "2", "2")
	assertErrorPrefix(t, r, "ERR")
}





func Test_TCL_tcl_del_a_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a", "b")
	r := cDo(t, client1, "DEL", "mylist")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "EXISTS", "mylist")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "LLEN", "mylist")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_lpushx_rpushx_generic(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "xlist")
	r := cDo(t, client1, "LPUSHX", "xlist", "a")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "LLEN", "xlist")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "RPUSHX", "xlist", "a")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "LLEN", "xlist")
	assertIntegerEq(t, r, 0)
}







func Test_TCL_tcl_llen_against_non_list_value_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "SET", "mylist", "foobar")
	r := cDo(t, client1, "LLEN", "mylist")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_llen_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "LLEN", "not-a-key")
	_ = r
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_lindex_against_non_list_value_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mylist", "foobar")
	r := cDo(t, client1, "LINDEX", "mylist", "0")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_lindex_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "LINDEX", "not-a-key", "10")
	_ = r
	assertNil(t, r)
}

func Test_TCL_tcl_lpush_against_non_list_value_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mylist", "foobar")
	r := cDo(t, client1, "LPUSH", "mylist", "0")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_rpush_against_non_list_value_error(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mylist", "foobar")
	r := cDo(t, client1, "RPUSH", "mylist", "0")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}





func Test_TCL_tcl_rpoplpush_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "srclist{t}", "dstlist{t}")
	r := cDo(t, client1, "RPOPLPUSH", "srclist{t}", "dstlist{t}")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "EXISTS", "srclist{t}")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "dstlist{t}")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_rpoplpush_against_non_list_src_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "srclist{t}", "dstlist{t}")
	cDo(t, client1, "SET", "srclist{t}", "x")
	r := cDo(t, client1, "RPOPLPUSH", "srclist{t}", "dstlist{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_rpoplpush_against_non_list_dst_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "srclist{t}", "dstlist{t}")
	cDo(t, client1, "RPUSH", "srclist{t}", "a", "b", "c", "d")
	cDo(t, client1, "SET", "dstlist{t}", "x")
	r := cDo(t, client1, "RPOPLPUSH", "srclist{t}", "dstlist{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}





func Test_TCL_tcl_lpop_rpop_lmpop_against_empty_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "non-existing-list{t}", "non-existing-list2{t}")
	r := cDo(t, client1, "LPOP", "non-existing-list{t}")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "RPOP", "non-existing-list2{t}")
	assertNil(t, r)
	r = cDo(t, client1, "LMPOP", "1", "non-existing-list{t}", "LEFT", "COUNT", "1")
	assertNil(t, r)
	r = cDo(t, client1, "LMPOP", "1", "non-existing-list{t}", "LEFT", "COUNT", "10")
	assertNil(t, r)
}

func Test_TCL_tcl_lpop_rpop_against_non_list_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "notalist{t}", "foo")
	r := cDo(t, client1, "LPOP", "notalist{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "RPOP", "notalist{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
}





func Test_TCL_tcl_lrange_inverted_indexes(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	for i := 0; i <= 10; i++ {
		cDo(t, client1, "RPUSH", "mylist", fmt.Sprint(i))
	}
	resp := cDo(t, client1, "LRANGE", "mylist", "6", "2")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}



func Test_TCL_tcl_lrange_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "LRANGE", "nosuchkey", "0", "1")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_lrange_start_gt_end_yields_empty_array_backward_compatibility(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "1", "2", "3")
	resp := cDo(t, client1, "LRANGE", "mylist", "1", "0")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
	resp = cDo(t, client1, "LRANGE", "mylist", "-1", "-2")
	items = unwrapArray(t, resp)
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}





func Test_TCL_tcl_lset_out_of_range_index(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a")
	r := cDo(t, client1, "LSET", "mylist", "10", "foo")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_lset_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "LSET", "nosuchkey", "10", "foo")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_lset_against_non_list_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "nolist", "foobar")
	r := cDo(t, client1, "LSET", "nolist", "0", "foo")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}





func Test_TCL_tcl_lrem_remove_non_existing_element(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "RPUSH", "mylist", "a", "b", "c")
	r := cDo(t, client1, "LREM", "mylist", "1", "nosuchelement")
	_ = r
	assertIntegerEq(t, r, 0)
}



func Test_TCL_tcl_lrem_deleting_objects_that_may_be_int_encoded(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myotherlist")
	cDo(t, client1, "RPUSH", "myotherlist", "a", "1", "2", "3")
	r := cDo(t, client1, "LREM", "myotherlist", "1", "2")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "LLEN", "myotherlist")
	assertIntegerEq(t, r, 3)
}

















func Test_TCL_tcl_blpop_with_negative_timeout(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "BLPOP", "blist1", "-1")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_brpop_with_negative_timeout(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "BRPOP", "blist1", "-1")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}







func Test_TCL_tcl_blpop_second_argument_is_not_a_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist1{t}", "blist2{t}")
	cDo(t, client1, "SET", "blist2{t}", "nolist")
	resp := cDo(t, client1, "BLPOP", "blist1{t}", "blist2{t}", "1")
	_ = resp
	assertErrorPrefix(t, resp, "WRONGTYPE")
}

func Test_TCL_tcl_blpop_timeout_value_out_of_range(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "BLPOP", "blist1", "0x7FFFFFFFFFFFFF")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}







func Test_TCL_tcl_brpoplpush_existing_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist{t}", "target{t}")
	cDo(t, client1, "RPUSH", "target{t}", "bar")
	cDo(t, client1, "RPUSH", "blist{t}", "a", "b", "c", "d")
	resp := cDo(t, client1, "BRPOPLPUSH", "blist{t}", "target{t}", "1")
	_ = resp
	assertBulkEq(t, resp, "d")
	r := cDo(t, client1, "LPOP", "target{t}")
	_ = r
	assertBulkEq(t, r, "d")
}





func Test_TCL_tcl_brpoplpush_with_wrong_source_type(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist{t}", "target{t}")
	cDo(t, client1, "SET", "blist{t}", "nolist")
	resp := cDo(t, client1, "BRPOPLPUSH", "blist{t}", "target{t}", "1")
	_ = resp
	assertErrorPrefix(t, resp, "WRONGTYPE")
}

func Test_TCL_tcl_brpoplpush_with_wrong_destination_type_nonblocking(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist{t}", "target{t}")
	cDo(t, client1, "SET", "target{t}", "nolist")
	cDo(t, client1, "LPUSH", "blist{t}", "foo")
	resp := cDo(t, client1, "BRPOPLPUSH", "blist{t}", "target{t}", "1")
	_ = resp
	assertErrorPrefix(t, resp, "WRONGTYPE")
}

func Test_TCL_tcl_blmove_right_left_existing_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist{t}", "target{t}")
	cDo(t, client1, "RPUSH", "target{t}", "bar")
	cDo(t, client1, "RPUSH", "blist{t}", "a", "b", "c", "d")
	resp := cDo(t, client1, "BLMOVE", "blist{t}", "target{t}", "RIGHT", "LEFT", "1")
	_ = resp
	assertBulkEq(t, resp, "d")
}

func Test_TCL_tcl_blmove_left_right_existing_list(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "blist{t}", "target{t}")
	cDo(t, client1, "RPUSH", "target{t}", "bar")
	cDo(t, client1, "RPUSH", "blist{t}", "a", "b", "c", "d")
	resp := cDo(t, client1, "BLMOVE", "blist{t}", "target{t}", "LEFT", "RIGHT", "1")
	_ = resp
	assertBulkEq(t, resp, "a")
	r := cDo(t, client1, "RPOP", "target{t}")
	_ = r
	assertBulkEq(t, r, "a")
}




































































