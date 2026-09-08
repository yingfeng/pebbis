package tcl

// Mechanical port of frogdb crates/redis-regression/tests/set_tcl.rs
// (Redis 8.6.0 unit/set.tcl scenarios).

import (
	"fmt"
	"testing"
)







func Test_TCL_tcl_smismember_smembers_scard_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "LPUSH", "mylist", "foo")
	r := cDo(t, client1, "SMISMEMBER", "mylist", "bar")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SMEMBERS", "mylist")
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SCARD", "mylist")
	assertErrorPrefix(t, r, "WRONGTYPE")
}



func Test_TCL_tcl_sadd_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mylist")
	cDo(t, client1, "LPUSH", "mylist", "foo")
	r := cDo(t, client1, "SADD", "mylist", "bar")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_sadd_an_integer_larger_than_64_bits(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset")
	cDo(t, client1, "SADD", "myset", "213244124402402314402033402")
	r := cDo(t, client1, "SISMEMBER", "myset", "213244124402402314402033402")
	_ = r
	assertIntegerEq(t, r, 1)
}









func Test_TCL_tcl_srem_variadic_version_with_more_args_needed_to_destroy_the_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset")
	cDo(t, client1, "SADD", "myset", "1", "2", "3")
	r := cDo(t, client1, "SREM", "myset", "1", "2", "3", "4", "5", "6", "7", "8")
	_ = r
	assertIntegerEq(t, r, 3)
}

func Test_TCL_tcl_sintercard_with_illegal_arguments(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SINTERCARD")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "SINTERCARD", "1")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "SINTERCARD", "0", "myset{t}")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "SINTERCARD", "a", "myset{t}")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "SINTERCARD", "2", "myset{t}")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "SINTERCARD", "1", "myset{t}", "LIMIT", "-1")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_sintercard_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set{t}", "key1{t}")
	cDo(t, client1, "SADD", "set{t}", "a", "b", "c")
	cDo(t, client1, "SET", "key1{t}", "x")
	r := cDo(t, client1, "SINTERCARD", "1", "key1{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SINTERCARD", "2", "set{t}", "key1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_sintercard_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "SINTERCARD", "1", "non-existing-key")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "SINTERCARD", "1", "non-existing-key", "LIMIT", "0")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "SINTERCARD", "1", "non-existing-key", "LIMIT", "10")
	assertIntegerEq(t, r, 0)
}









func Test_TCL_tcl_sdiff_with_first_set_empty(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1{t}", "set2{t}", "set3{t}")
	cDo(t, client1, "SADD", "set2{t}", "1", "2", "3", "4")
	cDo(t, client1, "SADD", "set3{t}", "a", "b", "c", "d")
	result := cDo(t, client1, "SDIFF", "set1{t}", "set2{t}", "set3{t}")
	_ = result
	items := unwrapArray(t, result)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_sdiff_with_same_set_two_times(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1")
	cDo(t, client1, "SADD", "set1", "a", "b", "c", "1", "2", "3", "4", "5", "6")
	result := cDo(t, client1, "SDIFF", "set1", "set1")
	_ = result
	items := unwrapArray(t, result)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_sdiff_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1{t}", "key1{t}")
	cDo(t, client1, "SET", "key1{t}", "x")
	r := cDo(t, client1, "SDIFF", "key1{t}", "noset{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SDIFF", "noset{t}", "key1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
	cDo(t, client1, "SADD", "set1{t}", "a", "b", "c")
	r = cDo(t, client1, "SDIFF", "key1{t}", "set1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SDIFF", "set1{t}", "key1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
}





func Test_TCL_tcl_sinter_should_handle_non_existing_key_as_empty(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1{t}", "set2{t}", "set3{t}")
	cDo(t, client1, "SADD", "set1{t}", "a", "b", "c")
	cDo(t, client1, "SADD", "set2{t}", "b", "c", "d")
	result := cDo(t, client1, "SINTER", "set1{t}", "set2{t}", "set3{t}")
	_ = result
	items := unwrapArray(t, result)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_sinter_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1{t}", "key1{t}")
	cDo(t, client1, "SET", "key1{t}", "x")
	r := cDo(t, client1, "SINTER", "key1{t}", "noset{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SINTER", "noset{t}", "key1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
}







func Test_TCL_tcl_spop_using_integers_knuth_and_floyd(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset")
	for i := 1; i <= 20; i++ {
		cDo(t, client1, "SADD", "myset", fmt.Sprint(i))
	}
	r := cDo(t, client1, "SCARD", "myset")
	_ = r
	assertIntegerEq(t, r, 20)
	cDo(t, client1, "SPOP", "myset", "1")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 19)
	cDo(t, client1, "SPOP", "myset", "2")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 17)
	cDo(t, client1, "SPOP", "myset", "3")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 14)
	cDo(t, client1, "SPOP", "myset", "10")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 4)
	cDo(t, client1, "SPOP", "myset", "10")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 0)
	cDo(t, client1, "SPOP", "myset", "1")
	r = cDo(t, client1, "SCARD", "myset")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_spop_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "SPOP", "nonexisting_key", "100")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_srandmember_count_of_0(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset")
	cDo(t, client1, "SADD", "myset", "a")
	resp := cDo(t, client1, "SRANDMEMBER", "myset", "0")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_srandmember_with_count_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "SRANDMEMBER", "nonexisting_key", "100")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	if len(items) != 0 {
		t.Fatalf("assert!(%s.is_empty()) failed", "items")
	}
}

func Test_TCL_tcl_srandmember_count_overflow(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset")
	cDo(t, client1, "SADD", "myset", "a")
	r := cDo(t, client1, "SRANDMEMBER", "myset", "-9223372036854775808")
	_ = r
	assertErrorPrefix(t, r, "ERR")
}



func Test_TCL_tcl_smove_non_existing_element(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "myset1{t}", "myset2{t}")
	cDo(t, client1, "SADD", "myset1{t}", "1", "a", "b")
	cDo(t, client1, "SADD", "myset2{t}", "2", "3", "4")
	r := cDo(t, client1, "SMOVE", "myset1{t}", "myset2{t}", "foo")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "SMOVE", "myset1{t}", "myset1{t}", "foo")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_smove_non_existing_src_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "noset{t}", "myset2{t}")
	cDo(t, client1, "SADD", "myset2{t}", "2", "3", "4")
	r := cDo(t, client1, "SMOVE", "noset{t}", "myset2{t}", "foo")
	_ = r
	assertIntegerEq(t, r, 0)
}



func Test_TCL_tcl_smove_wrong_src_key_type(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x{t}", "myset2{t}")
	cDo(t, client1, "SET", "x{t}", "10")
	cDo(t, client1, "SADD", "myset2{t}", "a")
	r := cDo(t, client1, "SMOVE", "x{t}", "myset2{t}", "foo")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}







func Test_TCL_tcl_sinterstore_against_non_existing_keys_should_delete_dstkey(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "set1{t}", "set2{t}", "setres{t}")
	cDo(t, client1, "SET", "setres{t}", "xxx")
	r := cDo(t, client1, "SINTERSTORE", "setres{t}", "foo111{t}", "bar222{t}")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "setres{t}")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_sunionstore_against_non_existing_keys_should_delete_dstkey(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "setres{t}")
	cDo(t, client1, "SET", "setres{t}", "xxx")
	r := cDo(t, client1, "SUNIONSTORE", "setres{t}", "foo111{t}", "bar222{t}")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "setres{t}")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_sunion_against_non_set(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "key1{t}", "set1{t}")
	cDo(t, client1, "SET", "key1{t}", "x")
	r := cDo(t, client1, "SUNION", "key1{t}", "noset{t}")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
	r = cDo(t, client1, "SUNION", "noset{t}", "key1{t}")
	assertErrorPrefix(t, r, "WRONGTYPE")
}




