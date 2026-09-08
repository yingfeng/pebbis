package tcl

// Mechanical port of frogdb crates/redis-regression/tests/string_tcl.rs
// (Redis 8.6.0 unit/string.tcl scenarios).

import (
	"testing"
)

func Test_TCL_tcl_set_and_get_an_item(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "foobar")
	r := cDo(t, client1, "GET", "x")
	_ = r
	assertBulkEq(t, r, "foobar")
}

func Test_TCL_tcl_set_and_get_an_empty_item(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "")
	r := cDo(t, client1, "GET", "x")
	_ = r
	assertBulkEq(t, r, "")
}



func Test_TCL_tcl_setnx_target_key_missing(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "novar")
	r := cDo(t, client1, "SETNX", "novar", "foobared")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "GET", "novar")
	assertBulkEq(t, r, "foobared")
}

func Test_TCL_tcl_setnx_target_key_exists(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "novar", "foobared")
	r := cDo(t, client1, "SETNX", "novar", "blabla")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "GET", "novar")
	assertBulkEq(t, r, "foobared")
}

func Test_TCL_tcl_setnx_against_not_expired_volatile_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x", "10")
	cDo(t, client1, "EXPIRE", "x", "10000")
	r := cDo(t, client1, "SETNX", "x", "20")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "GET", "x")
	assertBulkEq(t, r, "10")
}










func Test_TCL_tcl_getdel_command(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "GETDEL", "foo")
	_ = r
	assertBulkEq(t, r, "bar")
	r = cDo(t, client1, "GETDEL", "foo")
	assertNil(t, r)
}

func Test_TCL_tcl_mget(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "FLUSHDB")
	cDo(t, client1, "SET", "foo{t}", "BAR")
	cDo(t, client1, "SET", "bar{t}", "FOO")
	resp := cDo(t, client1, "MGET", "foo{t}", "bar{t}")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	assertBulkEqAny(t, items[0], "BAR")
	assertBulkEqAny(t, items[1], "FOO")
}





func Test_TCL_tcl_getset_set_new_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "GETSET", "foo", "xyz")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "xyz")
}

func Test_TCL_tcl_getset_replace_old_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "GETSET", "foo", "xyz")
	_ = r
	assertBulkEq(t, r, "bar")
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "xyz")
}

func Test_TCL_tcl_mset_base_case(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "MSET", "x{t}", "10", "y{t}", "foo bar", "z{t}", "x x x x x x x\n\n\r\n")
	_ = r
	assertOK(t, r)
	resp := cDo(t, client1, "MGET", "x{t}", "y{t}", "z{t}")
	_ = resp
	items := unwrapArray(t, resp)
	_ = items
	assertBulkEqAny(t, items[0], "10")
	assertBulkEqAny(t, items[1], "foo bar")
	assertBulkEqAny(t, items[2], "x x x x x x x\n\n\r\n")
}

func Test_TCL_tcl_mset_msetnx_wrong_number_of_args(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "MSET", "x{t}", "10", "y{t}", "foo bar", "z{t}")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "MSETNX", "x{t}", "20", "y{t}", "foo bar", "z{t}")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_mset_with_already_existing_same_key_twice(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x{t}", "x")
	r := cDo(t, client1, "MSET", "x{t}", "xxx", "x{t}", "yyy")
	_ = r
	assertOK(t, r)
	r = cDo(t, client1, "GET", "x{t}")
	assertBulkEq(t, r, "yyy")
}

func Test_TCL_tcl_msetnx_with_already_existent_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x1{t}", "y2{t}")
	cDo(t, client1, "SET", "x{t}", "existing")
	r := cDo(t, client1, "MSETNX", "x1{t}", "xxx", "y2{t}", "yyy", "x{t}", "20")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "x1{t}")
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "EXISTS", "y2{t}")
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_msetnx_with_not_existing_keys(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x1{t}", "y2{t}")
	r := cDo(t, client1, "MSETNX", "x1{t}", "xxx", "y2{t}", "yyy")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "GET", "x1{t}")
	assertBulkEq(t, r, "xxx")
	r = cDo(t, client1, "GET", "y2{t}")
	assertBulkEq(t, r, "yyy")
}

func Test_TCL_tcl_msetnx_with_not_existing_keys_same_key_twice(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "x1{t}")
	r := cDo(t, client1, "MSETNX", "x1{t}", "xxx", "x1{t}", "yyy")
	_ = r
	assertIntegerEq(t, r, 1)
	r = cDo(t, client1, "GET", "x1{t}")
	assertBulkEq(t, r, "yyy")
}

func Test_TCL_tcl_msetnx_with_already_existing_keys_same_key_twice(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "x1{t}", "yyy")
	r := cDo(t, client1, "MSETNX", "x1{t}", "xxx", "x1{t}", "zzz")
	_ = r
	assertIntegerEq(t, r, 0)
	r = cDo(t, client1, "GET", "x1{t}")
	assertBulkEq(t, r, "yyy")
}

func Test_TCL_tcl_strlen_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "STRLEN", "notakey")
	_ = r
	assertIntegerEq(t, r, 0)
}

func Test_TCL_tcl_strlen_against_integer_encoded_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "myinteger", "-555")
	r := cDo(t, client1, "STRLEN", "myinteger")
	_ = r
	assertIntegerEq(t, r, 4)
}

func Test_TCL_tcl_strlen_against_plain_string(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mystring", "foozzz0123456789 baz")
	r := cDo(t, client1, "STRLEN", "mystring")
	_ = r
	assertIntegerEq(t, r, 20)
}














func Test_TCL_tcl_setrange_against_string_encoded_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "foo")
	r := cDo(t, client1, "SETRANGE", "mykey", "0", "b")
	_ = r
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "boo")
	cDo(t, client1, "SET", "mykey", "foo")
	r = cDo(t, client1, "SETRANGE", "mykey", "0", "")
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "foo")
	cDo(t, client1, "SET", "mykey", "foo")
	r = cDo(t, client1, "SETRANGE", "mykey", "1", "b")
	assertIntegerEq(t, r, 3)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "fbo")
	cDo(t, client1, "SET", "mykey", "foo")
	r = cDo(t, client1, "SETRANGE", "mykey", "4", "bar")
	assertIntegerEq(t, r, 7)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "foo\x00bar")
}

func Test_TCL_tcl_setrange_against_integer_encoded_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "1234")
	r := cDo(t, client1, "SETRANGE", "mykey", "0", "2")
	_ = r
	assertIntegerEq(t, r, 4)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "2234")
	cDo(t, client1, "SET", "mykey", "1234")
	r = cDo(t, client1, "SETRANGE", "mykey", "0", "")
	assertIntegerEq(t, r, 4)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "1234")
	cDo(t, client1, "SET", "mykey", "1234")
	r = cDo(t, client1, "SETRANGE", "mykey", "1", "3")
	assertIntegerEq(t, r, 4)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "1334")
	cDo(t, client1, "SET", "mykey", "1234")
	r = cDo(t, client1, "SETRANGE", "mykey", "5", "2")
	assertIntegerEq(t, r, 6)
	r = cDo(t, client1, "GET", "mykey")
	assertBulkEq(t, r, "1234\x002")
}

func Test_TCL_tcl_setrange_against_key_with_wrong_type(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey")
	cDo(t, client1, "LPUSH", "mykey", "foo")
	r := cDo(t, client1, "SETRANGE", "mykey", "0", "bar")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}



func Test_TCL_tcl_getrange_against_non_existing_key(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "mykey")
	r := cDo(t, client1, "GETRANGE", "mykey", "0", "-1")
	_ = r
	assertBulkEq(t, r, "")
}

func Test_TCL_tcl_getrange_against_wrong_key_type(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "lkey1")
	cDo(t, client1, "LPUSH", "lkey1", "list")
	r := cDo(t, client1, "GETRANGE", "lkey1", "0", "-1")
	_ = r
	assertErrorPrefix(t, r, "WRONGTYPE")
}

func Test_TCL_tcl_getrange_against_string_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "Hello World")
	r := cDo(t, client1, "GETRANGE", "mykey", "0", "3")
	_ = r
	assertBulkEq(t, r, "Hell")
	r = cDo(t, client1, "GETRANGE", "mykey", "0", "-1")
	assertBulkEq(t, r, "Hello World")
	r = cDo(t, client1, "GETRANGE", "mykey", "-4", "-1")
	assertBulkEq(t, r, "orld")
	r = cDo(t, client1, "GETRANGE", "mykey", "5", "3")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "5", "5000")
	assertBulkEq(t, r, " World")
	r = cDo(t, client1, "GETRANGE", "mykey", "-5000", "10000")
	assertBulkEq(t, r, "Hello World")
	r = cDo(t, client1, "GETRANGE", "mykey", "0", "-100")
	assertBulkEq(t, r, "H")
}

func Test_TCL_tcl_getrange_against_string_value_negative_edge_cases(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "Hello World")
	r := cDo(t, client1, "GETRANGE", "mykey", "1", "-100")
	_ = r
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "-1", "-100")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-99")
	assertBulkEq(t, r, "H")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-100")
	assertBulkEq(t, r, "H")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-101")
	assertBulkEq(t, r, "")
}

func Test_TCL_tcl_getrange_against_integer_encoded_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "1234")
	r := cDo(t, client1, "GETRANGE", "mykey", "0", "2")
	_ = r
	assertBulkEq(t, r, "123")
	r = cDo(t, client1, "GETRANGE", "mykey", "0", "-1")
	assertBulkEq(t, r, "1234")
	r = cDo(t, client1, "GETRANGE", "mykey", "-3", "-1")
	assertBulkEq(t, r, "234")
	r = cDo(t, client1, "GETRANGE", "mykey", "5", "3")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "3", "5000")
	assertBulkEq(t, r, "4")
	r = cDo(t, client1, "GETRANGE", "mykey", "-5000", "10000")
	assertBulkEq(t, r, "1234")
	r = cDo(t, client1, "GETRANGE", "mykey", "0", "-100")
	assertBulkEq(t, r, "1")
}

func Test_TCL_tcl_getrange_against_integer_negative_edge_cases(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "mykey", "1234")
	r := cDo(t, client1, "GETRANGE", "mykey", "1", "-100")
	_ = r
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "-1", "-100")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-99")
	assertBulkEq(t, r, "1")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-100")
	assertBulkEq(t, r, "1")
	r = cDo(t, client1, "GETRANGE", "mykey", "-100", "-101")
	assertBulkEq(t, r, "")
}

func Test_TCL_tcl_coverage_substr(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "key", "abcde")
	r := cDo(t, client1, "SUBSTR", "key", "0", "0")
	_ = r
	assertBulkEq(t, r, "a")
	r = cDo(t, client1, "SUBSTR", "key", "0", "3")
	assertBulkEq(t, r, "abcd")
	r = cDo(t, client1, "SUBSTR", "key", "-4", "-1")
	assertBulkEq(t, r, "bcde")
	r = cDo(t, client1, "SUBSTR", "key", "-1", "-3")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "SUBSTR", "key", "7", "8")
	assertBulkEq(t, r, "")
	r = cDo(t, client1, "SUBSTR", "nokey", "0", "1")
	assertBulkEq(t, r, "")
}

func Test_TCL_tcl_extended_set_can_detect_syntax_errors(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	resp := cDo(t, client1, "SET", "foo", "bar", "non-existing-option")
	_ = resp
	assertErrorPrefix(t, resp, "ERR")
}

func Test_TCL_tcl_extended_set_nx_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "SET", "foo", "1", "NX")
	_ = r
	assertOK(t, r)
	r = cDo(t, client1, "SET", "foo", "2", "NX")
	assertNil(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "1")
}

func Test_TCL_tcl_extended_set_xx_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "SET", "foo", "1", "XX")
	_ = r
	assertNil(t, r)
	cDo(t, client1, "SET", "foo", "bar")
	r = cDo(t, client1, "SET", "foo", "2", "XX")
	assertOK(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "2")
}

func Test_TCL_tcl_extended_set_get_option(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "SET", "foo", "bar2", "GET")
	_ = r
	assertBulkEq(t, r, "bar")
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "bar2")
}

func Test_TCL_tcl_extended_set_get_option_with_no_previous_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "SET", "foo", "bar", "GET")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "bar")
}

func Test_TCL_tcl_extended_set_get_option_with_xx(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "SET", "foo", "baz", "GET", "XX")
	_ = r
	assertBulkEq(t, r, "bar")
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "baz")
}

func Test_TCL_tcl_extended_set_get_option_with_xx_and_no_previous_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "SET", "foo", "bar", "GET", "XX")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertNil(t, r)
}

func Test_TCL_tcl_extended_set_get_option_with_nx(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	r := cDo(t, client1, "SET", "foo", "bar", "GET", "NX")
	_ = r
	assertNil(t, r)
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "bar")
}

func Test_TCL_tcl_extended_set_get_option_with_nx_and_previous_value(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "SET", "foo", "baz", "GET", "NX")
	_ = r
	assertBulkEq(t, r, "bar")
	r = cDo(t, client1, "GET", "foo")
	assertBulkEq(t, r, "bar")
}













func Test_TCL_tcl_getrange_with_huge_ranges_github_issue_1844(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "SET", "foo", "bar")
	r := cDo(t, client1, "GETRANGE", "foo", "0", "4294967297")
	_ = r
	assertBulkEq(t, r, "bar")
}













func Test_TCL_tcl_append_modifies_the_encoding_from_int_to_raw(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	cDo(t, client1, "DEL", "foo")
	cDo(t, client1, "SET", "foo", "1")
	cDo(t, client1, "APPEND", "foo", "2")
	r := cDo(t, client1, "GET", "foo")
	_ = r
	assertBulkEq(t, r, "12")
	cDo(t, client1, "SET", "bar", "12")
	r = cDo(t, client1, "GET", "bar")
	assertBulkEq(t, r, "12")
}






































































func Test_TCL_tcl_msetex_error_cases(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "MSETEX")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "MSETEX", "key1", "val1", "EX", "10")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "MSETEX", "2", "key1{t}", "val1", "key2{t}")
	assertErrorPrefix(t, r, "ERR")
}

func Test_TCL_tcl_msetex_mutually_exclusive_flags(t *testing.T) {
	addr := startServer(t)
	_ = addr
	client1 := connect(t, addr)
	_ = client1
	r := cDo(t, client1, "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "NX", "XX", "EX", "10")
	_ = r
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "EX", "10", "PX", "5000")
	assertErrorPrefix(t, r, "ERR")
	r = cDo(t, client1, "MSETEX", "2", "key1{t}", "val1", "key2{t}", "val2", "KEEPTTL", "EX", "10")
	assertErrorPrefix(t, r, "ERR")
}
